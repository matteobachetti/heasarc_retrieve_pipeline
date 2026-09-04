"""
Mission-agnostic entry points: query HEASARC, download, and dispatch to processing.

This module holds the parts of the pipeline that do not depend on which mission the
data come from:

* the ADQL queries against the HEASARC master catalogues
  (``numaster``, ``nicermastr``, ``xtemaster``);
* the three download transports -- HTTPS directory scraping, anonymous S3, and a plain
  local copy for SciServer, all dispatched by :func:`recursive_download`;
* the ``MISSION_CONFIG`` dispatch table, which maps a mission name to its catalogue,
  its column names, its archive path builder and its processing flow;
* the two top-level flows, :func:`retrieve_heasarc_data_by_source_name` and
  :func:`retrieve_heasarc_data_by_obsid`.

See ``docs/technical_details.rst`` for a description of the whole pipeline, and
``docs/known_issues.rst`` for its currently known defects.
"""

import os
import re
import shutil
import glob
import typing
from datetime import datetime
from astropy.table import Table
from astroquery.heasarc import Heasarc
import pyvo
from astropy.coordinates import SkyCoord


from .nustar import (
    nu_longest_output_name,
    process_nustar_obsid,
    DEFAULT_CONFIG as NUSTAR_DEFAULT_CONFIG,
)
from . import heasoft
from .nicer import process_nicer_obsid, DEFAULT_CONFIG as NICER_DEFAULT_CONFIG
from .rxte import process_rxte_obsid, DEFAULT_CONFIG as RXTE_DEFAULT_CONFIG

from prefect import flow, task, get_run_logger
from prefect.task_runners import ProcessPoolTaskRunner

from .diagnostics import (
    catalogue_row,
    diagnostics_path,
    record_step,
    write_manifest,
)
from .utils import (
    NO_SCIENCE_DATA,
    absolute_config,
    check_name_length,
    get_logger,
    read_skipped_inputs,
    short_workspace,
)


def read_pgp_keys_file(pgp_keys_file):
    """
    Read PGP keys file mapping mission/obsid to passphrases.

    Parameters
    ----------
    pgp_keys_file : str or None
        Path to keys file, or None to use ~/.heasarc_retrieve_pgp_keys.

    Returns
    -------
    dict
        Mapping (mission, obsid) -> passphrase, or empty dict if file not found.
    """
    if pgp_keys_file is None:
        pgp_keys_file = os.path.expanduser("~/.heasarc_retrieve_pgp_keys")

    keys = {}
    if not os.path.exists(pgp_keys_file):
        return keys

    with open(pgp_keys_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 3:
                mission, obsid, passphrase = parts[0], parts[1], parts[2]
                keys[(mission, obsid)] = passphrase

    return keys


def encrypted_obsid_url(obsid: str):
    """
    Build the encrypted data URL for an OBSID.

    Parameters
    ----------
    obsid : str
        Observation identifier.

    Returns
    -------
    str
        HTTPS URL to the encrypted data directory.

    Examples
    --------
    >>> encrypted_obsid_url("31101028002")
    'https://heasarc.gsfc.nasa.gov/FTP/nustar/data/encrypted/11/3/31101028002/'
    """
    return (
        f"https://heasarc.gsfc.nasa.gov/FTP/nustar/data/encrypted/{obsid[1:3]}/{obsid[0]}/{obsid}/"
    )


def decrypt_obsid_directory(obsid_dir: str, passphrase: str):
    """
    Decrypt all .gpg files in an observation directory.

    Uses gpg in batch mode with the given passphrase. Encrypted files are
    deleted after decryption.

    Parameters
    ----------
    obsid_dir : str
        Path to the observation directory.
    passphrase : str
        PGP passphrase for decryption.

    Raises
    ------
    RuntimeError
        If decryption fails or no encrypted files are found.
    """
    import subprocess

    logger = get_logger()
    encrypted_files = glob.glob(os.path.join(obsid_dir, "**", "*.gpg"), recursive=True)

    if not encrypted_files:
        raise RuntimeError(f"No encrypted files found in {obsid_dir}")

    for encrypted_file in encrypted_files:
        decrypted_file = encrypted_file[:-4]  # Remove .gpg extension
        logger.info(f"Decrypting {encrypted_file}")
        try:
            subprocess.run(
                [
                    "gpg",
                    "--batch",
                    "--passphrase",
                    passphrase,
                    "--output",
                    decrypted_file,
                    encrypted_file,
                ],
                check=True,
                capture_output=True,
            )
            os.remove(encrypted_file)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to decrypt {encrypted_file}: {e.stderr.decode() if e.stderr else str(e)}"
            )


def _download_pysmartdl(url: str, dest: str):
    """
    Download a single URL with pySmartDL.

    pySmartDL splits the file into chunks and fetches them in parallel, which is
    noticeably faster than urllib against the HEASARC archive.

    Parameters
    ----------
    url : str
        Full URL of the file to download.
    dest : str
        Local destination path.

    Returns
    -------
    dest : str
        The path pySmartDL actually wrote to.
    expected_size : int or None
        The size the server promised, in bytes, or ``None`` if it sent no
        ``Content-Length``. This comes free: pySmartDL has already read the header to
        decide how to split the file into chunks, so verifying what arrived costs no
        second request.
    """
    from pySmartDL import SmartDL

    obj = SmartDL(url, dest)
    obj.start()
    # pySmartDL stores 0 when the server sent no Content-Length, which means "unknown"
    # rather than "empty" -- it disables the library's own size check too.
    expected_size = obj.get_final_filesize() or None
    return obj.get_dest(), expected_size


def remote_file_size(url: str):
    """
    The size a URL promises, from a HEAD request.

    Parameters
    ----------
    url : str
        Full URL of the file.

    Returns
    -------
    int or None
        ``Content-Length`` in bytes, or ``None`` if the server does not report one or
        the request fails. ``None`` means "unknown", never "empty".

    Notes
    -----
    Checked against the archive on 60 files of one NuSTAR observation: all 60 returned a
    ``Content-Length``, and all 60 matched the local file exactly. The S3 transport does
    not need this at all -- ``list_objects_v2`` already carries ``Size`` for every key.
    """
    from urllib.request import Request, urlopen

    try:
        with urlopen(Request(url, method="HEAD")) as response:
            length = response.headers.get("Content-Length")
        return int(length) if length is not None else None
    except Exception:
        return None


def file_needs_download(path: str, expected_size):
    """
    Whether a local file has to be fetched, given the size the archive reports.

    The local tree is a mirror and the archive is authoritative, so a file of the wrong
    size is not precious data to be protected -- it is a failed download that has been
    accepted as complete on every run since. Callers delete it and fetch it again.

    Parameters
    ----------
    path : str
        Local path.
    expected_size : int or None
        Size the archive reports. ``None`` when the server does not say.

    Returns
    -------
    needed : bool
        True if the file has to be transferred.
    reason : str
        Why, in words fit for a log line.

    Examples
    --------
    >>> file_needs_download("/nonexistent/file.evt", 100)
    (True, 'not present')
    """
    if not os.path.exists(path):
        return True, "not present"
    if expected_size is None:
        return False, "present, size not verifiable (no Content-Length)"

    local_size = os.path.getsize(path)
    if local_size != expected_size:
        return True, f"present but {local_size} bytes against {expected_size} expected"
    return False, f"present and complete ({local_size} bytes)"


def _remove_partial_download(dest: str):
    """
    Delete a failed transfer's leavings.

    pySmartDL fetches into ``<dest>.000``, ``<dest>.001``, ... and combines them at the
    end, so an interrupted download leaves the parts behind and, if it died during the
    combine, a truncated ``dest`` as well.
    """
    for leftover in [dest] + glob.glob(f"{dest}.[0-9][0-9][0-9]"):
        if os.path.exists(leftover):
            os.remove(leftover)


def download_cmd(url: str, dest: str):
    """
    Download one file, converting any exception into a return value.

    Parameters
    ----------
    url : str
        Full URL of the file to download.
    dest : str
        Local destination path.

    Returns
    -------
    fname : str or None
        The path that was written, or ``None`` if the download failed.
    expected_size : int or None
        The size the server promised, for the caller to check what arrived against.
    error : str or None
        The string form of the exception, or ``None`` on success.

    Notes
    -----
    What a failure *means* is :func:`download_node`'s decision, not this function's. This
    one only performs the transfer and reports what happened.
    """
    try:
        fname, expected_size = _download_pysmartdl(url, dest)
        return fname, expected_size, None
    except Exception as e:
        return None, None, str(e)


def parse_directory_index(html, url):
    """
    The files and subdirectories an Apache index page actually lists.

    An index page is mostly ``<a>`` elements, but only some of them are data. The four
    column-sort links (``href="?C=N;O=D"`` and friends) and the "Parent Directory" link
    are navigation, and the parent's ``href`` is *absolute* and points up the tree, so
    following it walks out of the observation and recurses.

    Reading each link's ``href`` and keeping only the relative ones separates the two
    cleanly. Reading the link *text* instead, which is what this used to do, does not:
    the sort links' text is ``Name``, ``Last modified``, ``Size`` and ``Description``, and
    those became five phantom files per directory -- 25 per NuSTAR observation.

    Parameters
    ----------
    html : str or bytes
        The index page.
    url : str
        URL of the directory the page describes, with a trailing slash. Entries are
        returned relative to it.

    Returns
    -------
    list of str
        Absolute URLs. Subdirectories keep their trailing slash, which is how callers
        tell them from files.

    Examples
    --------
    >>> page = '<a href="?C=N;O=D">Name</a><a href="sub/">sub/</a><a href="x.evt">x</a>'
    >>> parse_directory_index(page, "https://example.com/obs/")
    ['https://example.com/obs/sub/', 'https://example.com/obs/x.evt']
    """
    from bs4 import BeautifulSoup

    entries = []
    for anchor in BeautifulSoup(html, "html.parser").find_all("a"):
        href = anchor.get("href")
        if not href:
            continue
        # Query strings are the sort links; a leading slash or a scheme is an absolute
        # link, which on this page means the parent directory; ".." is the same escape
        # written relatively.
        if href.startswith(("?", "#", "/")) or "://" in href:
            continue
        if href == ".." or href.startswith(("./", "../")):
            continue
        entries.append((url + href).replace(" ", "%20"))

    return entries


def walk_remote_directory(url: str):
    """
    List every file below a remote directory, recursively.

    Scrapes the Apache-generated HTML index of ``url`` with BeautifulSoup, descends
    into every subdirectory, and returns a flat list of URLs. Directory URLs keep
    their trailing slash, which is how :func:`download_node` later tells directories
    from files.

    Parameters
    ----------
    url : str
        URL of the directory to list. Spaces are percent-encoded.

    Returns
    -------
    list of str or None
        All URLs found below ``url``, directories included, or ``None`` if the
        request returned an HTTP error.

    Notes
    -----
    The parsing is done by :func:`parse_directory_index`; this function adds the
    fetching and the recursion into subdirectories. It is a plain function, called
    recursively; :func:`get_remote_directory_listing` is the Prefect task that wraps
    it, so a whole tree is one task run.
    """
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError

    url = url.replace(" ", "%20")
    req = Request(url)
    try:
        a = urlopen(req).read()
    except HTTPError:
        return None

    urls = []
    for entry in parse_directory_index(a, url):
        urls.append(entry)
        if entry.endswith("/"):
            below = walk_remote_directory(entry)
            if below is not None:
                urls.extend(below)

    return urls


@task(task_run_name="get_remote_directory_listing_{url}")
def get_remote_directory_listing(url: str):
    """
    List every file below a remote directory, recursively.

    The single task run for a whole directory tree. The walk itself lives in
    :func:`walk_remote_directory`, a plain function, so that recursing into a
    subdirectory does not open a nested task run for every level of the tree.

    Parameters
    ----------
    url : str
        URL of the directory to list.

    Returns
    -------
    list of str or None
        All URLs found below ``url``, directories included, or ``None`` if the
        request returned an HTTP error.
    """
    return walk_remote_directory(url)


@task(task_run_name="download_{node}", retries=3, retry_delay_seconds=10)
def download_node(
    node: str,
    base_url: str,
    outdir: str,
    cut_ndirs: int = 0,
    test_str: str = ".",
    test: bool = False,
    verify: bool = True,
):
    """
    Download one node (file or directory) of a remote listing to its local mirror.

    The local path is obtained by stripping ``base_url`` from ``node`` and joining
    the remainder onto ``outdir``, so that the archive's directory structure is
    reproduced locally.

    Parameters
    ----------
    node : str
        URL of the file or directory. A trailing slash marks a directory.
    base_url : str
        Prefix to strip from ``node`` before building the local path. Usually the
        parent of the observation directory, so that the OBSID becomes the top
        local directory.
    outdir : str
        Local root directory of the download.
    cut_ndirs : int, optional
        Number of leading path components to drop from the remote path.
    test_str : str, optional
        Substring that must appear in the local path for the node to be fetched.
        The default ``"."`` effectively means "only names containing a dot". Pass
        ``None`` to disable the check.
    test : bool, optional
        If True, log what would happen but transfer nothing.
    verify : bool, optional
        Check a file that is already on disk against the size the archive reports, and
        fetch it again if they disagree. One HEAD request per existing file, about ten
        seconds over a NuSTAR observation.

    Returns
    -------
    str or None
        The local path, or ``None`` if there was nothing to do -- the node was filtered
        out by ``test_str``, or the file is already present and complete.

    Raises
    ------
    RuntimeError
        If the transfer failed, or if what arrived is not the size the archive promised.
        The task retries three times first; after that the observation stops rather than
        being reduced from incomplete data. Re-running picks up where it left off,
        because a file that is present and the right size is not fetched again.
    """
    logger = get_logger()
    local_ver = os.path.join(outdir, *node.replace(base_url, "").split("/")[cut_ndirs:])
    if test_str is not None and test_str not in local_ver:
        logger.debug(f"Ignoring {node}")
        return None

    is_dir = local_ver.endswith("/")

    if is_dir:
        if not test:
            os.makedirs(local_ver, exist_ok=True)
        else:
            logger.info(f"Faked creation of {local_ver}")
        return local_ver

    if os.path.exists(local_ver):
        expected_size = remote_file_size(node) if verify else None
        needed, reason = file_needs_download(local_ver, expected_size)
        if not needed:
            logger.info(f"{local_ver} {reason}")
            return None
        # A file of the wrong size is a failed download that has been accepted as
        # complete on every run since. The archive is authoritative; take it again.
        logger.warning(f"Re-downloading {local_ver}: {reason}")
        _remove_partial_download(local_ver)

    logger.info(f"Downloading {node} to {local_ver}")
    os.makedirs(os.path.dirname(local_ver) or ".", exist_ok=True)

    if test:
        logger.info(f"Faked download of {node} to {local_ver}")
        return local_ver

    fname, expected_size, exc_string = download_cmd(node, local_ver)
    if fname is None:
        _remove_partial_download(local_ver)
        raise RuntimeError(f"Error downloading {node} to {local_ver}: {exc_string}")

    needed, reason = file_needs_download(fname, expected_size)
    if needed:
        _remove_partial_download(fname)
        raise RuntimeError(f"Incomplete download of {node}: {reason}")
    logger.debug(f"{fname} {reason}")

    return local_ver


def _s3_client():
    """
    An anonymous client for the public ``nasa-heasarc`` bucket.

    Unsigned requests, so no AWS credentials are needed. Kept apart from the caller so
    the tests can put a stub bucket in its place.
    """
    import boto3
    import botocore

    config = botocore.client.Config(signature_version=botocore.UNSIGNED)
    return boto3.resource("s3", config=config).meta.client


def s3_key_destination(
    key: str,
    prefix: str,
    outdir: str,
    test_str: str = ".",
    re_include=None,
    re_exclude=None,
):
    """
    Local path a bucket key maps to, or ``None`` if it is filtered out.

    The last component of the prefix is the observation identifier, and it is kept: the
    key ``.../obs/09/9/90901333002/auxil/x.fits`` under the prefix ``.../obs/09/9/90901333002/``
    lands in ``<outdir>/90901333002/auxil/x.fits``, matching the other transports.

    Parameters
    ----------
    key : str
        Full key in the bucket.
    prefix : str
        Key prefix the listing was made with.
    outdir : str
        Local root directory of the download.
    test_str : str, optional
        Substring that must appear in the local path. ``None`` accepts everything.
    re_include : re.Pattern or None, optional
        Only keys matching this are kept.
    re_exclude : re.Pattern or None, optional
        Keys matching this are dropped, whatever ``re_include`` says.

    Returns
    -------
    str or None
        Local path, or ``None`` when a filter rejects the key.

    Examples
    --------
    >>> s3_key_destination("nustar/obs/90901333002/auxil/x.fits",
    ...                    "nustar/obs/90901333002/", "out")
    'out/90901333002/auxil/x.fits'
    """
    if re_include is not None and not re_include.search(key):
        return None
    if re_exclude is not None and re_exclude.search(key):
        return None

    above = "/".join(prefix.strip("/").split("/")[:-1])
    dest = os.path.join(outdir, key[len(above) + 1 :])
    if test_str is not None and test_str not in dest:
        return None
    return dest


@task(task_run_name="recursive_download_s3_{url}")
def recursive_download_s3(
    url: str,
    outdir: str,
    cut_ndirs: int = 0,
    test_str: str = ".",
    test: bool = False,
    re_include: str = "",
    re_exclude: str = "",
):
    """
    Mirror an observation directory from the public ``nasa-heasarc`` S3 bucket.

    Reads the bucket anonymously (unsigned requests), so no AWS credentials are
    needed. The local layout matches the other transports: files land under
    ``<outdir>/<OBSID>/...``.

    Parameters
    ----------
    url : str
        ``s3://<bucket>/<key prefix>`` pointing at the observation directory.
    outdir : str
        Local root directory of the download.
    cut_ndirs : int, optional
        Accepted for signature compatibility with the HTTPS transport; unused here.
    test_str : str, optional
        Substring that must appear in the local path for the key to be fetched.
    test : bool, optional
        If True, log what would happen but transfer nothing.
    re_include : str, optional
        Regular expression; only keys matching it are downloaded. Empty means "all".
    re_exclude : str, optional
        Regular expression; keys matching it are skipped. Empty means "none".

    Returns
    -------
    list of str
        Local paths of the files downloaded or already present.

    Raises
    ------
    RuntimeError
        If a transferred file does not end up the size the listing announced.

    Notes
    -----
    The listing is paginated: ``list_objects_v2`` returns at most 1000 keys per call, so
    a single call truncates any observation with more files than that. It also carries a
    ``Size`` for every key, which makes verifying the local copies free -- unlike the
    HTTPS transport, no extra request is needed.
    """
    from urllib.parse import urlparse

    os.makedirs(outdir, exist_ok=True)
    logger = get_logger()
    logger.info("Recursively downloading from S3...")

    parsed = urlparse(url)
    bucket_name = parsed.netloc

    logger.info("Enabling anonymous cloud data access ...")
    s3_client = _s3_client()

    path = url.replace(f"s3://{bucket_name}/", "")

    re_include = re.compile(re_include) if re_include != "" else None
    re_exclude = re.compile(re_exclude) if re_exclude != "" else None

    local_vers = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=path):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            dest = s3_key_destination(key, path, outdir, test_str, re_include, re_exclude)
            if dest is None:
                logger.debug(f"Ignoring {key}")
                continue

            needed, reason = file_needs_download(dest, obj.get("Size"))
            if not needed:
                logger.info(f"{dest} {reason}")
                local_vers.append(dest)
                continue
            if os.path.exists(dest):
                logger.warning(f"Re-downloading {dest}: {reason}")
                _remove_partial_download(dest)

            os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
            logger.info(f"Downloading s3://{bucket_name}/{key} to {dest}")
            if test:
                logger.info(f"Faked download of s3://{bucket_name}/{key} to {dest}")
                local_vers.append(dest)
                continue

            s3_client.download_file(bucket_name, key, dest)
            needed, reason = file_needs_download(dest, obj.get("Size"))
            if needed:
                _remove_partial_download(dest)
                raise RuntimeError(f"Incomplete download of {key}: {reason}")
            local_vers.append(dest)
    return local_vers


@flow(flow_run_name="recursive_download_https_{url}")
def recursive_download_https(
    url: str,
    outdir: str,
    cut_ndirs: int = 0,
    test_str: str = ".",
    test: bool = False,
    re_include: str = "",
    re_exclude: str = "",
    verify: bool = True,
):
    """
    Mirror an observation directory from the HEASARC HTTPS archive.

    Gets the recursive listing with :func:`get_remote_directory_listing`, applies the
    include/exclude regular expressions to the remote URLs, and fetches what is left
    with :func:`download_node`.

    Parameters
    ----------
    url : str
        ``https://heasarc.gsfc.nasa.gov/FTP/...`` URL of the observation directory.
    outdir : str
        Local root directory of the download.
    cut_ndirs : int, optional
        Number of leading path components to drop from each remote path.
    test_str : str, optional
        Substring that must appear in the local path for a node to be fetched.
    test : bool, optional
        If True, log what would happen but transfer nothing.
    re_include : str, optional
        Regular expression; only URLs matching it are downloaded. Empty means "all".
    re_exclude : str, optional
        Regular expression; URLs matching it are skipped. Empty means "none".
    verify : bool, optional
        Check files that are already on disk against the size the archive reports.

    Returns
    -------
    list or bool
        Local paths (``None`` where there was nothing to do), or ``False`` if the
        remote directory was empty or unreachable. A file that cannot be transferred
        completely raises out of :func:`download_node` rather than being reported here.

    Examples
    --------
    Fetch only the NuSTAR mode 01 and 06 event files of an observation, skipping the
    calibration modes 02 to 05::

        recursive_download_https(url, "out",
                                 re_include=r"[AB]0.*evt",
                                 re_exclude=r"[AB]0[2-5]")
    """
    re_include = re.compile(re_include) if re_include != "" else None
    re_exclude = re.compile(re_exclude) if re_exclude != "" else None

    # rec_down_file = os.path.join(outdir, "DOWNLOAD_DONE.txt")
    # if os.path.exists(rec_down_file):
    #     logger.info(f"Download already done for {url}")
    #     return
    logger = get_run_logger()
    logger.info("Getting remote directory listing...")
    listing = get_remote_directory_listing(url)
    if listing is None or listing == []:
        logger.warning(f"No data found in remote directory {url}")
        return False

    base_url = "/".join(url.rstrip("/").split("/")[:-1])
    local_vers = []
    os.makedirs(outdir, exist_ok=True)
    for node in listing:
        if re_include is not None and not re_include.search(node):
            logger.info(f"Skipping {node} because not included in {re_include.pattern}")
            continue
        if re_exclude is not None and re_exclude.search(node):
            logger.info(f"Skipping {node} because excluded in {re_exclude.pattern}")
            continue
        local_vers.append(
            download_node(
                node,
                base_url,
                outdir,
                cut_ndirs=cut_ndirs,
                test_str=test_str,
                test=test,
                verify=verify,
            )
        )
    # open(rec_down_file, "a").close()
    return local_vers


@task(task_run_name="copy_local_directory_{url}")
def copy_local_directory(url: str, outdir: str):
    """
    Copy an already-local archive directory into the output directory.

    This is the SciServer transport: there, the HEASARC archive is mounted under
    ``/FTP`` and "downloading" is a directory copy.

    Parameters
    ----------
    url : str
        Path of the source directory. Its basename (normally the OBSID) becomes the
        name of the copy.
    outdir : str
        Local root directory of the download.

    Returns
    -------
    generator
        ``os.walk`` over the newly created copy.
    """
    outpath = os.path.join(outdir, url.rstrip("/").split("/")[-1])
    logger = get_run_logger()
    logger.info(f"Copying local directory {url} to {outpath}")

    shutil.copytree(url.rstrip("/"), outpath, dirs_exist_ok=True)

    return os.walk(outpath)


@flow(flow_run_name="recursive_download_{url}")
def recursive_download(
    url: str,
    outdir: str,
    cut_ndirs: int = 0,
    test_str: str = ".",
    test: bool = False,
    re_include: str = "",
    re_exclude: str = "",
    verify: bool = True,
):
    """
    Fetch an observation directory, choosing the transport from the URL scheme.

    ``http...`` is scraped over HTTPS, ``s3://`` is read from the public S3 mirror,
    and anything else is treated as a local path and copied. All three produce the
    same local layout, ``<outdir>/<OBSID>/...``.

    Parameters
    ----------
    url : str
        Location of the observation directory: an HTTPS URL, an ``s3://`` URL, or a
        local path.
    outdir : str
        Local root directory of the download.
    cut_ndirs : int, optional
        Number of leading path components to drop from each remote path.
    test_str : str, optional
        Substring that must appear in the local path for a file to be fetched.
    test : bool, optional
        If True, log what would happen but transfer nothing.
    re_include : str, optional
        Regular expression; only paths matching it are downloaded. Empty means "all".
    re_exclude : str, optional
        Regular expression; paths matching it are skipped. Empty means "none".
    verify : bool, optional
        Check files that are already on disk against the size reported by the archive.

    Returns
    -------
    list, generator or bool
        Whatever the chosen transport returns: a list of local paths for HTTPS and
        S3, an ``os.walk`` generator for the local copy, or ``False`` if an HTTPS
        listing came back empty.
    """
    if url.startswith("http"):
        return recursive_download_https(
            url, outdir, cut_ndirs, test_str, test, re_include, re_exclude, verify
        )

    if url.startswith("s3://"):
        return recursive_download_s3(url, outdir, cut_ndirs, test_str, test, re_include, re_exclude)

    return copy_local_directory(url, outdir)  # For local directories, we just copy them directly


MISSION_CONFIG = {
    "nustar": {
        "table": "numaster",
        "expo_column": "exposure_a",
        # numaster means it: zero exposure_a is an observation with no data, and there is
        # no point downloading it.
        "zero_exposure_may_be_wrong": False,
        "additional": "solar_activity",
        "obsid_processing": process_nustar_obsid,
        "default_config": NUSTAR_DEFAULT_CONFIG,
        "name_column": "name",
        "longest_output_name": nu_longest_output_name,
    },
    "nicer": {
        "table": "nicermastr",
        "expo_column": "exposure",
        # nicermastr does not: it sometimes reports zero because NICER's own pipeline
        # filtered the data wrongly, and the data are fine. Download them and look.
        "zero_exposure_may_be_wrong": True,
        "additional": "",
        "obsid_processing": process_nicer_obsid,
        "default_config": NICER_DEFAULT_CONFIG,
        "name_column": "name",
    },
    "rxte": {
        "table": "xtemaster",
        "expo_column": "exposure",
        # Untested against xtemaster; assume the catalogue may be wrong, as for NICER.
        "zero_exposure_may_be_wrong": True,
        "additional": "cycle, prnb",
        "obsid_processing": process_rxte_obsid,
        "default_config": RXTE_DEFAULT_CONFIG,
        "name_column": "target_name",
    },
}


@task(task_run_name="read_config_{config_file}")
def read_config(config_file: str):
    """
    Read a YAML configuration file.

    Parameters
    ----------
    config_file : str
        Path of the YAML file.

    Returns
    -------
    dict
        The parsed configuration.

    Notes
    -----
    Unused, and currently broken: ``yaml.load`` requires an explicit ``Loader``
    argument from PyYAML 6 onwards. See issue 24 in ``docs/known_issues.rst``.
    """
    import yaml

    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config


@task(log_prints=True)
def retrieve_heasarc_table_by_position(
    ra_deg: float, dec_deg: float, mission: str = "nustar", radius_deg: float = 0.1
):
    """
    Cone-search a mission's master catalogue around a sky position.

    Builds and runs an ADQL query against the mission's HEASARC master catalogue,
    selecting every observation whose *pointing* falls within ``radius_deg`` of the
    given position and whose exposure passes :func:`exposure_condition` -- which drops
    planned-but-not-executed observations everywhere, and zero-exposure ones for NuSTAR
    only.

    Parameters
    ----------
    ra_deg : float
        Right ascension of the search centre, ICRS degrees.
    dec_deg : float
        Declination of the search centre, ICRS degrees.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``: ``"nustar"``, ``"nicer"`` or
        ``"rxte"``.
    radius_deg : float, optional
        Search radius in degrees. The default, 0.1 deg (6 arcmin), is conservative
        for an imaging instrument: NuSTAR's field of view is 12x12 arcmin, so a
        source can be well inside the field of an observation pointed further away
        than this.

    Returns
    -------
    astropy.table.Table
        One row per observation, ordered by time, with columns ``source_name``,
        ``obsid``, ``time``, the mission's exposure column, ``ra``, ``dec`` and
        ``__row``; plus ``public_date`` for NuSTAR and NICER (``xtemaster`` has no
        such column) and the mission's extra columns.

        ``__row`` is astroquery's internal row identifier and must be preserved: it
        is what ``Heasarc.locate_data`` needs in order to find the files.
    """
    logger = get_run_logger()
    logger.info(
        f"Retrieving HEASARC table for {mission} at RA: {ra_deg}, Dec: {dec_deg}, Radius: {radius_deg}"
    )

    expo_name = MISSION_CONFIG[mission]["expo_column"]
    additional = MISSION_CONFIG[mission]["additional"]
    table = MISSION_CONFIG[mission]["table"]
    source_name_col = MISSION_CONFIG[mission]["name_column"]

    select_columns = f"{source_name_col} as source_name, obsid, time, {expo_name}, ra, dec, __row"

    if mission != "rxte":
        select_columns += ", public_date"

    if additional:
        select_columns += f", {additional}"

    query = f"""SELECT {select_columns}
        FROM public.{table} as cat
        where
        contains(point('ICRS',cat.ra,cat.dec),circle('ICRS',{ra_deg},{dec_deg},{radius_deg}))=1
        and
        {exposure_condition(mission)} order by cat.time
        """

    results = Heasarc.query_tap(query).to_table()

    return results


def exposure_condition(mission):
    """
    The ADQL condition that keeps observations worth downloading.

    Every mission's master catalogue carries planned-but-not-executed observations with a
    null or negative exposure, and those are never wanted. Zero is the interesting case,
    and the missions do not agree on what it means:

    * ``numaster`` means it. A NuSTAR observation with ``exposure_a`` of zero has no data,
      and downloading it wastes time and disk.
    * ``nicermastr`` sometimes does not. NICER's own pipeline occasionally filters an
      observation away and records zero exposure for data that are perfectly usable, so a
      zero there is a reason to look, not a reason to skip.

    Parameters
    ----------
    mission : str
        One of the keys of ``MISSION_CONFIG``.

    Returns
    -------
    str
        An ADQL boolean expression over ``cat``.

    Examples
    --------
    >>> exposure_condition("nustar")
    'cat.exposure_a > 0'
    >>> exposure_condition("nicer")
    'cat.exposure >= 0'
    """
    expo_name = MISSION_CONFIG[mission]["expo_column"]
    if MISSION_CONFIG[mission]["zero_exposure_may_be_wrong"]:
        return f"cat.{expo_name} >= 0"
    return f"cat.{expo_name} > 0"


#: What an OBSID may look like. They go into the query text, so nothing else is allowed.
OBSID_RE = re.compile(r"[A-Za-z0-9_.-]+")


def obsid_query(obsid, mission: str = "nustar"):
    """
    The catalogue query that looks one or several observations up.

    Parameters
    ----------
    obsid : str or list of str
        One observation identifier, or several.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``.

    Returns
    -------
    str
        An ADQL query.

    Notes
    -----
    Unlike the cone search, this keeps zero-exposure observations for every mission: when
    an OBSID has been named explicitly, returning nothing at all is more confusing than
    returning the row and letting the reduction say what it found.

    Raises
    ------
    ValueError
        If no OBSID is given, or one of them is not an identifier.
    """
    obsids = [obsid] if isinstance(obsid, str) else list(obsid)
    if not obsids:
        raise ValueError("No OBSID to look up")
    for one in obsids:
        if not OBSID_RE.fullmatch(str(one)):
            raise ValueError(f"{one!r} is not a valid OBSID")

    expo_name = MISSION_CONFIG[mission]["expo_column"]
    additional = MISSION_CONFIG[mission]["additional"]
    table = MISSION_CONFIG[mission]["table"]
    name_column = MISSION_CONFIG[mission]["name_column"]
    if additional != "":
        additional = f", {additional}"
    wanted = ", ".join(f"'{one}'" for one in obsids)

    return f"""SELECT {name_column}, cycle, obsid, time, {expo_name}, ra, dec, __row {additional}
        FROM public.{table} as cat
        where
        cat.obsid IN ({wanted})
        and
        cat.{expo_name} >= 0 order by cat.time
        """


@task(task_run_name="retrieve_info_{mission}_obsids")
def retrieve_info_for_obsid(obsid, mission: str = "nustar"):
    """
    Look observations up in a mission's master catalogue, by OBSID.

    Parameters
    ----------
    obsid : str or list of str
        Observation identifier, or several of them, matched exactly. Several are one
        query, not one each.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``.

    Returns
    -------
    astropy.table.Table
        One row per OBSID found, with the mission's name, ``cycle``, ``obsid``, ``time``,
        exposure, ``ra``, ``dec``, ``__row`` and any mission-specific extra columns.

    Notes
    -----
    Unlike :func:`retrieve_heasarc_table_by_position`, this does not alias the
    mission's name column to ``source_name``, so the two functions return tables
    with slightly different schemas.
    """
    logger = get_logger()
    logger.info(f"Looking up {obsid} in the {mission} catalogue")

    results = Heasarc.query_tap(obsid_query(obsid, mission)).to_table()
    return results


@task
def get_source_position(source: str):
    """
    Resolve a source name to coordinates through SIMBAD/NED.

    Parameters
    ----------
    source : str
        Source name, as understood by ``SkyCoord.from_name``.

    Returns
    -------
    astropy.coordinates.SkyCoord
        The resolved position.
    """
    pos = SkyCoord.from_name(f"{source}")

    return pos


@task
def retrieve_heasarc_table_by_source_name(
    source: str, mission: str = "nustar", radius_deg: float = 0.1
):
    """
    Cone-search a mission's master catalogue around a named source.

    Convenience wrapper: resolves the name with :func:`get_source_position` and
    hands the result to :func:`retrieve_heasarc_table_by_position`.

    Parameters
    ----------
    source : str
        Source name, as understood by ``SkyCoord.from_name``.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``.
    radius_deg : float, optional
        Search radius in degrees.

    Returns
    -------
    astropy.table.Table
        One row per matching observation.
    """
    pos = get_source_position(source)
    results = retrieve_heasarc_table_by_position(
        pos.ra.deg, pos.dec.deg, mission=mission, radius_deg=radius_deg
    )
    return results


def locate_data(result_table, catalog_name):
    """Local stand-in for :meth:`astroquery.heasarc.Heasarc.locate_data`.

    ``Heasarc.locate_data`` keeps only the datalink rows whose ``content_type``
    is ``directory``, but the HEASARC datalink service now labels the
    observation-directory row ``text/html``, so it returns an empty table for
    every query (astroquery issue #3652). This is a verbatim copy of the
    astroquery method with the row selection replaced by a test on the access
    URL -- which is how ``sciserver`` and ``aws`` are derived a few lines below
    anyway. Rows that carry an ``error_message`` are kept, as astroquery keeps
    them: they mark observations with no public data products, typically ones
    still in their proprietary period.

    Delete this function and restore the ``Heasarc.locate_data`` call once
    astroquery #3652 is fixed.

    Parameters
    ----------
    result_table : :class:`astropy.table.Table`
        Output of a catalogue query, including the ``__row`` column.
    catalog_name : str
        Name of the HEASARC catalogue the rows came from, e.g. ``numaster``.

    Returns
    -------
    :class:`astropy.table.Table`
        Same columns as ``Heasarc.locate_data``: ``ID``, ``access_url``,
        ``sciserver``, ``aws``, ``content_length``, ``error_message``. For rows
        with an error message the three URL columns are empty strings.
    """
    query = pyvo.dal.adhoc.DatalinkQuery(
        baseurl=f"{Heasarc.VO_URL}/datalink/{catalog_name}",
        id=result_table["__row"],
        session=Heasarc._session,
    )
    dl_result = pyvo.dal.DALResults(
        query.execute_votable(post=True), url=query.queryurl, session=query._session
    ).to_table()

    # Include rows that point at a data directory and those that report errors
    # (usually meaning there are no public data products).
    dl_result = dl_result[
        [
            "/FTP/" in url or err != ""
            for url, err in zip(dl_result["access_url"], dl_result["error_message"])
        ]
    ]
    dl_result = dl_result[["ID", "access_url", "content_length", "error_message"]]

    # Add sciserver and s3 columns
    newcol = [
        f"/FTP/{row.split('FTP/')[1]}".replace("//", "/") if "FTP" in row else ""
        for row in dl_result["access_url"]
    ]
    dl_result.add_column(newcol, name="sciserver", index=2)
    newcol = [
        f"s3://{Heasarc.S3_BUCKET}/{row[5:]}" if row != "" else "" for row in dl_result["sciserver"]
    ]
    dl_result.add_column(newcol, name="aws", index=3)

    return dl_result


_WORKER_DIRECTORY = None


def prepare_worker(pfiles_root, work_root):
    """
    Give a worker process its own parameter files and its own directory.

    Called at the top of every observation, and does its work once per process: the
    second call in the same worker returns the directory the first one made. It is not
    the process pool's ``initializer`` because Prefect only grew that argument in 3.8,
    and the environment this pipeline runs in has 3.7.

    Two pieces of state are shared by every HEASOFT call in a process. The first is
    ``PFILES``: ``heasoftpy`` reads and rewrites ``<PFILES>/<tool>.par`` around each call,
    so concurrent calls delete the file under each other. Measured with 200 calls of
    ``ftlist``, eight at a time: 19 failures from threads sharing ``~/pfiles``, 9 from
    processes sharing it, **0** from processes with a private directory each. Since
    ``PFILES`` is an environment variable, and the environment belongs to the process,
    this only works one process at a time -- which is why an observation is a process.

    The second is the working directory, where several HEASOFT scripts drop scratch files
    under fixed names. Each worker stands in a directory of its own, so those cannot
    collide either. This is the one place in the package that calls ``os.chdir``, and it
    is the opposite of the pattern it replaced: set once, before any work, never during.

    The two are kept apart because they cost different things.
    :func:`heasarc_retrieve_pipeline.utils.short_workspace` puts ``pfiles_root`` on local
    temporary disk, where the 44-plus parameter-file rewrites of a single ``nupipeline``
    run cost nothing, and leaves ``work_root`` on the roomy filesystem by default, because
    a working directory was measured peaking at 182.5 MB for one observation -- about 90%
    of its raw data size, and scaling with it.

    Parameters
    ----------
    pfiles_root : str
        Directory to create this process's ``PFILES`` directory under. Kilobytes, written
        on every HEASOFT call, kept by nobody: it wants to be on the fastest local disk
        available.
    work_root : str
        Directory to create this process's working directory under. This is where HEASOFT
        scripts drop their bulky temporary trees, so it wants room more than it wants
        speed.

    Returns
    -------
    str
        This process's private working directory.
    """
    global _WORKER_DIRECTORY
    if _WORKER_DIRECTORY is not None:
        return _WORKER_DIRECTORY

    name = f"worker_{os.getpid()}"
    workdir = os.path.join(work_root, name)
    pfiles = os.path.join(pfiles_root, name, "pfiles")
    os.makedirs(pfiles, exist_ok=True)
    os.makedirs(workdir, exist_ok=True)
    headas = os.environ.get("HEADAS")
    if headas and os.path.isdir(headas):
        # heasoft remembers this value and puts it back if anything changes it; see
        # heasoft._hold_on_to_private_pfiles for why that turned out to be necessary.
        heasoft.use_private_pfiles(pfiles)
    os.chdir(workdir)

    _WORKER_DIRECTORY = workdir
    return workdir


def download_link_column(force_heasarc=False, force_s3=False, environ=None):
    """
    Which column of the datalink table the downloads should come from.

    The default is the public AWS S3 mirror, which is also what several observations at
    once should use: S3 serves parallel readers well, while the HTTPS archive at HEASARC
    is a directory-index scraper hitting one server.

    Parameters
    ----------
    force_heasarc, force_s3 : bool, optional
        Explicit choices, which win over everything else.
    environ : dict, optional
        Environment to inspect. Defaults to ``os.environ``.

    Returns
    -------
    str
        ``"aws"``, ``"access_url"`` (HEASARC over HTTPS) or ``"sciserver"`` (local copies).
    """
    environ = os.environ if environ is None else environ
    if force_s3:
        return "aws"
    if force_heasarc:
        return "access_url"
    if "SCISERVER_USER_ID" in environ:
        return "sciserver"
    return "aws"


def observation_work_items(
    result_table, links, link_col_name, source_position=None, mission="nustar", pgp_keys_file=None
):
    """
    One unit of work per observation that actually has data to download.

    Parameters
    ----------
    result_table : astropy.table.Table
        Catalogue rows, with ``obsid``, ``ra``, ``dec`` and ``__row``.
    links : astropy.table.Table
        Datalink answer, with ``ID`` and one column per mirror.
    link_col_name : str
        Which mirror column to take the URL from.
    source_position : astropy.coordinates.SkyCoord or None, optional
        Position to barycentre at. ``None`` means each observation's own pointing.
    mission : str, optional
        Mission name for encrypted data lookup.
    pgp_keys_file : str or None, optional
        Path to PGP keys file, or None to use ~/.heasarc_retrieve_pgp_keys.

    Returns
    -------
    list of dict
        ``obsid``, ``url``, ``ra``, ``dec`` and ``catalogue`` for each observation with
        public products. ``catalogue`` is every column the query returned, kept for the
        report; nothing in the reduction reads it. If encrypted, ``pgp_passphrase`` is
        included.

    Notes
    -----
    Links are matched to catalogue rows through the datalink ``ID``, not by position: the
    service does not return one usable row per input row. Observations with no public data
    products are checked against the PGP keys file. If found, the encrypted URL is used.
    If not found, the observation is logged and skipped.
    """
    logger = get_logger()
    link_by_row = {str(i).split("?")[-1]: row for i, row in zip(links["ID"], links)}
    pgp_keys = read_pgp_keys_file(pgp_keys_file)

    items = []
    for row in result_table:
        obsid = row["obsid"]
        link = link_by_row.get(row["__row"])

        url = None
        pgp_passphrase = None

        if link is not None and link[link_col_name]:
            url = link[link_col_name]
        elif (mission, obsid) in pgp_keys:
            url = encrypted_obsid_url(obsid)
            pgp_passphrase = pgp_keys[(mission, obsid)]
            logger.info(f"Using encrypted URL for OBSID {obsid}")
        else:
            logger.info(
                f"No public data products for OBSID {obsid} "
                "(still in its proprietary period?), skipping"
            )
            continue

        if source_position is not None:
            ra, dec = source_position.ra.deg, source_position.dec.deg
        else:
            ra, dec = row["ra"], row["dec"]

        item = dict(
            obsid=obsid,
            url=url,
            ra=ra,
            dec=dec,
            catalogue=catalogue_row(row),
        )
        if pgp_passphrase is not None:
            item["pgp_passphrase"] = pgp_passphrase

        items.append(item)

    return items


@task(task_run_name="observation_{obsid}")
def download_and_process_observation(
    obsid,
    url,
    ra,
    dec,
    outdir,
    mission,
    pfiles_root,
    work_root,
    flags=None,
    test=False,
    pgp_passphrase=None,
):
    """
    Download one observation and reduce it, in this process alone.

    This is the unit of parallelism: it runs in a worker of the process pool, and the
    first thing it does is give that process a private ``PFILES`` and a private working
    directory. Everything below it is sequential.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    url : str
        Where to download from, as chosen by :func:`download_link_column`.
    ra, dec : float
        Position to barycentre at.
    outdir : str
        Absolute output directory, shared by all observations. Each writes into its own
        ``<outdir>/<obsid>`` subtree. This is normally the short symbolic link made by
        :func:`heasarc_retrieve_pipeline.utils.short_workspace`, not the directory itself,
        so that HEASOFT never sees a file name long enough to be truncated.
    mission : str
        One of the keys of ``MISSION_CONFIG``.
    pfiles_root, work_root : str
        Where this process's private parameter files and working directory go; see
        :func:`prepare_worker`.
    flags : dict, optional
        Extra parameters for the mission's Level-2 pipeline.
    test : bool, optional
        If True, fake the download and do not process.
    pgp_passphrase : str, optional
        PGP passphrase for decryption, if this is encrypted data.
    """
    prepare_worker(pfiles_root, work_root)

    config = absolute_config(
        dict(input_data_path=outdir, out_data_path=outdir),
        MISSION_CONFIG[mission]["default_config"],
    )

    # The page is written whatever happens below, which is why it is here and not in
    # the mission flow: this is the only place that knows both the observation and the
    # output directory and still runs when the observation raises. A failed observation
    # is exactly the one somebody will want to look at.
    #
    # The record closes before the page is written, so that the page shows how the
    # observation ended rather than showing it as still running.
    try:
        with record_step(diagnostics_path(obsid, config), obsid, "observation") as rec:
            obsid_dir = os.path.join(outdir, obsid)
            pgp_decryption_sentinel = os.path.join(obsid_dir, ".pgp_decrypted")

            # If decryption sentinel exists, files were already downloaded and decrypted;
            # skip re-downloading them
            if os.path.exists(pgp_decryption_sentinel):
                logger = get_logger()
                logger.info(
                    f"Decryption sentinel found at {pgp_decryption_sentinel}; "
                    f"skipping download and decryption"
                )
                rec.skip("files already downloaded and decrypted in a prior run")
                return None

            recursive_download(url, outdir, test_str=".", test=test)
            if test:
                rec.skip("a test run: nothing was downloaded and nothing was processed")
                return None

            if pgp_passphrase is not None:
                decrypt_obsid_directory(obsid_dir, pgp_passphrase)
                # Create sentinel after successful decryption to mark this step as done
                with open(pgp_decryption_sentinel, "w") as f:
                    f.write(f"Decryption completed at {datetime.now().isoformat()}\n")

            # recursive_download is a flow, and a subflow call is synchronous and raises,
            # so the ordering is already guaranteed by the line above. Prefect 3 has no
            # flow.submit().
            result = MISSION_CONFIG[mission]["obsid_processing"](
                obsid, config=config, ra=ra, dec=dec, flags=flags
            )
            if result == NO_SCIENCE_DATA:
                # A slew is a real observation with nothing in it for this pipeline.
                rec.skip("the observation holds no science data")
            return result
    finally:
        write_page(obsid, outdir)


def write_page(obsid, outdir):
    """
    Write one observation's diagnostics page, or say why it could not be written.

    Imported here rather than at module scope: the report needs plotly, and a pipeline
    installed without it must still reduce data.

    A reporting failure must never turn a good observation into a failed one, and must
    never replace the exception an observation raised -- which is what an exception
    escaping a ``finally`` would do. So everything is caught, and the reduction goes on.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    outdir : str
        Run output directory.
    """
    try:
        from .report import write_observation_page

        write_observation_page(obsid, outdir)
    except Exception as error:
        get_logger().warning(
            f"Could not write the diagnostics page for {obsid}: {type(error).__name__}: {error}"
        )


@flow(flow_run_name="process_{mission}_observations")
def process_observations(items, outdir, mission, pfiles_root, work_root, flags=None, test=False):
    """
    Reduce every observation, one process each.

    The task runner is supplied by the caller through ``with_options``, because the number
    of workers and the directory their private state lives in are only known then.

    A failed observation is reported and the others carry on, which is what the old loop
    did with ``return_state=True``. The difference is that the failure is now named in the
    log and counted, instead of passing in silence.

    Parameters
    ----------
    items : list of dict
        As returned by :func:`observation_work_items`.
    outdir : str
        Absolute output directory.
    mission : str
        One of the keys of ``MISSION_CONFIG``.
    pfiles_root, work_root : str
        Where the workers' private parameter files and working directories go; see
        :func:`prepare_worker`.
    flags : dict, optional
        Extra parameters for the mission's Level-2 pipeline.
    test : bool, optional
        If True, fake the downloads and process nothing.

    Returns
    -------
    list of str
        The OBSIDs that failed. Observations whose processing returned
        :data:`heasarc_retrieve_pipeline.utils.NO_SCIENCE_DATA` -- NuSTAR slews, for
        instance -- are counted and logged separately, and are *not* in this list.
    """
    logger = get_run_logger()

    # The bundle before anything else: the workers write their pages as they finish, and
    # a page whose <script src> points at a file that is not there yet has no plots in it.
    write_shared_report_files(outdir)

    # Before anything is submitted, so that a run killed part way through still has one
    # of these for every observation it meant to do, and the index can say so.
    for item in items:
        try:
            write_manifest(
                diagnostics_path(item["obsid"], dict(out_data_path=outdir)),
                item["obsid"],
                item.get("catalogue"),
                url=item["url"],
                mission=mission,
                ra=item["ra"],
                dec=item["dec"],
            )
        except OSError as error:
            logger.warning(f"Could not record the manifest for {item['obsid']}: {error}")

    futures = []
    for item in items:
        kwargs = dict(
            outdir=outdir,
            mission=mission,
            pfiles_root=pfiles_root,
            work_root=work_root,
            flags=flags,
            test=test,
        )
        if "pgp_passphrase" in item:
            kwargs["pgp_passphrase"] = item["pgp_passphrase"]

        futures.append(
            download_and_process_observation.submit(
                item["obsid"],
                item["url"],
                item["ra"],
                item["dec"],
                **kwargs,
            )
        )

    failed = []
    no_science = []
    for item, future in zip(items, futures):
        try:
            if future.result() == NO_SCIENCE_DATA:
                no_science.append(item["obsid"])
        except Exception as exc:
            failed.append(item["obsid"])
            logger.error(f"OBSID {item['obsid']} failed: {type(exc).__name__}: {exc}")

    reduced = len(items) - len(failed) - len(no_science)
    logger.info(
        f"{reduced} of {len(items)} observations reduced, "
        f"{len(no_science)} held no science data, {len(failed)} failed"
    )
    if no_science:
        # Not an error: a slew is a real observation with nothing in it for this pipeline.
        logger.info(f"No science data in: {no_science}")
    if failed:
        logger.error(f"{len(failed)} of {len(items)} observations failed: {failed}")

    # An unusable mode-06 CHU subset is skipped and the observation still counts as
    # reduced. Naming the observations that skipped something is what makes that
    # auditable without reading the whole log.
    with_skips = [
        item["obsid"]
        for item in items
        if read_skipped_inputs(item["obsid"], dict(out_data_path=outdir))
    ]
    if with_skips:
        logger.info(
            f"{len(with_skips)} observation(s) had to skip an input; see "
            f"skipped_inputs.txt in each of {with_skips}"
        )

    index = write_shared_report_files(outdir, [item["obsid"] for item in items])
    if index is not None:
        logger.info(f"Open {index} to see the run")
    return failed


def write_shared_report_files(outdir, obsids=None):
    """
    Write the plotly bundle, and the index once the run has something to index.

    Called twice: at the head of the run for the bundle alone, so that the pages the
    workers write as they finish already resolve it, and at the tail for the index.

    Everything is caught and logged, for the same reason the per-observation page is: a
    report is worth less than a reduction. When the whole run is inside
    :func:`heasarc_retrieve_pipeline.utils.short_workspace`, ``outdir`` is the short
    symbolic link, and writing through it lands the bytes in the real tree while the link
    is still there.

    Parameters
    ----------
    outdir : str
        Run output directory.
    obsids : list of str, optional
        The observations of this run, in the order to list them. ``None`` writes only the
        bundle.

    Returns
    -------
    str or None
        The index path, when one was written.
    """
    try:
        from .report import write_index, write_plotly_bundle

        write_plotly_bundle(outdir)
        if obsids is None:
            return None
        return write_index(outdir, obsids)
    except Exception as error:
        get_logger().warning(
            f"Could not write the run report in {outdir}: {type(error).__name__}: {error}"
        )
        return None


@flow
def retrieve_and_process_data(
    result_table: Table,
    source_position: typing.Union[SkyCoord, None] = None,
    mission: str = "nustar",
    outdir: str = "out",
    test: bool = False,
    flags={},
    force_heasarc: bool = False,
    force_s3: bool = False,
    n_workers: int = 1,
    scratch_dir: typing.Union[str, None] = None,
    pgp_keys_file: typing.Union[str, None] = None,
):
    """
    Download and reduce every observation in a catalogue table.

    For each row: work out where the files live, mirror them into ``outdir``, then
    run the mission's processing flow on the resulting directory.

    The mirror is chosen in this order: ``force_s3`` and ``force_heasarc`` win if
    set; otherwise, if ``SCISERVER_USER_ID`` is in the environment the local
    SciServer paths are used; otherwise AWS S3.

    ``outdir`` is made absolute and handed to the processing flow as its
    ``input_data_path`` and ``out_data_path``, so nothing in the run depends on the
    process working directory.

    Parameters
    ----------
    result_table : astropy.table.Table
        Catalogue rows, as returned by :func:`retrieve_heasarc_table_by_position` or
        :func:`retrieve_info_for_obsid`. Must still contain the ``__row`` column.
    source_position : astropy.coordinates.SkyCoord or None, optional
        Position to use for barycentring. If ``None``, each observation's own
        pointing (``ra``, ``dec``) is used instead.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``.
    outdir : str, optional
        Directory to download into and process in.
    test : bool, optional
        If True, fake the downloads and stop after the first observation, without
        processing anything.
    flags : dict, optional
        Extra parameters forwarded to the mission's Level-2 pipeline task
        (``nupipeline``, ``nicerl2``).
    force_heasarc : bool, optional
        Download over HTTPS from HEASARC, whatever the environment suggests.
    force_s3 : bool, optional
        Download from the public AWS S3 mirror, whatever the environment suggests.
    n_workers : int, optional
        How many observations to reduce at the same time. Each gets a worker process of
        its own, with its own HEASOFT parameter files and its own working directory; see
        :func:`prepare_worker` for why that isolation is necessary. The default, 1, is one
        observation at a time -- in a worker process all the same, so there is one code
        path, not two.
    scratch_dir : str, optional
        Where the workers' working directories go. The default, ``<outdir>/.workers``, is
        the safe choice: HEASOFT's temporary trees were measured at 182.5 MB for a single
        observation, about 90% of its raw data size, so ``n_workers`` of them want
        gigabytes and a small shared ``/tmp`` is the wrong place for that. Point this at a
        local disk with room and the reduction gets faster.
    pgp_keys_file : str or None, optional
        Path to PGP keys file for encrypted data. If ``None``, defaults to
        ``~/.heasarc_retrieve_pgp_keys``. File format: ``mission obsid passphrase``.

    Returns
    -------
    astropy.table.Table
        The input table, unchanged.

    Raises
    ------
    ValueError
        If the longest file name the reduction would build is too long for HEASOFT, even
        under the short name :func:`~heasarc_retrieve_pipeline.utils.short_workspace`
        gives it. Raised before anything is downloaded.

    Notes
    -----
    Links are matched to catalogue rows through the datalink ``ID``, not by
    position: the datalink service does not return one usable row per input row.
    Observations with no public data products -- typically ones still in their
    proprietary period -- are logged and skipped.
    """
    outdir = os.path.abspath(outdir)
    os.makedirs(outdir, exist_ok=True)
    logger = get_run_logger()

    links = locate_data(result_table, MISSION_CONFIG[mission]["table"])
    # Restore this once astroquery #3652 is fixed, and delete ``locate_data`` above:
    # links = Heasarc.locate_data(
    #     result_table, catalog_name=MISSION_CONFIG[mission]["table"]
    # )
    link_col_name = download_link_column(force_heasarc, force_s3)
    if n_workers > 1 and link_col_name == "access_url":
        logger.warning(
            f"{n_workers} workers will scrape the HEASARC HTTPS archive in parallel. "
            "The S3 mirror (the default, force_s3=True) serves parallel readers better."
        )

    items = observation_work_items(
        result_table,
        links,
        link_col_name,
        source_position,
        mission=mission,
        pgp_keys_file=pgp_keys_file,
    )
    if test:
        items = items[:1]

    # The workers are given a short name for outdir, not outdir itself: some HEASOFT
    # builds truncate file names at 128 characters, and the pipeline adds 61 of its own
    # after the output root. Their parameter files go to local disk in the same place;
    # their working directories stay where there is room for them.
    with short_workspace(outdir, scratch_dir=scratch_dir) as workspace:
        # Before anything is downloaded: would the longest name this reduction builds
        # survive HEASOFT? On the cluster this run got as far as nusplitsc, 90 GB and one
        # Level-2 pipeline per observation later, before saying anything at all.
        longest_name = MISSION_CONFIG[mission].get("longest_output_name")
        if longest_name is not None:
            for item in items:
                check_name_length(longest_name(item["obsid"], dict(out_data_path=workspace.data)))

        runner = ProcessPoolTaskRunner(max_workers=n_workers)
        logger.info(f"Reducing {len(items)} observations, {n_workers} at a time")
        process_observations.with_options(task_runner=runner)(
            items,
            outdir=workspace.data,
            mission=mission,
            pfiles_root=workspace.pfiles,
            work_root=workspace.work,
            flags=flags,
            test=test,
        )

    return result_table


@flow
def retrieve_heasarc_data_by_source_name(
    source: str,
    outdir: str = "out",
    mission: str = "nustar",
    radius_deg: float = 0.1,
    test: bool = False,
    force_heasarc: bool = False,
    force_s3: bool = False,
    n_workers: int = 1,
    scratch_dir: typing.Union[str, None] = None,
    pgp_keys_file: typing.Union[str, None] = None,
):
    """
    Download and reduce every observation of a named source.

    Top-level entry point. Resolves the name, cone-searches the mission's master
    catalogue, and hands the results to :func:`retrieve_and_process_data`, which
    locates the downloadable products for each row.

    Parameters
    ----------
    source : str
        Source name, as understood by ``SkyCoord.from_name``.
    outdir : str, optional
        Directory to download into and process in.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``.
    radius_deg : float, optional
        Cone-search radius in degrees, applied to the observation pointing.
    test : bool, optional
        If True, fake the downloads and stop after the first observation.
    force_heasarc : bool, optional
        Download over HTTPS from HEASARC.
    force_s3 : bool, optional
        Download from the public AWS S3 mirror.
    n_workers : int, optional
        How many observations to reduce at the same time, one worker process each.
    scratch_dir : str, optional
        Where the workers' working directories go; see
        :func:`retrieve_and_process_data`.
    pgp_keys_file : str or None, optional
        Path to PGP keys file for encrypted data; see :func:`retrieve_and_process_data`.

    Returns
    -------
    astropy.table.Table
        The catalogue rows that were processed.

    Notes
    -----
    Unlike :func:`retrieve_heasarc_data_by_obsid`, this flow has no ``flags``
    argument, so Level-2 pipeline parameters cannot be customised when working by
    source name.
    """
    pos = get_source_position(source)

    results = retrieve_heasarc_table_by_position(
        pos.ra.deg, pos.dec.deg, mission=mission, radius_deg=radius_deg
    )
    results = retrieve_and_process_data(
        result_table=results,
        source_position=pos,
        mission=mission,
        outdir=outdir,
        test=test,
        force_heasarc=force_heasarc,
        force_s3=force_s3,
        n_workers=n_workers,
        scratch_dir=scratch_dir,
        pgp_keys_file=pgp_keys_file,
    )

    return results


@flow
def retrieve_heasarc_data_by_obsid(
    obsid: typing.Union[str, typing.List[str]],
    outdir: str = "out",
    mission: str = "nustar",
    test: bool = False,
    flags: dict = {},
    force_heasarc: bool = False,
    force_s3: bool = False,
    n_workers: int = 1,
    scratch_dir: typing.Union[str, None] = None,
    pgp_keys_file: typing.Union[str, None] = None,
):
    """
    Download and reduce observations, by OBSID.

    Top-level entry point. Looks the OBSIDs up in the mission's master catalogue and
    hands the result to :func:`retrieve_and_process_data`. Since no source position is
    given, each observation is barycentred at its own pointing coordinates (for NuSTAR,
    at the position measured from the image).

    Parameters
    ----------
    obsid : str or list of str
        Observation identifier, or a list of them. With ``n_workers`` above 1, several
        are reduced at the same time, one worker process each.
    outdir : str, optional
        Directory to download into and process in.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``.
    test : bool, optional
        If True, fake the download and skip processing.
    flags : dict, optional
        Extra parameters forwarded to the mission's Level-2 pipeline task.
    force_heasarc : bool, optional
        Download over HTTPS from HEASARC.
    force_s3 : bool, optional
        Download from the public AWS S3 mirror.
    n_workers : int, optional
        How many observations to reduce at the same time, one worker process each.
    scratch_dir : str, optional
        Where the workers' working directories go; see
        :func:`retrieve_and_process_data`.
    pgp_keys_file : str or None, optional
        Path to PGP keys file for encrypted data; see :func:`retrieve_and_process_data`.

    Returns
    -------
    astropy.table.Table or None
        The catalogue rows that were processed, or ``None`` if none of the OBSIDs is in
        the catalogue.
    """
    logger = get_run_logger()

    results = retrieve_info_for_obsid(obsid, mission=mission)
    if not results:
        logger.warning(f"No observations found for OBSID {obsid} in HEASARC query.")
        return None

    wanted = [obsid] if isinstance(obsid, str) else list(obsid)
    missing = set(wanted) - set(str(found) for found in results["obsid"])
    if missing:
        logger.warning(f"Not in the {mission} catalogue, skipped: {sorted(missing)}")

    results = retrieve_and_process_data(
        result_table=results,
        source_position=None,
        mission=mission,
        outdir=outdir,
        test=test,
        flags=flags,
        force_heasarc=force_heasarc,
        force_s3=force_s3,
        n_workers=n_workers,
        scratch_dir=scratch_dir,
        pgp_keys_file=pgp_keys_file,
    )
    return results
