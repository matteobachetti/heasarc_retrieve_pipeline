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
import sys
import glob
import traceback
import pytest
import warnings
import typing
from astropy.table import hstack, Table
from astroquery.heasarc import Heasarc
import pyvo
from astropy.coordinates import SkyCoord


from .nustar import process_nustar_obsid, DEFAULT_CONFIG as NUSTAR_DEFAULT_CONFIG
from .nicer import process_nicer_obsid, DEFAULT_CONFIG as NICER_DEFAULT_CONFIG
from .rxte import process_rxte_obsid, DEFAULT_CONFIG as RXTE_DEFAULT_CONFIG

from prefect import flow, task, get_run_logger

from .utils import absolute_config, get_logger


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
        "additional": "solar_activity",
        "obsid_processing": process_nustar_obsid,
        "default_config": NUSTAR_DEFAULT_CONFIG,
        "name_column": "name",
    },
    "nicer": {
        "table": "nicermastr",
        "expo_column": "exposure",
        "additional": "",
        "obsid_processing": process_nicer_obsid,
        "default_config": NICER_DEFAULT_CONFIG,
        "name_column": "name",
    },
    "rxte": {
        "table": "xtemaster",
        "expo_column": "exposure",
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
    given position and whose exposure is non-negative (planned-but-not-executed
    observations carry a null or negative exposure).

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
        cat.{expo_name} >= 0 order by cat.time
        """

    results = Heasarc.query_tap(query).to_table()

    return results


@task(task_run_name="retrieve_info_for_obsid_{obsid}")
def retrieve_info_for_obsid(obsid, mission: str = "nustar"):
    """
    Look up a single observation in a mission's master catalogue.

    Parameters
    ----------
    obsid : str
        Observation identifier, matched exactly.
    mission : str, optional
        One of the keys of ``MISSION_CONFIG``.

    Returns
    -------
    astropy.table.Table
        Zero or one row, with the mission's name, ``cycle``, ``obsid``, ``time``,
        exposure, ``ra``, ``dec``, ``__row`` and any mission-specific extra columns.

    Notes
    -----
    Unlike :func:`retrieve_heasarc_table_by_position`, this does not alias the
    mission's name column to ``source_name``, so the two functions return tables
    with slightly different schemas.
    """
    expo_name = MISSION_CONFIG[mission]["expo_column"]
    additional = MISSION_CONFIG[mission]["additional"]
    table = MISSION_CONFIG[mission]["table"]
    name_column = MISSION_CONFIG[mission]["name_column"]
    if additional != "":
        additional = f", {additional}"
    query = f"""SELECT {name_column}, cycle, obsid, time, {expo_name}, ra, dec, __row {additional}
        FROM public.{table} as cat
        where
        cat.obsid='{obsid}'
        and
        cat.{expo_name} >= 0 order by cat.time
        """

    results = Heasarc.query_tap(query).to_table()
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
        f"s3://{Heasarc.S3_BUCKET}/{row[5:]}" if row != "" else ""
        for row in dl_result["sciserver"]
    ]
    dl_result.add_column(newcol, name="aws", index=3)

    return dl_result


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

    Returns
    -------
    astropy.table.Table
        The input table, unchanged.

    Notes
    -----
    Links are matched to catalogue rows through the datalink ``ID``, not by
    position: the datalink service does not return one usable row per input row.
    Observations with no public data products -- typically ones still in their
    proprietary period -- are logged and skipped.
    """
    outdir = os.path.abspath(outdir)
    os.makedirs(outdir, exist_ok=True)
    # Absolute, once: the reduction used to be steered by chdir-ing into ``outdir``, which
    # made every path in the mission defaults ("./") mean whatever the process working
    # directory happened to be. See issue 26 in ``docs/known_issues.rst``.
    config = absolute_config(
        dict(input_data_path=outdir, out_data_path=outdir),
        MISSION_CONFIG[mission]["default_config"],
    )
    processing = MISSION_CONFIG[mission]["obsid_processing"]
    logger = get_run_logger()
    links = locate_data(result_table, MISSION_CONFIG[mission]["table"])
    # Restore this once astroquery #3652 is fixed, and delete ``locate_data`` above:
    # links = Heasarc.locate_data(
    #     result_table, catalog_name=MISSION_CONFIG[mission]["table"]
    # )
    # Match links to catalogue rows by identity, not by position: observations
    # with no public data products do come back, but not in a downloadable form.
    link_by_row = {str(i).split("?")[-1]: row for i, row in zip(links["ID"], links)}
    if force_s3:
        link_col_name = "aws"
    elif force_heasarc:
        link_col_name = "access_url"
    elif "SCISERVER_USER_ID" in os.environ:
        link_col_name = "sciserver"
    else:
        # Defaults to AWS
        link_col_name = "aws"

    for row in result_table:
        obsid = row["obsid"]
        if source_position is not None:
            ra = source_position.ra.deg
            dec = source_position.dec.deg
        else:
            ra = row["ra"]
            dec = row["dec"]

        link = link_by_row.get(row["__row"])
        if link is None or not link[link_col_name]:
            logger.info(
                f"No public data products for OBSID {obsid} "
                "(still in its proprietary period?), skipping"
            )
            continue

        recursive_download(link[link_col_name], outdir, test_str=".", test=test)
        if test:
            break
        # Heasarc.download_data(link, host=host, location=outdir)

        # recursive_download is a flow, and a subflow call is synchronous and raises:
        # the ordering is already guaranteed by the line above. Prefect 3 has no
        # flow.submit(), so there is no future to declare here either.
        processing(
            obsid,
            config=config,
            ra=ra,
            dec=dec,
            flags=flags,
            return_state=True,
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
    )

    return results


@flow
def retrieve_heasarc_data_by_obsid(
    obsid: str,
    outdir: str = "out",
    mission: str = "nustar",
    test: bool = False,
    flags: dict = {},
    force_heasarc: bool = False,
    force_s3: bool = False,
):

    """
    Download and reduce a single observation, by OBSID.

    Top-level entry point. Looks the OBSID up in the mission's master catalogue and
    hands the single-row result to :func:`retrieve_and_process_data`. Since no
    source position is given, the observation is barycentred at its own pointing
    coordinates (for NuSTAR, at the position measured from the image).

    Parameters
    ----------
    obsid : str
        Observation identifier.
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

    Returns
    -------
    astropy.table.Table or None
        The catalogue row that was processed, or ``None`` if the OBSID is not in the
        catalogue.
    """
    logger = get_run_logger()

    results = retrieve_info_for_obsid(obsid, mission=mission)
    if not results:
        logger.warning(f"No observations found for OBSID {obsid} in HEASARC query.")
        return None

    results = retrieve_and_process_data(
        result_table=results,
        source_position=None,
        mission=mission,
        outdir=outdir,
        test=test,
        flags=flags,
        force_heasarc=force_heasarc,
        force_s3=force_s3,
    )
    return results
