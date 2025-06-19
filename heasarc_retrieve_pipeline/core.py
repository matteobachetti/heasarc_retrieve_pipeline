import os
import re
import shutil
import sys
import glob
import traceback
import pytest
from astropy.table import hstack
from astroquery.heasarc import Heasarc

from .nustar import nu_heasarc_raw_data_path as nu_raw_data_path
from .nustar import process_nustar_obsid
from .nicer import ni_raw_data_path, process_nicer_obsid

from prefect import flow, task, get_run_logger


def _download_pysmartdl(url: str, dest: str):
    from pySmartDL import SmartDL

    obj = SmartDL(url, dest)
    obj.start()
    return obj.get_dest()


def remote_data_url(mission, obsid, time):
    url = (
        "https://heasarc.gsfc.nasa.gov/"
        + MISSION_CONFIG[mission]["path_func"](obsid, time=time)
        + "/"
    )
    return url


def download_cmd(url: str, dest: str):
    try:
        return _download_pysmartdl(url, dest), None
    except Exception as e:
        return None, str(e)


@task
def get_remote_directory_listing(url: str):
    """Give the list of files in the remote directory."""
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError

    from bs4 import BeautifulSoup

    url = url.replace(" ", "%20")
    req = Request(url)
    try:
        a = urlopen(req).read()
    except HTTPError:
        return None

    soup = BeautifulSoup(a, "html.parser")
    x = soup.find_all("a")
    urls = []
    for i in x:
        file_name = i.extract().get_text()
        url_new = url + file_name
        url_new = url_new.replace(" ", "%20")
        if file_name[-1] == "/" and file_name[0] != ".":
            urls.append(url_new)
            url_new = get_remote_directory_listing.fn(url_new)
            if url_new is None:
                continue
            urls.extend(url_new)
        else:
            urls.append(url_new)

    return urls


@task(task_run_name="download_{node}")
def download_node(
    node: str,
    base_url: str,
    outdir: str,
    cut_ndirs: int = 0,
    test_str: str = ".",
    test: bool = False,
):
    logger = get_run_logger()
    local_ver = os.path.join(outdir, *node.replace(base_url, "").split("/")[cut_ndirs:])
    if test_str is not None and test_str not in local_ver:
        logger.debug(f"Ignoring {node}")
        return None
    logger.info(f"Downloading {node} to {local_ver}")
    if os.path.exists(local_ver):
        logger.info(f"{local_ver} exists")
        return None

    is_dir = local_ver.endswith("/")

    if is_dir:
        if not test:
            os.makedirs(local_ver, exist_ok=True)
        else:
            logger.info(f"Faked creation of {local_ver}")
        return local_ver

    if not test:
        fname, exc_string = download_cmd(node, local_ver)
    else:
        logger.info(f"Faked download of {node} to {local_ver}")
        fname = local_ver
    if fname is None:
        logger.warning(f"Error downloading {node}: {exc_string}")
    # print(node, local_ver)
    return local_ver


@flow
def recursive_download(
    url: str,
    outdir: str,
    cut_ndirs: int = 0,
    test_str: str = ".",
    test: bool = False,
    re_include: str = "",
    re_exclude: str = "",
):

    if not url.startswith("http"):
        outpath = os.path.join(outdir, url.rstrip("/").split("/")[-1])
        logger = get_run_logger()
        logger.info(f"Copying local directory {url} to {outpath}")

        shutil.copytree(url.rstrip("/"), outpath, dirs_exist_ok=True)

        return os.walk(outpath)

    re_include = re.compile(re_include) if re_include != "" else None
    re_exclude = re.compile(re_exclude) if re_exclude != "" else None

    # rec_down_file = os.path.join(outdir, "DOWNLOAD_DONE.txt")
    # if os.path.exists(rec_down_file):
    #     logger.info(f"Download already done for {url}")
    #     return
    logger = get_run_logger()
    logger.info("Getting remote directory listing...")
    listing = get_remote_directory_listing.fn(url)
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
            )
        )
    # open(rec_down_file, "a").close()
    return local_vers


MISSION_CONFIG = {
    "nustar": {
        "table": "numaster",
        "path_func": nu_raw_data_path,
        "expo_column": "exposure_a",
        "additional": "solar_activity",
        "obsid_processing": process_nustar_obsid,
    },
    "nicer": {
        "table": "nicermastr",
        "path_func": ni_raw_data_path,
        "expo_column": "exposure",
        "additional": "",
        "obsid_processing": process_nicer_obsid,
    },
}


@task
def read_config(config_file: str):
    import yaml

    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config


@task(log_prints=True)
def retrieve_heasarc_table_by_position(
    ra_deg: float, dec_deg: float, mission: str = "nustar", radius_deg: float = 0.1
):

    expo_name = MISSION_CONFIG[mission]["expo_column"]
    additional = MISSION_CONFIG[mission]["additional"]
    table = MISSION_CONFIG[mission]["table"]
    if additional != "":
        additional = f", {additional}"
    query = f"""SELECT name, cycle, obsid, time, {expo_name}, ra, dec, public_date, __row {additional}
        FROM public.{table} as cat
        where
        contains(point('ICRS',cat.ra,cat.dec),circle('ICRS',{ra_deg},{dec_deg},{radius_deg}))=1
        and
        cat.{expo_name} > 0 order by cat.time
        """

    # results = tap_service.search(query)
    results = Heasarc.query_tap(query).to_table()

    return results


@task
def get_source_position(source: str):
    import astropy.coordinates as coord

    pos = coord.SkyCoord.from_name(f"{source}")

    return pos


@task
def retrieve_heasarc_table_by_source_name(
    source: str, mission: str = "nustar", radius_deg: float = 0.1
):
    pos = get_source_position.fn(source)
    results = retrieve_heasarc_table_by_position.fn(
        pos.ra.deg, pos.dec.deg, mission=mission, radius_deg=radius_deg
    )
    return results


@task
def retrieve_heasarc_data_by_source_name_old(
    source: str,
    outdir: str = "out",
    mission: str = "nustar",
    radius_deg: float = 0.1,
    test: bool = False,
):

    logger = get_run_logger()
    pos = get_source_position(source)

    results = retrieve_heasarc_table_by_position.fn(
        pos.ra.deg, pos.dec.deg, mission=mission, radius_deg=radius_deg
    )

    for row in results:
        logger.info(f"{row['obsid']}, {row['time']}")
    cwd = os.getcwd()
    for obsid, time in zip(results["obsid"], results["time"]):
        os.chdir(cwd)
        url = remote_data_url(mission, obsid, time)
        recursive_download(
            url,
            outdir,
            cut_ndirs=0,
            test_str=".",
            test=test,
            wait_for=[remote_data_url],
        )
        if test:
            break
        os.chdir(outdir)
        process_nustar_obsid(
            obsid,
            config=None,
            ra=pos.ra.deg,
            dec=pos.dec.deg,
            wait_for=[recursive_download],
            return_state=True,
        )

    return results


@flow
def retrieve_heasarc_data_by_source_name(
    source: str,
    outdir: str = "out",
    mission: str = "nustar",
    radius_deg: float = 0.1,
    test: bool = False,
):

    logger = get_run_logger()
    pos = get_source_position(source)

    results = retrieve_heasarc_table_by_position.fn(
        pos.ra.deg, pos.dec.deg, mission=mission, radius_deg=radius_deg
    )
    try:
        # Work around for astroquery bug
        Heasarc._last_catalog_name = MISSION_CONFIG[mission]["table"]
        links = Heasarc.locate_data(results)
    except Exception as e:
        logger.error(f"Error using astroquery to locate data: {str(e)}")
        return retrieve_heasarc_data_by_source_name_old.fn(
            source=source,
            outdir=outdir,
            mission=mission,
            radius_deg=radius_deg,
            test=test,
        )
    results = hstack([results, links])
    for row in results:
        logger.info(f"{row['obsid']}, {row['time']}")
    cwd = os.getcwd()
    for obsid, time, sci_dir, remote_url in zip(
        results["obsid"], results["time"], results["sciserver"], results["access_url"]
    ):
        os.chdir(cwd)
        if "SCISERVER_USER_ID" in os.environ:
            url = sci_dir
        else:
            url = remote_url
        recursive_download(
            url,
            outdir,
            cut_ndirs=0,
            test_str=".",
            test=test,
            wait_for=[remote_data_url],
        )
        if test:
            break
        os.chdir(outdir)
        process_nustar_obsid(
            obsid,
            config=None,
            ra=pos.ra.deg,
            dec=pos.dec.deg,
            wait_for=[recursive_download],
            return_state=True,
        )

    return results
