import os
import sys
import glob
import traceback
import pytest
from .nustar import nu_raw_data_path
from .nicer import ni_raw_data_path

from prefect import flow, task, get_run_logger


def _download_pysmartdl(url, dest):
    from pySmartDL import SmartDL

    obj = SmartDL(url, dest)
    obj.start()
    return obj.get_dest()


def download_cmd(url, dest):
    try:
        return _download_pysmartdl(url, dest)
    except Exception:
        return None


@task
def get_remote_directory_listing(url):
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


@task
def download_node(node, base_url, outdir, cut_ndirs=0, test_str=".", test=False):
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
        fname = download_cmd(node, local_ver)
    else:
        logger.info(f"Faked download of {node} to {local_ver}")
        fname = local_ver
    if fname is None:
        raise (Exception(f"Error downloading {node}"))
    # print(node, local_ver)
    return local_ver


@flow
def recursive_download(
    url, outdir, cut_ndirs=0, test_str=".", test=False, re_include=None, re_exclude=None
):
    logger = get_run_logger()
    logger.info("Getting remote directory listing...")
    listing = get_remote_directory_listing.fn(url)
    if listing is None or listing == []:
        logger.warning(f"No data found in remote directory {url}")
        return False

    base_url = "/".join(url.rstrip("/").split("/")[:-1])
    local_vers = []
    for node in listing:
        if re_include is not None and not re_include.search(node):
            logger.info(f"Skipping {node} because not included in {re_include.pattern}")
            continue
        if re_exclude is not None and re_exclude.search(node):
            logger.info(f"Skipping {node} because excluded in {re_include.pattern}")
            continue
        local_vers.append(
            download_node(node, base_url, outdir, cut_ndirs=cut_ndirs, test_str=test_str, test=test)
        )
    return local_vers


MISSION_CONFIG = {
    "nustar": {
        "table": "numaster",
        "path_func": nu_raw_data_path,
        "expo_column": "exposure_a",
        "additional": "solar_activity",
    },
    "nicer": {
        "table": "nicermastr",
        "path_func": ni_raw_data_path,
        "expo_column": "exposure",
        "additional": "",
    },
}


@task
def read_config(config_file):
    import yaml

    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config


@task(log_prints=True)
def retrieve_heasarc_table_by_position(ra_deg, dec_deg, mission="nustar", radius_deg=0.1):
    import astropy.coordinates as coord
    import pyvo as vo

    tap_services = vo.regsearch(servicetype="table", keywords=["heasarc"])

    expo_name = MISSION_CONFIG[mission]["expo_column"]
    additional = MISSION_CONFIG[mission]["additional"]
    table = MISSION_CONFIG[mission]["table"]

    query = f"""SELECT name, cycle, obsid, time, {expo_name}, ra, dec, public_date {additional}
        FROM public.{table} as cat
        where
        contains(point('ICRS',cat.ra,cat.dec),circle('ICRS',{ra_deg},{dec_deg},{radius_deg}))=1
        and
        cat.{expo_name} > 0 order by cat.time
        """

    print(query)
    results = tap_services[0].search(query).to_table()
    return results


@task
def retrieve_heasarc_table_by_source_name(source, mission="nustar", radius_deg=0.1):
    import astropy.coordinates as coord

    pos = coord.SkyCoord.from_name(f"{source}")

    return retrieve_heasarc_table_by_position.fn(
        pos.ra.deg, pos.dec.deg, mission=mission, radius_deg=0.1
    )


@flow
def retrieve_heasarc_data_by_source_name(
    source, outdir="out", mission="nustar", radius_deg=0.1, test=False
):
    import astropy.coordinates as coord

    logger = get_run_logger()
    pos = coord.SkyCoord.from_name(f"{source}")

    results = retrieve_heasarc_table_by_position.fn(
        pos.ra.deg, pos.dec.deg, mission=mission, radius_deg=radius_deg
    )
    for row in results:
        logger.info(f"{row['obsid']}, {row['time']}")
    for obsid, time in zip(results["obsid"], results["time"]):
        url = (
            "https://heasarc.gsfc.nasa.gov/"
            + MISSION_CONFIG[mission]["path_func"](obsid, time=time)
            + "/"
        )
        recursive_download(url, outdir, cut_ndirs=0, test_str=".", test=test)
        if test:
            break
    return results


# test that the retrieval works
@pytest.mark.parametrize(
    "mission",
    ["nustar", "nicer"],
)
def test_retrieve_heasarc_table_by_source_name(mission):
    results = retrieve_heasarc_table_by_source_name.fn("M82 X-2", mission=mission)
    assert len(results) > 0


@pytest.mark.parametrize(
    "mission",
    ["nustar", "nicer"],
)
def test_retrieve_heasarc_data_by_source_name(mission):
    results = retrieve_heasarc_data_by_source_name("M82 X-2", mission=mission, test=True)
    assert len(results) > 0


def test_recursive_download():
    import shutil
    import re

    results = recursive_download(
        "https://heasarc.gsfc.nasa.gov/FTP/nustar/data/obs/00/8/80002092003/",
        "out_test",
        cut_ndirs=0,
        test_str=".",
        test=False,
        re_include=re.compile(r"[AB]0.*evt"),
        re_exclude=re.compile(r"[AB]0[2-5]"),
    )
    assert len(results) == 2
    shutil.rmtree("out_test")
