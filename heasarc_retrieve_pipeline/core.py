import os
import re
import glob
import subprocess
import time
import fnmatch
from astropy.time import Time
from prefect import flow, task, get_run_logger
from prefect.futures import wait
from astropy.coordinates import SkyCoord
from .nustar import nu_heasarc_raw_data_path as nu_raw_data_path
from .nustar import process_nustar_obsid
from .nicer3 import ni_heasarc_raw_data_path as ni_raw_data_path
from .nicer3 import process_nicer_obsid
from .rxte import rxte_heasarc_raw_data_path as rxte_raw_data_path
from .rxte import process_rxte_obsid


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


@task(task_run_name="get_s3_links_for_{obsid}")
def get_s3_download_links(
    mission: str, obsid: str, time_mjd: float, cycle: str = None, prnb: str = None
) -> list[str]:

    logger = get_run_logger()
    mission = mission.lower()

    base_obs_dir = ""

    if mission == "nicer":
        if time_mjd is None:
            logger.error(f"Cannot generate NICER S3 links for {obsid}: 'time_mjd' is required.")
            return []
        t = Time(float(time_mjd), format="mjd")
        year_month = f"{t.datetime.year}_{t.datetime.month:02d}"
        base_obs_dir = f"https://nasa-heasarc.s3.amazonaws.com/nicer/data/obs/{year_month}/{obsid}"

    elif mission == "rxte":
        if not cycle or not prnb:
            logger.error(
                f"Cannot generate RXTE S3 links for {obsid}: 'cycle' and 'prnb' are required."
            )
            return []
        base_obs_dir = (
            f"https://nasa-heasarc.s3.amazonaws.com/rxte/data/archive/AO{cycle}/P{prnb}/{obsid}"
        )

    else:
        logger.error(f"Mission '{mission}' is not supported for S3 link generation.")
        return []

    try:
        relative_paths = MISSION_CONFIG[mission]["path_func"](obsid, cycle=cycle, prnb=prnb)

        full_urls = [f"{base_obs_dir}/{path.lstrip('/')}" for path in relative_paths]
        return full_urls
    except TypeError:
        relative_paths = MISSION_CONFIG[mission]["path_func"](obsid)
        full_urls = [f"{base_obs_dir}/{path.lstrip('/')}" for path in relative_paths]
        return full_urls
    except KeyError:
        logger.error(f"Could not find a path function for mission '{mission}'.")
        return []


def download_cmd(url: str, dest: str):
    try:
        return _download_pysmartdl(url, dest), None
    except Exception as e:
        return None, str(e)


@task
def get_remote_directory_listing(url: str):
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError
    from bs4 import BeautifulSoup
    import time

    max_retries = 3
    retry_delay = 1
    url = url.replace(" ", "%20")
    req = Request(url)
    for attempt in range(max_retries):
        try:
            a = urlopen(req).read()
            break
        except HTTPError:
            return None
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
    soup = BeautifulSoup(a, "html.parser")
    x = soup.find_all("a")
    urls = []
    exclude_headers = ["Name", "Last modified", "Size", "Description", "Parent Directory"]
    for i in x:
        file_name = i.extract().get_text()
        if file_name in exclude_headers:
            continue
        url_new = url + file_name
        url_new = url_new.replace(" ", "%20")
        if file_name[-1] == "/" and file_name[0] != ".":
            urls.append(url_new)
            sub_urls = get_remote_directory_listing.fn(url_new)
            if sub_urls is None:
                continue
            urls.extend(sub_urls)
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
    return local_ver


@task(task_run_name="download_node_s3{node}")
def download_node_s3(
    node: str,
    base_url: str,
    outdir: str,
    cut_ndirs: int = 0,
    test_str: str = ".",
    test: bool = False,
):
    logger = get_run_logger()

    relative_path_from_s3_root = node.replace(base_url, "")
    path_segments = relative_path_from_s3_root.split("/")

    try:
        local_ver = os.path.join(outdir, *path_segments[cut_ndirs:])
    except IndexError:
        logger.error(f"Could not parse node URL segments for {node}")
        return None

    if test_str not in local_ver:
        logger.debug(f"Ignoring {node}")
        return None

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

    parent_dir = os.path.dirname(local_ver)
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)

    logger.info(f"Downloading {node} to {local_ver}")

    if not test:
        result = download_cmd(node, local_ver)
        if result is None:
            logger.warning(f"Failed to download {node}: download_cmd returned None")
            return None
        fname, exc_string = result
        if fname is None:
            logger.warning(f"Failed to download {node}: {exc_string}")
            return None
        logger.info(f"Downloaded to {fname}")
    else:
        logger.info(f"Faked download of {node} to {local_ver}")
        fname = local_ver

    return local_ver


@flow(flow_run_name="download_s3_{mission}_{obsid}")
def download_from_s3(
    mission: str, obsid: str, base_outdir: str, time_mjd: float, cycle: str = None, prnb: str = None
):

    logger = get_run_logger()
    mission = mission.lower()

    download_links = get_s3_download_links(
        mission=mission, obsid=obsid, time_mjd=time_mjd, cycle=cycle, prnb=prnb
    )

    if not download_links:
        logger.error(f"Could not generate S3 download links for {obsid}. Halting flow.")
        return None

    try:
        s3 = MISSION_CONFIG[mission]
        cut_ndirs_for_mission = s3["cut_ndirs"]
    except KeyError:
        logger.error(
            f"Mission '{mission}' not found in MISSION_S3_CONFIG. Cannot determine 'cut_ndirs'."
        )
        return None

    obsid_data_dir = os.path.join(base_outdir, obsid)
    os.makedirs(obsid_data_dir, exist_ok=True)

    s3_root_url = "https://nasa-heasarc.s3.amazonaws.com/"
    download_futures = []

    logger.info(
        f"Submitting downloads for {mission} OBSID {obsid} with cut_ndirs={cut_ndirs_for_mission}."
    )

    for node_url in download_links:
        future = download_node_s3.submit(
            node=node_url,
            base_url=s3_root_url,
            outdir=obsid_data_dir,
            cut_ndirs=cut_ndirs_for_mission,
            test=False,
            wait_for=[get_s3_download_links],
        )
        download_futures.append(future)
    wait(download_futures)

    logger.info(f"All S3 downloads for OBSID {obsid} have been submitted.")
    return obsid_data_dir


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
    import time

    logger = get_run_logger()

    max_retries = 3
    for attempt in range(max_retries):
        try:
            re_include = re.compile(re_include) if re_include != "" else None
            re_exclude = re.compile(re_exclude) if re_exclude != "" else None

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
                    logger.debug(f"Skipping {node} because not included in {re_include.pattern}")
                    continue
                if re_exclude is not None and re_exclude.search(node):
                    logger.debug(f"Skipping {node} because excluded in {re_exclude.pattern}")
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
            return local_vers

        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)} Retrying ;)")
            continue


MISSION_CONFIG = {
    "nustar": {
        "table": "numaster",
        "path_func": nu_raw_data_path,
        "expo_column": "exposure_a",
        "additional": "",
        "obsid_processing": process_nustar_obsid,
        "s3_download": False,
        "select_columns": "name, obsid, time, exposure_a, ra, dec, public_date",
    },
    "nicer": {
        "table": "nicermastr",
        "path_func": ni_raw_data_path,
        "expo_column": "exposure",
        "additional": "",
        "obsid_processing": process_nicer_obsid,
        "cut_ndirs": 5,
        "s3_download": True,
        "select_columns": "name, obsid, time, exposure, ra, dec, public_date",
    },
    "rxte": {
        "table": "xtemaster",
        "path_func": rxte_raw_data_path,
        "expo_column": "exposure",
        "additional": "cycle, prnb",
        "obsid_processing": process_rxte_obsid,
        "cut_ndirs": 6,
        "s3_download": True,
        "select_columns": "target_name, obsid, time, exposure, ra, dec, cycle, prnb",
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
    import astropy.coordinates as coord
    import pyvo as vo

    tap_services = vo.regsearch(servicetype="table", keywords=["heasarc"])
    tap_service = tap_services[0].service

    expo_name = MISSION_CONFIG[mission]["expo_column"]
    additional = MISSION_CONFIG[mission]["additional"]
    table = MISSION_CONFIG[mission]["table"]
    if additional != "":
        additional = f", {additional}"
    query = f"""SELECT name, cycle, obsid, time, {expo_name}, ra, dec, public_date {additional}
        FROM public.{table} as cat
        where
        contains(point('ICRS',cat.ra,cat.dec),circle('ICRS',{ra_deg},{dec_deg},{radius_deg}))=1
        and
        cat.{expo_name} > 0 order by cat.time
        """

    results = tap_service.search(query)
    return results


@task(log_prints=True)
def retrieve_heasarc_table_by_obsid(obsid: str, mission: str = "rxte"):
    """
    Queries the HEASARC TAP service for a specific OBSID.
    """
    import pyvo as vo
    from pyvo.dal.exceptions import DALFormatError

    tap_services = vo.regsearch(servicetype="table", keywords=["heasarc"])
    if not tap_services:
        raise RuntimeError("not found")
    tap_service = tap_services[0].service

    expo_name = MISSION_CONFIG[mission]["expo_column"]
    table = MISSION_CONFIG[mission]["table"]
    select_cols = MISSION_CONFIG[mission]["select_columns"]

    query = f"""SELECT {select_cols}
       FROM public.{table} as cat
       WHERE
       cat.obsid = '{obsid}'
       AND
       cat.{expo_name} >= 0
       """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            results = tap_service.search(query)
            return results
        except DALFormatError as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying ;)")
            else:
                raise


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


@flow
def retrieve_heasarc_data_by_source_name(
    source: str,
    outdir: str,
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
def retrieve_heasarc_data_by_obsid(
    source: str,
    obsid: str,
    base_outdir: str,
    mission: str = "nicer",
    test: bool = False,
    flags: dict = {},
):
    logger = get_run_logger()
    mission = mission.lower()

    results = retrieve_heasarc_table_by_obsid(obsid=obsid, mission=mission)

    if not results:
        logger.error(
            f"No entry found for OBSID {obsid} in the '{MISSION_CONFIG[mission]['table']}' table."
        )
        return None

    row_to_process = results[0]
    logger.info(f"Found matching observation for OBSID {obsid}")

    obsid_to_process = str(row_to_process["obsid"])
    time_for_url = row_to_process.get("time")
    cycle = str(row_to_process.get("cycle", ""))
    prnb = str(row_to_process.get("prnb", ""))

    obsid_download_dir = os.path.join(base_outdir, obsid_to_process)
    os.makedirs(obsid_download_dir, exist_ok=True)

    if MISSION_CONFIG[mission].get("s3_download", False):
        logger.info(f"Attempting S3 download for {mission} OBSID {obsid_to_process}.")
        download_from_s3(
            mission=mission,
            obsid=obsid_to_process,
            base_outdir=base_outdir,
            time_mjd=time_for_url,
            cycle=cycle,
            prnb=prnb,
        )
    else:
        logger.info(f"Using recursive download for {mission} OBSID {obsid_to_process}.")
        url = remote_data_url(mission, obsid_to_process, time_for_url)
        recursive_download(
            url,
            os.path.join(base_outdir, obsid_to_process),
            cut_ndirs=0,
            test_str=".",
            test=test,
            re_include=".*",
            re_exclude="none",
        )
    pos = get_source_position(source)
    download_dir = os.path.join(base_outdir, obsid_to_process)
    config = {"input_data_path": download_dir, "out_data_path": download_dir}
    processing_func = MISSION_CONFIG[mission]["obsid_processing"]

    logger.info(
        f"Submitting processing for OBSID {obsid_to_process} using {processing_func.__name__}"
    )

    processing_func(
        obsid_to_process,
        config=config,
        ra=pos.ra.deg,
        dec=pos.dec.deg,
        flags=flags,
    )
    return results
