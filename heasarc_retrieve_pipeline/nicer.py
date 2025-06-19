import re

from astropy.time import Time

import glob

from datetime import timedelta

from prefect import flow, task, get_run_logger

from prefect.tasks import task_input_hash

import os

try:
    HAS_HEASOFT = True
    import heasoftpy as hsp
except ImportError:
    HAS_HEASOFT = False
    print("Warning: heasoftpy not installed. NICER L2 pipeline functionality will be disabled.")


DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")


valid_re = re.compile(r"ni[0-9]{11}")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="ni_remote_raw_path_{obsid}",
)
def ni_heasarc_raw_data_path(obsid):
    rel_paths = [
        # Directories
        "auxil/",
        "log/",
        "xti/",
        "xti/event_cl/",
        "xti/event_uf/",
        "xti/hk/",
        "xti/products/",
        # files
        f"log/ni{obsid}_errlog.html",
        f"log/ni{obsid}_joblog.html",
        f"auxil/ni{obsid}.orb.gz",
        f"auxil/ni{obsid}.cat",
        f"auxil/ni{obsid}.mkf.gz",
        f"auxil/ni{obsid}.att.gz",
        f"xti/hk/ni{obsid}_0mpu0.hk.gz",
        f"xti/hk/ni{obsid}_0mpu1.hk.gz",
        f"xti/hk/ni{obsid}_0mpu2.hk.gz",
        f"xti/hk/ni{obsid}_0mpu3.hk.gz",
        f"xti/hk/ni{obsid}_0mpu4.hk.gz",
        f"xti/hk/ni{obsid}_0mpu5.hk.gz",
        f"xti/hk/ni{obsid}_0mpu6.hk.gz",
        f"xti/event_cl/ni{obsid}_0mpu7_cl.evt.gz",
        f"xti/event_cl/ni{obsid}_0mpu7_ufa.evt.gz",
        f"xti/event_uf/ni{obsid}_0mpu0_uf.evt.gz",
        f"xti/event_uf/ni{obsid}_0mpu1_uf.evt.gz",
        f"xti/event_uf/ni{obsid}_0mpu2_uf.evt.gz",
        f"xti/event_uf/ni{obsid}_0mpu3_uf.evt.gz",
        f"xti/event_uf/ni{obsid}_0mpu4_uf.evt.gz",
        f"xti/event_uf/ni{obsid}_0mpu5_uf.evt.gz",
        f"xti/event_uf/ni{obsid}_0mpu6_uf.evt.gz",
        f"xti/products/ni{obsid}_lc.png",
        f"xti/products/ni{obsid}_pi.png",
        f"xti/products/ni{obsid}mpu7_load.xcm.gz",
        f"xti/products/ni{obsid}mpu7_sr.lc.gz",
        f"xti/products/ni{obsid}mpu7_sr.pha.gz",
        f"xti/products/ni{obsid}mpu7_bg.pha.gz",
        f"xti/products/ni{obsid}mpu7.arf.gz",
        f"xti/products/ni{obsid}mpu7.rmf.gz",
        f"xti/products/ni{obsid}mpu7_sk.arf.gz",
    ]

    return rel_paths


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="ni_base_output_{obsid}",
)
def ni_base_output_path(config):
    return os.path.join(config["out_data_path"])


@task
def ni_pipeline_output_path(config):
    return os.path.join(config["out_data_path"], "l2files")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="ni_pipeline_done_file_{obsid}",
)
def ni_pipeline_done_file(obsid, config):
    return os.path.join(ni_pipeline_output_path.fn(config), "PIPELINE_DONE.TXT")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nicerl2_{obsid}",
)
def ni_run_l2_pipeline(obsid, config, flags=None):

    logger = get_run_logger()
    if not HAS_HEASOFT:
        logger.error("heasoftpy not installed, cannot run NICER L2 pipeline.")
        raise ImportError("heasoftpy not installed")

    ev_dir = ni_pipeline_output_path.fn(config=config)
    os.makedirs(ev_dir, exist_ok=True)

    full_pipe_done_file_path = os.path.join(ev_dir, "PIPELINE_DONE.TXT")

    if os.path.exists(full_pipe_done_file_path):
        logger.info(
            f"Data for {obsid} already preprocessed. Done file found at: {full_pipe_done_file_path}"
        )
        return ev_dir

    nicerl2_hsp_task = hsp.HSPTask("nicerl2")
    logger.info("Running Nicer L2 pipeline for OBSID: %s", obsid)

    datadir = config["out_data_path"]

    os.makedirs(ev_dir, exist_ok=True)
    logger.info(f"Ensuring desired final output directory exists: {ev_dir}")

    params = {
        "indir": datadir,
        "cldir": ev_dir,
        "clobber": True,
        "chatter": 5,
        # "threshfilter": "ALL".
        "gzip": "yes",
    }
    if flags:
        params.update(flags)

    result = nicerl2_hsp_task(**params)
    print("return code:", result.returncode)
    if result.returncode != 0:
        logger.error(f"nicerl2 failed: {result.stderr}")
        raise RuntimeError("nicerl2 failed")
    open(full_pipe_done_file_path, "a").close()

    return ev_dir


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=90),
    task_run_name="ni_barycenter_{infile}",
)
def barycenter_file(infile: str, attorb: str, ra: float, dec: float):
    logger = get_run_logger()
    logger.info(f"Barycentering {infile}")

    outfile = infile.replace(".evt", "_bary.evt")
    logger.info(f"Output file: {outfile}")
    print("bu")
    hsp.barycorr(
        infile=infile,
        outfile=outfile,
        ra=ra,
        dec=dec,
        ephem="JPLEPH.430",
        refframe="ICRS",
        clobber="yes",
        orbitfiles=attorb,
    )

    return outfile


@flow(flow_run_name="ni_barycenter_{obsid}")
def barycenter_data(obsid: str, ra: float, dec: float, config: dict):
    logger = get_run_logger()
    outdir = ni_base_output_path.fn(config=config)
    logger.info(f"Barycentering NICER data in directory {outdir}")
    infile = os.path.join(outdir, "l2files", f"ni{obsid}_0mpu7_cl.evt")
    if not os.path.exists(infile):
        raise FileNotFoundError(f"Event file not found: {infile} delete the data and retry :)")

    orbit_file = os.path.join(outdir, "auxil", f"ni{obsid}.orb.gz")
    if not os.path.exists(orbit_file):
        raise FileNotFoundError(f"Orbit file not found: {orbit_file}")

    return barycenter_file(infile=infile, attorb=orbit_file, ra=float(ra), dec=float(dec))


@flow
def process_nicer_obsid(obsid: str, config: dict = None, ra="NONE", dec="NONE", flags=None):
    current_config = DEFAULT_CONFIG if config is None else config
    logger = get_run_logger()
    logger.info(f"Processing Nicer observation {obsid}")
    base_output_dir_for_obsid = ni_base_output_path.fn(config=current_config)
    os.makedirs(base_output_dir_for_obsid, exist_ok=True)
    logger.info(f"Ensured base output directory exists: {base_output_dir_for_obsid}")
    ni_run_l2_pipeline(obsid, config=current_config, flags=flags)
    barycenter_data(obsid, ra=ra, dec=dec, config=current_config)
    logger.info(f"Finished processing Nicer observation {obsid}")
