"""
NICER reduction: the ``nicerl2`` Level-2 pipeline, then barycentring.

NICER is a collimated instrument with a field of view of roughly 3 arcmin and no
imaging capability, so there is no equivalent of the NuSTAR source-separation step:
everything inside the field of view ends up in the same event file.

The entry point is :func:`process_nicer_obsid`, which runs :func:`ni_run_l2_pipeline`
and then :func:`barycenter_data`.

``nicerl2`` performs the standard NICER screening -- calibration, merging of the seven
Measurement/Power Units, and good-time selection on orbital day/night, undershoot and
overshoot rates and pointing offset -- and produces the single cleaned file
``ni<OBSID>_0mpu7_cl.evt`` ("0mpu7" meaning all seven MPUs, i.e. all 52 active
detectors combined).

No spectral extraction is performed. NICER's background cannot be measured from the
data themselves, since there is no off-source region; it requires one of the community
background models, none of which is invoked here.

See ``docs/technical_details.rst`` for more detail.
"""

import re

from astropy.time import Time

import glob

from datetime import timedelta

from prefect import flow, task, get_run_logger

from prefect.tasks import task_input_hash
import subprocess
import os

from .barycenter import barycenter_file

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
    task_run_name="ni_base_output_{obsid}",
)
def ni_base_output_path(config, obsid):
    """
    Top-level output directory of an observation.

    Parameters
    ----------
    config : dict
        Must contain ``out_data_path``.
    obsid : str
        Observation identifier.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>``. This is also where the downloaded raw data live, and
        what is passed to ``nicerl2`` as ``indir``.
    """
    return os.path.join(config["out_data_path"], obsid)


@task
def ni_pipeline_output_path(config, obsid):
    """
    Directory for the ``nicerl2`` output of an observation.

    Parameters
    ----------
    config : dict
        Must contain ``out_data_path``.
    obsid : str
        Observation identifier.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/l2files/``.
    """
    return os.path.join(config["out_data_path"], obsid + "/l2files/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="ni_pipeline_done_file_{obsid}",
)
def ni_pipeline_done_file(obsid, config):
    """
    Path of the sentinel file marking a finished ``nicerl2`` run.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/l2files/PIPELINE_DONE.TXT``.
    """
    return os.path.join(ni_pipeline_output_path.fn(obsid, config), "PIPELINE_DONE.TXT")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nicerl2_{obsid}",
)
def ni_run_l2_pipeline(obsid, config, flags=None):

    """
    Run the ``nicerl2`` Level-2 pipeline on one observation.

    ``nicerl2`` is invoked as an external command through ``subprocess`` rather than
    through ``heasoftpy``, with stdout and stderr captured to ``nicerl2_process_<OBSID>.log``
    and ``.err`` in the output directory. ``heasoftpy`` is still required to be importable,
    as a proxy for "HEASOFT is set up at all".

    Returns immediately if the ``PIPELINE_DONE.TXT`` sentinel already exists.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    flags : dict, optional
        Extra ``nicerl2`` parameters, merged over the defaults (``clobber``, ``chatter``).
        This is how non-standard screening -- a relaxed undershoot or overshoot cut, for
        instance -- is requested.

    Returns
    -------
    str
        The ``l2files`` directory.

    Raises
    ------
    ImportError
        If ``heasoftpy`` is not available.
    FileNotFoundError
        If the ``nicerl2`` command is not on ``PATH``.
    RuntimeError
        If ``nicerl2`` exits with a non-zero return code.
    """
    logger = get_run_logger()
    if not HAS_HEASOFT:
        logger.error("heasoftpy not installed, cannot run NICER L2 pipeline.")
        raise ImportError("heasoftpy not installed")

    ev_dir = ni_pipeline_output_path.fn(config=config, obsid=obsid)
    os.makedirs(ev_dir, exist_ok=True)

    full_pipe_done_file_path = os.path.join(ev_dir, "PIPELINE_DONE.TXT")

    if os.path.exists(full_pipe_done_file_path):
        logger.info(
            f"Data for {obsid} already preprocessed. Done file found at: {full_pipe_done_file_path}"
        )
        return ev_dir

    nicerl2_hsp_task = hsp.HSPTask("nicerl2")
    logger.info("Running Nicer L2 pipeline for OBSID: %s", obsid)

    datadir = ni_base_output_path.fn(config=config, obsid=obsid)
    os.makedirs(ev_dir, exist_ok=True)
    logger.info(f"Ensuring desired final output directory exists: {ev_dir}")

    params = {
        "indir": datadir,
        "cldir": ev_dir,
        "clobber": True,
        "chatter": 5,
    }
    if flags:
        params.update(flags)
    command = ["nicerl2"]
    for key, value in params.items():
        command.append(f"{key}={value}")

    log_file_path = os.path.join(ev_dir, f"nicerl2_process_{obsid}.log")
    error_log_path = os.path.join(ev_dir, f"nicerl2_process_{obsid}.err")

    logger.info(f"Executing command: {' '.join(command)}")
    try:
        with open(log_file_path, "w") as log_f, open(error_log_path, "w") as err_f:
            result = subprocess.run(command, stdout=log_f, stderr=err_f, check=False)
        if result.returncode != 0:
            with open(error_log_path, "r") as f:
                error_output = f.read()
            logger.error(f"nicerl2 FAILED! Error:\n{error_output}")
            raise RuntimeError(f"nicerl2 failed. See log: {error_log_path}")
    except FileNotFoundError:
        logger.error("FATAL: 'nicerl2' command not found.")
        raise

    logger.info(f"nicerl2 completed successfully for OBSID: {obsid}")
    with open(full_pipe_done_file_path, "w") as f:
        f.write(f"Completed on {Time.now().iso}")

    return ev_dir


@flow(flow_run_name="ni_barycenter_{obsid}")
def barycenter_data(obsid: str, ra: float, dec: float, config: dict):
    """
    Barycentre the cleaned event file of a NICER observation.

    Uses the shared implementation in :mod:`heasarc_retrieve_pipeline.barycenter`, with the
    orbit file from the observation's ``auxil`` directory. See
    :func:`heasarc_retrieve_pipeline.barycenter.barycenter_file` for what barycentring does
    and why the source position matters.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    ra, dec : float
        Source position in degrees.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        Path of the barycentred event file.

    Raises
    ------
    FileNotFoundError
        If the cleaned event file or the orbit file is missing.

    Notes
    -----
    Both file names are hardcoded, uncompressed for the event file and gzipped for the
    orbit file. A ``nicerl2`` run that leaves its output compressed differently will not be
    found.
    """
    logger = get_run_logger()
    outdir = ni_base_output_path.fn(config=config, obsid=obsid)
    logger.info(f"Barycentering NICER data in directory {outdir}")
    infile = os.path.join(outdir, "l2files", f"ni{obsid}_0mpu7_cl.evt")
    if not os.path.exists(infile):
        raise FileNotFoundError(f"Event file not found: {infile} delete the data and retry :)")

    orbit_file = os.path.join(outdir, "auxil", f"ni{obsid}.orb.gz")
    if not os.path.exists(orbit_file):
        raise FileNotFoundError(f"Orbit file not found: {orbit_file}")

    return barycenter_file(
        infile=infile,
        attorb=orbit_file,
        ra=float(ra),
        dec=float(dec),
    )


@flow
def process_nicer_obsid(obsid: str, config={}, ra="NONE", dec="NONE", flags=None):
    """
    Reduce one NICER observation end to end: ``nicerl2``, then barycentring.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict, optional
        Pipeline configuration. Must contain ``out_data_path``; note that the default is
        an empty dict rather than ``None``, so the fallback to ``DEFAULT_CONFIG`` never
        fires and calling this without a config raises ``KeyError`` (issue 27 in
        ``docs/known_issues.rst``).
    ra, dec : float or str, optional
        Source position in degrees, used for barycentring.
    flags : dict, optional
        Extra ``nicerl2`` parameters.
    """
    current_config = DEFAULT_CONFIG if config is None else config
    logger = get_run_logger()
    logger.info(f"Processing Nicer observation {obsid}")
    base_output_dir_for_obsid = ni_base_output_path.fn(config=current_config, obsid=obsid)
    os.makedirs(base_output_dir_for_obsid, exist_ok=True)
    logger.info(f"Ensured base output directory exists: {base_output_dir_for_obsid}")
    ni_run_l2_pipeline(obsid, config=current_config, flags=flags)
    barycenter_data(obsid, ra=ra, dec=dec, config=current_config)
    logger.info(f"Finished processing Nicer observation {obsid}")
