"""
Barycentric correction of event arrival times, shared by the mission modules.
"""

import os
from datetime import timedelta
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash

try:
    import heasoftpy as hsp

    HAS_HEASOFT = True
except ImportError:
    HAS_HEASOFT = False


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=90),
    task_run_name="barycenter_{infile}",
)
def barycenter_file(infile: str, attorb: str, ra, dec):
    """
    Barycentre one event file with HEASOFT ``barycorr``.

    Converts photon arrival times from the spacecraft frame to the solar system barycentre,
    removing the up to ~500 s light-travel-time modulation caused by the Earth's and the
    satellite's motion around the Sun. This is a prerequisite for any coherent timing
    analysis -- pulsar timing, orbital searches -- and it is **position-dependent**: an
    error in the assumed source position translates directly into a timing error, of order
    the position error in radians times 500 s.

    Uses the JPL DE430 ephemeris in the ICRS reference frame.

    Parameters
    ----------
    infile : str
        Event file to barycentre.
    attorb : str
        Attitude/orbit file for the observation.
    ra, dec : float
        Source position in degrees. Its accuracy sets the timing accuracy.

    Returns
    -------
    str
        Path of the barycentred file, ``<infile with .evt -> _bary.evt>``.

    Raises
    ------
    ImportError
        If ``heasoftpy`` is not available.
    FileNotFoundError
        If ``barycorr`` returned without creating the output file.
    """
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy is required for barycenter correction but is not installed.")

    logger = get_run_logger()
    logger.info(f"Barycentering {infile}")

    outfile = infile.replace(".evt", "_bary.evt")
    logger.info(f"Output file: {outfile}")

    hsp.barycorr(
        infile=infile,
        outfile=outfile,
        ra=ra,
        dec=dec,
        ephem="JPLEPH.430",
        refframe="ICRS",
        clobber="yes",
        orbitfiles=attorb,
        chatter=5,
    )

    if not os.path.exists(outfile):
        raise FileNotFoundError(f"Barycentered output file not created: {outfile}")

    return outfile