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
    task_run_name="barycenter_{infile}_ra{ra}_dec{dec}_to_{outfile}_{overwrite}",
)
def barycenter_file(infile, attorb, ra=None, dec=None, overwrite=1, outfile=None):
    """
    Barycentre one event file with HEASOFT ``barycorr``.

    Converts photon arrival times from the spacecraft frame to the solar system
    barycentre, removing the up to ~500 s light-travel-time modulation caused by the
    Earth's and the satellite's motion. This is a prerequisite for any coherent timing
    analysis, and it is **position-dependent**: an error in the assumed RA/Dec translates
    directly into a timing error.

    Uses the JPL DE430 ephemeris in the ICRS frame.

    Parameters
    ----------
    infile : str
        Event file to barycentre.
    attorb : str
        Attitude/orbit file, as produced by ``nupipeline`` (``nu<OBSID><FPM>.attorb``).
    ra, dec : float, optional
        Source position in degrees. Accuracy here directly sets the timing accuracy.
    overwrite : int, optional
        If 1, overwrite existing output file. If 0, do not overwrite.
    outfile : str, optional
        Output file name. If None, a default name is used.

    Returns
    -------
    str
        Path of the barycentered file.

    Notes
    -----
    :mod:`heasarc_retrieve_pipeline.barycenter` holds a second implementation, which NICER
    uses. It is not interchangeable with this one -- it is a plain function rather than a
    Prefect task and takes no ``src`` -- but it does two things this one does not: it fails
    with a clear message when heasoftpy is missing, and it checks that ``barycorr``
    actually wrote the output. See issue 14 in ``docs/known_issues.rst``.
    """
    logger = get_run_logger()
    logger.info(f"Barycentering {infile}")

    if outfile is None:
        outfile = infile.replace(".evt", "_bary.evt")
    logger.info(f"Output file: {outfile}")

    if os.path.exists(outfile) and not overwrite:
        logger.info(f"Output file {outfile} already exists, skipping")
        return outfile

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
    if not os.path.exists(outfile):
        raise FileNotFoundError(f"Barycentered output file not created: {outfile}")

    return outfile
