"""
Barycentric correction of event arrival times, shared by the mission modules.
"""

import os
from prefect import task, get_run_logger

from .utils import splitext_improved

try:
    import heasoftpy as hsp

    HAS_HEASOFT = True
except ImportError:
    HAS_HEASOFT = False


def barycentered_file_name(infile):
    """
    Name of the barycentred version of an event file.

    Inserts ``_bary`` before the extension, whatever the extension is, and keeps any
    compression suffix last. Missions do not agree on what to call an event file --
    ``.evt``, ``.fits``, ``.ds``, ``evt2.fits`` -- and the naive
    ``infile.replace(".evt", "_bary.evt")`` this replaces does nothing at all to a name
    with no ``.evt`` in it, handing back an output name equal to the input.

    Parameters
    ----------
    infile : str
        Event file path.

    Returns
    -------
    str
        The barycentred file name, in the same directory.

    Examples
    --------
    >>> barycentered_file_name("nu123A01_cl.evt")
    'nu123A01_cl_bary.evt'
    >>> barycentered_file_name("nu123A01_cl.evt.gz")
    'nu123A01_cl_bary.evt.gz'
    >>> barycentered_file_name("P0123_events.ds")
    'P0123_events_bary.ds'
    """
    root, ext = splitext_improved(infile)
    return root + "_bary" + ext

@task(
    task_run_name="barycenter_{infile}_ra{ra}_dec{dec}_to_{outfile}_overwrite_{overwrite}",
)
def barycenter_file(infile, attorb, ra=None, dec=None, overwrite=False, outfile=None):
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
        Orbit, or Attitude/orbit, file, as produced by the relevant mission pipeline.
    ra, dec : float, optional
        Source position in degrees. Accuracy here directly sets the timing accuracy.
    overwrite : bool, optional
        If True, overwrite existing output file. If False, do not overwrite.
    outfile : str, optional
        Output file name. If None, :func:`barycentered_file_name` builds it.

    Returns
    -------
    str
        Path of the barycentered file.

    Raises
    ------
    ImportError
        If ``heasoftpy`` is not available.
    FileNotFoundError
        If ``barycorr`` returned without creating the output file.

    """
    logger = get_run_logger()
    logger.info(f"Barycentering {infile}")
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy is required for barycenter correction but is not installed.")
    if outfile is None:
        outfile = barycentered_file_name(infile)
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
        chatter=5,
    )
    if not os.path.exists(outfile):
        raise FileNotFoundError(f"Barycentered output file not created: {outfile}")

    return outfile
