"""
Barycentric correction of event arrival times, shared by the mission modules.

Two tools can do it. The default is the ``barycenter`` package, which handles every
mission in this pipeline with one code path and any JPL ephemeris, given by name
(``"DE430"``) or as the path to a local ``.bsp`` kernel. The fallback is each mission's
own tool -- HEASOFT ``barycorr``, SAS ``barycen``, CIAO ``axbary`` -- chosen with
``barycenter_tool: official`` in the configuration, and used automatically, with a
warning, if the package cannot be imported.
"""

import os
import re

from prefect import task

from .utils import get_logger, splitext_improved

from . import heasoft
from .heasoft import HAS_HEASOFT

try:
    # Absolute import: this module shares the package's name, and is not it.
    import barycenter as _barycenter_package  # noqa: F401

    HAS_BARYCENTER = True
except ImportError:
    HAS_BARYCENTER = False

#: The ``barycenter`` package, one code path for every mission.
PACKAGE_TOOL = "barycenter"

#: The mission's own tool: ``barycorr``, ``barycen`` or ``axbary``.
OFFICIAL_TOOL = "official"

#: Accepted values of the ``barycenter_tool`` configuration key.
BARYCENTER_TOOLS = (PACKAGE_TOOL, OFFICIAL_TOOL)

#: JPL ephemeris used everywhere unless ``barycenter_ephemeris`` says otherwise. Every
#: mission on one ephemeris, so that their times can be compared with each other.
DEFAULT_EPHEMERIS = "DE430"


def barycenter_tool(config=None):
    """
    Which tool barycenters, from the ``barycenter_tool`` configuration key.

    Defaults to :data:`PACKAGE_TOOL`. When the package is asked for but not installed,
    falls back to :data:`OFFICIAL_TOOL` with a warning rather than failing a reduction
    that the mission's own tool could still finish.

    Parameters
    ----------
    config : dict, optional
        Pipeline configuration.

    Returns
    -------
    str
        One of :data:`BARYCENTER_TOOLS`.

    Raises
    ------
    ValueError
        If the configuration names a tool that is not in :data:`BARYCENTER_TOOLS`.

    Examples
    --------
    >>> barycenter_tool({"barycenter_tool": "official"})
    'official'
    """
    requested = (config or {}).get("barycenter_tool", PACKAGE_TOOL)
    if requested not in BARYCENTER_TOOLS:
        raise ValueError(f"barycenter_tool must be one of {BARYCENTER_TOOLS}, not {requested!r}.")
    if requested == PACKAGE_TOOL and not HAS_BARYCENTER:
        get_logger().warning(
            "The barycenter package cannot be imported: falling back to the mission's own "
            "barycentering tool."
        )
        return OFFICIAL_TOOL
    return requested


def barycenter_ephemeris(config=None):
    """
    The JPL ephemeris to barycenter with, from the ``barycenter_ephemeris`` key.

    A name such as ``"DE430"``, or a path to a ``.bsp`` kernel, which the ``barycenter``
    package reads without touching the network. Defaults to :data:`DEFAULT_EPHEMERIS`.

    Examples
    --------
    >>> barycenter_ephemeris()
    'DE430'
    >>> barycenter_ephemeris({"barycenter_ephemeris": "/data/de440.bsp"})
    '/data/de440.bsp'
    """
    return (config or {}).get("barycenter_ephemeris", DEFAULT_EPHEMERIS)


def official_ephemeris_number(ephem):
    """
    The number in a ``DEnnn`` ephemeris name, the only form the mission tools accept.

    ``barycorr`` reads ``JPLEPH.<nnn>`` and ``barycen`` ``DE<nnn>``, and each only from
    the kernels its installation ships; neither can open a ``.bsp`` file.

    Raises
    ------
    ValueError
        If ``ephem`` is not a ``DEnnn`` name.

    Examples
    --------
    >>> official_ephemeris_number("DE430")
    '430'
    >>> official_ephemeris_number("de200")
    '200'
    """
    match = re.fullmatch(r"de(\d{3})", str(ephem).strip().lower())
    if match is None:
        raise ValueError(
            f"The mission's own barycentering tools only know the ephemerides they ship, "
            f"by DEnnn name; {ephem!r} needs barycenter_tool: barycenter."
        )
    return match.group(1)


def barycenter_with_package(
    infile, orbit, outfile, ra=None, dec=None, ephem=DEFAULT_EPHEMERIS, clockfile=None
):
    """
    Barycenter one event file with the ``barycenter`` package.

    Writes a new file and leaves the input alone. Events, GTI boundaries and the time
    keywords are all corrected, and the header is set to ``TIMESYS = TDB``.

    Parameters
    ----------
    infile : str
        Event file, on spacecraft time.
    orbit : str
        The mission's orbit file. The package recognises the mission from ``TELESCOP``.
    outfile : str
        Output file; overwritten if it exists.
    ra, dec : float, optional
        Source position in degrees. ``None`` reads the target out of the header, which
        is the pointing and not necessarily the source.
    ephem : str, optional
        JPL ephemeris name or ``.bsp`` path.
    clockfile : str, optional
        Spacecraft clock file. ``None`` lets the package find the mission's own (for
        NuSTAR it fetches the current one from the CALDB); ``"none"`` disables it.

    Returns
    -------
    str
        ``outfile``.
    """
    from barycenter import apply_barycenter_correction

    apply_barycenter_correction(
        infile,
        orbit,
        outfile=outfile,
        ra=ra,
        dec=dec,
        ephem=ephem,
        clockfile=clockfile,
        overwrite=True,
    )
    if not os.path.exists(outfile):
        raise FileNotFoundError(f"Barycentered output file not created: {outfile}")
    return outfile


def barycentered_file_name(infile):
    """
    Name of the barycentered version of an event file.

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
        The barycentered file name, in the same directory.

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
def barycenter_file(
    infile,
    attorb,
    ra=None,
    dec=None,
    overwrite=False,
    outfile=None,
    tool=None,
    ephem=DEFAULT_EPHEMERIS,
):
    """
    Barycenter one event file, with the ``barycenter`` package or HEASOFT ``barycorr``.

    Converts photon arrival times from the spacecraft frame to the solar system
    barycenter, removing the up to ~500 s light-travel-time modulation caused by the
    Earth's and the satellite's motion. This is a prerequisite for any coherent timing
    analysis, and it is **position-dependent**: an error in the assumed RA/Dec translates
    directly into a timing error.

    Uses the JPL DE430 ephemeris in the ICRS frame unless told otherwise. The fallback,
    ``barycorr``, serves the missions this function is called for -- NuSTAR, NICER and
    RXTE -- and not XMM or Chandra, which have wrappers of their own.

    Parameters
    ----------
    infile : str
        Event file to barycenter.
    attorb : str
        Orbit, or Attitude/orbit, file, as produced by the relevant mission pipeline.
    ra, dec : float, optional
        Source position in degrees. Accuracy here directly sets the timing accuracy.
    overwrite : bool, optional
        If True, overwrite existing output file. If False, do not overwrite.
    outfile : str, optional
        Output file name. If None, :func:`barycentered_file_name` builds it.
    tool : str, optional
        One of :data:`BARYCENTER_TOOLS`. ``None`` means :func:`barycenter_tool`'s default.
    ephem : str, optional
        JPL ephemeris: a ``DEnnn`` name, or for the package also a ``.bsp`` path.

    Returns
    -------
    str
        Path of the barycentered file.

    Raises
    ------
    ImportError
        If ``barycorr`` is to be used and ``heasoftpy`` is not available.
    FileNotFoundError
        If ``barycorr`` returned without creating the output file.

    """
    logger = get_logger()
    logger.info(f"Barycentering {infile}")
    if outfile is None:
        outfile = barycentered_file_name(infile)
    logger.info(f"Output file: {outfile}")

    # Before the HEASOFT check on purpose: a product that is already there is already
    # there, and re-walking a finished reduction should not need the tools that made it.
    if os.path.exists(outfile) and not overwrite:
        logger.info(f"Output file {outfile} already exists, skipping")
        return outfile

    if tool is None:
        tool = barycenter_tool()
    if tool == PACKAGE_TOOL:
        return barycenter_with_package(infile, attorb, outfile, ra=ra, dec=dec, ephem=ephem)

    if not HAS_HEASOFT:
        raise ImportError("heasoftpy is required for barycenter correction but is not installed.")

    heasoft.run(
        "barycorr",
        produces=outfile,
        infile=infile,
        outfile=outfile,
        ra=ra,
        dec=dec,
        ephem=f"JPLEPH.{official_ephemeris_number(ephem)}",
        refframe="ICRS",
        clobber="yes",
        orbitfiles=attorb,
        chatter=5,
    )
    if not os.path.exists(outfile):
        raise FileNotFoundError(f"Barycentered output file not created: {outfile}")

    return outfile
