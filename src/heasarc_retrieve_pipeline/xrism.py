"""
XRISM/Resolve reduction for timing: the archive's cleaned events, merged, then barycentred.

Resolve is a 6x6 microcalorimeter array covering about 3 arcmin, with event times
assigned to 5 microseconds. It has no source-separation step to speak of: everything
inside the array is "the source", as for NICER. Xtend, the CCD camera, is not reduced:
its frames are seconds long, too slow for most pulsars.

The entry point is :func:`process_xrism_obsid`: :func:`reduce_observation` merges the
cleaned files, then :func:`barycentre_resolve` corrects them with the ``barycenter``
package or, as a fallback, HEASOFT ``barycen``.

The reduction does not rerun ``xapipeline``. The archive's ``resolve/event_cl`` files
are already calibrated, clock-corrected (``CLOCKAPP = T``) and screened, and are what this
module starts from.

See ``docs/technical_details.rst`` for more detail.
"""

import glob
import os

import numpy as np
from astropy.io import fits
from prefect import flow, task

from . import heasoft
from .barycenter import (
    PACKAGE_TOOL,
    barycenter_ephemeris,
    barycenter_tool,
    barycenter_with_package,
    barycentered_file_name,
)
from .utils import absolute_config, get_logger, merge_intervals

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")

#: What of an XRISM observation directory the reduction reads, matched on the whole
#: remote path so that the directory anchors hold.
#:
#: * ``auxil/xa<OBSID>.orb``: the orbit file, which barycentring reads.
#: * ``resolve/event_cl/xa<OBSID>rsl_*_cl.evt``: every cleaned Resolve event file. An
#:   observation can have several, one per filter-wheel position: 101002010 (the Crab,
#:   2024-10-03) has a 10.3 ks ``px1000`` file with ``FILTER = OPEN`` and a 16 s ``px0000``
#:   one with ``FILTER = UNDEF``, taken while the wheel moved, which fills a gap between
#:   the first file's good time intervals.
#:
#: Measured on 101002010: 62 MB of the observation's several hundred. The rest is
#: unfiltered events, housekeeping, the 33 MB attitude file and the Xtend data.
DOWNLOAD_RE = (
    r"(?:/auxil/xa[0-9]{9}\.orb(?:\.gz)?$"
    r"|/resolve/event_cl/xa[0-9]{9}rsl_[^/]*_cl\.evt(?:\.gz)?$)"
)


def xrism_download_filter(config):
    """
    What of an XRISM observation directory to download.

    Called through :func:`~heasarc_retrieve_pipeline.core.mission_download_filter`. There
    is one route, so the configuration is not read.

    Parameters
    ----------
    config : dict
        The run's configuration.

    Returns
    -------
    dict
        ``re_include`` for :func:`~heasarc_retrieve_pipeline.core.recursive_download`.

    Examples
    --------
    >>> sorted(xrism_download_filter({}))
    ['re_include']
    """
    return {"re_include": DOWNLOAD_RE}


def xrism_base_output_path(config, obsid):
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
        ``<out_data_path>/<OBSID>``, which is also where the raw data were downloaded.
    """
    return os.path.join(config["out_data_path"], obsid)


def _archive_files(root, *parts):
    """Files matching a glob under ``root``, sorted, without macOS ``._`` sidecars."""
    found = glob.glob(os.path.join(root, *parts))
    return sorted(f for f in found if not os.path.basename(f).startswith("._"))


def find_xrism_inputs(raw_data_dir):
    """
    The files of a downloaded observation that the reduction reads.

    Parameters
    ----------
    raw_data_dir : str
        The observation directory, with the archive's ``auxil`` and ``resolve``
        subdirectories.

    Returns
    -------
    dict
        ``event_files``, a sorted list of the cleaned Resolve event files (empty if there
        are none), and ``orbit_file``, a path or ``None``.
    """
    event_files = _archive_files(raw_data_dir, "resolve", "event_cl", "xa*rsl_*_cl.evt*")
    orbits = _archive_files(raw_data_dir, "auxil", "xa*.orb*")
    if len(orbits) > 1:
        get_logger().warning(f"Several orbit files in {raw_data_dir}; using {orbits[0]}")
    return {"event_files": event_files, "orbit_file": orbits[0] if orbits else None}


#: Resolve's event grades, by ``ITYPE``: high, medium and low resolution, primary or
#: secondary. The grade says how isolated in time an event was from its neighbours on the
#: same pixel, which sets how well its *energy* is measured; every grade carries a time.
ITYPE_NAMES = ("Hp", "Mp", "Ms", "Lp", "Ls")

#: Grades kept by default: all of them. Measured on the Crab (101002010, the
#: ``px1000`` file, folded on the Jodrell Bank ephemeris): Z^2_2 is 7954 with all five,
#: 6228 without Ls. Ls carries a pulse only a little weaker per event than Hp (Z^2_2/N =
#: 0.0073 against 0.0091), and is a quarter of the events.
DEFAULT_ITYPES = (0, 1, 2, 3, 4)

#: The calibration pixel, lit by its own 55Fe source and outside the sky aperture. The
#: archive's cleaned files already leave it out; it is excluded again in case one doesn't.
CALIBRATION_PIXEL = 12

#: Keywords that would be wrong in the merged file if copied from the first input.
STALE_KEYWORDS = ("CHECKSUM", "DATASUM")


def _intervals(gti_data):
    return np.column_stack([gti_data["START"], gti_data["STOP"]]).astype(float)


def _check_disjoint(gtis, event_files):
    """Raise if the good time intervals of two different files overlap."""
    tagged = sorted(
        (start, stop, i) for i, intervals in enumerate(gtis) for start, stop in intervals
    )
    for (_, stop, i), (start, _, j) in zip(tagged, tagged[1:]):
        if i != j and start < stop:
            raise ValueError(
                f"{os.path.basename(event_files[i])} and {os.path.basename(event_files[j])} "
                f"overlap in time (at {start}): merging them would count events twice."
            )


def resolve_screened_events(
    event_files, outfile, itypes=DEFAULT_ITYPES, exclude_pixels=(CALIBRATION_PIXEL,)
):
    """
    Merge an observation's cleaned Resolve event files into one, keeping some grades.

    The archive writes one cleaned file per filter-wheel position. They cover different
    times, so merging them is concatenation: events sorted by time, good time intervals
    joined. Files that overlap in time are refused rather than double counted.

    Parameters
    ----------
    event_files : list of str
        Cleaned event files, ``xa<OBSID>rsl_*_cl.evt``.
    outfile : str
        Output file; overwritten if it exists.
    itypes : iterable of int, optional
        Grades to keep (see :data:`ITYPE_NAMES`).
    exclude_pixels : iterable of int, optional
        Pixels to drop.

    Returns
    -------
    str or None
        ``outfile``, or ``None`` if no event survived.

    Raises
    ------
    ValueError
        If the files' good time intervals overlap, or their columns differ.

    Notes
    -----
    ``EXPOSURE`` is the sum of the inputs' ``EXPOSURE``, and ``ONTIME`` the length of the
    joined intervals; ``FILTER`` becomes a comma-separated list when the inputs disagree.
    The header otherwise comes from the first file, which is fine for every time keyword:
    all files of an observation share ``MJDREF`` and ``TIMESYS``.
    """
    logger = get_logger()
    itypes = list(itypes)
    exclude_pixels = list(exclude_pixels)

    parts, gtis, filters, exposure = [], [], [], 0.0
    for i, path in enumerate(event_files):
        with fits.open(path) as hdul:
            events = hdul["EVENTS"]
            if i == 0:
                primary_header = hdul[0].header.copy()
                header = events.header.copy()
                columns = events.columns
                gti_header = hdul["GTI"].header.copy()
            elif events.columns.names != columns.names:
                raise ValueError(f"{path} has different columns from {event_files[0]}")
            data = events.data
            keep = np.isin(data["ITYPE"], itypes) & ~np.isin(data["PIXEL"], exclude_pixels)
            parts.append(data[keep].copy())
            gtis.append(_intervals(hdul["GTI"].data))
            filters.append(str(events.header.get("FILTER", "UNKNOWN")).strip())
            exposure += float(events.header.get("EXPOSURE", 0.0))
        logger.info(
            f"{os.path.basename(path)}: FILTER={filters[-1]}, kept {keep.sum()} of {len(keep)}"
        )

    _check_disjoint(gtis, event_files)
    total = sum(len(part) for part in parts)
    if total == 0:
        return None

    merged = fits.BinTableHDU.from_columns(columns, nrows=total, header=header)
    start = 0
    for part in parts:
        for name in columns.names:
            merged.data[name][start : start + len(part)] = part[name]
        start += len(part)
    order = np.argsort(merged.data["TIME"], kind="stable")
    for name in columns.names:
        merged.data[name][:] = merged.data[name][order]

    union = merge_intervals(np.concatenate(gtis))
    tstart, tstop = float(union[0, 0]), float(union[-1, 1])
    ontime = float((union[:, 1] - union[:, 0]).sum())
    gti = fits.BinTableHDU.from_columns(
        [
            fits.Column("START", "D", unit="s", array=union[:, 0]),
            fits.Column("STOP", "D", unit="s", array=union[:, 1]),
        ],
        header=gti_header,
    )
    for hdu_header in (merged.header, gti.header, primary_header):
        for keyword in STALE_KEYWORDS:
            hdu_header.remove(keyword, ignore_missing=True)
        hdu_header["TSTART"] = tstart
        hdu_header["TSTOP"] = tstop
    merged.header["EXPOSURE"] = exposure
    merged.header["ONTIME"] = ontime
    merged.header["FILTER"] = ",".join(dict.fromkeys(filters))
    merged.header["ITYPES"] = (",".join(str(t) for t in itypes), "Resolve grades kept")
    for path, filt in zip(event_files, filters):
        merged.header["HISTORY"] = f"Merged {os.path.basename(path)} (FILTER={filt})"
    merged.header["HISTORY"] = "Merged by heasarc_retrieve_pipeline.xrism"

    fits.HDUList([fits.PrimaryHDU(header=primary_header), merged, gti]).writeto(
        outfile, overwrite=True
    )
    return outfile


#: ``barycen``'s reference frame, and with it the ephemeris: ``FK5`` is DE200 and ``ICRS``
#: is DE440, and there is no other choice. Both were checked against the ``barycenter``
#: package to 89 ns, but ``barycen`` writes ``PLEPHEM = 'JPL-DE200'`` whichever it used, so
#: only FK5 leaves a header that tells the truth. ``refframe`` also says what frame ``ra``
#: and ``dec`` are in, and DE200 timing models such as Jodrell Bank's give FK5 positions.
BARYCEN_REFFRAME = "FK5"

#: The ephemeris :data:`BARYCEN_REFFRAME` implies.
BARYCEN_EPHEMERIS = "DE200"


def _barycen(infile, orbit_file, outfile, ra, dec, config):
    """Run HEASOFT ``barycen`` on DE200, warning if the run asked for another ephemeris."""
    asked = barycenter_ephemeris(config)
    if "barycenter_ephemeris" in config and asked.strip().upper() != BARYCEN_EPHEMERIS:
        get_logger().warning(
            f"barycen is run on {BARYCEN_EPHEMERIS}, not {asked}: it cannot read a kernel, "
            "and its DE440 mode mislabels its output. Use barycenter_tool: barycenter."
        )
    heasoft.run(
        "barycen",
        produces=outfile,
        infile=infile,
        outfile=outfile,
        orbfile=orbit_file,
        ra=float(ra),
        dec=float(dec),
        refframe=BARYCEN_REFFRAME,
        clobber="yes",
        chatter=2,
    )
    return BARYCEN_EPHEMERIS


def barycentre_resolve(infile, orbit_file, ra, dec, outfile=None, config=None):
    """
    Barycentre a Resolve event file.

    The ``barycenter`` package does it by default, on ``barycenter_ephemeris`` (DE430
    unless configured otherwise), like every other mission. With ``barycenter_tool:
    official`` HEASOFT ``barycen`` does it instead, always on DE200: see
    :data:`BARYCEN_REFFRAME` for why. Events and good time intervals are both corrected,
    and ``TIMESYS`` becomes ``TDB``.

    Parameters
    ----------
    infile : str
        Event file, on spacecraft time.
    orbit_file : str
        The observation's ``xa<OBSID>.orb``.
    ra, dec : float
        The position to barycentre at: the *searched* one, never a header or detected
        one, since an error here goes straight into the arrival times.
    outfile : str, optional
        Defaults to :func:`~heasarc_retrieve_pipeline.barycenter.barycentered_file_name`.
    config : dict, optional
        Read for ``barycenter_tool`` and ``barycenter_ephemeris``.

    Returns
    -------
    str
        ``outfile``.

    Raises
    ------
    RuntimeError
        If the tool fails, or reports success and leaves the file on spacecraft time.
    """
    config = config or {}
    if outfile is None:
        outfile = barycentered_file_name(infile)
    tool = barycenter_tool(config)
    if tool == PACKAGE_TOOL:
        ephemeris = barycenter_ephemeris(config)
        barycenter_with_package(
            infile, orbit_file, outfile, ra=float(ra), dec=float(dec), ephem=ephemeris
        )
    else:
        ephemeris = _barycen(infile, orbit_file, outfile, ra, dec, config)
    timesys = fits.getheader(outfile, "EVENTS").get("TIMESYS")
    if timesys != "TDB":
        raise RuntimeError(f"{tool} returned success but left {outfile} on {timesys}")
    get_logger().info(f"Barycentred {os.path.basename(outfile)} with {tool} on {ephemeris}")
    return outfile


@task(name="reduce_xrism_obsid")
def reduce_observation(raw_data_dir: str, obsid: str, config: dict):
    """
    Merge one observation's cleaned Resolve files into ``l2_files/<OBSID>_rsl_cl.evt``.

    Parameters
    ----------
    raw_data_dir : str
        The downloaded observation directory.
    obsid : str
        Observation identifier.
    config : dict
        ``resolve_itypes`` chooses the grades (default :data:`DEFAULT_ITYPES`).

    Returns
    -------
    str or None
        The merged file, or ``None`` if there are no cleaned Resolve files or nothing in
        them survives.
    """
    logger = get_logger()
    event_files = find_xrism_inputs(raw_data_dir)["event_files"]
    if not event_files:
        logger.warning(f"{obsid}: no cleaned Resolve event file")
        return None

    l2_dir = os.path.join(raw_data_dir, "l2_files")
    os.makedirs(l2_dir, exist_ok=True)
    outfile = os.path.join(l2_dir, f"{obsid}_rsl_cl.evt")
    written = resolve_screened_events(
        event_files, outfile, itypes=config.get("resolve_itypes", DEFAULT_ITYPES)
    )
    if written is None:
        logger.warning(f"{obsid}: no Resolve event survived screening")
    return written


@flow
def process_xrism_obsid(obsid: str, config=None, flags=None, ra: float = None, dec: float = None):
    """
    Reduce one XRISM/Resolve observation for timing: merge the cleaned files, barycentre.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict, optional
        Pipeline configuration; ``None`` means :data:`DEFAULT_CONFIG`.
    flags : dict, optional
        Accepted for signature compatibility with the other missions, and ignored.
    ra, dec : float, optional
        The position to barycentre at, the searched one. Without it, or without an orbit
        file, the reduction stops at the merged file.

    Returns
    -------
    str or None
        The barycentred event file, the merged one if it could not be barycentred, or
        ``None`` if there was nothing to reduce.
    """
    current_config = absolute_config(config, DEFAULT_CONFIG)
    logger = get_logger()
    logger.info(f"Processing XRISM observation {obsid}")
    raw_data_dir = xrism_base_output_path(config=current_config, obsid=obsid)
    os.makedirs(raw_data_dir, exist_ok=True)

    merged = reduce_observation(raw_data_dir, obsid, current_config)
    if merged is None:
        return None

    orbit_file = find_xrism_inputs(raw_data_dir)["orbit_file"]
    if ra is None or dec is None or orbit_file is None:
        logger.warning(
            f"{obsid}: not barycentred -- "
            + ("no orbit file" if orbit_file is None else "no position given")
        )
        return merged

    barycentred = barycentre_resolve(merged, orbit_file, ra, dec, config=current_config)
    logger.info(f"XRISM processing complete: {barycentred}")
    return barycentred
