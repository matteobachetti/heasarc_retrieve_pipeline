"""
What the ECLAIRs and GRM reductions share: finding an ObsID's files, the geometric cuts
of :mod:`~heasarc_retrieve_pipeline.svom_geometry` put together, their diagnostics, and
barycentring.
"""

import glob
import os
from collections import defaultdict

import numpy as np
from astropy.io import fits
from astropy.table import Table, vstack

from . import svom_geometry as geo
from .barycenter import (
    OFFICIAL_TOOL,
    barycenter_ephemeris,
    barycenter_tool,
    barycenter_with_package,
    barycentered_file_name,
)
from .diagnostics import diagnostics_path, record_step
from .utils import get_logger, intersect_intervals, mask_from_gti, update_time_bounds

#: Orbit and attitude extensions.
ORBIT_EXTNAME = "SVO-ORB-CNV"
ATTITUDE_EXTNAME = "SVO-ATT-CNV"

#: The SAA table for each value of the ``*_saa`` configuration keys.
SAA_EXTNAMES = {"deep": "SAA-DEEP-ECL", "ext": "SAA-EXT-ECL"}

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")

#: Spacing of the off-axis angle kept for the report's figure. The attitude is sampled
#: every second; a day of it at full rate would make a page of megabytes.
OFFAXIS_FIGURE_STEP_S = 10.0

#: Header keywords of an input file that do not describe a joined output, and are dropped.
COMMON_STALE_KEYWORDS = ("BURST_ID", "CHECKSUM", "DATASUM", "DATE-BEG", "DATE-END")

#: Keywords copied from the events into the GTI extension.
GTI_KEYWORDS = (
    "TELESCOP",
    "INSTRUME",
    "DETNAM",
    "TIMESYS",
    "TIMEREF",
    "TIMEUNIT",
    "MJDREFI",
    "MJDREFF",
    "OBS_ID",
    "PASS_ID",
)


def is_data_file(path):
    """A FITS file, not one of the ``._*`` files macOS leaves on exFAT disks."""
    name = os.path.basename(path)
    return not name.startswith("._") and name.endswith((".fits", ".fits.gz"))


def find_inputs(raw_data_dir):
    """
    The observation's files, by the name of their first extension.

    E.g. ``ECL-EVT-CAL``, ``ECL-GTI-CAL-GRP``, ``EBOUNDS`` (GRM events), ``SVO-ORB-CNV``,
    ``SVO-ATT-CNV``, ``SAA-DEEP-ECL``, ``SAA-EXT-ECL``; each a sorted list, empty if there
    are none.
    """
    found = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(raw_data_dir, "*"))):
        if os.path.isfile(path) and is_data_file(path):
            found[fits.getheader(path, 1).get("EXTNAME", "").strip()].append(path)
    return found


def total_time(gti):
    """The total length of a list of intervals."""
    gti = np.asarray(gti, dtype=float).reshape(-1, 2)
    return float(np.sum(gti[:, 1] - gti[:, 0]))


def geometry_cuts(inputs, source=None, axes=("POINTING",), max_offaxis_deg=None, saa=None):
    """
    The geometric cuts of an observation for each instrument axis, in the order applied.

    ``orbit``, where the orbit is measured; ``orbit_jumps``, without its position jumps
    (:func:`~heasarc_retrieve_pipeline.svom_geometry.orbit_jump_gti`); with a ``source``,
    ``visible`` (above the Earth's limb), ``attitude`` (where the attitude is known) and
    ``offaxis`` (within ``max_offaxis_deg`` of the axis); with ``saa`` (``"deep"`` or ``"ext"``), ``saa``.
    Only ``offaxis`` differs from one axis to the next; the orbit and attitude are read
    once.

    Parameters
    ----------
    inputs : dict
        From :func:`find_inputs`.
    axes : list of str
        ``"POINTING"`` (ECLAIRs) or ``"GRDX_n"`` (GRM detector ``n``).

    Returns
    -------
    cuts : list of dict
        One per axis.
    orbit : astropy.table.Table
        The joined orbit, for the report.
    angles : list
        One per axis: ``(times, degrees)``, the angle between the source and the axis at
        each attitude sample, or ``None`` without a ``source``.
    """
    orbit, _ = geo.read_aux_series(inputs[ORBIT_EXTNAME])
    times = np.asarray(orbit["TIME"], dtype=float)
    common = dict(orbit=geo.orbit_coverage_gti(orbit), orbit_jumps=geo.orbit_jump_gti(orbit))
    offaxis = [None] * len(axes)
    angles = [None] * len(axes)
    if source is not None:
        common["visible"] = geo.visibility_gti(orbit, source)
        attitude, _ = geo.read_aux_series(inputs[ATTITUDE_EXTNAME])
        common["attitude"] = geo.coverage_gti(attitude["TIME"])
        times_att = np.asarray(attitude["TIME"], dtype=float)
        for i, axis in enumerate(axes):
            ra, dec = geo.axis_columns(attitude, axis)
            offaxis[i] = geo.offaxis_gti(attitude["TIME"], ra, dec, source, max_offaxis_deg)
            angles[i] = (times_att, geo.offaxis_angle(ra, dec, source))
    saa_cut = None
    if saa is not None:
        table = geo.read_saa(inputs[SAA_EXTNAMES[saa]])
        saa_cut = geo.saa_gti(table, times[0], times[-1])
    cuts = []
    for cut in offaxis:
        these = dict(common)
        if cut is not None:
            these["offaxis"] = cut
        if saa_cut is not None:
            these["saa"] = saa_cut
        cuts.append(these)
    return cuts, orbit, angles


def discontinuity_counts(orbit, gti=None):
    """
    How many ``ORIGIN`` and ``FLAG_PVT`` changes and position jumps the orbit has, or has
    inside ``gti``.
    """
    found = geo.orbit_discontinuities(orbit)
    inside = np.ones(len(found), bool) if gti is None else mask_from_gti(found["time"], gti)
    return {
        kind: int((inside & (found["kind"] == kind)).sum())
        for kind in ("ORIGIN", "FLAG_PVT", "jump")
    }


def apply_cuts(gti, cuts, first, summary):
    """
    Intersect ``gti`` with each cut in order.

    Sets ``exposure_<first>`` and ``exposure_<name>`` in ``summary``, the good time left.

    Returns
    -------
    list of (str, numpy.ndarray)
        ``(first, gti)``, then the good time left after each cut, for
        :func:`count_removed` and the report.
    """
    steps = [(first, gti)]
    summary[f"exposure_{first}"] = total_time(gti)
    for name, cut in cuts.items():
        gti = intersect_intervals(gti, cut)
        summary[f"exposure_{name}"] = total_time(gti)
        steps.append((name, gti))
    return steps


def count_removed(times, steps, summary):
    """
    Which events the final good time keeps, and how many each cut removed.

    An event is charged to the first cut that leaves it out. Sets ``removed_<name>`` for
    every cut and ``outside_gti``, their total plus any event outside the first GTI.

    Returns
    -------
    numpy.ndarray
        Boolean mask, the events inside the final GTI.
    """
    inside = mask_from_gti(times, steps[0][1])
    summary["outside_gti"] = int((~inside).sum())
    for name, gti in steps[1:]:
        kept = inside & mask_from_gti(times, gti)
        summary[f"removed_{name}"] = int((inside & ~kept).sum())
        inside = kept
    summary["outside_gti"] += sum(summary[f"removed_{name}"] for name, _ in steps[1:])
    return inside


def record_screening(rec, summary, steps, orbit=None, angles=None, max_offaxis_deg=None):
    """
    Write one pass's or detector's screening into its diagnostics record.

    The summary's scalars, the cut names in order, and the good time after each as
    ``gti_<name>`` arrays; the orbit discontinuities left inside the final good time; and,
    with ``angles``, the off-axis angle's minimum, median and maximum over the good time
    *before* the off-axis cut -- what a different threshold would have kept -- with the
    angle every :data:`OFFAXIS_FIGURE_STEP_S` for the figure. A unit that writes nothing
    is marked skipped, naming the cut that left no good time if one did.
    """
    names = [name for name, _ in steps]
    rec.value(**summary, cuts=names)
    rec.array(**{f"gti_{name}": np.asarray(gti, float).reshape(-1, 2) for name, gti in steps})
    if orbit is not None:
        rec.value(discontinuities_in_gti=discontinuity_counts(orbit, steps[-1][1]))
    if angles is not None:
        times, degrees = angles
        before = steps[names.index("offaxis") - 1][1] if "offaxis" in names else steps[-1][1]
        seen = degrees[mask_from_gti(times, before)]
        rec.value(
            max_offaxis_deg=max_offaxis_deg,
            **{
                f"offaxis_{stat}_deg": float(fun(seen)) if seen.size else None
                for stat, fun in (("min", np.min), ("median", np.median), ("max", np.max))
            },
        )
        stride = max(1, int(round(OFFAXIS_FIGURE_STEP_S / np.median(np.diff(times)))))
        rec.array(offaxis_time=times[::stride], offaxis_deg=degrees[::stride])
    if not summary["written"]:
        empty = next((name for name, gti in steps if not total_time(gti)), None)
        rec.skip(f"no good time left after the {empty} cut" if empty else "no event left")


def barycentre_events(infile, orbit_files, ra, dec, config=None):
    """
    Barycentre one event file with the ``barycenter`` package, from every orbit file given.

    There is no official tool for SVOM (``barycorr`` refuses ``TELESCOP = SVOM``), so
    ``barycenter_tool: official`` is overridden, with a warning.
    """
    config = config or {}
    if barycenter_tool(config) == OFFICIAL_TOOL:
        get_logger().warning("No official barycentring tool for SVOM: using the barycenter package")
    outfile = barycentered_file_name(infile)
    return barycenter_with_package(
        infile,
        list(orbit_files),
        outfile,
        ra=float(ra),
        dec=float(dec),
        ephem=barycenter_ephemeris(config),
    )


def barycentre_units(written, orbit_files, ra, dec, obsid, config):
    """
    Barycentre each pass's or detector's file, each in its own ``barycenter`` record.

    Without a position or an orbit file nothing is barycentred, and each record is marked
    skipped with the reason.

    Parameters
    ----------
    written : dict
        ``{key: event file}``, the key naming the pass or detector.

    Returns
    -------
    list of str
        The barycentred files, or the input files if nothing could be.
    """
    reason = "no orbit file" if not orbit_files else "no position given"
    if orbit_files and ra is not None and dec is not None:
        reason = None
    else:
        get_logger().warning(f"{obsid}: not barycentred -- {reason}")
    out = []
    for key, infile in written.items():
        with record_step(diagnostics_path(obsid, config), obsid, "barycenter", key=key) as rec:
            if reason is not None:
                rec.skip(reason)
                out.append(infile)
                continue
            rec.value(
                ra=float(ra),
                dec=float(dec),
                ephem=barycenter_ephemeris(config),
                orbit_files=len(orbit_files),
            )
            outfile = barycentre_events(infile, orbit_files, ra, dec, config=config)
            rec.value(barycentered_file=os.path.basename(outfile))
            out.append(outfile)
    return out


def join_events(tables):
    """
    Join event tables in time order, keeping rows identical in every column once.

    Returns
    -------
    table : astropy.table.Table
    n_duplicates : int

    Examples
    --------
    >>> a = Table(dict(TIME=[2.0, 1.0], PI=[5, 6]))
    >>> b = Table(dict(TIME=[2.0, 2.0], PI=[5, 7]))
    >>> table, n = join_events([a, b])
    >>> table["TIME"].tolist(), table["PI"].tolist(), n
    ([1.0, 2.0, 2.0], [6, 5, 7], 1)
    """
    table = vstack(tables, metadata_conflicts="silent")
    columns = [np.asarray(table[name]) for name in table.colnames]
    table = table[np.lexsort(columns[::-1])]
    same = np.ones(max(len(table) - 1, 0), bool)
    for name in table.colnames:
        values = np.asarray(table[name])
        same &= values[1:] == values[:-1]
    return table[np.r_[True, ~same] if len(table) else []], int(same.sum())


def _clean(header, stale_keywords):
    header = header.copy()
    for key in stale_keywords:
        header.remove(key, ignore_missing=True, remove_all=True)
    return header


def write_events(
    table, gti, primary, header, outfile, extname, stale_keywords, livetime=None, extra_hdus=()
):
    """
    Write joined events under an input file's headers, with their GTI.

    ``TSTART``, ``TSTOP`` and ``TELAPSE`` describe the GTI and ``ONTIME`` is its total.
    Without ``livetime``, an ``EXPOSURE`` already there becomes ``ONTIME`` times ``DEADC``
    (1 if missing or blank);
    with it, the events get ``LIVETIME`` and ``EXPOSURE`` equal to it, and ``DEADC`` their
    ratio to ``ONTIME``. ``stale_keywords`` are dropped from both headers.

    Parameters
    ----------
    table : astropy.table.Table
    gti : numpy.ndarray
    primary, header : astropy.io.fits.Header
        Of the first input file: its primary header and its events'.
    outfile, extname : str
    stale_keywords : tuple of str
    livetime : float, optional
    extra_hdus : list of HDU
        Appended after the GTI, e.g. ``EBOUNDS``.
    """
    table.meta.clear()  # Table.read put the first file's keywords there, stale ones too
    events = fits.table_to_hdu(table)
    for card in _clean(header, stale_keywords).cards:
        if card.keyword in ("COMMENT", "HISTORY") or card.keyword not in events.header:
            events.header.append(card)
    events.name = extname

    gti_hdu = fits.BinTableHDU(Table(dict(START=gti[:, 0], STOP=gti[:, 1])), name="GTI")
    for key in GTI_KEYWORDS:
        if key in events.header:
            gti_hdu.header[key] = events.header[key]

    primary_hdu = fits.PrimaryHDU(header=_clean(primary, stale_keywords))
    hdul = fits.HDUList([primary_hdu, events, gti_hdu, *extra_hdus])
    update_time_bounds(hdul, gti)
    ontime = total_time(gti)
    for hdu in hdul[:2]:
        if "ONTIME" in hdu.header:
            hdu.header["ONTIME"] = ontime
        if "EXPOSURE" in hdu.header and livetime is None:
            deadc = hdu.header.get("DEADC")  # missing, or present without a value
            hdu.header["EXPOSURE"] = ontime * (1.0 if deadc is None else float(deadc))
    if livetime is not None:
        events.header["ONTIME"] = (ontime, "[s] total of the GTI")
        events.header["LIVETIME"] = (livetime, "[s] ONTIME minus the summed DEAD_TIME")
        events.header["EXPOSURE"] = (livetime, "[s] equal to LIVETIME")
        events.header["DEADC"] = (livetime / ontime if ontime else 0.0, "LIVETIME / ONTIME")
    hdul.writeto(outfile, overwrite=True)
    return outfile
