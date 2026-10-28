"""
What the ECLAIRs and GRM reductions share: finding an ObsID's files, the geometric cuts
of :mod:`~heasarc_retrieve_pipeline.svom_geometry` put together, and barycentring.
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
from .utils import get_logger, update_time_bounds

#: Orbit and attitude extensions.
ORBIT_EXTNAME = "SVO-ORB-CNV"
ATTITUDE_EXTNAME = "SVO-ATT-CNV"

#: The SAA table for each value of the ``*_saa`` configuration keys.
SAA_EXTNAMES = {"deep": "SAA-DEEP-ECL", "ext": "SAA-EXT-ECL"}

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")

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
    """
    orbit, _ = geo.read_aux_series(inputs[ORBIT_EXTNAME])
    times = np.asarray(orbit["TIME"], dtype=float)
    common = dict(orbit=geo.orbit_coverage_gti(orbit), orbit_jumps=geo.orbit_jump_gti(orbit))
    offaxis = [None] * len(axes)
    if source is not None:
        common["visible"] = geo.visibility_gti(orbit, source)
        attitude, _ = geo.read_aux_series(inputs[ATTITUDE_EXTNAME])
        common["attitude"] = geo.coverage_gti(attitude["TIME"])
        for i, axis in enumerate(axes):
            ra, dec = geo.axis_columns(attitude, axis)
            offaxis[i] = geo.offaxis_gti(attitude["TIME"], ra, dec, source, max_offaxis_deg)
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
    return cuts, orbit


def discontinuity_counts(orbit):
    """How many ``ORIGIN`` and ``FLAG_PVT`` changes and position jumps the orbit has."""
    found = geo.orbit_discontinuities(orbit)
    return {kind: int((found["kind"] == kind).sum()) for kind in ("ORIGIN", "FLAG_PVT", "jump")}


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
    Without ``livetime``, an ``EXPOSURE`` already there becomes ``ONTIME`` times ``DEADC``;
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
            hdu.header["EXPOSURE"] = ontime * float(hdu.header.get("DEADC", 1.0))
    if livetime is not None:
        events.header["ONTIME"] = (ontime, "[s] total of the GTI")
        events.header["LIVETIME"] = (livetime, "[s] ONTIME minus the summed DEAD_TIME")
        events.header["EXPOSURE"] = (livetime, "[s] equal to LIVETIME")
        events.header["DEADC"] = (livetime / ontime if ontime else 0.0, "LIVETIME / ONTIME")
    hdul.writeto(outfile, overwrite=True)
    return outfile
