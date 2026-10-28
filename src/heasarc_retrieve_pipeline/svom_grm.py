"""
SVOM/GRM reduction for timing: one event file per detector, then barycentred.

The Gamma-Ray Monitor has three NaI detectors (GRDs) pointing in different directions.
Each ``GRM-EVT`` file covers an ObsID (a day or so), with one event table per detector,
``EVENTS01..03`` (``DETNAM = GRD_0n``), an ``EBOUNDS`` table (channel to energy; ``PI``
is the channel) and one ``GTI``, which in the file surveyed is exactly where there are
data, and already leaves out the deep SAA. About 100 million events per detector per day.

Each detector is reduced on its own, :func:`reduce_detector`, into
``l2_files/<obsid>_grd<n>.evt`` (extension ``EVENTS``):

1. The GTI is the files' own, each clipped to its ``TSTART``-``TSTOP``, cut on the
   geometry of :func:`~heasarc_retrieve_pipeline.svom_common.geometry_cuts`: orbit and
   attitude coverage, the source above the Earth's limb, the source within
   :data:`DEFAULT_MAX_OFFAXIS_DEG` of *this detector's* axis (``GRDX_n``) and, if asked
   for, outside the SAA.
2. The dead time is ``DEAD_TIME`` summed over *every* event inside that GTI, selected or
   not: each kept the detector busy. Where files overlap, the earlier one's events count.
3. Events are kept with ``FLAG``, ``EVT_TYPE`` and ``ANTI_COIN`` all 0 and ``PI`` in the
   channels entirely inside the energy band. Read from the energies they carry in the
   file surveyed (not from a description): ``EVT_TYPE`` 1 are all in the top channel
   (overflow), ``ANTI_COIN`` 1 only between 24 and 62 keV (most likely the americium
   calibration source, 59.5 keV); ``GAIN_TYPE`` 1 are the events above ~1.3 MeV, which
   the band removes by itself.
4. The files are joined in time order, rows identical in every column kept once, and
   events outside the GTI dropped.

The output has ``ONTIME`` (the GTI total), ``LIVETIME`` (``ONTIME`` minus the dead time),
``EXPOSURE`` equal to ``LIVETIME`` and ``DEADC``, and a copy of ``EBOUNDS``. What each
step removed goes in ``l2_files/<obsid>_grm.ecsv``, one row per detector.

Configuration keys:

``grm_energy_band_kev``
    ``[emin, emax]``. Default :data:`DEFAULT_ENERGY_BAND_KEV`.
``grm_max_offaxis_deg``
    Default :data:`DEFAULT_MAX_OFFAXIS_DEG`.
``grm_saa``
    ``"deep"`` or ``"ext"``: also cut on that SAA table (made for ECLAIRs; there is none
    for GRM). Default: no SAA cut beyond the file's own GTI.
"""

import os

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Table
from prefect import flow, task

from .svom_common import (
    COMMON_STALE_KEYWORDS,
    DEFAULT_CONFIG,
    barycentre_events,
    discontinuity_counts,
    find_inputs,
    geometry_cuts,
    join_events,
    total_time,
    write_events,
)
from .utils import (
    absolute_config,
    get_logger,
    good_intervals,
    intersect_intervals,
    mask_from_gti,
    merge_intervals,
)

#: The detectors, numbered as in ``EVENTS0n`` and ``GRDX_n``.
DETECTORS = (1, 2, 3)

#: First extension of a GRM event file, by which :func:`find_inputs` files it.
FIRST_EXTNAME = "EBOUNDS"

#: Name of the event extension in the output.
OUTPUT_EXTNAME = "EVENTS"

#: Columns that must be 0 for an event to be kept.
REJECT_COLUMNS = ("FLAG", "EVT_TYPE", "ANTI_COIN")

#: Default energy band, keV: a first guess, to be tuned on the Crab.
DEFAULT_ENERGY_BAND_KEV = (20.0, 300.0)

#: Largest angle between a detector's axis and the source, in degrees.
DEFAULT_MAX_OFFAXIS_DEG = 60.0

#: Header keywords of the input files that do not describe a joined output.
STALE_KEYWORDS = COMMON_STALE_KEYWORDS + ("INFILE01",)

#: The counts in a detector's summary, in the order they are taken.
COUNTS = (
    "files",
    "read",
    "rejected",
    "out_of_band",
    "outside_file_gti",
    "duplicates",
    "outside_gti",
    "written",
)


def events_extname(detector):
    return f"EVENTS{detector:02d}"


def band_channels(ebounds, band_kev):
    """
    The channels entirely inside an energy band.

    Examples
    --------
    >>> ebounds = dict(CHANNEL=[0, 1, 2], E_MIN=[5, 20, 100], E_MAX=[20, 100, 300])
    >>> band_channels(ebounds, (10, 300)).tolist()
    [1, 2]
    """
    channel = np.asarray(ebounds["CHANNEL"])
    inside = (np.asarray(ebounds["E_MIN"]) >= band_kev[0]) & (
        np.asarray(ebounds["E_MAX"]) <= band_kev[1]
    )
    if not inside.any():
        raise ValueError(f"No channel lies entirely inside {list(band_kev)} keV")
    return channel[inside]


def _file_gti(path, extname):
    """A file's own GTI, clipped to its span, and the span."""
    with fits.open(path) as hdul:
        header = hdul[extname].header
        span = np.array([[header["TSTART"], header["TSTOP"]]], dtype=float)
        gti = np.column_stack([hdul["GTI"].data["START"], hdul["GTI"].data["STOP"]])
    return intersect_intervals(gti, span), span


def reduce_detector(files, detector, outfile, cuts=None, band_kev=DEFAULT_ENERGY_BAND_KEV):
    """
    Reduce one detector of one ObsID; see the module docstring.

    Parameters
    ----------
    files : list of str
        The ObsID's GRM event files.
    detector : int
        1, 2 or 3.
    outfile : str
    cuts : dict, optional
        ``{name: gti}``, applied in order to the union of the files' GTIs.
    band_kev : (float, float)

    Returns
    -------
    outfile : str or None
        ``None`` if no event is left.
    summary : dict
        The :data:`COUNTS`; ``dead_time`` and ``livetime`` in seconds; ``exposure_file``
        and ``exposure_<name>``, the good time left after each cut; ``emin_kev`` and
        ``emax_kev``, the band actually kept.
    """
    extname = events_extname(detector)
    cuts = cuts or {}
    summary = dict.fromkeys(COUNTS, 0)
    files = sorted(files, key=lambda path: fits.getheader(path, extname)["TSTART"])
    own = [_file_gti(path, extname) for path in files]

    gti = merge_intervals(np.concatenate([g for g, _ in own]))
    summary["exposure_file"] = total_time(gti)
    for name, cut in cuts.items():
        gti = intersect_intervals(gti, cut)
        summary[f"exposure_{name}"] = total_time(gti)

    tables, headers, covered, dead_us = [], None, np.zeros((0, 2)), 0.0
    for path, (file_gti, span) in zip(files, own):
        with fits.open(path, memmap=True) as hdul:
            data = hdul[extname].data
            if headers is None:
                ebounds = hdul["EBOUNDS"]
                channels = band_channels(ebounds.data, band_kev)
                in_band = np.isin(ebounds.data["CHANNEL"], channels)
                summary["emin_kev"] = float(ebounds.data["E_MIN"][in_band].min())
                summary["emax_kev"] = float(ebounds.data["E_MAX"][in_band].max())
                headers = (hdul[0].header.copy(), hdul[extname].header.copy())
                extra = fits.BinTableHDU(ebounds.data.copy(), ebounds.header.copy())
            time = data["TIME"]
            summary["read"] += len(time)

            fresh = intersect_intervals(gti, good_intervals(covered, *span[0]))
            dead_us += float(np.sum(data["DEAD_TIME"][mask_from_gti(time, fresh)], dtype=float))
            covered = merge_intervals(np.concatenate([covered, span]))

            keep = np.ones(len(time), bool)
            for name in REJECT_COLUMNS:
                keep &= data[name] == 0
            summary["rejected"] += int((~keep).sum())
            in_band = np.isin(data["PI"], channels)
            summary["out_of_band"] += int((keep & ~in_band).sum())
            keep &= in_band
            inside = mask_from_gti(time, file_gti)
            summary["outside_file_gti"] += int((keep & ~inside).sum())
            keep &= inside
            tables.append(Table({name: np.asarray(data[name][keep]) for name in data.names}))
        summary["files"] += 1

    summary["dead_time"] = dead_us * 1e-6
    summary["livetime"] = total_time(gti) - summary["dead_time"]
    table, summary["duplicates"] = join_events(tables)
    good = mask_from_gti(table["TIME"], gti)
    summary["outside_gti"] = int((~good).sum())
    table = table[good]
    summary["written"] = len(table)
    if not len(table):
        get_logger().warning(f"{os.path.basename(outfile)}: no event left")
        return None, summary
    written = write_events(
        table,
        gti,
        *headers,
        outfile,
        OUTPUT_EXTNAME,
        STALE_KEYWORDS,
        livetime=summary["livetime"],
        extra_hdus=[extra],
    )
    return written, summary


@task(name="reduce_grm_obsid")
def reduce_observation(raw_data_dir: str, obsid: str, config: dict, source=None):
    """
    Reduce every detector of one ObsID; see the module docstring.

    Returns
    -------
    written : list of str
        One event file per detector with events left.
    orbit_files : list of str
    """
    logger = get_logger()
    inputs = find_inputs(raw_data_dir)
    files = inputs[FIRST_EXTNAME]
    if not files:
        logger.warning(f"{obsid}: no GRM event file")
        return [], []

    orbit_files = inputs["SVO-ORB-CNV"]
    cuts, meta = [{}] * len(DETECTORS), {}
    if orbit_files:
        cuts, orbit = geometry_cuts(
            inputs,
            source=source,
            axes=[f"GRDX_{n}" for n in DETECTORS],
            max_offaxis_deg=config.get("grm_max_offaxis_deg", DEFAULT_MAX_OFFAXIS_DEG),
            saa=config.get("grm_saa"),
        )
        meta = discontinuity_counts(orbit)
    else:
        logger.warning(f"{obsid}: no orbit file; only the files' own GTI applies")

    l2_dir = os.path.join(raw_data_dir, "l2_files")
    os.makedirs(l2_dir, exist_ok=True)
    written, rows = [], []
    for detector, detector_cuts in zip(DETECTORS, cuts):
        outfile, summary = reduce_detector(
            files,
            detector,
            os.path.join(l2_dir, f"{obsid}_grd{detector}.evt"),
            cuts=detector_cuts,
            band_kev=config.get("grm_energy_band_kev", DEFAULT_ENERGY_BAND_KEV),
        )
        rows.append(dict(summary, detector=detector))
        if outfile is not None:
            written.append(outfile)
    table = Table(rows=rows)
    table = table[["detector"] + [name for name in table.colnames if name != "detector"]]
    table.meta["orbit_discontinuities"] = meta
    table.write(os.path.join(l2_dir, f"{obsid}_grm.ecsv"), overwrite=True)
    return written, orbit_files


@flow
def process_grm_obsid(obsid: str, config=None, flags=None, ra: float = None, dec: float = None):
    """
    Reduce one SVOM/GRM ObsID for timing: one file per detector, barycentred.

    Parameters
    ----------
    obsid : str
    config : dict, optional
        Pipeline configuration; see the module docstring for the GRM keys.
    flags : dict, optional
        Accepted for signature compatibility with the other missions, and ignored.
    ra, dec : float, optional
        The searched position, in degrees. Without it neither the Earth nor the
        off-axis cut is made, and nothing is barycentred.

    Returns
    -------
    list of str or None
        The barycentred files, the unbarycentred ones if there is no position or orbit,
        or ``None`` if no detector has events left.
    """
    current_config = absolute_config(config, DEFAULT_CONFIG)
    logger = get_logger()
    logger.info(f"Processing SVOM/GRM observation {obsid}")
    raw_data_dir = os.path.join(current_config["out_data_path"], obsid)
    source = None if ra is None or dec is None else SkyCoord(ra, dec, unit="deg")

    written, orbit_files = reduce_observation(raw_data_dir, obsid, current_config, source=source)
    if not written:
        return None
    if source is None or not orbit_files:
        logger.warning(
            f"{obsid}: not barycentred -- "
            + ("no orbit file" if not orbit_files else "no position given")
        )
        return written
    return [
        barycentre_events(infile, orbit_files, ra, dec, config=current_config) for infile in written
    ]
