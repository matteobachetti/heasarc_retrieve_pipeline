"""
RXTE/PCA reduction and day fusion: a pure-astropy re-implementation of the screening.

Unlike the NuSTAR and NICER modules, the reduction here uses no HEASOFT at all except for
the barycentre correction. It reads the standard filter file, builds one good time
interval list *per PCU* from the standard screening conditions, and applies them to the
GoodXenon event files with numpy masking.

Two stages, in this order:

``process_rxte_obsid``
    One pointing. Screens every GoodXenon file against its own unit's intervals, merges
    them into a single event list with absolute times, and barycentres it at the position
    being searched. The output keeps ``PCUID``, ``ANODEID`` and ``PHA``, so the choice of
    units and xenon layers can still be made afterwards and stingray can calibrate the
    energies from ``TEVTB2``.

``join_rxte_events``
    Several pointings. RXTE observations of a faint source are short and scattered, and
    fusing the pointings of a few days is what makes a coherent pulsation search possible
    at all. The good time intervals are unioned, so the time between pointings stays a gap.

Only event-mode data can be processed; pointings that ran only binned modes are skipped
with a warning. The products are a first look, not calibrated spectra: the number of
active PCUs changes within an interval, so the effective area is not constant and no
response can be attached, and no deadtime correction is applied.

See ``docs/technical_details.rst`` for the screening criteria and what they omit, and
``docs/known_issues.rst`` for known defects.
"""

import glob
import os
import warnings

import numpy as np
from astropy.io import fits
from astropy.io.fits.verify import VerifyWarning
from astropy.table import Table
from prefect import flow, task

from .barycenter import barycenter_ephemeris, barycenter_file, barycenter_tool
from .utils import absolute_config, get_logger, mask_from_gti, merge_intervals

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")

#: What of an RXTE observation directory the reduction reads, matched on the whole remote
#: path so that the directory anchors hold.
#:
#: Measured on the 870 pointings within a degree of M82 on 2026-09-17: these files are
#: 1.05 GB of the archive's 13.0 GB. The rest is mostly HEXTE, the other PCA modes, and a
#: 7.8 MB copy of the calibration directory in every pointing.
#:
#: * ``pca/GX_*.evt.gz``: the GoodXenon event files, with ``PCUID``, ``ANODEID`` and
#:   ``PHA`` already decoded by the archive. A pointing can have several, and all of them
#:   are needed. The raw GoodXenon halves (``FS37``/``FS3b``) are what these were made
#:   from, and are not needed.
#: * ``pca/FS4a_*.gz``: Standard2, the binned mode every pointing has; cheap, and the only
#:   input a background model or a spectrum could be built from later.
#: * ``stdprod/x*.xfl.gz``: the filter file the good time intervals are built from. Its
#:   preview plot, ``stdprod/GIFS/x*_xfl.gif``, is excluded by the directory anchor.
#: * ``orbit/FPorbit_*``: the spacecraft orbit, which barycorr reads.
#:
#: ``clock/`` is left out on purpose: for RXTE, barycorr ignores its ``clockfile``
#: parameter and applies ``tdc.dat`` from the HEASOFT reference data instead.
DOWNLOAD_RE = (
    r"(?:/pca/GX_[^/]*\.evt\.gz$"
    r"|/pca/FS4a_[^/]*\.gz$"
    r"|/stdprod/x[^/]*\.xfl\.gz$"
    r"|/orbit/FPorbit_[^/]*$)"
)


def rxte_download_filter(config):
    """
    What of an RXTE observation directory to download.

    Called through :func:`~heasarc_retrieve_pipeline.core.mission_download_filter`. RXTE
    has one route, so the configuration is not read.

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
    >>> sorted(rxte_download_filter({}))
    ['re_include']
    """
    return {"re_include": DOWNLOAD_RE}


#: The standard PCA screening, as the RXTE cook book states it.
#:
#: ``elv_min``
#:     Earth elevation angle, in degrees. Below 10 the target is close enough to the limb
#:     for the atmosphere to add absorption and albedo background.
#: ``offset_max``
#:     Pointing offset, in degrees. The collimator response falls off over about a degree,
#:     so 0.02 (1.2 arcmin) keeps the effective area flat and rejects slews.
#: ``saa_min``
#:     Minutes since the last South Atlantic Anomaly passage. The particle background
#:     decays for about half an hour afterwards. ``TIME_SINCE_SAA`` is *negative* when no
#:     passage falls in the recorded window, and negative is good time.
#: ``electron_max``
#:     Electron ratio, which flags detector breakdown.
#:
#: Measured on three pointings on 2026-09-18: relative to elevation and offset alone, the
#: SAA and electron cuts cost about 28% of the exposure in 1997 and 2004 and about 2% in
#: 2006-09, which is 685 of the 868 M82 pointings.
DEFAULT_SCREENING = {
    "elv_min": 10.0,
    "offset_max": 0.02,
    "saa_min": 30.0,
    "electron_max": 0.1,
}

#: The Proportional Counter Units, all five of them.
ALL_PCUS = (0, 1, 2, 3, 4)

#: The units the reduction screens for unless told otherwise.
#:
#: PCU0 lost its propane veto layer in May 2000, and without it it sees far more particle
#: background: measured on the M82 pointings of 2004, PCU0 gives 11.2 counts/s in the top
#: layer between 3 and 15 keV against 6.5-7 for the others, all of the difference being
#: background. Dropping it costs 562 ks of unit-time out of 2697 ks and buys a much
#: cleaner search.
#:
#: PCU1 lost its own propane layer in December 2006 and has the same problem afterwards,
#: but it is on for only 3.5 ks of the M82 archive after that date, so it is kept rather
#: than made a special case. Pass ``pcus`` to change any of this.
DEFAULT_PCUS = (1, 2, 3, 4)


def _contiguous_intervals(mask, times, timedel):
    """
    Collapse a boolean mask over evenly sampled times into intervals.

    ``TIMEPIXR`` is 0 in every RXTE filter file, so a sample stamped ``t`` covers
    ``[t, t + TIMEDEL)`` -- the stamp is the start of the bin, not its centre.
    """
    good = np.where(np.asarray(mask))[0]
    if good.size == 0:
        return np.zeros((0, 2))
    return merge_intervals(np.column_stack([times[good], times[good] + timedel]))


def rxte_pcu_gtis(filter_file, pcus=ALL_PCUS, screening=None):
    """
    Good time intervals of one observation, one interval list per PCU.

    Reads the standard filter file, which samples the housekeeping every ``TIMEDEL``
    seconds (16 s throughout the mission), applies the standard screening of
    :data:`DEFAULT_SCREENING`, and returns what each Proportional Counter Unit was doing.

    The result is **per PCU on purpose**. ``NUM_PCU_ON`` records how many units were on,
    never which, and they are not on at the same times: in the 1997 pointing
    20303-02-06-00, PCU0 and PCU1 give twelve intervals where PCU2 gives seven. One
    interval list for the whole observation therefore cannot describe the collecting area,
    and an exposure computed from it is wrong for every unit.

    Parameters
    ----------
    filter_file : str
        ``stdprod/x*.xfl.gz``, compressed or not.
    pcus : iterable of int, optional
        Which units to screen. The default is all five.
    screening : dict, optional
        Overrides for :data:`DEFAULT_SCREENING`.

    Returns
    -------
    dict
        ``{pcu: (N, 2) array}``, in seconds on the same scale as the event file's
        ``TIME + TIMEZERO``. A unit that was never on, or was screened away entirely, is
        present with an empty array rather than missing: "off" and "screened out" are not
        the caller's problem to tell apart.

    Notes
    -----
    ``ELECTRONn`` is ``NaN`` in a few samples where the unit is flagged on, and the
    comparison drops them -- missing housekeeping is not evidence of a healthy detector.
    It is also identically zero once a unit has lost its propane veto layer (PCU0 in 2000,
    PCU1 in December 2006), which makes the cut a no-op there rather than a wrong one.
    """
    cuts = dict(DEFAULT_SCREENING, **(screening or {}))

    with fits.open(filter_file) as hdul:
        table = Table(hdul[1].data)
        header = hdul[1].header
    timedel = float(header.get("TIMEDEL", 16.0))
    # The filter file and the event file carry the same TIMEZERO. Adding it to the events
    # alone, as this module used to, shifted every interval boundary by 3.4 s.
    times = np.asarray(table["Time"], dtype=float) + float(header.get("TIMEZERO", 0.0))

    since_saa = np.asarray(table["TIME_SINCE_SAA"], dtype=float)
    common = (
        (np.asarray(table["ELV"], dtype=float) > cuts["elv_min"])
        & (np.asarray(table["OFFSET"], dtype=float) < cuts["offset_max"])
        & ((since_saa > cuts["saa_min"]) | (since_saa < 0))
    )

    gtis = {}
    for pcu in pcus:
        electron = np.asarray(table[f"ELECTRON{pcu}"], dtype=float)
        mask = common & (np.asarray(table[f"PCU{pcu}_ON"]) == 1) & (electron < cuts["electron_max"])
        gtis[pcu] = _contiguous_intervals(mask, times, timedel)
    return gtis


def _archive_files(root, *parts):
    """
    Sorted matches of a glob under an observation directory, without macOS's sidecars.

    The M82 archive copy lives on an exFAT drive, where macOS writes a ``._<name>``
    AppleDouble file next to every file it touches. They are not FITS and astropy chokes
    on them, so they are filtered out by name rather than by trying to open them.
    """
    found = glob.glob(os.path.join(root, *parts))
    return sorted(f for f in found if not os.path.basename(f).startswith("._"))


def find_rxte_inputs(raw_data_dir):
    """
    The files of one downloaded pointing that the reduction reads.

    Parameters
    ----------
    raw_data_dir : str
        The observation directory, as :func:`rxte_download_filter` leaves it.

    Returns
    -------
    dict
        ``event_files`` (every ``pca/GX_*.evt.gz``, sorted), ``filter_file`` and
        ``orbit_file``, the last two ``None`` when absent. An empty ``event_files`` is
        reported rather than raised: seven M82 pointings have no event-mode data at all,
        and a batch has to walk past them.
    """
    event_files = _archive_files(raw_data_dir, "pca", "GX_*.evt*")
    filter_files = _archive_files(raw_data_dir, "stdprod", "x*.xfl*")
    orbit_files = _archive_files(raw_data_dir, "orbit", "FPorbit_*")
    return {
        "event_files": event_files,
        "filter_file": filter_files[0] if filter_files else None,
        "orbit_file": orbit_files[0] if orbit_files else None,
    }


#: Header keywords the screened output inherits from the first event file. Stingray reads
#: these files directly and turns ``PHA`` into keV from ``TEVTB2`` and the epoch, so losing
#: any of them silently costs the energy calibration.
KEPT_KEYWORDS = (
    "TELESCOP",
    "INSTRUME",
    "DATAMODE",
    "OBJECT",
    "OBS_ID",
    "RA_OBJ",
    "DEC_OBJ",
    "RA_PNT",
    "DEC_PNT",
    "EQUINOX",
    "RADECSYS",
    "MJDREFI",
    "MJDREFF",
    "TIMESYS",
    "TIMEUNIT",
    "TIMEREF",
    "TASSIGN",
    "TIMEDEL",
    "TEVTB2",
    "TDDES2",
)


#: Time keywords a GTI extension must repeat from the event extension.
#:
#: ``barycorr`` corrects a GTI extension only if it can read its own time scale, and skips
#: it in silence otherwise. Verified on 94123-01-19-00: without these, the events moved by
#: 298.3 s and the intervals did not, leaving a file that looked barycentred and whose
#: interval boundaries were five minutes wrong.
GTI_TIME_KEYWORDS = ("MJDREFI", "MJDREFF", "TIMESYS", "TIMEUNIT", "TIMEREF", "TASSIGN")


def _write(hdus, outfile):
    """
    Write an event file, without the warning ``TEVTB2`` provokes on every one of them.

    Its value is 65 characters with the quotes, which fills the card and leaves no room
    for the ``/`` that astropy writes even when the comment is empty. Nothing is lost --
    the value reads back byte for byte -- but the warning would be printed for every
    pointing and every merged window.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Card is too long", category=VerifyWarning)
        fits.HDUList(hdus).writeto(outfile, overwrite=True)


def _copy_keywords(target, source, keywords):
    """
    Copy keywords between headers, comment and all -- except when it does not fit.

    ``TEVTB2`` is 65 characters of value, which leaves no room on the card for the
    ``/`` that even an empty comment is written with, and astropy warns on every file.
    """
    for keyword in keywords:
        if source is None or keyword not in source:
            continue
        comment = source.comments[keyword]
        if comment and len(str(source[keyword])) < 60:
            target[keyword] = (source[keyword], comment)
        else:
            target[keyword] = source[keyword]


def _gti_hdu(intervals, name, header=None):
    """A GTI extension, written even when there are no intervals to put in it."""
    intervals = np.asarray(intervals, dtype=float).reshape(-1, 2)
    hdu = fits.BinTableHDU(
        Table([intervals[:, 0], intervals[:, 1]], names=("START", "STOP")), name=name
    )
    hdu.header["TELESCOP"] = "XTE"
    hdu.header["TIMEZERO"] = 0.0
    hdu.header["HDUCLASS"] = "OGIP"
    hdu.header["HDUCLAS1"] = "GTI"
    hdu.header["HDUCLAS2"] = "ALL"
    hdu.header["TUNIT1"] = "s"
    hdu.header["TUNIT2"] = "s"
    # A default rather than a copy: an interval list this module writes is in seconds
    # whatever the input said, and barycorr needs the unit to be there at all.
    hdu.header["TIMEUNIT"] = "s"
    for keyword in GTI_TIME_KEYWORDS:
        if header is not None and keyword in header:
            hdu.header[keyword] = header[keyword]
    if len(intervals):
        hdu.header["TSTART"] = float(intervals[0][0])
        hdu.header["TSTOP"] = float(intervals[-1][1])
    return hdu


def rxte_screened_events(event_files, gtis, outfile, extra_header=None):
    """
    Merge the GoodXenon files of one pointing and keep what its PCUs were collecting.

    Every event is screened against **its own** unit's intervals, so an event recorded by
    PCU3 while only PCU2 was in good time is dropped. A unit missing from ``gtis`` --
    which is how PCU0 gets left out -- contributes nothing.

    The output is an ``XTE_SE`` event extension with absolute times and ``TIMEZERO = 0``,
    a ``GTI`` extension holding the union over the units, and one ``GTI_PCU<n>`` extension
    per unit so that the collecting area can be reconstructed afterwards.

    Parameters
    ----------
    event_files : list of str
        The pointing's ``pca/GX_*.evt.gz`` files, compressed or not. A pointing can have
        several -- 25 of the 863 M82 pointings do -- and all of them are read.
    gtis : dict
        ``{pcu: (N, 2) array}``, as :func:`rxte_pcu_gtis` returns.
    outfile : str
        Where to write.
    extra_header : dict, optional
        Keywords to add to the event extension.

    Returns
    -------
    str or None
        ``outfile``, or ``None`` if nothing survived, in which case no file is written.

    Notes
    -----
    The ``Event`` column is not carried over. It is the raw 24-bit field that ``PCUID``,
    ``ANODEID`` and ``PHA`` were decoded from: checked against 1.66 million archive events,
    bits 7-9 reproduce ``PCUID`` and bits 16-23 reproduce ``PHA`` exactly, bit 0 is always
    1, bits 1-6 are always 0, and bits 10-15 are a one-hot re-encoding of ``ANODEID``. It
    carries nothing, and keeping it only invites a second, different decoding.

    ``EXPOSURE`` and ``ONTIME`` are the good time actually kept, not the unfiltered
    observation's. The first version of this module inherited them unchanged, so every
    rate computed from its output was too low.
    """
    union = merge_intervals(
        np.concatenate(
            [np.asarray(g, dtype=float).reshape(-1, 2) for g in gtis.values()] + [np.zeros((0, 2))]
        )
    )
    if len(union) == 0:
        return None

    columns, header = [], None
    for path in sorted(event_files):
        with fits.open(path) as hdul:
            hdu = hdul["XTE_SE"]
            data = hdu.data
            if header is None:
                header = hdu.header
            times = np.asarray(data["TIME"], dtype=float) + float(hdu.header.get("TIMEZERO", 0.0))
            pcuid = np.asarray(data["PCUID"])
            keep = np.zeros(times.size, dtype=bool)
            for pcu, intervals in gtis.items():
                keep |= (pcuid == pcu) & mask_from_gti(times, np.asarray(intervals, dtype=float))
            if not keep.any():
                continue
            columns.append(
                (
                    times[keep],
                    pcuid[keep],
                    np.asarray(data["ANODEID"])[keep],
                    np.asarray(data["PHA"])[keep],
                )
            )

    if not columns:
        return None

    times, pcuid, anodeid, pha = (np.concatenate(c) for c in zip(*columns))
    order = np.argsort(times, kind="stable")
    table = Table(
        [times[order], pcuid[order], anodeid[order], pha[order]],
        names=("TIME", "PCUID", "ANODEID", "PHA"),
    )

    events = fits.BinTableHDU(table, name="XTE_SE")
    _copy_keywords(events.header, header, KEPT_KEYWORDS)
    events.header["TIMEZERO"] = (0.0, "absolute times: TIMEZERO is already in TIME")
    events.header["TSTART"] = (float(union[0][0]), "start of the first good time interval")
    events.header["TSTOP"] = (float(union[-1][1]), "end of the last good time interval")
    exposure = float((union[:, 1] - union[:, 0]).sum())
    events.header["ONTIME"] = (exposure, "good time, union over the PCUs")
    events.header["EXPOSURE"] = (exposure, "good time, union over the PCUs; no deadtime")
    events.header["PCUS"] = (",".join(str(p) for p in sorted(gtis)), "PCUs screened for")
    for keyword, value in (extra_header or {}).items():
        events.header[keyword] = value
    events.header["HISTORY"] = "Screened by heasarc_retrieve_pipeline.rxte, per PCU"

    hdus = [fits.PrimaryHDU(), events, _gti_hdu(union, "GTI", events.header)]
    hdus += [_gti_hdu(gtis[pcu], f"GTI_PCU{pcu}", events.header) for pcu in sorted(gtis)]
    _write(hdus, outfile)
    return outfile


def rxte_base_output_path(config, obsid):
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


@task(name="reduce_rxte_obsid")
def reduce_observation(raw_data_dir: str, obsid: str, config: dict):
    """
    Screen and merge one pointing into a single event file.

    Parameters
    ----------
    raw_data_dir : str
        The downloaded observation directory.
    obsid : str
        Observation identifier.
    config : dict
        ``pcus`` chooses the units (default :data:`DEFAULT_PCUS`), ``screening`` overrides
        :data:`DEFAULT_SCREENING`.

    Returns
    -------
    str or None
        The screened event file, or ``None`` if the pointing has no event-mode data, no
        filter file, or nothing that survives screening.
    """
    logger = get_logger()
    inputs = find_rxte_inputs(raw_data_dir)
    if not inputs["event_files"]:
        logger.warning(f"{obsid}: no GoodXenon event file; only binned modes were run")
        return None
    if inputs["filter_file"] is None:
        logger.warning(f"{obsid}: no standard filter file, so nothing can be screened")
        return None

    gtis = rxte_pcu_gtis(
        inputs["filter_file"],
        pcus=config.get("pcus", DEFAULT_PCUS),
        screening=config.get("screening"),
    )
    l2_dir = os.path.join(raw_data_dir, "l2_files")
    os.makedirs(l2_dir, exist_ok=True)
    outfile = os.path.join(l2_dir, f"{obsid}_cl.evt")

    written = rxte_screened_events(
        inputs["event_files"], gtis, outfile, extra_header={"OBS_ID": obsid}
    )
    if written is None:
        logger.warning(f"{obsid}: nothing survived screening")
        return None

    for pcu, intervals in sorted(gtis.items()):
        exposure = float((intervals[:, 1] - intervals[:, 0]).sum()) if len(intervals) else 0.0
        logger.info(f"{obsid}: PCU{pcu} {exposure:.0f} s in {len(intervals)} intervals")
    return written


@flow
def process_rxte_obsid(obsid: str, config=None, flags=None, ra: float = None, dec: float = None):
    """
    Reduce one RXTE/PCA observation: screen per PCU, merge, barycentre.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict, optional
        Pipeline configuration. ``out_data_path`` says where the download is; ``pcus`` and
        ``screening`` are passed to :func:`reduce_observation`. ``None`` means
        :data:`DEFAULT_CONFIG` -- and it has to be ``None`` rather than ``{}``, because
        ``absolute_config`` falls back only on ``None`` (issue 27 in
        ``docs/known_issues.rst``).
    flags : dict, optional
        Accepted for signature compatibility with the other missions, and ignored.
    ra, dec : float, optional
        The position to barycentre at. This is the *searched* position, never a detected
        or header one: an error here goes straight into the arrival times. Without it, and
        without an orbit file, the reduction stops at the screened file.

    Returns
    -------
    str or None
        The barycentred event file, the screened one if it could not be barycentred, or
        ``None`` if there was nothing to reduce.
    """
    current_config = absolute_config(config, DEFAULT_CONFIG)
    logger = get_logger()
    logger.info(f"Processing RXTE observation {obsid}")
    raw_data_dir = rxte_base_output_path(config=current_config, obsid=obsid)
    os.makedirs(raw_data_dir, exist_ok=True)

    screened = reduce_observation(raw_data_dir, obsid, current_config)
    if screened is None:
        logger.info(f"Pipeline for OBSID {obsid} stopped as no usable data was found.")
        return None

    orbit_file = find_rxte_inputs(raw_data_dir)["orbit_file"]
    if ra is None or dec is None or orbit_file is None:
        logger.warning(
            f"{obsid}: not barycentred -- "
            + ("no orbit file" if orbit_file is None else "no position given")
        )
        return screened

    barycentered = barycenter_file(
        screened,
        orbit_file,
        ra=ra,
        dec=dec,
        overwrite=True,
        tool=barycenter_tool(current_config),
        ephem=barycenter_ephemeris(current_config),
    )
    logger.info(f"RXTE processing complete: {barycentered}")
    return barycentered


#: Modified Julian Dates of the four PCA gain changes, each of which starts a new epoch.
#:
#: The high voltage was retuned on 1996-03-21, 1996-04-15, 1999-03-22 and 2000-05-13, and
#: the same pulse height means a different energy either side of each. Epoch 5 runs from
#: the last of them to the end of the mission.
GAIN_EPOCH_EDGES = (50163.0, 50188.0, 51259.0, 51677.0)

#: Which anode identifiers make up each xenon layer. The two per layer are the left and
#: right halves of the same volume; the layer number is what matters.
#:
#: Layer 1 is the top one, nearest the window. It sees the largest fraction of a faint
#: source's 2-10 keV counts against the smallest background, so it is the default
#: selection for a source like M82 X-2 that is far below the background.
LAYER_ANODES = {1: (10, 11), 2: (20, 21), 3: (30, 31)}

#: The layers kept when joining pointings, unless told otherwise.
DEFAULT_LAYERS = (1,)


def pca_gain_epoch(mjd):
    """
    The PCA gain epoch a date falls in, 1 to 5.

    Parameters
    ----------
    mjd : float
        Modified Julian Date. A date on a gain change belongs to the epoch it starts.

    Returns
    -------
    int
        The epoch number, as the PCA calibration uses it.
    """
    return 1 + int(np.searchsorted(GAIN_EPOCH_EDGES, float(mjd), side="right"))


def _mjd_of(header, met):
    """A mission elapsed time turned into an MJD with the file's own reference."""
    return float(header.get("MJDREFI", 0)) + float(header.get("MJDREFF", 0.0)) + met / 86400.0


def join_rxte_events(files, outfile, pcus=None, layers=DEFAULT_LAYERS, extra_header=None):
    """
    Fuse several reduced pointings into one event list, to be searched as a whole.

    RXTE pointings on M82 are short -- a median of about a kilosecond -- and a coherent
    search over one of them has no sensitivity to a 0.73 Hz pulsar at the flux of X-2.
    Joining the pointings of a few days multiplies the exposure while the frequency drift
    stays inside one Fourier bin, which is the only way this search can work at all.

    The inputs must be barycentred and must share a gain epoch: the merged file carries a
    single ``TEVTB2`` and a single epoch, and stingray turns ``PHA`` into keV from them,
    so mixing epochs would silently mislabel the energies of half the events.

    Parameters
    ----------
    files : list of str
        Reduced pointings, as :func:`process_rxte_obsid` writes them. Order does not
        matter; the output is sorted by time.
    outfile : str
        Where to write.
    pcus : sequence of int, optional
        Keep only these units. ``None`` keeps whatever each pointing was screened for.
    layers : sequence of int, optional
        Keep only these xenon layers, by :data:`LAYER_ANODES`. Defaults to the top layer;
        ``None`` keeps all three.
    extra_header : dict, optional
        Keywords to add to the event extension.

    Returns
    -------
    str or None
        ``outfile``, or ``None`` if the selection kept no events, in which case no file is
        written.

    Raises
    ------
    ValueError
        If the pointings span more than one gain epoch, or disagree on ``TEVTB2``.

    Notes
    -----
    The good time intervals are the union of the inputs', so the time between two
    pointings stays a gap: ``EXPOSURE`` is the good time summed over the intervals and
    never the span from the first event to the last.
    """
    anodes = None
    if layers is not None:
        anodes = np.array([a for layer in layers for a in LAYER_ANODES[layer]])

    chunks, intervals, headers, epochs, formats = [], [], [], set(), set()
    for path in sorted(files):
        with fits.open(path) as hdul:
            hdu = hdul["XTE_SE"]
            header = hdu.header
            data = hdu.data
            epochs.add(pca_gain_epoch(_mjd_of(header, float(header["TSTART"]))))
            if "TEVTB2" in header:
                formats.add(header["TEVTB2"].strip())
            headers.append(header)
            for extension in hdul:
                if extension.name == "GTI":
                    intervals.append(
                        np.column_stack(
                            [
                                np.asarray(extension.data["START"], dtype=float),
                                np.asarray(extension.data["STOP"], dtype=float),
                            ]
                        )
                    )
            times = np.asarray(data["TIME"], dtype=float) + float(header.get("TIMEZERO", 0.0))
            pcuid = np.asarray(data["PCUID"])
            anodeid = np.asarray(data["ANODEID"])
            keep = np.ones(times.size, dtype=bool)
            if pcus is not None:
                keep &= np.isin(pcuid, np.asarray(pcus))
            if anodes is not None:
                keep &= np.isin(anodeid, anodes)
            if keep.any():
                chunks.append(
                    (times[keep], pcuid[keep], anodeid[keep], np.asarray(data["PHA"])[keep])
                )

    if len(epochs) > 1:
        raise ValueError(
            f"these pointings span gain epochs {sorted(epochs)}; PHA means a different "
            "energy in each, so they cannot share one calibrated event list"
        )
    if len(formats) > 1:
        raise ValueError(f"these pointings disagree on TEVTB2: {sorted(formats)}")
    if not chunks:
        return None

    times, pcuid, anodeid, pha = (np.concatenate(c) for c in zip(*chunks))
    order = np.argsort(times, kind="stable")
    union = merge_intervals(np.concatenate(intervals + [np.zeros((0, 2))]))

    events = fits.BinTableHDU(
        Table(
            [times[order], pcuid[order], anodeid[order], pha[order]],
            names=("TIME", "PCUID", "ANODEID", "PHA"),
        ),
        name="XTE_SE",
    )
    _copy_keywords(events.header, headers[0], [k for k in KEPT_KEYWORDS if k != "OBS_ID"])
    events.header["TIMEZERO"] = (0.0, "absolute times: TIMEZERO is already in TIME")
    events.header["TSTART"] = (float(union[0][0]), "start of the first good time interval")
    events.header["TSTOP"] = (float(union[-1][1]), "end of the last good time interval")
    exposure = float((union[:, 1] - union[:, 0]).sum())
    events.header["ONTIME"] = (exposure, "good time summed over the pointings")
    events.header["EXPOSURE"] = (exposure, "good time summed over the pointings; no deadtime")
    events.header["GAINEPOC"] = (sorted(epochs)[0], "PCA gain epoch, one for the whole file")
    events.header["NPOINT"] = (len(headers), "pointings joined")
    kept_pcus = sorted(set(int(p) for p in pcuid))
    events.header["PCUS"] = (",".join(str(p) for p in kept_pcus), "PCUs present")
    events.header["LAYERS"] = (
        ",".join(str(layer) for layer in layers) if layers is not None else "1,2,3",
        "xenon layers kept",
    )
    for keyword, value in (extra_header or {}).items():
        events.header[keyword] = value
    for obsid in [h["OBS_ID"] for h in headers if "OBS_ID" in h]:
        events.header["HISTORY"] = f"joined {obsid}"

    _write([fits.PrimaryHDU(), events, _gti_hdu(union, "GTI", events.header)], outfile)
    return outfile


def observation_windows(starts, stops, max_span, max_gap=None):
    """
    Group pointings into the stretches that can be searched coherently together.

    A window is grown greedily from the earliest pointing not yet in one, and closed as
    soon as adding the next would take it past ``max_span``. The span is the knob that
    matters: it is how far the frequency model has to hold, and the number of trials in a
    blind search grows with it.

    Parameters
    ----------
    starts, stops : sequence of float
        Start and stop times of each pointing, in the same units as ``max_span``. They
        need not arrive sorted; the groups come back in time order and so do their members.
    max_span : float
        Longest window, from the start of its first pointing to the stop of its last.
    max_gap : float, optional
        Close a window rather than bridge a gap longer than this. ``None`` bridges any gap
        that fits inside ``max_span``.

    Returns
    -------
    list of list of int
        Indices into ``starts``, one list per window.
    """
    starts = np.asarray(starts, dtype=float)
    stops = np.asarray(stops, dtype=float)
    order = np.argsort(starts, kind="stable")

    groups, current = [], []
    window_start = last_stop = None
    for index in order:
        if current and (
            stops[index] - window_start > max_span
            or (max_gap is not None and starts[index] - last_stop > max_gap)
        ):
            groups.append(current)
            current = []
        if not current:
            window_start = starts[index]
        current.append(int(index))
        last_stop = max(last_stop, stops[index]) if current[:-1] else stops[index]
    if current:
        groups.append(current)
    return groups


#: When each PCU lost its propane veto layer, as a Modified Julian Date.
#:
#: The propane layer sits in front of the xenon and vetoes charged particles. PCU0 lost
#: its window on 2000-05-12 and PCU1 on 2006-12-25, and from then on their top xenon layer
#: collects a particle background several times the others'. The units keep working, and
#: for a bright source they are still worth using -- for a source far below the background,
#: like M82 X-2, they are not.
PROPANE_LOST = {0: 51676.0, 1: 54094.0}


def pcus_with_propane_veto(mjd):
    """
    The PCUs that still had their propane veto layer on a given date.

    The usual advice to drop PCU0 (and later PCU1) outright costs real exposure early in
    the mission: before 2000-05-12 every unit still had its veto, and the 1997 pointings of
    M82 had only PCU1 and PCU2 switched on, so PCU0 is half again as much collecting area
    for that block.

    Parameters
    ----------
    mjd : float
        Modified Julian Date. A unit is kept on the day it loses its window and dropped
        from the day after, matching how the date is quoted.

    Returns
    -------
    tuple of int
        The usable units, in order.
    """
    return tuple(p for p in ALL_PCUS if float(mjd) <= PROPANE_LOST.get(p, np.inf))
