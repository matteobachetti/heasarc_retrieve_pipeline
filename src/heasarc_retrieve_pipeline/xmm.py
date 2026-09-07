"""
XMM-Newton (EPIC) reduction.

The mission reduced here is XMM-Newton's EPIC cameras -- pn, MOS1 and MOS2 -- in both
imaging and timing modes. RGS and the Optical Monitor are out of scope.

Two things make XMM cheaper than it looks. Its astrometry is good enough that the position
the user gives goes straight into the extraction region, so none of the source-finding
machinery NuSTAR needs applies. And the archive is already reduced: ESA bulk-reprocessed
XMM with SAS 21.51 in November 2024, so the PPS (Pipeline Processing System) event lists
in the archive are what ``epproc`` and ``emproc`` would produce today, minus hours of CPU
per exposure. Reading them is the default route; reprocessing an ODF (Observation Data
File set) from scratch is available through ``config["products"] = "odf"``.

The tasks themselves are run by :mod:`heasarc_retrieve_pipeline.sas`, which is also where
the reason SAS is an environment requirement rather than a dependency is written down.

Most of this module needs no SAS at all: what a PPS file name means, which exposures an
observation contains, where everything goes, what the configuration is, and which stretches
of an exposure survive the flare screening. That is deliberate. There is no
continuous-integration job anywhere that can run a real SAS task, so everything that can be
decided without one is, and the SAS calls that remain are kept thin -- build the argument,
run the task, parse the answer -- with the arguments and the parsing themselves pure
functions that the offline suite does cover.

PPS file names
--------------

A PPS product is named ``P<OBSID><INST><EXPID><PRODUCT><NNNN>.<EXT>``::

    P 0153950401 PN S003 TIEVLI 0000 .FTZ
      |          |  |    |      |     `- FTZ (gzipped FITS), ASC, HTM, PDF
      |          |  |    |      `------- serial number, almost always 0000
      |          |  |    `-------------- six-character product code
      |          |  `------------------- exposure: S (scheduled) or U, and three digits
      |          `---------------------- instrument, or OB/EP for the whole observation
      `--------------------------------- the OBSID

Every field is needed. The instrument says which camera, the product code says which mode,
and the exposure is what pairs an event list with its flare light curve.
"""

import copy
import glob
import gzip
import os
import re
import shutil
from dataclasses import dataclass
from typing import Optional

import numpy as np

from prefect import flow

from .barycenter import barycentered_file_name
from .diagnostics import diagnostics_path, no_record, record_step
from .utils import (
    NO_SCIENCE_DATA,
    absolute_config,
    get_logger,
    good_intervals,
    intervals_above_threshold,
    intervals_removed,
    tool_log_file,
)

#: Configuration a run starts from. ``products`` chooses the route: ``"pps"`` reads the
#: archive's own reduction, ``"odf"`` reprocesses from the raw telemetry. See
#: :func:`xmm_config` for why callers must go through it rather than read this directly.
DEFAULT_CONFIG = dict(
    out_data_path="./",
    input_data_path="./",
    products="pps",
    sas_ccfpath=None,
    src_radius_arcsec=30.0,
    bkg_inner_factor=1.5,
    bkg_outer_factor=3.0,
    # ``None`` means "use the threshold PPS itself chose for this exposure" -- see
    # :func:`xmm_flare_threshold` for why there is no useful number to put here. A
    # dictionary overrides it, keyed by camera (``pn``, ``mos1``, ``mos2``) or by family
    # (``pn``, ``mos``).
    flare_rate_limit=None,
    # The SAS cookbook's numbers, which apply to the light curve the ODF route builds
    # with ``evselect`` above 10 keV, and to nothing else. Kept here so that step 10 does
    # not have to rediscover them.
    odf_flare_rate_limit=dict(pn=0.4, mos=0.35),
    # Warn when the flare screening removes more than this much of an exposure.
    flare_warn_fraction=0.25,
    # Warn when the nearest PPS detection is further than this from the position asked
    # for. Only ever a warning -- see :func:`xmm_check_source_position`.
    position_warn_arcsec=10.0,
    # Timing-mode extraction, in inclusive ``RAWX`` detector columns, keyed by camera or
    # camera family. pn's are the cookbook's; the absence of MOS is deliberate and is
    # what makes a MOS timing exposure skip rather than extract at an invented column --
    # see :func:`xmm_timing_regions`.
    timing_src_rawx=dict(pn=(31, 45)),
    timing_bkg_rawx=dict(pn=(3, 5)),
    # How far from one an ``epatplot`` pattern ratio has to be, in its own error bars,
    # before the spectrum is called piled up -- see :class:`PileupRatios`.
    pileup_warn_sigma=3.0,
    # Spectral grouping. 25 counts a bin is the usual minimum for chi-squared fitting to
    # be approximately valid; ``oversample`` keeps a group from being narrower than 1/3 of
    # the instrument's resolution there, so that the bins stay roughly independent.
    spectrum_min_counts=25,
    spectrum_oversample=3,
)

#: The EPIC cameras, by the two-character instrument code PPS names them with. The codes
#: this does *not* contain matter as much as the ones it does: ``R1`` and ``R2`` are the
#: RGS spectrometers, ``OM`` is the Optical Monitor, and ``EP``, ``OB`` and ``RG`` mark
#: products that belong to the observation rather than to one camera.
EPIC_INSTRUMENTS = {"PN": "pn", "M1": "mos1", "M2": "mos2"}

#: Imaging mode: the camera reads a sky image, and an extraction region is a circle on it.
IMAGING = "imaging"

#: Timing mode: one dimension is collapsed to read the source faster, so there is no sky
#: image and an extraction region is a strip of detector columns. Timing is what pn is in
#: on every modern observation of a bright X-ray binary, which is the population this
#: pipeline is aimed at.
TIMING = "timing"

#: Which mode an event list holds, by product code. ``PIEVLI`` is pn imaging and
#: ``MIEVLI`` is MOS imaging; ``TIEVLI`` is timing from either camera. The camera is read
#: from the name's instrument field, not from here.
EVENT_LIST_MODES = {"PIEVLI": IMAGING, "MIEVLI": IMAGING, "TIEVLI": TIMING}

#: Product code of the background flare time series, one per exposure. It serves both
#: modes of an exposure, and PPS does not always write one -- see :class:`Exposure`.
FLARE_LIGHTCURVE_PRODUCT = "FBKTSR"

#: Product code of the calibration index file: the CIF the SOC itself used to make the
#: PPS products, and therefore the right value for ``SAS_CCF`` on the PPS route.
CALIBRATION_INDEX_PRODUCT = "CALIND"

#: What the time system keyword reads once ``barycen`` has done its work. Checking it is
#: the only real evidence the correction happened: the task edits its input in place, so
#: ``produces=IN_PLACE(...)`` can only confirm that the copy we made ourselves still
#: exists. This is ``epatplot``'s lesson again -- the task's answer is a header keyword.
BARYCENTRED_TIMESYS = "TDB"

#: And what the reference position becomes.
BARYCENTRED_TIMEREF = "SOLARSYSTEM"

#: What ``epproc`` and ``emproc`` leave behind. Their file names are not parsed: SAS has
#: changed them between releases, and every event list carries the same identity in its
#: header anyway -- see :func:`xmm_exposures_from_odf`.
ODF_EVENT_LIST_GLOB = "*Evts.ds"

#: The two tasks that turn raw telemetry into event lists, and the camera family each
#: covers. They are run separately and a failure of one does not stop the other: an
#: observation with no pn is ordinary, and MOS data is worth reducing without it.
ODF_PIPELINE_TASKS = {"epproc": "pn", "emproc": "mos"}

#: SAS's own instrument names, to the camera names this module reasons in. Written by the
#: same code path in a PPS product and in an ``epproc`` one, which is what makes reading
#: them safer than parsing either route's file names.
SAS_INSTRUMENTS = {"EPN": "pn", "EMOS1": "mos1", "EMOS2": "mos2"}

#: ``DATAMODE``, to :data:`IMAGING` and :data:`TIMING`.
SAS_DATA_MODES = {"IMAGING": IMAGING, "TIMING": TIMING}

#: Header keywords naming an event list's camera, exposure and mode.
INSTRUMENT_KEYWORD = "INSTRUME"
EXPOSURE_ID_KEYWORD = "EXPIDSTR"
DATA_MODE_KEYWORD = "DATAMODE"

#: Suffix of the summary file ``odfingest`` writes and ``barycen`` reads through
#: ``SAS_ODF``. The one the archive ships is ``SUM.ASC``, which is a different file in a
#: different format: ``barycen`` rejects it by name.
ODF_SUMMARY_SUFFIX = "SUM.SAS"

#: How the archive's compressed ODF constituents have to be renamed when staged.
#: HEASARC serves ``.FIT.gz`` and ``.ASC.gz``; SAS reads ``.FIT`` and ``.ASC``. The
#: obvious third option, SAS's own ``.FTZ`` for a gzipped ``.FIT``, is a trap here:
#: ``odfingest`` reads ``.FTZ`` elsewhere but does not *find* one while scanning an ODF
#: directory, so a ``.FTZ``-staged ODF ingests as though the housekeeping were absent and
#: writes a truncated summary that ``barycen`` then rejects with ``UnexpectedEOF``.
#: Measured on ``0870940101``; see docs/xmm_integration_plan.md.
ODF_STAGED_SUFFIXES = {".FIT.gz": ".FIT", ".ASC.gz": ".ASC"}

#: Product code of the maximum-likelihood source list, used only to cross-check the
#: position the user gave. The Optical Monitor writes one under this code too, so the
#: instrument field has to be checked as well -- see :func:`xmm_source_list_file`.
SOURCE_LIST_PRODUCT = "OBSMLI"

#: PPS products the reduction reads, and the extension each of them is the data in. Any
#: instrument may carry these: the parser decides afterwards which camera a file belongs
#: to, and the RGS light curve that comes along costs four kilobytes. Naming the cameras
#: here as well as in :data:`EPIC_INSTRUMENTS` would be two lists that can disagree.
PPS_PRODUCTS_WANTED = {
    "PIEVLI": "FTZ",  # pn imaging event list
    "MIEVLI": "FTZ",  # MOS imaging event list
    "TIEVLI": "FTZ",  # timing event list, either camera
    "FBKTSR": "FTZ",  # background flare time series, one per exposure
    "CALIND": "FTZ",  # calibration index -- becomes SAS_CCF
    "ATTTSR": "FTZ",  # attitude
    "ORBTSR": "FTZ",  # orbit, which the barycentring needs
}

#: PPS products wanted only in their EPIC copy. ``OBSMLI`` is the reason this is a
#: separate list: the Optical Monitor emits one too, so a filter matching the product code
#: alone downloads an optical catalogue and the position cross-check compares an X-ray
#: source with it. ``REGION`` and ``SUMMAR`` are here because the EPIC ones are the ones
#: worth keeping, not because anything would break.
EPIC_PRODUCTS_WANTED = {
    "OBSMLI": "FTZ",  # maximum-likelihood source list
    "REGION": "ASC",  # the regions PPS itself extracted with
    "SUMMAR": "HTM",  # the summary page a human opens
}

#: ODF files downloaded on *both* routes: the observation's housekeeping. Five megabytes,
#: almost all of it ``RAS.ASC``, and the PPS products carry no orbit or attitude of their
#: own, so the barycentring has nowhere else to read them from. Named by the last part of
#: the ODF file name, which is ``<revolution>_<OBSID>_SCX00000<CODE>.<EXT>``.
ODF_HOUSEKEEPING_WANTED = (
    "ATS.FIT",  # attitude history
    "RAS.ASC",  # raw attitude
    "ROS.ASC",  # reconstructed orbit
    "SUM.ASC",  # the observation summary odfingest reads
    "TCS.FIT",  # time correlation
    "TCX.FIT",  # time correlation, extended
)


def _alternation(codes):
    """``(?:A|B|C)`` from an iterable, in a fixed order so the regex is reproducible."""
    return "(?:" + "|".join(sorted(codes)) + ")"


def _pps_include_pattern():
    """
    The part of the download filter that matches PPS products.

    Anchored at both ends: on ``/PPS/`` at the front, so nothing outside the archive's own
    reduction can match, and on the extension at the back, so the ``.PDF`` and ``.PNG``
    pictures PPS writes beside its data files are left at the archive. A picture of a
    light curve has the same name as the light curve.
    """
    any_instrument = "".join(
        [
            r"[A-Z0-9]{2}[A-Z]\d{3}",
            _alternation(PPS_PRODUCTS_WANTED),
            r"\d{4}\.FTZ",
        ]
    )
    epic_only = _alternation(
        rf"{code}\d{{4}}\.{extension}" for code, extension in EPIC_PRODUCTS_WANTED.items()
    )
    return rf"/PPS/P\d{{10}}(?:{any_instrument}|EPX000{epic_only})$"


def _odf_housekeeping_pattern():
    """
    The part of the download filter that matches the ODF housekeeping.

    ``.gz`` is optional because the archive gzips most of these and not ``SUM.ASC``, and
    because a local mirror may have unpacked them.
    """
    codes = _alternation(name.replace(".", r"\.") for name in ODF_HOUSEKEEPING_WANTED)
    return rf"/ODF/\d{{4}}_\d{{10}}_SCX00000{codes}(?:\.gz)?$"


#: What the PPS route downloads: the archive's own reduction of the EPIC cameras, and the
#: housekeeping both routes need. Measured on the whole of Her X-1 ``0153950401``, this is
#: 19 files and 39.8 MB out of 461 files and 205.8 MB.
PPS_DOWNLOAD_RE = f"(?:{_pps_include_pattern()})|(?:{_odf_housekeeping_pattern()})"

#: What the ODF route downloads: the raw telemetry, all of it. There is no cheaper answer
#: -- ``odfingest`` wants the directory it was given, not a chosen part of it.
ODF_DOWNLOAD_RE = r"/ODF/"


def xmm_download_filter(config):
    """
    What of an XMM observation directory to download, for the route this run is taking.

    An observation is 200 MB to 1.2 GB and a reduction of the EPIC cameras wants about a
    fortieth of it, so this is what decides whether a long observation is an ordinary
    download. It matters more than it looks: *short* and *small* are different axes, and
    the filter decouples them. SAX J1808 ``0804330201`` is 35 ks and 393 MB in the
    archive; on the PPS route it is a 77 MB download.

    Called through ``core.mission_download_filter``, which is why it returns keyword
    arguments for :func:`~heasarc_retrieve_pipeline.core.recursive_download` rather than a
    pattern. The regular expression is matched against the whole remote name -- an HTTPS
    URL or an S3 bucket key, depending on the transport -- so it is anchored on the
    ``/PPS/`` and ``/ODF/`` the two spellings have in common.

    Parameters
    ----------
    config : dict
        The run's configuration. Only ``products`` is read.

    Returns
    -------
    dict
        ``re_include`` for :func:`~heasarc_retrieve_pipeline.core.recursive_download`.

    Raises
    ------
    ValueError
        If ``products`` is neither ``"pps"`` nor ``"odf"``. Falling back to "download
        everything" would answer a typo in a configuration file with a gigabyte.

    Examples
    --------
    >>> sorted(xmm_download_filter({"products": "odf"}))
    ['re_include']
    """
    products = config.get("products", DEFAULT_CONFIG["products"])
    if products == "pps":
        return {"re_include": PPS_DOWNLOAD_RE}
    if products == "odf":
        return {"re_include": ODF_DOWNLOAD_RE}
    raise ValueError(f"XMM has a 'pps' route and an 'odf' route, not {products!r}.")


#: The archive's own reduction of an observation lives in this subdirectory, and the raw
#: telemetry in that one. Compared case-insensitively and without the trailing slash, so
#: that the answer does not depend on which transport did the listing.
PPS_DIRECTORY = "PPS"
ODF_DIRECTORY = "ODF"


def xmm_route_from_listing(entries):
    """
    Which route an observation directory can support, read off its top level.

    Parameters
    ----------
    entries : iterable of str
        Names directly under the observation directory, as
        :func:`~heasarc_retrieve_pipeline.core.list_archive_directory` returns them.

    Returns
    -------
    str or None
        ``"pps"``, ``"odf"``, or ``None`` when the directory holds neither. ``None`` is
        not a third route: it means this function has been shown something it does not
        recognise, and the caller should leave the route where it was rather than turn an
        unknown into a quarter of a gigabyte of download.

    Examples
    --------
    >>> xmm_route_from_listing(["4XMM/", "ODF/", "PPS/", "om_mosaic/"])
    'pps'
    >>> xmm_route_from_listing(["ODF/"])
    'odf'
    """
    names = {entry.strip("/").upper() for entry in entries}
    if PPS_DIRECTORY in names:
        return "pps"
    if ODF_DIRECTORY in names:
        return "odf"
    return None


def xmm_resolve_config(config, url):
    """
    The configuration this observation will be reduced with, after looking at the archive.

    Called through ``core.mission_resolve_config``, before the download.

    ``xmmmaster``'s ``pps_flag`` is a hint and not a guarantee. ``0973390101`` is flagged
    ``Y`` and HEASARC mirrors no PPS directory for it: 103 files, every one under
    ``ODF/``. So the flag is not read at all, and the route is settled by one listing of
    the observation directory -- which costs a single request, against a download of tens
    of megabytes that would otherwise arrive with nothing to reduce in it.

    The change only ever goes one way, from ``"pps"`` to ``"odf"``. Asking to reprocess
    from the telemetry is legitimate even when the archive's own products are there -- an
    old ``sas_version``, or a doubt about the products -- so a run that asked for the ODF
    route keeps it, and the archive is not listed at all.

    Parameters
    ----------
    config : dict or None
        What the caller asked for; merged over the defaults by :func:`xmm_config`.
    url : str
        Where this observation will be downloaded from.

    Returns
    -------
    dict
        A complete configuration. The caller's dictionary is not modified.
    """
    # Imported here and not at the top of the module: ``core`` imports every mission, so a
    # mission that imported ``core`` in return could not be loaded at all.
    from .core import list_archive_directory

    config = xmm_config(config)
    logger = get_logger()

    if config["products"] != "pps":
        logger.info(f"Reducing from the ODF as asked; not looking at what {url} holds")
        return config

    # The two ways of learning nothing are kept apart, because they are different facts:
    # one is about this machine's network and the other about the archive.
    entries = list_archive_directory(url)
    if entries is None:
        logger.warning(f"Could not list {url}; going on with the {config['products']} route")
        return config

    available = xmm_route_from_listing(entries)
    if available is None:
        logger.warning(
            f"{url} holds neither a {PPS_DIRECTORY} nor an {ODF_DIRECTORY} directory; "
            f"going on with the {config['products']} route"
        )
        return config

    if available != config["products"]:
        logger.info(
            f"{url} holds no {PPS_DIRECTORY} directory, so this observation is reduced "
            f"from its ODF rather than from the archive's own products"
        )
        config["products"] = available
    return config


#: A PPS file name, field by field. Anchored at both ends on purpose: the summary pages
#: are named ``PP<OBSID>EEVLIS000_0.HTM``, which contains ``EVLI`` and would otherwise be
#: read as an event list of an observation called ``0153950401E``.
PPS_NAME_RE = re.compile(
    r"^P(?P<obsid>\d{10})"
    r"(?P<instrument>[A-Z0-9]{2})"
    r"(?P<expid>[A-Z]\d{3})"
    r"(?P<product>[A-Z0-9]{6})"
    r"(?P<index>\d{4})"
    r"\.(?P<extension>[A-Z]{3})$"
)


@dataclass(frozen=True)
class PpsName:
    """
    The fields of one PPS file name. See the module docstring for the layout.

    Attributes
    ----------
    obsid : str
        The ten-digit observation identifier.
    instrument : str
        Two-character instrument code, as written -- ``"PN"``, ``"M1"``, ``"OM"``.
        :data:`EPIC_INSTRUMENTS` turns the ones we reduce into camera names.
    expid : str
        Exposure identifier: ``"S"`` for a scheduled exposure or ``"U"`` for an
        unscheduled one, and three digits. ``"X000"`` on products that belong to the whole
        observation.
    product : str
        Six-character product code -- ``"TIEVLI"``, ``"FBKTSR"``, ``"CALIND"``.
    index : str
        Serial number, four digits.
    extension : str
        ``"FTZ"`` for gzipped FITS, and ``"ASC"``, ``"HTM"`` or ``"PDF"`` for the
        companions PPS writes beside it.
    """

    obsid: str
    instrument: str
    expid: str
    product: str
    index: str
    extension: str


@dataclass(frozen=True)
class Exposure:
    """
    One camera reading in one mode, and the files that hold it.

    The identity of an exposure is ``(instrument, expid, mode)``, and the mode is not
    decoration. MOS ``FastUncompressed`` puts the central CCD in timing and the outer six
    in imaging, and PPS writes *both* under one exposure identifier -- ``M1S004`` of
    ``0153950401`` is exactly that. Keyed on the camera and the exposure alone, one of the
    two would be dropped in silence.

    Attributes
    ----------
    instrument : str
        ``"pn"``, ``"mos1"`` or ``"mos2"``.
    expid : str
        Exposure identifier, ``"S004"``.
    mode : str
        :data:`IMAGING` or :data:`TIMING`.
    event_list : str
        Path of the event list.
    flare_lightcurve : str or None
        Path of the exposure's background time series, which the flare screening
        thresholds. ``None`` when PPS wrote none: this is not an oversight, and it happens
        on real data -- ``0153950401`` has no ``FBKTSR`` for its pn timing exposure.
    submode : str or None
        ``"FastTiming"``, ``"PrimeFullWindow"``. A header keyword, not a name field, so it
        is ``None`` until something opens the file.
    """

    instrument: str
    expid: str
    mode: str
    event_list: str
    flare_lightcurve: Optional[str] = None
    submode: Optional[str] = None


def xmm_config(config):
    """
    The configuration one XMM reduction runs with: the caller's, over the defaults.

    ``utils.absolute_config`` replaces the whole configuration with the mission default
    only when it is given ``None``. A caller who names some keys and not others -- which
    is what ``core.download_and_process_observation`` does, passing the two paths and
    nothing else -- gets exactly what they passed, and every default then has to be
    repeated at each point of use. That is how ``config["products"]`` would come to raise
    ``KeyError`` halfway through a reduction. Merging once, here, keeps the defaults in
    the one place they are written down.

    Parameters
    ----------
    config : dict or None
        What the caller asked for. ``None`` means "all defaults".

    Returns
    -------
    dict
        A new dictionary. Neither the caller's nor :data:`DEFAULT_CONFIG` is modified, and
        the two path entries are absolute.

    Examples
    --------
    >>> xmm_config({"products": "odf"})["src_radius_arcsec"]
    30.0
    >>> xmm_config(None)["products"]
    'pps'
    """
    # Deep, because ``flare_rate_limit`` is a dictionary of its own: a shallow copy would
    # hand every run the same one, and the first reduction to adjust a threshold would
    # adjust it for the whole process.
    merged = copy.deepcopy(DEFAULT_CONFIG)
    merged.update(copy.deepcopy(config or {}))
    return absolute_config(merged, DEFAULT_CONFIG)


def parse_pps_name(name):
    """
    Read a PPS file name into its fields, or say it is not one.

    Parameters
    ----------
    name : str
        A file name, with no directory part.

    Returns
    -------
    PpsName or None
        ``None`` for anything that is not a PPS product name.

    Examples
    --------
    >>> parse_pps_name("P0153950401PNS003TIEVLI0000.FTZ").product
    'TIEVLI'
    >>> parse_pps_name("PP0153950401EEVLIS000_0.HTM") is None
    True
    """
    match = PPS_NAME_RE.match(os.path.basename(name))
    return PpsName(**match.groupdict()) if match else None


#: Keyword PPS writes into a ``FBKTSR``'s ``RATE`` header: "Optimised flare cut
#: threshold", in the same counts/s the ``RATE`` column is in. It is the SOC's own answer
#: for that one exposure, and it is the default this module thresholds at.
FLARE_THRESHOLD_KEYWORD = "FLCUTTHR"

#: Extension of a ``FBKTSR`` holding the time series.
FLARE_LIGHTCURVE_EXTENSION = "RATE"


@dataclass(frozen=True)
class FlareLightCurve:
    """
    One exposure's background time series, as much of it as the screening needs.

    Attributes
    ----------
    time : numpy.ndarray
        Bin *centres*, in the mission time of the event lists.
    rate : numpy.ndarray
        Background count rate. ``NaN`` in bins with no exposure, and real curves have
        them -- 322 of the 6181 bins of Mkn 421's pn.
    rate_error : numpy.ndarray or None
        Its uncertainty, recorded for the figure and not used in the arithmetic.
    cadence : float
        Bin width, ``TIMEDEL``. 26 s for MOS and 10 s for pn on the observations measured.
    tstart, tstop : float
        Bounds of the exposure the curve covers.
    pps_threshold : float or None
        :data:`FLARE_THRESHOLD_KEYWORD`, if the file carries one.
    """

    time: np.ndarray
    rate: np.ndarray
    rate_error: Optional[np.ndarray]
    cadence: float
    tstart: float
    tstop: float
    pps_threshold: Optional[float]


def read_flare_lightcurve(path):
    """
    Read a PPS ``FBKTSR`` background time series.

    ``TIME`` holds bin *centres* -- verified against the archive, where the first sample
    sits exactly half a ``TIMEDEL`` after ``TSTART`` and the spacing is exactly
    ``TIMEDEL``. That is what :func:`~heasarc_retrieve_pipeline.utils.intervals_above_threshold`
    assumes when it takes a sample to cover ``[t - cadence/2, t + cadence/2]``, so the
    two agree without anything having to be shifted.

    Parameters
    ----------
    path : str
        The ``FBKTSR`` file.

    Returns
    -------
    FlareLightCurve
    """
    from astropy.io import fits

    with fits.open(path) as hdul:
        table = hdul[FLARE_LIGHTCURVE_EXTENSION]
        header = table.header
        time = np.asarray(table.data["TIME"], dtype=float)
        # PPS marks a bin with no exposure using a *signalling* NaN -- bit pattern
        # 0x7f800001, not the 0x7fc00000 a quiet NaN has. Widening one of those to double
        # raises the processor's invalid-operation flag, which numpy reports as
        # "invalid value encountered in cast"; a real Mkn 421 light curve has 322 of them
        # and would warn every time it was read. The value that comes out is an ordinary
        # quiet NaN, so nothing downstream needs to know about any of this.
        with np.errstate(invalid="ignore"):
            rate = np.asarray(table.data["RATE"], dtype=float)
            error = "ERROR" in table.columns.names
            rate_error = np.asarray(table.data["ERROR"], dtype=float) if error else None

    cadence = header.get("TIMEDEL")
    if cadence is None:
        cadence = float(np.median(np.diff(time))) if time.size > 1 else 0.0

    return FlareLightCurve(
        time=time,
        rate=rate,
        rate_error=rate_error,
        cadence=float(cadence),
        tstart=float(header["TSTART"]),
        tstop=float(header["TSTOP"]),
        pps_threshold=header.get(FLARE_THRESHOLD_KEYWORD),
    )


def camera_family(instrument):
    """
    ``"pn"`` or ``"mos"``, the two names the SAS documentation uses for these detectors.

    Examples
    --------
    >>> camera_family("mos2")
    'mos'
    >>> camera_family("pn")
    'pn'
    """
    return "mos" if instrument.startswith("mos") else instrument


def xmm_flare_threshold(curve, instrument, config):
    """
    The background rate above which this exposure is counted as flaring, and where it
    came from.

    **The SAS cookbook's 0.4 and 0.35 counts/s do not belong here**, and this is the one
    thing about the flare screening that had to be measured rather than reasoned about.
    Those numbers are for a light curve you build yourself with ``evselect`` above 10 keV
    over the whole field -- which is what the ODF route does, and what
    ``config["odf_flare_rate_limit"]`` keeps them for. A PPS ``FBKTSR`` is made by
    ``epiclccorr`` and is on quite another scale. Measured across the archive:

    ==========================  ========  ======  =========
    exposure                    median      max   FLCUTTHR
    ==========================  ========  ======  =========
    ``0153950401`` MOS1 S004        36.0    54.2       54.2
    ``0153950401`` MOS2 S005        45.1   102.4       82.1
    ``0123700101`` pn S003           2.2  1351.9        3.4
    ``0123700101`` MOS1 S001         1.0   228.4        1.8
    ``0123700101`` MOS1 U002        20.5    67.4       43.4
    ``0804330201`` MOS1 S002         0.9     1.7        1.7
    ==========================  ========  ======  =========

    A fixed 0.35 would throw away every bin of the first row and none of the last. And
    the third and fifth rows are the same camera in the same observation, twenty times
    apart, which is why no single number can do this job at all.

    PPS has already done it, per exposure, and written the answer into the file. When it
    finds no flare it sets the keyword fractionally above the largest rate in the curve,
    so nothing is cut -- and since the keyword is stored at full precision and the rates
    are single precision, that comparison has no edge case.

    Parameters
    ----------
    curve : FlareLightCurve
        The exposure's background time series.
    instrument : str
        ``"pn"``, ``"mos1"`` or ``"mos2"``.
    config : dict
        ``flare_rate_limit`` is read: ``None`` to use PPS's own value, or a dictionary
        keyed by camera or by camera family. The camera wins over its family.

    Returns
    -------
    tuple
        ``(threshold, source)``, where ``source`` is ``"config"``, ``"pps"``, or ``None``
        when there is no threshold to be had and the exposure cannot be screened.
    """
    limits = config.get("flare_rate_limit") or {}
    for key in (instrument, camera_family(instrument)):
        if key in limits:
            return float(limits[key]), "config"

    if curve.pps_threshold is not None:
        return float(curve.pps_threshold), "pps"
    return None, None


def xmm_flare_gti(exposure, config, rec=None):
    """
    The stretches of one exposure that the background was quiet enough to keep.

    Soft protons funnelled by the mirrors raise EPIC's background by orders of magnitude
    for minutes at a time, and the standard treatment is to cut on a background light
    curve. All of that is arithmetic on numbers PPS has already produced, so no SAS task
    is involved and the whole of it is testable offline.

    Two things are recorded rather than decided here. The exposure that survives is
    recorded because a cut that removed too much and a cut that removed nothing both
    leave an output file that looks perfectly good, and the light curve is recorded
    because the threshold only means something drawn against it.

    **A timing exposure is screened with the imaging curve of the same exposure**, where
    there is one. MOS ``FastUncompressed`` reads its central CCD in timing and its outer
    six in imaging, so the background curve of ``M1S004`` is built from a field the timing
    event list does not have. That is not a reason to skip the cut: soft-proton flares
    illuminate the whole detector, so a flare seen in the outer CCDs is happening during
    the timing readout too. The curve's provenance is recorded, so the choice is visible.

    Parameters
    ----------
    exposure : Exposure
        Which camera, and where its light curve is.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    rec : :class:`heasarc_retrieve_pipeline.diagnostics.StepRecord`, optional
        Where the numbers go. ``None`` records nothing.

    Returns
    -------
    numpy.ndarray or None
        Shape ``(N, 2)``, sorted and disjoint. Empty when the whole exposure was flaring.
        ``None`` when the exposure could not be screened at all, which is not a failure:
        PPS wrote no ``FBKTSR`` for the pn timing exposure of Her X-1, and that is real
        data rather than a broken download.
    """
    logger = get_logger()
    if rec is None:
        rec = no_record()

    if exposure.flare_lightcurve is None:
        reason = f"{exposure.instrument} {exposure.expid} has no background light curve"
        logger.info(f"Not screening for flares: {reason}")
        rec.skip(reason)
        return None

    curve = read_flare_lightcurve(exposure.flare_lightcurve)
    threshold, source = xmm_flare_threshold(curve, exposure.instrument, config)
    if threshold is None:
        reason = f"{os.path.basename(exposure.flare_lightcurve)} carries no flare threshold"
        logger.warning(f"Not screening for flares: {reason}")
        rec.skip(reason)
        return None

    flaring = intervals_above_threshold(curve.time, curve.rate, threshold, cadence=curve.cadence)
    # ``good_intervals`` merges, clips and sorts what it is given, so its result already
    # has the three properties a GTI list must have and needs no tidying afterwards.
    gti = good_intervals(flaring, curve.tstart, curve.tstop)

    whole = np.array([[curve.tstart, curve.tstop]], dtype=float)
    before = float(curve.tstop - curve.tstart)
    after = float(np.sum(gti[:, 1] - gti[:, 0])) if gti.size else 0.0
    removed_fraction = 1.0 - after / before if before > 0 else 0.0

    rec.value(
        instrument=exposure.instrument,
        expid=exposure.expid,
        mode=exposure.mode,
        threshold=threshold,
        threshold_source=source,
        bin_seconds=curve.cadence,
        light_curve=os.path.basename(exposure.flare_lightcurve),
        exposure_before=before,
        exposure_after=after,
        removed_fraction=removed_fraction,
        n_intervals_kept=len(gti),
    )
    arrays = dict(
        lc_time=curve.time,
        lc_rate=curve.rate,
        gti_before=whole,
        gti_after=gti,
        removed=intervals_removed(whole, gti),
    )
    if curve.rate_error is not None:
        arrays["lc_rate_err"] = curve.rate_error
    rec.array(**arrays)

    where = f"{exposure.instrument} {exposure.expid} {exposure.mode}"
    if removed_fraction > config["flare_warn_fraction"]:
        logger.warning(
            f"Flare screening removed {removed_fraction:.0%} of {where} "
            f"({before - after:.0f} s of {before:.0f} s) above {threshold:g} counts/s"
        )
    else:
        logger.info(f"Flare screening kept {after:.0f} s of {before:.0f} s of {where}")
    return gti


def xmm_base_output_path(obsid, config):
    """
    Top-level output directory of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>``.
    """
    return os.path.join(config["out_data_path"], obsid)


def xmm_pps_path(obsid, config):
    """
    Directory holding the archive's own reduction of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str
        ``<input_data_path>/<OBSID>/PPS``. Capitalised because that is how HEASARC serves
        it, and a case-insensitive filesystem would hide the mistake until the pipeline
        ran on Linux.
    """
    return os.path.join(config["input_data_path"], obsid, "PPS")


def xmm_odf_path(obsid, config):
    """
    Directory holding the raw telemetry of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str
        ``<input_data_path>/<OBSID>/ODF``. Downloaded on both routes: it is 3.8 MB of
        housekeeping, and it is what the barycentring needs.
    """
    return os.path.join(config["input_data_path"], obsid, "ODF")


def xmm_pipeline_output_path(obsid, config):
    """
    Where the cleaned event lists go.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl``. The name is NuSTAR's, and deliberately so:
        ``report.OBSERVATION_SUBDIRECTORIES`` already recognises it, so ``hrp-report``
        finds an XMM tree without being taught anything.
    """
    return os.path.join(xmm_base_output_path(obsid, config), "event_cl")


def xmm_product_output_path(obsid, config):
    """
    Where the spectra and their responses go.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/products``, for the same reason as
        :func:`xmm_pipeline_output_path`.
    """
    return os.path.join(xmm_base_output_path(obsid, config), "products")


def _pps_files(obsid, config):
    """
    Every PPS file of an observation, as ``(PpsName, path)`` pairs, sorted by name.

    Files whose names are not PPS product names are dropped, which is what keeps the
    summary pages out. An observation that was never downloaded gives an empty list rather
    than raising: whether there is anything to reduce is the caller's decision to make.
    """
    found = []
    for path in sorted(glob.glob(os.path.join(xmm_pps_path(obsid, config), "*"))):
        parsed = parse_pps_name(path)
        if parsed is not None:
            found.append((parsed, path))
    return found


def _observation_level_file(obsid, config, product, instrument=None, extension="FTZ"):
    """
    The one file of an observation carrying this product code, or ``None``.

    ``instrument`` narrows the search when the code is not unique to one part of the
    telescope, and ``extension`` picks the data file out of the companions PPS writes
    beside it.
    """
    for parsed, path in _pps_files(obsid, config):
        if parsed.product != product or parsed.extension != extension:
            continue
        if instrument is not None and parsed.instrument != instrument:
            continue
        return path
    return None


def xmm_calind_file(obsid, config):
    """
    The calibration index file of an observation, or ``None`` if it was not downloaded.

    On the PPS route this is what ``SAS_CCF`` should point at: it is the index the SOC
    used to make the products being read, so the calibration the reduction applies is the
    calibration the products were made with.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path of the ``CALIND`` product.
    """
    return _observation_level_file(obsid, config, CALIBRATION_INDEX_PRODUCT)


#: What the index this pipeline builds is called. A bare name, not a path: ``cifbuild``
#: runs in the directory the index belongs in, so nothing long reaches a header card.
CALIBRATION_INDEX_NAME = "ccf.cif"

#: Header keyword holding the start of an observation, and the whole input to
#: :func:`xmm_build_calibration_index`.
OBSERVATION_DATE_KEYWORD = "DATE-OBS"


def cifbuild_date(value):
    """
    A ``DATE-OBS`` value as ``cifbuild``'s ``observationdate`` wants it.

    Parameters
    ----------
    value : str
        ``"2002-03-27T21:11:14"``, or a bare date.

    Returns
    -------
    str
        ``"2002-03-27"``. The time of day is dropped because calibration constituents are
        valid over epochs measured in months, not seconds.

    Examples
    --------
    >>> cifbuild_date("2021-04-06T23:02:03")
    '2021-04-06'
    """
    return str(value).split("T")[0].strip()


def xmm_observation_date(event_list):
    """
    When an observation started, read from an event list this pipeline already downloaded.

    This is what makes building the calibration index free of the ODF: the date is in a
    file the PPS route fetches anyway, so there is nothing extra to download.

    Parameters
    ----------
    event_list : str
        Any event list of the observation.

    Returns
    -------
    str or None
        ``"YYYY-MM-DD"``, or ``None`` when no header carries ``DATE-OBS``.
    """
    from astropy.io import fits

    with fits.open(event_list) as hdul:
        for hdu in hdul:
            if OBSERVATION_DATE_KEYWORD in hdu.header:
                return cifbuild_date(hdu.header[OBSERVATION_DATE_KEYWORD])
    return None


def xmm_calibration_index_path(obsid, config):
    """
    Where the calibration index this pipeline builds goes.

    Beside the cleaned events rather than in the downloaded tree, because it is an output
    of the reduction and not something the archive gave us.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl/ccf.cif``.
    """
    return os.path.join(xmm_pipeline_output_path(obsid, config), CALIBRATION_INDEX_NAME)


def xmm_build_calibration_index(obsid, config, event_list, env=None, log_to=None):
    """
    Build the calibration index ``SAS_CCF`` will point at, from the observation date.

    **This is the normal path, not a fallback** -- decided by Matteo, 2026-09-07. The
    reasoning is worth keeping next to the code, because the obvious alternative looks
    better than it is.

    An observation's downloaded ``CALIND`` is the index the SOC used for the November 2024
    reprocessing, and it names the calibration issues current *then*: Her X-1's names
    ``XMM_BORESIGHT_0029.CCF``, where ESA's set now holds 0036. A mirror of ESA's *Valid
    CCF Set* -- which ESA assembles daily and describes as everything needed to process any
    XMM-Newton ODF at the current date -- therefore cannot satisfy an archival ``CALIND``,
    and ``ecoordconv`` fails outright. The fix is not to fetch the superseded issue. That
    would be asking for worse calibration: the full ~1550-file history exists to reproduce
    what was known at a past moment, and a new analysis wants what is known now.

    So the index is built here instead, from the observation's own date, which selects the
    constituents valid for its epoch at their current issue. Measured at 26 s on Her X-1,
    against 524 s for a single pn ``arfgen``, and it happens once per observation rather
    than once per exposure.

    One consequence, stated rather than discovered: PPS event lists were generated with the
    November 2024 calibration, so responses built against a newer index are marginally
    inconsistent with the ``PI`` values in those events. That is the ordinary situation for
    anyone reanalysing archival data with current SAS, and the alternative is the
    superseded calibration just ruled out. ``CALIND`` is still downloaded -- it is about
    90 kB -- so both indices can be named on the report page and the choice stays visible.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    event_list : str
        Any event list of the observation, read for its ``DATE-OBS``.
    env : dict, optional
        Environment for the task, from :func:`heasarc_retrieve_pipeline.sas.sas_environment`.
        ``SAS_CCFPATH`` has to reach the task through it, or through the inherited
        environment; ``SAS_CCF`` is what this call produces and is not needed yet.
    log_to : str, optional
        File the task's output goes to.

    Returns
    -------
    str
        Path of the index, for ``SAS_CCF``.

    Raises
    ------
    ValueError
        When the event list carries no ``DATE-OBS``. Guessing a date would silently pick
        the wrong calibration epoch, which is the one failure mode that does not announce
        itself.
    """
    from . import sas

    date = xmm_observation_date(event_list)
    if date is None:
        raise ValueError(
            f"{event_list} carries no {OBSERVATION_DATE_KEYWORD}, so the calibration "
            "epoch of this observation is unknown and the index cannot be built"
        )

    output = xmm_calibration_index_path(obsid, config)
    os.makedirs(os.path.dirname(output), exist_ok=True)

    sas.run(
        "cifbuild",
        produces=output,
        log_to=log_to,
        env=env,
        cwd=os.path.dirname(output),
        calindexset=os.path.basename(output),
        withobservationdate="yes",
        observationdate=date,
        fullpath="yes",
    )
    return output


def xmm_source_list_file(obsid, config):
    """
    The EPIC maximum-likelihood source list, or ``None`` if it was not downloaded.

    Used only to cross-check the position the user gave -- never to override it, and never
    to fail an observation. It misses real targets: on the Crab, the nearest detection is
    329 arcsec from the pulsar, because the nebula is extended and piled up and
    point-source detection does not find it at all.

    The instrument code is checked, and that is not fussiness. The Optical Monitor emits an
    ``OBSMLI`` too, so ``0153950401`` carries both ``EPX000OBSMLI`` and ``OMX000OBSMLI``;
    matching on the product code alone would cross-check an X-ray position against an
    optical catalogue.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path of the EPIC ``OBSMLI`` product.
    """
    return _observation_level_file(obsid, config, SOURCE_LIST_PRODUCT, instrument="EP")


def xmm_exposures_from_pps(obsid, config):
    """
    What an observation holds, read from the archive's own reduction.

    The front end of the PPS route, and pure parsing: no SAS, no FITS, nothing but file
    names and a directory listing.

    An empty list means there is no EPIC science data here at all, and the flow reports
    that as ``utils.NO_SCIENCE_DATA`` -- a real observation with nothing in it for this
    pipeline, counted separately and left on disk, the treatment a NuSTAR slew gets. Note
    that the test is "no event lists **of any mode**": an observation with nothing but
    timing data is science, not an empty one.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    list of Exposure
        Ordered pn, MOS1, MOS2, then by exposure and mode, so that a run is reproducible
        and the report reads in the order an observer would expect.
    """
    lightcurves = {}
    event_lists = []
    for parsed, path in _pps_files(obsid, config):
        if parsed.instrument not in EPIC_INSTRUMENTS:
            continue
        if parsed.product == FLARE_LIGHTCURVE_PRODUCT and parsed.extension == "FTZ":
            lightcurves[(parsed.instrument, parsed.expid)] = path
        elif parsed.product in EVENT_LIST_MODES and parsed.extension == "FTZ":
            event_lists.append((parsed, path))

    # Mode is in the key and not left to the stability of the listing: the two modes of
    # one exposure are two entries with everything else equal, and which of them comes
    # first should be a decision rather than an accident of alphabetical order.
    cameras = list(EPIC_INSTRUMENTS)
    event_lists.sort(
        key=lambda found: (
            cameras.index(found[0].instrument),
            found[0].expid,
            EVENT_LIST_MODES[found[0].product] != IMAGING,
        )
    )

    return [
        Exposure(
            instrument=EPIC_INSTRUMENTS[parsed.instrument],
            expid=parsed.expid,
            mode=EVENT_LIST_MODES[parsed.product],
            event_list=path,
            flare_lightcurve=lightcurves.get((parsed.instrument, parsed.expid)),
        )
        for parsed, path in event_lists
    ]


#: Header keyword naming the read-out window: ``"PrimeFullWindow"``, ``"PrimePartialW3"``,
#: ``"FastTiming"``. Nothing in a PPS file name carries it.
SUBMODE_KEYWORD = "SUBMODE"

#: Column holding which CCD an event landed on. The window is a property of one chip, so
#: the reach measurement needs it to keep the outer chips from widening a windowed one.
CCD_COLUMN = "CCDNR"

#: What fraction of the events on a chip :func:`xmm_window_reach_arcsec` discards at each
#: end before calling the rest the edge, as a percentage. Not zero: the extremes are set
#: by a few events with badly reconstructed sky coordinates, which inflated one real
#: measurement by a third. At 0.1 a chip holding a hundred thousand events still keeps a
#: hundred beyond the edge, so a genuine corner is not clipped away.
WINDOW_EDGE_PERCENTILE = 0.1

#: The fewest events discarded at each end whatever the percentage says. A percentage
#: alone cannot clip two stray events out of a hundred, and a nearly empty exposure is
#: exactly where one stray does the most damage.
WINDOW_EDGE_MIN_CLIP = 2


def read_submode(event_list):
    """
    Which read-out window an exposure used, from its event list header.

    Parameters
    ----------
    event_list : str
        Path of the event list.

    Returns
    -------
    str or None
        ``"PrimeFullWindow"``, ``"PrimePartialW3"``, ``"FastTiming"``; ``None`` when no
        header carries :data:`SUBMODE_KEYWORD`.
    """
    from astropy.io import fits

    with fits.open(event_list) as hdul:
        for hdu in hdul:
            if SUBMODE_KEYWORD in hdu.header:
                return str(hdu.header[SUBMODE_KEYWORD]).strip()
    return None


def xmm_with_submodes(exposures):
    """
    The same exposures, each carrying the submode read from its own event list.

    A separate pass rather than something :func:`xmm_exposures_from_pps` does, and
    deliberately: that function is pure parsing -- file names and a directory listing, no
    FITS -- which is what lets the offline suite cover the whole front end. Opening the
    files is a different kind of work and is kept where it can be skipped.

    An exposure whose event list cannot be opened keeps ``submode=None`` rather than
    failing the observation. The submode drives a warning and nothing else, so an
    unreadable header is a reason to say less, not a reason to stop.

    Parameters
    ----------
    exposures : list of Exposure
        From :func:`xmm_exposures_from_pps`.

    Returns
    -------
    list of Exposure
        New objects; the ones passed in are not modified.
    """
    logger = get_logger()
    filled = []
    for exposure in exposures:
        submode = None
        try:
            submode = read_submode(exposure.event_list)
        except OSError as problem:
            logger.warning(f"Could not read the submode of {exposure.event_list}: {problem}")
        filled.append(copy.replace(exposure, submode=submode))
    return filled


@dataclass
class WindowFit:
    """
    Whether the background annulus lands on exposed detector, and by how much.

    Attributes
    ----------
    submode : str or None
        The read-out window, for the page to name.
    reach_arcsec : float
        How far there is exposure from the source, measured from the events.
    needed_arcsec : float
        Outer radius of the background annulus the configuration asks for.
    fits : bool
        Whether the second is inside the first.
    """

    submode: Optional[str]
    reach_arcsec: float
    needed_arcsec: float
    fits: bool


def xmm_window_reach_arcsec(event_list, x, y):
    """
    How far from a sky position there is exposed detector, measured from the events.

    **Measured rather than tabulated, and that is the design.** The obvious alternative is
    a table of submodes and their window sizes out of the Users Handbook. The number
    actually wanted is not the window size, though: it is how far there is exposure from
    *this* source in *this* observation, which the window, a chip gap and the edge of the
    field all bound. Measuring it needs no remembered constant and no list of submodes to
    keep current as ESA adds them.

    Only the CCD the source lands on is measured. On MOS ``PrimePartialW3`` the central
    chip is windowed to about 5.5 arcmin while the outer six read out whole, so a bounding
    box over all of them would report the full field and never warn about anything.

    **The edge is a percentile, not the extreme**, and that was measured rather than
    guessed. Taking the minimum and maximum made MOS1's windowed chip on ``0870940101``
    span 8.8 by 11.4 arcmin, when ``PrimePartialW3`` reads out 300 by 300 raw pixels --
    about 5.5. A handful of events with badly reconstructed sky coordinates is enough to
    do that, and the error runs the dangerous way: an inflated reach *suppresses* a
    warning. On that observation's pn exposure the extremes said 102.5 arcsec, which
    clears the 90 the annulus asks for, while :data:`WINDOW_EDGE_PERCENTILE` says 87.6,
    which does not -- and ``BACKSCAL`` settles it independently, coming out 6.01 where the
    radii imply 6.75, an 11 per cent area deficit that only exists if the annulus really
    is clipped.

    The remaining bias runs the safe way: a short exposure of a sparse field does not fill
    its chip to the edges, so the reach comes out short and the check warns when it need
    not have. Warning too often is the cheap error here.

    Parameters
    ----------
    event_list : str
        A cleaned event list with sky coordinates.
    x, y : float
        Sky position of the source, from :func:`xmm_source_sky_position`.

    Returns
    -------
    float or None
        Arcsec to the nearest edge of the source's chip; ``None`` when the file has no sky
        columns, which is what a timing exposure looks like.
    """
    from astropy.io import fits

    with fits.open(event_list) as hdul:
        for hdu in hdul:
            data = getattr(hdu, "data", None)
            names = getattr(getattr(data, "columns", None), "names", None)
            if names is not None and {"X", "Y", CCD_COLUMN} <= set(names):
                break
        else:
            return None

        sky_x = np.asarray(data["X"], dtype=float)
        sky_y = np.asarray(data["Y"], dtype=float)
        chip = np.asarray(data[CCD_COLUMN])
        # The source's own chip is the one its nearest event landed on. Nothing hunts for
        # the chip whose footprint contains the position: an event is direct evidence
        # that there is exposure there, and a footprint would have to be looked up.
        on_chip = chip == chip[np.argmin(np.hypot(sky_x - x, sky_y - y))]

    sky_x, sky_y = sky_x[on_chip], sky_y[on_chip]
    if sky_x.size == 0:
        return None

    clip = max(WINDOW_EDGE_MIN_CLIP, int(sky_x.size * WINDOW_EDGE_PERCENTILE / 100.0))
    if 2 * clip >= sky_x.size:
        # Too few events to discard any and still have a chip left. Whatever they say is
        # all there is, and the caller is warned rather than told nothing.
        clip = 0

    def edges(values):
        ordered = np.sort(values)
        return ordered[clip], ordered[len(ordered) - 1 - clip]

    x_low, x_high = edges(sky_x)
    y_low, y_high = edges(sky_y)
    reach = min(x - x_low, x_high - x, y - y_low, y_high - y)
    return float(max(reach, 0.0)) * SKY_PIXEL_ARCSEC


def xmm_check_extraction_window(exposure, config, events, x, y, rec=None):
    """
    Warn when the background annulus asks for more detector than the exposure has.

    **It warns and never fails**, and the reason is worth stating because it is not "we
    are being lenient". A clipped background region is not silently wrong: SAS's
    ``backscale`` measures the exposed area of a region, and ``BACKSCAL`` follows it --
    which is why Her X-1's source-to-background ratio came out 5.17 against the 5.00 its
    strips imply. What a clipped region costs is *counts*, so the background is noisier
    than it looks, and in the limit there is nothing there at all. That is a judgement for
    whoever reads the page, not a reason to refuse an observation.

    Raised by the pn imaging acceptance target. M82's MOS exposures are
    ``PrimePartialW3``, whose central chip is about 5.5 arcmin across, so the default
    30 arcsec source radius and ``bkg_outer_factor=3.0`` fit with room -- and a caller who
    widens the radius past roughly 55 arcsec stops fitting, with nothing to say so until
    now.

    Parameters
    ----------
    exposure : Exposure
        Which camera, exposure and mode, and where its events are.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    events : str
        The **cleaned** event list, from :func:`xmm_clean_event_list` -- not the exposure's
        own ``event_list``, which is the archive's unscreened one. Measuring the raw list
        answers a different question and answers it wrongly: it carries flagged events out
        to the chip edges, which on ``0870940101``'s pn put the reach at 96.1 arcsec
        against the cleaned 87.6, on either side of the 90 the annulus asks for. The
        extraction runs on the cleaned events, so the window they cover is the window that
        matters.
    x, y : float
        Sky position of the source, from :func:`xmm_source_sky_position`.
    rec : Recorder, optional
        Diagnostics recorder.

    Returns
    -------
    WindowFit or None
        ``None`` for a timing exposure, which has no sky image and so no window to fall
        off, and for an event list with no sky columns.
    """
    if exposure.mode != IMAGING:
        return None

    reach = xmm_window_reach_arcsec(events, x, y)
    if reach is None:
        return None

    needed = config["src_radius_arcsec"] * config["bkg_outer_factor"]
    fit = WindowFit(
        submode=exposure.submode,
        reach_arcsec=reach,
        needed_arcsec=needed,
        fits=needed <= reach,
    )

    rec = rec or no_record()
    rec.value(
        window_submode=fit.submode,
        window_reach_arcsec=fit.reach_arcsec,
        window_needed_arcsec=fit.needed_arcsec,
        window_fits=fit.fits,
    )

    if not fit.fits:
        get_logger().warning(
            f"{exposure.instrument} {exposure.expid}: the background annulus reaches "
            f"{needed:.1f} arcsec but this {fit.submode or 'exposure'} has exposure only "
            f"to {reach:.1f} arcsec from the source. The region is clipped, so the "
            "background is noisier than its area suggests; BACKSCAL still follows the "
            "exposed area, so the scaling stays right."
        )
    return fit


#: Energy range the report draws a spectrum over, in keV. The screening band of
#: :data:`SCREENING_EXPRESSIONS` -- ``PI in [200:12000]`` -- since outside it there are no
#: counts to draw and a log axis would give the empty channels most of the picture.
SPECTRUM_PLOT_BAND_KEV = (0.2, 12.0)

#: Extension of a PPS ``OBSMLI`` holding the detections.
SOURCE_LIST_EXTENSION = "SRCLIST"

#: Column of ``SRCLIST`` carrying the EPIC-combined flux, in erg cm^-2 s^-1. The table has
#: 249 columns and no count rate anywhere, so brightness is reported as a flux or not at
#: all.
SOURCE_LIST_FLUX_COLUMN = "EP_TOT_FLUX"

#: Size of one XMM sky pixel, in arcseconds, from the ``ecoordconv`` documentation
#: (SAS 22.1.0, table 1). Every region radius passes through it, so it is named rather
#: than written out: a bare ``0.05`` in an expression says nothing about what it is.
SKY_PIXEL_ARCSEC = 0.05

#: Standard EPIC event screening, by camera family and mode.
#:
#: ``#XMMEA_EP`` and ``#XMMEA_EM`` are macros SAS expands from the calibration -- they
#: stand for a list of event attributes that changes with the calibration, which is
#: exactly why the screening is left to ``evselect`` instead of being reimplemented on the
#: event table here.
#:
#: ``FLAG==0`` is applied to pn and not to MOS on purpose. It is the strictest possible
#: cut, and on MOS it also throws away good events near the chip edges; the standard SAS
#: threads apply it to pn alone.
#:
#: **The mode changes one number, not the macro.** There is no timing-specific screening
#: macro to change to: ``#XMMEA_EP``, ``#XMMEA_EM`` and ``#XMMEA_SM`` are the only EPIC
#: ones in SAS 22.1.0. What does differ is MOS's pattern cut, and the authority for that
#: is SAS's own automatic reduction: ``xmmextractor`` documents applying ``PATTERN<=4``,
#: ``FLAG==0`` and ``#XMMEA_EP`` to pn, and ``PATTERN<=12`` (imaging) or ``PATTERN==0``
#: (timing) with ``#XMMEA_EM`` to MOS. A MOS timing read-out has one dimension collapsed,
#: so a multi-pixel pattern there is not the split charge cloud it is in an image.
SCREENING_EXPRESSIONS = {
    ("pn", IMAGING): "#XMMEA_EP && (PATTERN<=4) && (PI in [200:12000]) && FLAG==0",
    ("pn", TIMING): "#XMMEA_EP && (PATTERN<=4) && (PI in [200:12000]) && FLAG==0",
    ("mos", IMAGING): "#XMMEA_EM && (PATTERN<=12) && (PI in [200:12000])",
    ("mos", TIMING): "#XMMEA_EM && (PATTERN==0) && (PI in [200:12000])",
}

#: Extension name of the good time interval files this module writes.
GTI_EXTENSION = "STDGTI"


def xmm_screening_expression(instrument, mode, gti_file=None):
    """
    The ``evselect`` expression that cleans one camera's events.

    Parameters
    ----------
    instrument : str
        ``"pn"``, ``"mos1"`` or ``"mos2"``.
    mode : str
        :data:`IMAGING` or :data:`TIMING`. Required rather than defaulted, because a
        timing exposure screened as an image is wrong in a way nothing downstream would
        notice -- see :data:`SCREENING_EXPRESSIONS`.
    gti_file : str, optional
        Good time intervals to apply as well, as written by :func:`write_gti_file`.
        ``None`` screens on event attributes alone, which is what an exposure with no
        background light curve gets.

    Returns
    -------
    str

    Raises
    ------
    KeyError
        For a camera or a mode this module does not know.

    Examples
    --------
    >>> xmm_screening_expression("mos1", IMAGING)
    '#XMMEA_EM && (PATTERN<=12) && (PI in [200:12000])'
    >>> xmm_screening_expression("mos1", TIMING)
    '#XMMEA_EM && (PATTERN==0) && (PI in [200:12000])'
    >>> xmm_screening_expression("mos1", IMAGING, gti_file="flare.gti")
    '#XMMEA_EM && (PATTERN<=12) && (PI in [200:12000]) && gti(flare.gti,TIME)'
    """
    expression = SCREENING_EXPRESSIONS[(camera_family(instrument), mode)]
    if gti_file is None:
        return expression
    return f"{expression} && gti({gti_file},TIME)"


def write_gti_file(path, gti):
    """
    Write good time intervals where ``evselect`` can read them.

    ``evselect`` cannot be handed an array: its ``gti()`` selector names a file, and
    ``selectlib`` requires that file to be an OGIP-standard good time interval table.
    Writing the intervals :func:`xmm_flare_gti` already computed, rather than rebuilding
    them inside SAS with ``tabgtigen``, keeps the intervals that get recorded on the report
    and the intervals that get applied to the events the same intervals. Two derivations of
    one answer is two answers waiting to disagree.

    Parameters
    ----------
    path : str or pathlib.Path
        File to write. Overwritten if it exists.
    gti : numpy.ndarray
        Shape ``(N, 2)``. May be empty, and an empty file is a meaningful one -- it says
        keep nothing, which is what a wholly flared exposure earns.

    Returns
    -------
    str
        The path written, so a caller can pass it straight to
        :func:`xmm_screening_expression`.
    """
    from astropy.io import fits

    gti = np.atleast_2d(np.asarray(gti, dtype=float)).reshape(-1, 2)
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", unit="s", array=gti[:, 0]),
            fits.Column(name="STOP", format="D", unit="s", array=gti[:, 1]),
        ],
        name=GTI_EXTENSION,
    )
    hdu.header["HDUCLASS"] = ("OGIP", "File conforms to OGIP standards")
    hdu.header["HDUCLAS1"] = ("GTI", "Extension contains good time intervals")
    hdu.header["HDUCLAS2"] = ("STANDARD", "Standard good time intervals")
    path = str(path)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return path


def arcsec_to_sky_pixels(arcsec):
    """
    Convert an angle on the sky to XMM sky pixels.

    Examples
    --------
    >>> arcsec_to_sky_pixels(30.0)
    600.0
    """
    return arcsec / SKY_PIXEL_ARCSEC


def circle_region(x, y, radius_arcsec):
    """
    A circular selection in sky coordinates, as SAS spells it.

    Examples
    --------
    >>> circle_region(26000.0, 25000.0, 30.0)
    '((X,Y) IN circle(26000.0000,25000.0000,600.0000))'
    """
    radius = arcsec_to_sky_pixels(radius_arcsec)
    return f"((X,Y) IN circle({x:.4f},{y:.4f},{radius:.4f}))"


def annulus_region(x, y, inner_arcsec, outer_arcsec):
    """
    An annular selection in sky coordinates, as SAS spells it.

    Examples
    --------
    >>> annulus_region(26000.0, 25000.0, 45.0, 90.0)
    '((X,Y) IN annulus(26000.0000,25000.0000,900.0000,1800.0000))'
    """
    inner = arcsec_to_sky_pixels(inner_arcsec)
    outer = arcsec_to_sky_pixels(outer_arcsec)
    return f"((X,Y) IN annulus({x:.4f},{y:.4f},{inner:.4f},{outer:.4f}))"


def xmm_extraction_regions(x, y, config):
    """
    The source and background selections for a point source at a sky position.

    The background is an annulus around the source rather than a circle elsewhere on the
    detector, because EPIC's background varies across the field of view and a concentric
    ring is the closest sample there is. It has to clear the point spread function, which
    is what ``bkg_inner_factor`` is for.

    Parameters
    ----------
    x, y : float
        Sky coordinates, from :func:`xmm_source_sky_position`.
    config : dict
        A complete configuration, from :func:`xmm_config`.

    Returns
    -------
    tuple of str
        ``(source, background)``, both ``evselect`` expressions.
    """
    radius = config["src_radius_arcsec"]
    return (
        circle_region(x, y, radius),
        annulus_region(
            x, y, radius * config["bkg_inner_factor"], radius * config["bkg_outer_factor"]
        ),
    )


def rawx_region(first, last):
    """
    A strip of detector columns, as SAS spells it.

    Examples
    --------
    >>> rawx_region(31, 45)
    '(RAWX in [31:45])'
    """
    return f"(RAWX in [{first}:{last}])"


def _configured_strip(setting, instrument, config):
    """
    One ``RAWX`` strip out of the configuration, by camera then by family.

    The two-level lookup is :func:`xmm_flare_threshold`'s, for the same reason: the
    physics is usually a property of the camera family and occasionally of one camera.
    """
    strips = config[setting] or {}
    strip = strips.get(instrument, strips.get(camera_family(instrument)))
    return None if strip is None else tuple(strip)


def xmm_timing_regions(instrument, config):
    """
    The source and background column strips of a timing exposure.

    **Only pn has default strips, and that is a decision rather than an omission.**
    ``RAWX in [31:45]`` for the source and ``[3:5]`` for the background are the pn Timing
    cookbook's numbers, measured on a read-out whose source column is fixed by the
    boresight. MOS Timing has no equivalent number worth trusting: the source column
    depends on where in the central CCD the target sits, and SAS's own driver
    (``lib/perl5/run_epatplot.pl``) accordingly builds the MOS strip around a source
    position it is given instead of around a constant. Rather than invent one, a MOS
    timing exposure is skipped, loudly, until either a strip is put in the configuration
    or this module learns to find the source column in the data.

    Parameters
    ----------
    instrument : str
        ``"pn"``, ``"mos1"`` or ``"mos2"``.
    config : dict
        A complete configuration, from :func:`xmm_config`.

    Returns
    -------
    tuple of str or None
        ``(source, background)``, both ``evselect`` expressions, or ``None`` when this
        camera has no strips configured. ``None`` is a skip, not a failure.

    Examples
    --------
    >>> xmm_timing_regions("pn", xmm_config({}))
    ('(RAWX in [31:45])', '(RAWX in [3:5])')
    """
    source = _configured_strip("timing_src_rawx", instrument, config)
    background = _configured_strip("timing_bkg_rawx", instrument, config)
    if source is None or background is None:
        get_logger().warning(
            f"No RAWX extraction strips are configured for {instrument} in timing mode, "
            f"so its timing products are skipped. Only pn has defaults: the source "
            f"column of a MOS timing read-out depends on where the target sits in the "
            f"central CCD, and this pipeline will not guess it. Set both "
            f"`timing_src_rawx` and `timing_bkg_rawx` for {instrument} in the "
            f"configuration to extract it anyway."
        )
        return None
    return rawx_region(*source), rawx_region(*background)


def xmm_exposure_regions(exposure, config, sky=None):
    """
    Where one exposure's source and background events come from, whatever its mode.

    Imaging gets a circle and an annulus on the sky; timing gets two strips of detector
    columns. Everything downstream -- the pile-up check, the spectra -- asks this one
    question and does not have to know which kind of answer it got.

    Parameters
    ----------
    exposure : Exposure
        Which camera and which mode.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    sky : tuple of float, optional
        ``(x, y)`` sky pixel position, from :func:`xmm_source_sky_position`. Required for
        an imaging exposure and ignored for a timing one, which has no sky image to
        convert into.

    Returns
    -------
    tuple of str or None
        ``(source, background)``, or ``None`` for a timing exposure with no strips
        configured -- see :func:`xmm_timing_regions`.

    Raises
    ------
    ValueError
        If an imaging exposure is given no sky position. That is a caller's mistake, not
        a property of the data, so it is raised rather than skipped.
    """
    if exposure.mode == TIMING:
        return xmm_timing_regions(exposure.instrument, config)
    if sky is None:
        raise ValueError(
            f"{exposure.instrument}{exposure.expid} is an imaging exposure and needs a "
            f"sky position to extract at"
        )
    return xmm_extraction_regions(sky[0], sky[1], config)


#: The line ``ecoordconv`` prints the sky position on. Anchored at the start of the line so
#: that ``DETX:`` and ``IM_X:`` cannot be mistaken for it. The task's documentation states
#: that these strings may be searched for in a script and that every effort is made to keep
#: them constant between versions, which is what makes parsing standard output defensible
#: here -- ``ecoordconv`` writes no file at all, so there is nothing else to read.
ECOORDCONV_SKY_RE = re.compile(r"^\s*X:\s*Y:\s+(\S+)\s+(\S+)\s*$", re.MULTILINE)


def parse_ecoordconv_sky_position(text):
    """
    The sky position out of what ``ecoordconv`` printed.

    Parameters
    ----------
    text : str
        Standard output of the task.

    Returns
    -------
    tuple of float
        ``(x, y)`` in sky pixels.

    Raises
    ------
    ValueError
        If no sky position was printed. This is deliberately loud: ``ecoordconv`` exits
        zero when it cannot convert, so a quiet default would put the extraction region at
        ``(0, 0)`` -- the corner of the detector -- and everything downstream would carry
        on looking perfectly healthy.

    Examples
    --------
    >>> parse_ecoordconv_sky_position(" Theta: Phi: 18.5 2.6\\n X: Y: 27010 26888\\n")
    (27010.0, 26888.0)
    """
    match = ECOORDCONV_SKY_RE.search(text)
    if match is None:
        raise ValueError(f"ecoordconv printed no sky position:\n{text}")
    return float(match.group(1)), float(match.group(2))


def xmm_source_sky_position(event_list, ra, dec, env=None, log_to=None):
    """
    Where a celestial position falls on one exposure's sky image.

    The sky frame is per-exposure -- it is tied to the attitude solution -- so this is
    asked once per event list rather than once per observation.

    **Timing exposures never come here.** A timing read-out has no sky image, only a
    detector column, so the region is a ``RAWX`` strip instead and this conversion has
    nothing to convert.

    Parameters
    ----------
    event_list : str
        Event list defining the sky frame.
    ra, dec : float
        The position asked for, in degrees.
    env : dict, optional
        Environment for the task, from :func:`heasarc_retrieve_pipeline.sas.sas_environment`.
    log_to : str, optional
        File the task's output is appended to, beside being read.

    Returns
    -------
    tuple of float
        ``(x, y)`` in sky pixels.
    """
    from . import sas

    result = sas.run(
        "ecoordconv",
        produces=[],
        capture=True,
        log_to=log_to,
        env=env,
        imageset=event_list,
        withcoords="yes",
        coordtype="eqpos",
        x=ra,
        y=dec,
    )
    return parse_ecoordconv_sky_position(result.stdout)


@dataclass(frozen=True)
class NearestDetection:
    """
    The closest thing the archive's own source detection found to a given position.

    Attributes
    ----------
    ra, dec : float
        Position of the detection, in degrees.
    offset_arcsec : float
        How far it is from the position asked for.
    flux : float or None
        ``EP_TOT_FLUX``, in erg cm^-2 s^-1. ``None`` when the column is absent.
    next_offset_arcsec : float or None
        Offset of the second-nearest detection, or ``None`` when there is only one. This
        is the number that says whether to believe the match: a detection 1.4 arcsec away
        with the runner-up at 34 arcsec is unambiguous, and two at similar distances are
        not.
    """

    ra: float
    dec: float
    offset_arcsec: float
    flux: Optional[float] = None
    next_offset_arcsec: Optional[float] = None


def nearest_detection(source_list, ra, dec):
    """
    Find a position in a PPS source list.

    Parameters
    ----------
    source_list : str or None
        Path of an ``OBSMLI``, from :func:`xmm_source_list_file`.
    ra, dec : float
        The position asked for, in degrees.

    Returns
    -------
    NearestDetection or None
        ``None`` when there is no source list, or it holds no detections. Neither is a
        failure -- see :func:`xmm_check_source_position`.
    """
    from astropy.io import fits
    from astropy.coordinates import SkyCoord

    if source_list is None or not os.path.exists(source_list):
        return None

    with fits.open(source_list) as hdul:
        table = hdul[SOURCE_LIST_EXTENSION]
        if table.data is None or len(table.data) == 0:
            return None
        detections = SkyCoord(
            np.asarray(table.data["RA"], dtype=float),
            np.asarray(table.data["DEC"], dtype=float),
            unit="deg",
        )
        columns = table.columns.names
        fluxes = (
            np.asarray(table.data[SOURCE_LIST_FLUX_COLUMN], dtype=float)
            if SOURCE_LIST_FLUX_COLUMN in columns
            else None
        )

    offsets = SkyCoord(ra, dec, unit="deg").separation(detections).arcsec
    order = np.argsort(offsets)
    best = int(order[0])
    return NearestDetection(
        ra=float(detections.ra.deg[best]),
        dec=float(detections.dec.deg[best]),
        offset_arcsec=float(offsets[best]),
        flux=None if fluxes is None else float(fluxes[best]),
        next_offset_arcsec=float(offsets[order[1]]) if len(order) > 1 else None,
    )


def xmm_check_source_position(obsid, config, ra, dec, rec=None):
    """
    Cross-check the position asked for against the archive's own source detection.

    **This warns and never fails, and that is the whole design.** The ``OBSMLI`` source
    list misses real targets: on the Crab, ``0611180201``, the nearest detection is 328.79
    arcsec from the pulsar, because the nebula is extended and piled up and
    maximum-likelihood point-source detection does not find a point source there at all. A
    pipeline that aborted on a large offset would refuse the Crab. Compare Her X-1,
    ``0153950401``, where the nearest detection is 1.42 arcsec away with the next at 33.8
    arcsec -- an unambiguous match. The offset separates those two cases for a reader; it
    does not decide anything, and it never moves the extraction region.

    What it does catch is the mistake worth catching: a position typed wrong, or a target
    that is simply not in this observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    ra, dec : float
        The position asked for, in degrees.
    rec : :class:`heasarc_retrieve_pipeline.diagnostics.StepRecord`, optional
        Where the numbers go. ``None`` records nothing.

    Returns
    -------
    NearestDetection or None
    """
    logger = get_logger()
    if rec is None:
        rec = no_record()

    source_list = xmm_source_list_file(obsid, config)
    if source_list is None:
        reason = f"{obsid} has no EPIC source list to check the position against"
        logger.info(reason)
        rec.skip(reason)
        return None

    found = nearest_detection(source_list, ra, dec)
    if found is None:
        reason = f"{os.path.basename(source_list)} holds no detections"
        logger.info(f"Not checking the position: {reason}")
        rec.skip(reason)
        return None

    rec.value(
        ra=ra,
        dec=dec,
        detection_ra=found.ra,
        detection_dec=found.dec,
        offset_arcsec=found.offset_arcsec,
        flux=found.flux,
        next_offset_arcsec=found.next_offset_arcsec,
        source_list=os.path.basename(source_list),
    )

    if found.offset_arcsec > config["position_warn_arcsec"]:
        logger.warning(
            f"The nearest EPIC detection to the position asked for is "
            f"{found.offset_arcsec:.2f} arcsec away in {os.path.basename(source_list)}. "
            f"Extracting at the position asked for regardless -- the source list misses "
            f"extended and piled-up sources."
        )
    else:
        logger.info(
            f"The position asked for matches an EPIC detection {found.offset_arcsec:.2f} "
            f"arcsec away"
        )
    return found


def _exposure_stem(exposure):
    """
    The name every output of one exposure is built on, ``"mos1S004_imaging"``.

    The mode belongs in the name and is not decoration: MOS ``FastUncompressed`` writes an
    imaging and a timing event list under one exposure identifier, so a stem without the
    mode would have the second overwrite the first.
    """
    return f"{exposure.instrument}{exposure.expid}_{exposure.mode}"


def xmm_cleaned_event_list_path(obsid, exposure, config):
    """
    Where one exposure's screened events go.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    exposure : Exposure
        Which camera, exposure and mode.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl/<camera><expid>_<mode>_cl.evt``.
    """
    return os.path.join(
        xmm_pipeline_output_path(obsid, config), f"{_exposure_stem(exposure)}_cl.evt"
    )


def xmm_flare_gti_path(obsid, exposure, config):
    """
    Where one exposure's flare good time intervals go.

    Beside the events they filtered, so that a reduction can be read off the directory
    without a manifest.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    exposure : Exposure
        Which camera, exposure and mode.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl/<camera><expid>_<mode>_flare.gti``.
    """
    return os.path.join(
        xmm_pipeline_output_path(obsid, config), f"{_exposure_stem(exposure)}_flare.gti"
    )


def xmm_clean_event_list(obsid, exposure, config, gti=None, env=None, log_to=None):
    """
    Screen one exposure's events with ``evselect``.

    The screening stays inside SAS deliberately. ``#XMMEA_EP`` and ``#XMMEA_EM`` are
    calibration-driven macros, not a fixed list of bits, so reimplementing them on the
    event table here would freeze today's calibration into this file and quietly go stale.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    exposure : Exposure
        Which camera, exposure and mode, and where its events are.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    gti : numpy.ndarray, optional
        Good time intervals to apply as well, from :func:`xmm_flare_gti`. ``None`` -- which
        is what an exposure with no background light curve gives -- screens on event
        attributes alone.
    env : dict, optional
        Environment for the task, from :func:`heasarc_retrieve_pipeline.sas.sas_environment`.
    log_to : str, optional
        File the task's output goes to.

    Returns
    -------
    str
        Path of the cleaned event list.
    """
    from . import sas

    output = xmm_cleaned_event_list_path(obsid, exposure, config)
    os.makedirs(os.path.dirname(output), exist_ok=True)

    gti_file = None
    if gti is not None:
        gti_file = write_gti_file(xmm_flare_gti_path(obsid, exposure, config), gti)

    sas.run(
        "evselect",
        produces=output,
        log_to=log_to,
        env=env,
        table=exposure.event_list,
        withfilteredset="yes",
        filteredset=output,
        keepfilteroutput="yes",
        expression=xmm_screening_expression(exposure.instrument, exposure.mode, gti_file=gti_file),
    )
    return output


def xmm_source_event_list_path(obsid, exposure, config):
    """
    Where the events inside one exposure's source region go.

    The pile-up check needs them, because pile-up is a property of the region a spectrum
    is built from and not of the field around it.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl/<camera><expid>_<mode>_src.evt``.
    """
    return os.path.join(
        xmm_pipeline_output_path(obsid, config), f"{_exposure_stem(exposure)}_src.evt"
    )


#: Extension of the diagram ``epatplot`` draws, measured rather than assumed.
#:
#: The task's ``device`` parameter still offers PGPLOT devices and still defaults to
#: ``/VCPS``, which is PostScript, but SAS 22.1.0 draws the plot from Python instead and
#: warns "Only format supported now is pdf". Asked for ``..._pat.ps`` it writes
#: ``..._pat.pdf`` and reports success, so a caller checking for the name it asked for
#: fails on a run that worked -- which is how this was found, on ``0153950401``. Pinned
#: here as ``especget``'s output names are to be: measured against one SAS version, named,
#: and easy to find when a version changes it.
PILEUP_PLOT_EXTENSION = "pdf"


def xmm_pileup_plot_path(obsid, exposure, config):
    """
    Where one exposure's ``epatplot`` diagram goes, beside the events it describes.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl/<camera><expid>_<mode>_pat.pdf`` -- see
        :data:`PILEUP_PLOT_EXTENSION` for why the extension is not the ``.ps`` the task's
        own parameters suggest.
    """
    return os.path.join(
        xmm_pipeline_output_path(obsid, config),
        f"{_exposure_stem(exposure)}_pat.{PILEUP_PLOT_EXTENSION}",
    )


#: The keywords ``epatplot`` appends to the event set it was given: the observed-to-model
#: singles and doubles pattern fractions and their one-sigma errors. Documented output,
#: not reverse-engineered -- the task's own page says the two numbers "are printed both to
#: the console and on the plot and are appended to the input event set as attributes
#: SNGL_OTM and DBLE_OTM". Keywords are read here in preference to the console for the
#: same reason ``ecoordconv``'s output is parsed only because it writes no file at all.
PILEUP_KEYWORDS = ("SNGL_OTM", "ESGL_OTM", "DBLE_OTM", "EDBL_OTM")


@dataclass(frozen=True)
class PileupRatios:
    """
    What ``epatplot`` measured about one source region.

    Attributes
    ----------
    singles, doubles : float
        Observed-to-model pattern fractions over 0.5-2.0 keV. Both are 1.0 when there is
        no pile-up. When there is, two photons landing in one frame are read as one event
        of a larger pattern, so singles go missing and doubles are made: the singles ratio
        falls below one and the doubles ratio rises above it.
    singles_error, doubles_error : float
        One-sigma errors, as the task reports them.
    """

    singles: float
    singles_error: float
    doubles: float
    doubles_error: float

    def is_piled_up(self, sigma=3.0):
        """
        Whether either ratio is far enough from one to mean something.

        The error bars decide, not the ratio: 0.95 +/- 0.02 is a detection of pile-up and
        0.95 +/- 0.05 is a short exposure.

        Examples
        --------
        >>> PileupRatios(0.82, 0.02, 1.31, 0.03).is_piled_up()
        True
        >>> PileupRatios(0.95, 0.05, 1.00, 0.05).is_piled_up()
        False
        """
        return bool(
            self.singles + sigma * self.singles_error < 1.0
            or self.doubles - sigma * self.doubles_error > 1.0
        )


def read_pileup_ratios(path):
    """
    The pile-up numbers ``epatplot`` wrote onto an event set.

    Every header is searched rather than one named, because "attribute of the event set"
    is SAS's own wording and which block carries it is not part of what the task
    documents.

    Parameters
    ----------
    path : str
        The event set ``epatplot`` was run on.

    Returns
    -------
    PileupRatios or None
        ``None`` when the keywords are absent -- an ``epatplot`` that plotted but could
        not fit, which is a step to skip and not a zero to record.
    """
    from astropy.io import fits

    with fits.open(path) as hdul:
        for hdu in hdul:
            if all(keyword in hdu.header for keyword in PILEUP_KEYWORDS):
                return PileupRatios(*(float(hdu.header[key]) for key in PILEUP_KEYWORDS))
    return None


def xmm_pileup_check(obsid, exposure, config, events, sky=None, rec=None, env=None, log_to=None):
    """
    Measure the pile-up of one exposure's source region with ``epatplot``.

    **A diagnostic and nothing else.** Pile-up is corrected by throwing the middle of the
    point spread function away -- an annulus instead of a circle -- and that changes which
    photons the science is done with. Deciding it automatically would silently hand back a
    different spectrum than the one that was asked for, so this measures, records and
    warns, and the decision stays with the person reading the page.

    The source region is cut out first, for both modes: a circle on the sky for an imaging
    exposure, a strip of detector columns for a timing one.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    exposure : Exposure
        Which camera, exposure and mode.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    events : str
        The cleaned event list, from :func:`xmm_clean_event_list`.
    sky : tuple of float, optional
        ``(x, y)`` sky position, from :func:`xmm_source_sky_position`. Needed by imaging
        exposures only.
    rec : :class:`heasarc_retrieve_pipeline.diagnostics.StepRecord`, optional
        Where the numbers go. ``None`` records nothing.
    env : dict, optional
        Environment for the tasks, from
        :func:`heasarc_retrieve_pipeline.sas.sas_environment`.
    log_to : str, optional
        File the tasks' output goes to.

    Returns
    -------
    PileupRatios or None
        ``None`` when the check could not be made -- a camera with no timing strip
        configured, or an ``epatplot`` that wrote no numbers.
    """
    from . import sas

    logger = get_logger()
    if rec is None:
        rec = no_record()

    regions = xmm_exposure_regions(exposure, config, sky=sky)
    if regions is None:
        reason = (
            f"{exposure.instrument}{exposure.expid} has no extraction region in "
            f"{exposure.mode} mode, so its pile-up cannot be measured"
        )
        logger.warning(reason)
        rec.skip(reason)
        return None

    source_events = xmm_source_event_list_path(obsid, exposure, config)
    os.makedirs(os.path.dirname(source_events), exist_ok=True)
    sas.run(
        "evselect",
        produces=source_events,
        log_to=log_to,
        env=env,
        table=events,
        withfilteredset="yes",
        filteredset=source_events,
        keepfilteroutput="yes",
        expression=regions[0],
    )

    plot = xmm_pileup_plot_path(obsid, exposure, config)
    sas.run(
        "epatplot",
        produces=plot,
        log_to=log_to,
        env=env,
        set=source_events,
        outdir=os.path.dirname(plot),
        useplotfile="yes",
        plotfile=os.path.basename(plot),
        # The default, said out loud: it is what writes the ratios onto the event set,
        # and reading them back is the whole point of the call.
        modifyinset="yes",
    )

    ratios = read_pileup_ratios(source_events)
    if ratios is None:
        reason = f"epatplot wrote no pattern ratios onto {os.path.basename(source_events)}"
        logger.warning(reason)
        rec.skip(reason)
        return None

    piled_up = ratios.is_piled_up(sigma=config["pileup_warn_sigma"])
    rec.value(
        singles=ratios.singles,
        singles_error=ratios.singles_error,
        doubles=ratios.doubles,
        doubles_error=ratios.doubles_error,
        piled_up=piled_up,
        region=regions[0],
        plot=os.path.basename(plot),
    )

    numbers = (
        f"singles {ratios.singles:.3f} +/- {ratios.singles_error:.3f}, "
        f"doubles {ratios.doubles:.3f} +/- {ratios.doubles_error:.3f}"
    )
    if piled_up:
        logger.warning(
            f"{exposure.instrument}{exposure.expid} looks piled up: {numbers}, where "
            f"both should be 1. Its spectrum will be biased. Extracting it anyway -- "
            f"excluding the core of the point spread function is a decision for whoever "
            f"reads {os.path.basename(plot)}, not for the pipeline."
        )
    else:
        logger.info(f"{exposure.instrument}{exposure.expid} shows no pile-up: {numbers}")
    return ratios


@dataclass(frozen=True)
class SpectrumProducts:
    """
    The five files one exposure's spectrum is made of.

    Attributes
    ----------
    source, background : str
        The two spectra, with ``BACKSCAL`` set to the geometric area of each region.
    arf : str
        Effective area -- the mirror vignetting, the encircled energy of the region, and
        the area lost to bad pixels and chip gaps.
    rmf : str
        Redistribution matrix, from ``rmfgen``. The slow part of the extraction.
    grouped : str
        The source spectrum binned for fitting, and the file to open in XSPEC: it carries
        ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE``, so the other four follow it.
    """

    source: str
    background: str
    arf: str
    rmf: str
    grouped: str


def xmm_spectrum_paths(obsid, exposure, config):
    """
    Where one exposure's spectral products go, and what they are called.

    The names are ours, not ``especget``'s. Its ``filestem`` convention writes
    ``<stem>_src.ds``, ``<stem>_bgd.ds``, ``<stem>_src.arf`` and ``<stem>_src.rmf``, which
    is a convention that could change between SAS versions -- the trap ``epatplot``'s
    ``.ps`` that is really a ``.pdf`` sprang in step 8. Naming all four outright with
    ``withfilestem=no`` makes the version irrelevant.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    exposure : Exposure
        Which camera, exposure and mode.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    SpectrumProducts
        Paths under ``<out_data_path>/<OBSID>/products``.
    """
    products = xmm_product_output_path(obsid, config)
    stem = os.path.join(products, _exposure_stem(exposure))
    return SpectrumProducts(
        source=f"{stem}_src.pi",
        background=f"{stem}_bkg.pi",
        arf=f"{stem}.arf",
        rmf=f"{stem}.rmf",
        grouped=f"{stem}_grp.pi",
    )


def xmm_calculate_spectra(
    obsid, exposure, config, events, ra, dec, sky=None, rec=None, env=None, log_to=None
):
    """
    Extract one exposure's source and background spectra with their responses.

    ``especget`` is a metatask and does the whole extraction in one call: ``evselect``
    for both spectra, ``arfgen`` for the effective area and for the ``BACKSCAL`` areas of
    the two regions, and ``rmfgen`` for the redistribution matrix. ``specgroup`` then bins
    the source spectrum for fitting and writes the names of the other three into its
    header, so that opening the grouped spectrum in XSPEC brings the rest with it.

    **The position asked for is handed to ``arfgen`` explicitly.** Left alone, ``arfgen``
    takes the source position from the centre of the extraction region -- which is right
    for a circle on the sky and meaningless for a strip of detector columns, where the
    centre of the strip says nothing about where along it the source sits. In timing mode
    it would otherwise fall back on the ``SRCPOS`` keyword or, failing that, on
    ``RAWY=190``. The vignetting and encircled-energy corrections depend on that position,
    so it is given rather than inferred.

    **pn and MOS are not co-added.** They are different detectors with different
    responses, so the three spectra of an observation are meant to be fitted jointly, each
    with its own ARF and RMF. ``epicspeccombine`` is the tool for making one file of them
    if that is ever wanted.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    exposure : Exposure
        Which camera, exposure and mode.
    config : dict
        A complete configuration, from :func:`xmm_config`.
    events : str
        The cleaned event list, from :func:`xmm_clean_event_list`.
    ra, dec : float
        The position to extract at, in degrees. The position asked for, never the
        ``OBSMLI`` detection -- see :func:`xmm_check_source_position`.
    sky : tuple of float, optional
        ``(x, y)`` sky position, from :func:`xmm_source_sky_position`. Needed by imaging
        exposures only.
    rec : :class:`heasarc_retrieve_pipeline.diagnostics.StepRecord`, optional
        Where the numbers go. ``None`` records nothing.
    env : dict, optional
        Environment for the tasks, from
        :func:`heasarc_retrieve_pipeline.sas.sas_environment`.
    log_to : str, optional
        File the tasks' output goes to.

    Returns
    -------
    SpectrumProducts or None
        ``None`` when the exposure has no extraction region -- see
        :func:`xmm_timing_regions`.
    """
    from . import sas

    logger = get_logger()
    if rec is None:
        rec = no_record()

    regions = xmm_exposure_regions(exposure, config, sky=sky)
    if regions is None:
        reason = (
            f"{exposure.instrument}{exposure.expid} has no extraction region in "
            f"{exposure.mode} mode, so no spectrum is extracted"
        )
        logger.warning(reason)
        rec.skip(reason)
        return None

    source_region, background_region = regions
    paths = xmm_spectrum_paths(obsid, exposure, config)
    stem = _exposure_stem(exposure)
    products = os.path.dirname(paths.source)
    os.makedirs(products, exist_ok=True)

    # The tasks run in the products directory and are given bare file names, because they
    # write the names they are given into the spectrum's BACKFILE, RESPFILE and ANCRFILE
    # -- and a FITS header card holds 80 characters, which an output path here exceeds on
    # its own. See the ``cwd`` argument of :func:`heasarc_retrieve_pipeline.sas.run`.
    name = {key: os.path.basename(value) for key, value in vars(paths).items()}

    logger.info(
        f"Extracting the spectrum of {exposure.instrument}{exposure.expid} from "
        f"{source_region}. rmfgen is the slow part of this."
    )
    sas.run(
        "especget",
        produces=[paths.source, paths.background, paths.arf, paths.rmf],
        log_to=log_to,
        env=env,
        cwd=products,
        table=os.path.abspath(events),
        srcexp=source_region,
        backexp=background_region,
        withfilestem="no",
        srcspecset=name["source"],
        bckspecset=name["background"],
        srcarfset=name["arf"],
        srcrmfset=name["rmf"],
        withsourcepos="yes",
        sourcecoords="eqpos",
        sourcex=ra,
        sourcey=dec,
    )

    sas.run(
        "specgroup",
        produces=paths.grouped,
        log_to=log_to,
        env=env,
        cwd=products,
        spectrumset=name["source"],
        groupedset=name["grouped"],
        mincounts=config["spectrum_min_counts"],
        oversample=config["spectrum_oversample"],
        rmfset=name["rmf"],
        arfset=name["arf"],
        backgndset=name["background"],
        # Fills BACKFILE, RESPFILE and ANCRFILE, so that the grouped spectrum is the only
        # file a fit has to be pointed at.
        addfilenames="yes",
    )

    for which, path in (("src", paths.source), ("bkg", paths.background)):
        curve = read_xmm_spectrum(path, paths.rmf)
        if curve is not None:
            # `spec_<stem>_<src|bkg>_<...>` is report.spectrum_figure's convention, and
            # following it is what makes the spectra draw. NuSTAR writes one record
            # holding several extractions, so the stem is how it tells them apart; XMM
            # writes one record per exposure and repeats the stem, which costs nothing
            # and keeps one reader for both.
            rec.array(**{f"spec_{stem}_{which}_{key}": value for key, value in curve.items()})

    rec.value(
        source_spectrum=os.path.basename(paths.source),
        background_spectrum=os.path.basename(paths.background),
        arf=os.path.basename(paths.arf),
        rmf=os.path.basename(paths.rmf),
        grouped_spectrum=os.path.basename(paths.grouped),
        source_region=source_region,
        background_region=background_region,
        min_counts=config["spectrum_min_counts"],
        oversample=config["spectrum_oversample"],
        # What the page should draw. Without it the figure falls back on NuSTAR's
        # 3-79 keV, which would throw away all but the hard tail of an XMM spectrum.
        energy_band=list(SPECTRUM_PLOT_BAND_KEV),
    )
    return paths


def read_xmm_spectrum(spectrum, rmf):
    """
    One spectrum as a drawable curve, with its energy scale taken from its response.

    NuSTAR's equivalent converts channel to energy with that mission's linear relation.
    XMM has no such number to hardcode and needs none: the ``EBOUNDS`` extension of the
    RMF ``especget`` just produced says what each channel is worth, which is exact and
    survives a change of spectral binning.

    This is for looking at, not for fitting. The uncertainty is Poisson on the counts,
    which is right for an ungrouped spectrum and an underestimate for a grouped one, so it
    is the ungrouped ``_src.pi`` that the reduction records rather than the ``_grp.pi``
    it also writes.

    Parameters
    ----------
    spectrum : str
        A PHA spectrum, carrying ``CHANNEL`` and either ``COUNTS`` or ``RATE``.
    rmf : str
        A response with an ``EBOUNDS`` extension.

    Returns
    -------
    dict or None
        ``energy`` (keV), ``rate`` (counts/s/keV) and ``rate_err``, or ``None`` if either
        file is missing or does not hold what this needs. A diagnostic that cannot be
        drawn is not a failed extraction.
    """
    from astropy.io import fits

    logger = get_logger()
    try:
        with fits.open(rmf) as hdul:
            bounds = hdul["EBOUNDS"].data
            edges = {
                int(channel): (float(low), float(high))
                for channel, low, high in zip(bounds["CHANNEL"], bounds["E_MIN"], bounds["E_MAX"])
            }

        with fits.open(spectrum) as hdul:
            data = hdul["SPECTRUM"].data
            header = hdul["SPECTRUM"].header
            columns = {name.upper() for name in data.columns.names}
            exposure = float(header.get("EXPOSURE") or header.get("ONTIME") or 1.0)
            if exposure <= 0:
                exposure = 1.0
            if "COUNTS" in columns:
                counts = np.asarray(data["COUNTS"], dtype=float)
            elif "RATE" in columns:
                counts = np.asarray(data["RATE"], dtype=float) * exposure
            else:
                return None
            channels = np.asarray(data["CHANNEL"], dtype=int)
    except (OSError, KeyError, AttributeError) as error:
        logger.warning(f"Could not read the spectrum {spectrum}: {error}")
        return None

    # Matched on channel number, not on row order: a spectrum need not start at channel
    # zero, and a response may describe channels the spectrum does not carry.
    described = np.array([channel in edges for channel in channels])
    channels, counts = channels[described], counts[described]
    if channels.size == 0:
        return None
    low = np.array([edges[channel][0] for channel in channels])
    high = np.array([edges[channel][1] for channel in channels])
    width = np.where(high > low, high - low, 1.0)

    return dict(
        energy=0.5 * (low + high),
        rate=counts / exposure / width,
        rate_err=np.sqrt(np.maximum(counts, 0)) / exposure / width,
    )


def _time_system(event_list):
    """
    ``(TIMESYS, TIMEREF)`` of the first extension that names them.

    Parameters
    ----------
    event_list : str
        Event file to read.

    Returns
    -------
    tuple
        The two keyword values, either of which may be ``None``.
    """
    from astropy.io import fits

    with fits.open(event_list) as hdul:
        for hdu in hdul:
            if "TIMESYS" in hdu.header:
                return hdu.header.get("TIMESYS"), hdu.header.get("TIMEREF")
    return None, None


def xmm_run_odf_pipeline(obsid, config, env=None, log_to=None):
    """
    Turn one observation's raw telemetry into event lists with ``epproc`` and ``emproc``.

    The ODF route's expensive step, and the reason that route exists: these tasks apply
    the current calibration to the raw frames, where a PPS event list carries whatever
    calibration the SOC had when it was made.

    Both tasks are run, and **a failure of one does not stop the other**. An observation
    with no pn exposure is ordinary, and a MOS-only reduction is worth having; the
    observation fails only if neither task left an event list behind, which is checked
    rather than inferred from return codes.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    env : dict, optional
        SAS environment, with ``SAS_CCF`` and ``SAS_ODF`` already pointing at this
        observation's calibration index and summary file.
    log_to : callable, optional
        Called with a task name, returning the file to send that task's output to.

    Returns
    -------
    str
        The directory the event lists are in.

    Raises
    ------
    RuntimeError
        If neither task produced an event list.
    """
    from . import sas

    logger = get_logger()
    events = xmm_odf_events_path(obsid, config)
    os.makedirs(events, exist_ok=True)

    for task in ODF_PIPELINE_TASKS:
        logger.info(f"{obsid}: running {task}; this is the slow part of the ODF route")
        try:
            # The tasks take no arguments: they read SAS_ODF and SAS_CCF from the
            # environment and write into the working directory. `produces=[]` because
            # their output names are theirs to choose -- what they left is checked below.
            sas.run(task, produces=[], log_to=log_to(task) if log_to else None, env=env, cwd=events)
        except Exception as error:  # noqa: BLE001 -- one camera failing is not fatal
            logger.warning(f"{obsid}: {task} failed ({error}); continuing without it")

    produced = glob.glob(os.path.join(events, ODF_EVENT_LIST_GLOB))
    if not produced:
        raise RuntimeError(
            f"{obsid}: neither {' nor '.join(ODF_PIPELINE_TASKS)} produced an event list "
            f"in {events}."
        )
    logger.info(f"{obsid}: the ODF pipeline produced {len(produced)} event lists")
    return events


def _event_list_identity(event_list):
    """
    ``(instrument, expid, mode, submode)`` read from an event list's header.

    Returns ``None`` for a file that names a camera this pipeline does not reduce -- the
    RGS spectrometers and the Optical Monitor -- or a mode it does not know.

    Parameters
    ----------
    event_list : str
        Event file to read.

    Returns
    -------
    tuple or None
    """
    from astropy.io import fits

    with fits.open(event_list) as hdul:
        for hdu in hdul:
            header = hdu.header
            if INSTRUMENT_KEYWORD not in header or DATA_MODE_KEYWORD not in header:
                continue
            instrument = SAS_INSTRUMENTS.get(str(header[INSTRUMENT_KEYWORD]).strip())
            mode = SAS_DATA_MODES.get(str(header[DATA_MODE_KEYWORD]).strip().upper())
            if instrument is None or mode is None:
                continue
            return (
                instrument,
                str(header.get(EXPOSURE_ID_KEYWORD, "")).strip(),
                mode,
                read_submode(event_list),
            )
    return None


def xmm_exposures_from_odf(obsid, config):
    """
    What an observation holds, read from the event lists ``epproc`` and ``emproc`` made.

    The ODF route's twin of :func:`xmm_exposures_from_pps`, and deliberately *not* its
    mirror image. That one parses PPS file names, which are an archive product with a
    published, stable convention. These names are SAS's own and have changed between
    releases, so the identity is taken from the header keywords instead -- ``INSTRUME``,
    ``EXPIDSTR``, ``DATAMODE`` -- which SAS writes from the same code whichever route made
    the file. A release that renames its outputs then costs nothing here.

    There is no flare light curve: the ODF has no ``FBKTSR``, and one is built per exposure
    by :func:`xmm_odf_flare_lightcurve` once the exposures are known.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    list of Exposure
        Ordered pn, MOS1, MOS2, then by exposure and mode, as the PPS route orders them.
        Empty when the pipeline produced no EPIC event list at all.
    """
    found = []
    for path in sorted(
        glob.glob(os.path.join(xmm_odf_events_path(obsid, config), ODF_EVENT_LIST_GLOB))
    ):
        try:
            identity = _event_list_identity(path)
        except OSError:
            get_logger().warning(f"{obsid}: could not read {os.path.basename(path)}, skipping it")
            continue
        if identity is not None:
            found.append((identity, path))

    cameras = list(SAS_INSTRUMENTS.values())
    found.sort(key=lambda item: (cameras.index(item[0][0]), item[0][1], item[0][2] != IMAGING))

    return [
        Exposure(
            instrument=instrument,
            expid=expid,
            mode=mode,
            event_list=path,
            flare_lightcurve=None,
            submode=submode,
        )
        for (instrument, expid, mode, submode), path in found
    ]


def xmm_odf_events_path(obsid, config):
    """
    Where ``epproc`` and ``emproc`` write their event lists.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl/odf_events``. Its own directory rather than
        beside the cleaned lists: the tasks write several files per exposure and a glob
        over ``event_cl`` would then have to tell them apart from our own outputs.
    """
    return os.path.join(xmm_pipeline_output_path(obsid, config), "odf_events")


def xmm_staged_odf_path(obsid, config):
    """
    Directory the ODF is staged into for ``odfingest``.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl/odf``. Under the *output* tree, not beside the
        download: staging decompresses, and the downloaded tree is the one thing a rerun
        should be able to treat as read-only.
    """
    return os.path.join(xmm_pipeline_output_path(obsid, config), "odf")


def xmm_stage_odf(obsid, config):
    """
    Copy the observation's ODF into a directory ``odfingest`` can read.

    Decompresses ``.FIT.gz`` and ``.ASC.gz`` to plain ``.FIT`` and ``.ASC`` -- see
    :data:`ODF_STAGED_SUFFIXES` for why the compression cannot simply be kept. Anything
    already uncompressed is copied unchanged.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path`` and ``out_data_path``.

    Returns
    -------
    str or None
        The staging directory, or ``None`` if the observation has no ODF downloaded --
        which is not an error here, only the end of the barycentring road.
    """
    source = xmm_odf_path(obsid, config)
    if not os.path.isdir(source):
        return None

    staged = xmm_staged_odf_path(obsid, config)
    os.makedirs(staged, exist_ok=True)
    for path in sorted(glob.glob(os.path.join(source, "*"))):
        name = os.path.basename(path)
        for compressed, plain in ODF_STAGED_SUFFIXES.items():
            if name.endswith(compressed):
                target = os.path.join(staged, name[: -len(compressed)] + plain)
                with gzip.open(path, "rb") as raw, open(target, "wb") as out:
                    shutil.copyfileobj(raw, out)
                break
        else:
            shutil.copy(path, os.path.join(staged, name))
    return staged


def xmm_odf_summary(obsid, config, env=None, log_to=None):
    """
    Ingest the staged ODF and return the summary file ``barycen`` needs.

    ``odfingest`` warns ``NoScienceFiles`` here and that warning is expected, not a
    failure: only the 3.8 MB of housekeeping is downloaded on the PPS route, and the
    observation's start and stop are recoverable from it alone. What matters is the
    ``SUM.SAS`` it writes, which is checked for rather than assumed.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path`` and ``out_data_path``.
    env : dict, optional
        SAS environment. ``SAS_ODF`` is set from the staging directory on a copy of it,
        because ``odfingest`` reads the directory to scan from there.
    log_to : str, optional
        File to send the task's output to.

    Returns
    -------
    str or None
        Path of the ``SUM.SAS``, or ``None`` if there was no ODF to ingest.
    """
    from . import sas

    staged = xmm_stage_odf(obsid, config)
    if staged is None:
        return None

    ingest_env = dict(env or os.environ)
    ingest_env["SAS_ODF"] = staged
    sas.run(
        "odfingest",
        produces=[],
        log_to=log_to,
        env=ingest_env,
        cwd=staged,
        odfdir=staged,
        outdir=staged,
    )

    summaries = sorted(glob.glob(os.path.join(staged, f"*{ODF_SUMMARY_SUFFIX}")))
    if not summaries:
        get_logger().warning(
            f"{obsid}: odfingest wrote no {ODF_SUMMARY_SUFFIX}, so this observation "
            "cannot be barycentred."
        )
        return None
    return summaries[0]


def xmm_barycenter(obsid, config, events, summary, env=None, log_to=None, rec=None):
    """
    Write a barycentred copy of one cleaned event list.

    Converts arrival times from the spacecraft to the solar system barycentre, which is
    what makes a coherent timing search possible: over one XMM orbit the correction
    changes by a couple of seconds, against the ~1 s periods this pipeline's targets pulse
    at. The original file is left alone and the correction is applied to a copy, because
    ``barycen`` edits in place and an event list whose times are silently no longer
    spacecraft times is a trap for everything downstream.

    HEASOFT ``barycorr`` is *not* usable here, which is worth stating because it is the
    obvious thing to reach for and the pipeline already wraps it for NuSTAR. Its own help
    limits it to RXTE, Swift, Chandra, NuSTAR and NICER; run on XMM data with the PPS
    ``ORBTSR`` orbit it fails with "Invalid Observatory/Spacecraft position vector",
    before reading a single event, and no renaming of columns or rescaling of units gets
    past that. See docs/xmm_integration_plan.md.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Pipeline configuration.
    events : str
        Cleaned event list to barycentre.
    summary : str
        ODF summary file from :func:`xmm_odf_summary`, for ``SAS_ODF``.
    env : dict, optional
        SAS environment; ``SAS_ODF`` is overridden on a copy of it.
    log_to : str, optional
        File to send the task's output to.
    rec : Recorder, optional
        Diagnostics recorder.

    Returns
    -------
    str or None
        Path of the barycentred file, or ``None`` if there was no summary to work from.
    """
    from . import sas

    rec = rec or no_record()
    if summary is None:
        rec.value(barycentered=False, reason="no ODF summary")
        return None

    output = barycentered_file_name(events)
    shutil.copy(events, output)

    bary_env = dict(env or os.environ)
    bary_env["SAS_ODF"] = summary
    sas.run(
        "barycen",
        produces=sas.IN_PLACE(output),
        log_to=log_to,
        env=bary_env,
        table=f"{output}:EVENTS",
        withtable="yes",
    )

    timesys, timeref = _time_system(output)
    if timesys != BARYCENTRED_TIMESYS:
        raise ValueError(
            f"barycen returned success but left {os.path.basename(output)} on "
            f"{timesys or 'no'} time rather than {BARYCENTRED_TIMESYS}."
        )

    rec.value(
        barycentered=True,
        barycentered_file=os.path.basename(output),
        summary=summary,
        timesys=timesys,
        timeref=timeref,
    )
    return output


@flow(flow_run_name="xmm_{obsid}")
def process_xmm_obsid(obsid, config=None, ra="NONE", dec="NONE", flags=None):
    """
    Reduce one XMM-Newton observation end to end.

    Builds the calibration index, then per EPIC camera, exposure and mode: the flare good
    time intervals, a screened event list, the extraction regions, a pile-up measurement
    and a grouped spectrum with its background, ARF and RMF.

    ``config=None`` rather than ``{}``, so that the fallback to :data:`DEFAULT_CONFIG`
    actually fires. NICER and RXTE take ``{}`` and therefore never fall back, which is
    known issue 27; this is the same mistake not repeated.

    One thing here is deliberately *not* NuSTAR's shape: ``ra`` and ``dec`` are used as
    given and never overridden. NuSTAR measures a position off its own image and reduces
    at whatever it finds, which is better than the catalogue pointing when the detection
    is right and reduces the wrong source when it is not. XMM's astrometry does not need
    that, so the position asked for is the position extracted, and the PPS source list is
    only ever consulted to *report* how far the nearest detection lies.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict, optional
        Pipeline configuration; :data:`DEFAULT_CONFIG` where it says nothing.
    ra, dec : float or str, optional
        Source position in degrees. Required for anything beyond screening: the regions,
        the responses and the position cross-check are all built from it.
    flags : dict, optional
        Accepted for the signature every mission's entry point shares. Nothing reads it
        yet; per-task overrides belong in ``config``.

    Returns
    -------
    str or None
        :data:`heasarc_retrieve_pipeline.utils.NO_SCIENCE_DATA` when the observation holds
        no EPIC event lists of any mode, and ``None`` otherwise.
    """
    from . import sas

    config = xmm_config(absolute_config(config, DEFAULT_CONFIG))
    logger = get_logger()
    logger.info(f"Processing XMM-Newton observation {obsid}")

    if config["products"] != "pps":
        raise NotImplementedError(
            "Reprocessing from the ODF is not built yet: only the PPS route runs today. "
            "Set config['products'] = 'pps', or leave it unset."
        )

    exposures = xmm_exposures_from_pps(obsid, config)
    if not exposures:
        # Not a failure, and not counted as one. An XMM observation can be real, public
        # and downloaded and still hold nothing for this pipeline -- four of the twenty
        # pointings at M82 have no EPIC exposure at all. The data stay on disk.
        logger.warning(f"{obsid} holds no EPIC event lists of any mode. Nothing to reduce.")
        return NO_SCIENCE_DATA

    exposures = xmm_with_submodes(exposures)
    logger.info(
        f"{obsid}: "
        + ", ".join(
            f"{e.instrument} {e.expid} {e.mode} ({e.submode or 'submode unknown'})"
            for e in exposures
        )
    )

    for directory in (
        xmm_pipeline_output_path(obsid, config),
        xmm_product_output_path(obsid, config),
    ):
        os.makedirs(directory, exist_ok=True)

    diagnostics = diagnostics_path(obsid, config)

    # Once per observation, and first: everything below reaches calibration through the
    # environment this produces. See xmm_build_calibration_index for why the index is
    # built rather than taken from the downloaded CALIND.
    index = xmm_build_calibration_index(
        obsid, config, exposures[0].event_list, log_to=tool_log_file("cifbuild", obsid, config)
    )
    env = sas.sas_environment(ccf=index, ccfpath=config["sas_ccfpath"])

    # Once per observation, like the calibration index: every exposure shares the ODF.
    summary = xmm_odf_summary(
        obsid, config, env=env, log_to=tool_log_file("odfingest", obsid, config)
    )

    with record_step(diagnostics, obsid, "source_position") as rec:
        rec.value(ra=ra, dec=dec, calibration_index=index)
        xmm_check_source_position(obsid, config, ra, dec, rec=rec)

    for exposure in exposures:
        stem = _exposure_stem(exposure)
        logger.info(f"{obsid}: reducing {stem}")

        with record_step(diagnostics, obsid, "flare_filtering", key=stem) as rec:
            gti = xmm_flare_gti(exposure, config, rec=rec)

        events = xmm_clean_event_list(
            obsid,
            exposure,
            config,
            gti=gti,
            env=env,
            log_to=tool_log_file(f"evselect_{stem}", obsid, config),
        )

        # A timing read-out has no sky image to convert into, so the conversion is not
        # merely wasteful there -- it has no answer. Its regions are RAWX strips instead.
        sky = None
        if exposure.mode == IMAGING:
            sky = xmm_source_sky_position(
                events,
                ra,
                dec,
                env=env,
                log_to=tool_log_file(f"ecoordconv_{stem}", obsid, config),
            )
            with record_step(diagnostics, obsid, "source_region", key=stem) as rec:
                xmm_check_extraction_window(exposure, config, events, *sky, rec=rec)

        with record_step(diagnostics, obsid, "pileup_check", key=stem) as rec:
            xmm_pileup_check(
                obsid,
                exposure,
                config,
                events,
                sky=sky,
                rec=rec,
                env=env,
                log_to=tool_log_file(f"epatplot_{stem}", obsid, config),
            )

        with record_step(diagnostics, obsid, "barycenter", key=stem) as rec:
            xmm_barycenter(
                obsid,
                config,
                events,
                summary,
                env=env,
                rec=rec,
                log_to=tool_log_file(f"barycen_{stem}", obsid, config),
            )

        with record_step(diagnostics, obsid, "calculate_spectra", key=stem) as rec:
            xmm_calculate_spectra(
                obsid,
                exposure,
                config,
                events,
                ra,
                dec,
                sky=sky,
                rec=rec,
                env=env,
                log_to=tool_log_file(f"especget_{stem}", obsid, config),
            )

    logger.info(f"Finished processing XMM-Newton observation {obsid}")
    return None
