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

This module holds the part that needs no SAS at all: what a PPS file name means, which
exposures an observation contains, where everything goes, and what the configuration is.
All of it is testable offline, which matters more here than for the other missions --
there is no continuous-integration job anywhere that can run a real SAS task.

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
import os
import re
from dataclasses import dataclass
from typing import Optional

import numpy as np

from .diagnostics import no_record
from .utils import (
    absolute_config,
    get_logger,
    good_intervals,
    intervals_above_threshold,
    intervals_removed,
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
