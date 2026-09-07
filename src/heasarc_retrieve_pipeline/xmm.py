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

from .utils import absolute_config

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
    flare_rate_limit=dict(pn=0.4, mos=0.35),
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
