"""
Chandra (ACIS + HRC) reduction.

The mission reduced here is Chandra, in all four detector configurations -- ACIS-I,
ACIS-S, HRC-I and HRC-S -- and all their modes, with transmission gratings collected but
never re-extracted. The reduction tasks belong to the CXC's CIAO (Chandra Interactive
Analysis of Observations) rather than to HEASOFT or SAS, and they are run by
:mod:`heasarc_retrieve_pipeline.ciao`.

Chandra is cheaper than XMM was for three reasons worth stating up front, because they
explain why this module is so much smaller than :mod:`heasarc_retrieve_pipeline.xmm`.
**A Chandra observation is one detector in one mode**, so XMM's ``(instrument, expid,
mode)`` fan-out has no analogue and the per-exposure machinery is simply not needed.
Chandra's astrometry is sub-arcsecond, so the position the user gives goes straight into
the extraction region and nothing has to find a source. And the archive is already
reduced: the level-2 products under ``primary/`` are what CIAO would produce, so reading
them is the default route and ``chandra_repro`` is available through
``config["products"] = "repro"``.

The full design, and every archive measurement this module rests on, is in
``docs/chandra_integration_plan.md``.

The archive layout
------------------

HEASARC mirrors the whole Chandra archive at
``chandra/data/byobsid/<last digit of obsid>/<obsid>/``, laid out as::

    00README
    oif.fits                      observation index
    primary/                      the archive's level-2 reduction
        <inst>f<obsid>N<ver>_evt2.fits.gz          the event list
        <inst>f<obsid>_<seq>N<ver>_dtf1.fits.gz    dead time, HRC only
        orbitf<met>N<ver>_eph1.fits.gz             orbit ephemeris
        pcadf<obsid>_<seq>N<ver>_asol1.fits.gz     aspect solution
        responses/                                 grating ARFs and RMFs
    secondary/                    level-1 telemetry, and what made level 2

Two things about that layout are easy to get wrong and are pinned by tests. The bad-pixel
file is under ``primary/`` for ACIS and ``secondary/`` for HRC, so a filter anchored on
one directory silently drops it for a whole instrument. And four different files end in
``_eph1.fits.gz`` -- orbit, lunar, solar and angles -- of which only the orbit one
barycentres anything.
"""

import copy
import glob
import gzip
import os
import re
import shutil
import dataclasses
from dataclasses import dataclass
from itertools import takewhile
from typing import Optional

import numpy as np
from astropy.io import fits
from astropy.time import Time

from prefect import flow

from .diagnostics import diagnostics_path, no_record, record_step
from .utils import (
    NO_SCIENCE_DATA,
    absolute_config,
    get_logger,
    good_intervals,
    intersect_intervals,
    intervals_above_threshold,
    merge_intervals,
    read_pha_spectrum,
    tool_log_file,
)

#: The pipeline's defaults for a Chandra run.
#:
#: ``products`` chooses the route: ``"archive"`` reads the level-2 products the archive
#: already holds, ``"repro"`` runs ``chandra_repro`` over the level-1 telemetry. The
#: archive is the default by Matteo's ruling of 2026-09-12, mirroring XMM's PPS/ODF split
#: -- with ``CALDBVER`` reported as a staleness diagnostic rather than used to decide the
#: route.
#:
#: ``src_radius_arcsec = None`` means "ask ``psfsize_srcs``", and is the default because
#: Chandra's PSF grows from about one arcsecond on-axis to over ten at eight arcminutes
#: off-axis. A fixed radius -- XMM's approach -- would be wrong at both ends.
#:
#: ``cc_source_halfwidth_pix`` and ``cc_background_pix`` are Continuous Clocking's, where
#: there is no circle to draw and the regions are strips of ``chipx``. They are a
#: starting guess and are flagged as one: CC mode is 1.7% of the archive, none of it has
#: been run through this module yet, and open item 6 of the plan is exactly this.
DEFAULT_CONFIG = {
    "out_data_path": "./",
    "input_data_path": "./",
    "products": "archive",
    "caldb": None,
    "psf_ecf": 0.9,
    "psf_energy_kev": 1.0,
    "src_radius_arcsec": None,
    "bkg_inner_factor": 1.5,
    "bkg_outer_factor": 3.0,
    "flare_sigma": 3.0,
    "flare_bin_seconds": 200.0,
    "flare_energy_ev": (500, 7000),
    "flare_min_bins": 20,
    "flare_warn_fraction": 0.1,
    "flare_max_removed_fraction": 0.3,
    "hrc_veto_ratio_threshold": 0.99,
    "pileup_percentile": 90.0,
    "pileup_image_pixels": 1024,
    "spectrum_min_counts": 15,
    "spectrum_weight": False,
    "spectrum_correct_psf": True,
    "cc_source_halfwidth_pix": 3,
    "cc_background_pix": (10, 30),
}

#: The archive's own reduction, and only the parts of it a reduction reads.
#:
#: Anchored on the whole remote name -- an HTTPS URL or an S3 key, depending on the
#: transport -- and on both the directory and the full file name.
#:
#: One family really is ambiguous by name: ``orbitf..._eph1``, ``lunarf..._eph1``,
#: ``solarf..._eph1`` and ``anglesf..._eph1``, of which only the first barycentres
#: anything. Two independent things exclude the other three -- the ``orbitf`` prefix and
#: the ``primary/`` anchor, since the rest live under ``secondary/ephem/`` -- and the
#: tests confirm that dropping either alone is survivable and dropping both is not.
#: Elsewhere the directory anchor is the defence that carries the weight, because the
#: file-name patterns are literal: ``osol1`` and ``std_dtfstat1`` look like near-misses
#: for ``asol1`` and ``dtf1`` but cannot match either pattern at all.
#:
#: Measured on 2026-09-12 over three full S3 listings: 15.4 MB of 51.0 on ``6298``
#: (HRC-I), 88.7 of 264.5 on ``17661`` (HRC-S), 180.2 of 458.5 on ``2749`` (ACIS-S with
#: HETG) -- 30%, 34% and 39%. On ``2749``, 155 MB of the 180 is ``responses/`` alone.
ARCHIVE_DOWNLOAD_RE = (
    r"(?:/primary/[^/]*_evt2\.fits\.gz$"  # the event list
    r"|/primary/[^/]*_asol1\.fits\.gz$"  # aspect solution, which specextract needs
    r"|/primary/[^/]*_fov1\.fits\.gz$"  # field of view
    r"|/primary/[^/]*_dtf1\.fits\.gz$"  # HRC dead time -- and the timing discriminator
    r"|/primary/orbitf[^/]*_eph1\.fits\.gz$"  # orbit ephemeris, for axbary
    r"|/primary/[^/]*_pha2\.fits\.gz$"  # grating spectra, collected as they are
    r"|/primary/responses/[^/]*_(?:arf|rmf)2\.fits\.gz$"  # and their responses
    r"|/(?:primary|secondary)/[^/]*_bpix1\.fits\.gz$"  # bad pixels -- BOTH directories
    r"|/secondary/[^/]*_(?:msk1|flt1)\.fits\.gz$"  # mask and the observation's GTI
    r"|/oif\.fits$)"  # observation index
)

#: What the reprocessing route leaves behind, which is all it can afford to leave behind.
#:
#: ``chandra_repro`` is handed a directory and decides for itself what in it to read, so
#: this excludes rather than includes. Three kinds of file are safe to drop because no
#: tool opens them: the verification-and-validation report, the preview JPEGs, and the
#: preview images that duplicate data held elsewhere.
#:
#: The report alone is worth more than everything else here -- 10.4 MB of ``6298``,
#: 59.9 MB of ``17661``, 101.7 MB of ``2749``. Measured at 79%, 77% and 78% of the three
#: directories kept.
REPRO_DOWNLOAD_EXCLUDE_RE = r"(?:\.pdf(?:\.gz)?$|\.jpg$|_img2\.fits\.gz$)"


def chandra_download_filter(config):
    """
    What of a Chandra observation directory to download, for the route this run is taking.

    Called through :func:`~heasarc_retrieve_pipeline.core.mission_download_filter`, which
    is why it returns keyword arguments for
    :func:`~heasarc_retrieve_pipeline.core.recursive_download` rather than a pattern. The
    two routes answer in different ways -- the archive route names what it wants, the
    reprocessing route names what it does not -- and that asymmetry is real: a reduction
    reads a known handful of level-2 products, while ``chandra_repro`` wants the
    ``secondary/`` tree entire.

    Parameters
    ----------
    config : dict
        The run's configuration. Only ``products`` is read.

    Returns
    -------
    dict
        ``re_include`` or ``re_exclude`` for
        :func:`~heasarc_retrieve_pipeline.core.recursive_download`.

    Raises
    ------
    ValueError
        If ``products`` is neither ``"archive"`` nor ``"repro"``. Falling back to
        "download everything" would answer a typo in a configuration file with half a
        gigabyte per observation.

    Examples
    --------
    >>> sorted(chandra_download_filter({}))
    ['re_include']
    >>> sorted(chandra_download_filter({"products": "repro"}))
    ['re_exclude']
    """
    products = config.get("products", DEFAULT_CONFIG["products"])
    if products == "archive":
        return {"re_include": ARCHIVE_DOWNLOAD_RE}
    if products == "repro":
        return {"re_exclude": REPRO_DOWNLOAD_EXCLUDE_RE}
    raise ValueError(f"Chandra has an 'archive' route and a 'repro' route, not {products!r}.")


def chandra_config(config):
    """
    The configuration one Chandra reduction runs with: the caller's, over the defaults.

    The twin of ``xmm.xmm_config``, for the same reason: ``utils.absolute_config`` only
    falls back to the defaults when handed ``None``, and
    ``core.download_and_process_observation`` hands every mission a dictionary holding the
    two paths and nothing else. Merged once here, ``config["products"]`` cannot raise
    ``KeyError`` halfway through a reduction.

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
    >>> chandra_config({"products": "repro"})["psf_ecf"]
    0.9
    >>> chandra_config(None)["products"]
    'archive'
    """
    merged = copy.deepcopy(DEFAULT_CONFIG)
    merged.update(copy.deepcopy(config or {}))
    return absolute_config(merged, DEFAULT_CONFIG)


#: A level-2 event list, as it appears in a listing of an observation's ``primary/``.
LEVEL2_EVENT_LIST_RE = r"_evt2\.fits(?:\.gz)?$"


def chandra_route_from_listing(entries):
    """
    Which route an observation's ``primary/`` directory can support.

    Parameters
    ----------
    entries : iterable of str
        Names directly under ``<observation>/primary/``, as
        :func:`~heasarc_retrieve_pipeline.core.list_archive_directory` returns them.

    Returns
    -------
    str or None
        ``"archive"`` when a level-2 event list is there, ``"repro"`` when the directory
        was listed and holds none. ``None`` for an empty listing, which is not evidence of
        anything: an observation with no ``primary/`` at all is not one this module
        recognises, and turning it into a full level-1 download would be a guess.

    Examples
    --------
    >>> chandra_route_from_listing(["acisf05644N004_evt2.fits.gz", "pcadf05644_asol1.fits.gz"])
    'archive'
    >>> chandra_route_from_listing(["pcadf05644_000N001_asol1.fits.gz"])
    'repro'
    >>> chandra_route_from_listing([]) is None
    True
    """
    names = [str(entry).strip("/") for entry in entries]
    if not names:
        return None
    if any(re.search(LEVEL2_EVENT_LIST_RE, name) for name in names):
        return "archive"
    return "repro"


def chandra_resolve_config(config, url):
    """
    The configuration this observation will be reduced with, after looking at the archive.

    Called through ``core.mission_resolve_config``, before the download, and shaped like
    ``xmm.xmm_resolve_config``. The question asked is narrower than XMM's: every Chandra
    observation directory has the same top level (``primary/``, ``secondary/``,
    ``oif.fits``), so the answer is one level down, in whether ``primary/`` holds a
    level-2 event list. One request, against a download that would otherwise arrive with
    nothing the archive route can reduce.

    The change only ever goes one way, from ``"archive"`` to ``"repro"``. A run that asked
    for the reprocessing route keeps it and the archive is not listed at all.

    Parameters
    ----------
    config : dict or None
        What the caller asked for; merged over the defaults by :func:`chandra_config`.
    url : str
        Where this observation will be downloaded from.

    Returns
    -------
    dict
        A complete configuration. The caller's dictionary is not modified.
    """
    # Imported here and not at the top of the module: ``core`` imports every mission.
    from .core import list_archive_directory

    config = chandra_config(config)
    logger = get_logger()

    if config["products"] != "archive":
        logger.info(f"Reprocessing with chandra_repro as asked; not looking at what {url} holds")
        return config

    primary = url.rstrip("/") + "/primary/"
    entries = list_archive_directory(primary)
    if entries is None:
        logger.warning(f"Could not list {primary}; going on with the archive route")
        return config

    available = chandra_route_from_listing(entries)
    if available is None:
        logger.warning(f"{primary} is empty; going on with the archive route")
        return config

    if available != config["products"]:
        logger.info(
            f"{primary} holds no level-2 event list, so this observation is reprocessed "
            f"with chandra_repro rather than read off the archive's own products"
        )
        config["products"] = available
    return config


def chandra_obsid(obsid):
    """
    An observation identifier as the archive files it: decimal, unpadded.

    ``chanmaster.obsid`` is an integer, and HEASARC mirrors an observation under that
    integer's plain decimal form -- ``chandra/data/byobsid/8/6298/``. The download
    therefore lands in ``<input_data_path>/6298``, and every directory this pipeline makes
    for the observation matches it. File *names* are the other convention; see
    :func:`chandra_padded_obsid`.

    Parameters
    ----------
    obsid : int or str
        Observation identifier, in any spelling: an ``int``, a NumPy integer, ``"6298"``
        or the zero-padded ``"06298"``.

    Returns
    -------
    str
        The unpadded decimal form.

    Raises
    ------
    ValueError
        If it is not an integer identifier. Accepting anything else would build a path
        out of it, and a mistyped OBSID that silently becomes a directory name is found
        much later than one that raises here.

    Examples
    --------
    >>> chandra_obsid(6298), chandra_obsid("06298")
    ('6298', '6298')
    """
    text = str(obsid).strip()
    if not text.isdigit():
        raise ValueError(f"{obsid!r} is not a Chandra OBSID")
    return str(int(text))


def chandra_padded_obsid(obsid):
    """
    An observation identifier as the archive *names files* with it: five digits, padded.

    The archive's own file names pad -- ``hrcf06298N006_evt2.fits.gz``,
    ``acisf02749N004_evt2.fits.gz`` -- so output stems pad too, and an unpadded stem would
    both sort wrongly and fail to match the archive it came from.

    Parameters
    ----------
    obsid : int or str
        Observation identifier, in any spelling :func:`chandra_obsid` accepts.

    Returns
    -------
    str
        Five digits, or more for an identifier too long to fit. Chandra has not reached
        six digits; truncating one if it ever did would be worse than a long name.

    Examples
    --------
    >>> chandra_padded_obsid(6298), chandra_padded_obsid(2749)
    ('06298', '02749')
    """
    return chandra_obsid(obsid).zfill(5)


def chandra_file_stem(obsid, detector, mode):
    """
    The stem every output file of an observation is named from.

    Following the rule Matteo set for XMM on 2026-09-08: every file names its own
    observation, because these files leave the tree that gives them context. Because a
    Chandra observation is one detector in one mode, the stem is simpler than XMM's --
    there is no exposure to name::

        chandra06298_hrci_imaging_src.evt
        chandra17661_hrcs_timing_bary.evt
        chandra02749_aciss_hetg_src.pi

    ``mode`` is taken rather than worked out. For HRC the honest mode is not in the header
    at all -- it follows from the dead-time file's veto ratio -- so the caller settles it
    and hands it here. See ``docs/chandra_integration_plan.md``.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    detector : str
        One of the labels :func:`chandra_detector` returns.
    mode : str
        Short mode label, already decided.

    Returns
    -------
    str
        ``chandra<padded obsid>_<detector>_<mode>``.

    Raises
    ------
    ValueError
        If ``detector`` or ``mode`` is empty or carries anything but letters, digits and
        underscores. A path separator reaching a file name is how an output escapes the
        directory it was meant for.

    Examples
    --------
    >>> chandra_file_stem(6298, "hrci", "imaging")
    'chandra06298_hrci_imaging'
    """
    for label, value in (("detector", detector), ("mode", mode)):
        if not value or not str(value).replace("_", "").isalnum():
            raise ValueError(f"{label} must be a plain label, not {value!r}")
    return f"chandra{chandra_padded_obsid(obsid)}_{detector}_{mode}"


#: Where the Science Instrument Module parks for each ACIS configuration, in millimetres.
#:
#: These are the nominal aimpoints, and they are also the medians of the measurement
#: below: an observation that does not offset the SIM sits exactly here.
ACIS_NOMINAL_SIM_Z = {"acisi": -233.587, "aciss": -190.143}

#: ``SIM_Z`` above this is ACIS-S, below it is ACIS-I.
#:
#: Measured on 2026-09-12 over 150 randomly chosen archived ACIS observations, 75 of each
#: configuration as ``chanmaster.detector`` labels them:
#:
#: ============ ========================= ==========
#: Catalogue    ``SIM_Z`` range           Median
#: ============ ========================= ==========
#: ``ACIS-I``   -238.274 .. -214.099      -233.587
#: ``ACIS-S``   -195.973 .. -182.134      -190.143
#: ============ ========================= ==========
#:
#: The two do not overlap: 18.126 mm separate the most positive ACIS-I from the most
#: negative ACIS-S. This threshold sits in that gap with about 9 mm of margin either way.
ACIS_SIM_Z_THRESHOLD = -205.0

#: The chip an on-axis source lands on, per configuration: I3 for ACIS-I, S3 for ACIS-S.
ACIS_AIMPOINT_CHIP = {"acisi": 3, "aciss": 7}


def chandra_chips(header):
    """
    Which CCDs were read out, from ``DETNAM``.

    ``DETNAM`` is a chip list and its digits are chip identifiers: **0-3 are the ACIS-I
    array** (I0-I3) and **4-9 are the ACIS-S array** (S0-S5). So ``ACIS-012367`` is the
    whole ACIS-I array read out with S2 and S3 alongside it, and ``ACIS-456789`` is the
    whole ACIS-S array.

    Observations routinely switch on chips from both arrays, which is why this cannot say
    which configuration an observation is -- :func:`chandra_detector` reads ``SIM_Z`` for
    that -- but it is what says which chip a source lands on.

    Parameters
    ----------
    header : dict or astropy.io.fits.Header
        An event list header. ``DETNAM`` is read; ``INSTRUME`` decides whether it is a
        chip list at all.

    Returns
    -------
    list of int
        Chip identifiers in order, or an empty list for HRC, which is a microchannel
        plate and has no CCDs.

    Examples
    --------
    >>> chandra_chips({"INSTRUME": "ACIS", "DETNAM": "ACIS-012367"})
    [0, 1, 2, 3, 6, 7]
    """
    if str(header.get("INSTRUME", "")).upper().startswith("HRC"):
        return []
    detnam = str(header.get("DETNAM", ""))
    return [int(digit) for digit in detnam.partition("-")[2] if digit.isdigit()]


def chandra_detector(header):
    """
    Which detector configuration an observation used, as an output-name label.

    HRC says so itself: its ``DETNAM`` *is* the configuration, ``HRC-I`` or ``HRC-S``.

    ACIS does not. Its ``DETNAM`` names the chips that were switched on, and both
    aimpoint chips are frequently on at once -- measured on 2026-09-12, **50 of 150
    randomly chosen archived ACIS observations had both chip 3 and chip 7 reading out**,
    and those 50 span both configurations. No rule written on the chip set can tell them
    apart. What can is ``SIM_Z``, the Science Instrument Module's parked position, which
    separates the two with an 18.1 mm gap and no overlap. See :data:`ACIS_SIM_Z_THRESHOLD`.

    Parameters
    ----------
    header : dict or astropy.io.fits.Header
        An event list header, carrying ``INSTRUME``, ``DETNAM`` and -- for ACIS --
        ``SIM_Z``.

    Returns
    -------
    str
        ``"acisi"``, ``"aciss"``, ``"hrci"`` or ``"hrcs"``.

    Raises
    ------
    ValueError
        If ``INSTRUME`` is neither ACIS nor HRC, if an HRC ``DETNAM`` names neither
        detector, or if an ACIS header carries no ``SIM_Z``. Guessing at any of these
        would put a wrong detector into every output file name of the observation, and
        nothing downstream would notice.

    Examples
    --------
    >>> chandra_detector({"INSTRUME": "ACIS", "DETNAM": "ACIS-456789", "SIM_Z": -187.125})
    'aciss'
    >>> chandra_detector({"INSTRUME": "HRC", "DETNAM": "HRC-S"})
    'hrcs'
    """
    instrument = str(header.get("INSTRUME", "")).strip().upper()
    detnam = str(header.get("DETNAM", "")).strip().upper()

    if instrument == "HRC":
        if detnam in ("HRC-I", "HRC-S"):
            return detnam.replace("-", "").lower()
        raise ValueError(f"DETNAM {detnam!r} is neither HRC-I nor HRC-S")

    if instrument != "ACIS":
        raise ValueError(f"INSTRUME {instrument!r} is neither ACIS nor HRC")

    sim_z = header.get("SIM_Z")
    if sim_z is None:
        raise ValueError(
            "an ACIS header with no SIM_Z cannot be told from an ACIS-I one: DETNAM "
            f"{detnam!r} names chips, not a configuration"
        )
    return "aciss" if float(sim_z) > ACIS_SIM_Z_THRESHOLD else "acisi"


def chandra_archive_path(obsid, config):
    """
    Directory the observation was downloaded into.

    Named with :func:`chandra_obsid`, the archive's own unpadded spelling, because that is
    what the transports produce: the last component of the bucket prefix
    ``chandra/data/byobsid/8/6298/`` is kept, so the files land under ``<input>/6298``.
    Looking under the padded ``06298`` finds an empty directory and reports an observation
    with no science data.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str
        ``<input_data_path>/<OBSID>``.
    """
    return os.path.join(config["input_data_path"], chandra_obsid(obsid))


def chandra_base_output_path(obsid, config):
    """
    Top-level output directory of an observation.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>``, unpadded to match the download.
    """
    return os.path.join(config["out_data_path"], chandra_obsid(obsid))


def chandra_pipeline_output_path(obsid, config):
    """
    Where the cleaned event lists go.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_cl``. The name is NuSTAR's, and deliberately so:
        ``report.OBSERVATION_SUBDIRECTORIES`` already recognises it, so ``hrp-report``
        finds a Chandra tree without being taught anything. XMM kept it for the same
        reason.
    """
    return os.path.join(chandra_base_output_path(obsid, config), "event_cl")


def chandra_product_output_path(obsid, config):
    """
    Where the spectra and their responses go.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/products``, for the same reason as
        :func:`chandra_pipeline_output_path`.
    """
    return os.path.join(chandra_base_output_path(obsid, config), "products")


def chandra_repro_path(obsid, config):
    """
    Where ``chandra_repro`` writes, and where the reprocessing route reads first.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/repro``. Its own directory beside ``event_cl`` and
        ``products``, not inside either: ``chandra_repro`` writes a dozen files of its own
        choosing, and a glob over ``event_cl`` would then have to tell them apart from the
        cleaned lists. The last component is ``repro`` because that is what CIAO's own
        default calls it, so a directory listing reads the same as the documentation.
    """
    return os.path.join(chandra_base_output_path(obsid, config), "repro")


def _glob_both_ways(root, pattern):
    """
    Every file under one root matching a glob, sorted, gzipped or not.

    The archive gzips its products and ``chandra_repro`` does not, so each pattern is
    tried both ways.
    """
    found = set()
    for suffix in ("", ".gz"):
        found.update(glob.glob(os.path.join(root, pattern + suffix)))
    return sorted(found)


def _product_candidates(obsid, config, archive, repro):
    """
    The ``(root, pattern)`` pairs to try for one family of product, best first.

    On the archive route there is one pair and it is the download directory. On the
    reprocessing route the ``repro/`` directory is tried first and the download directory
    second, because ``chandra_repro`` re-makes four products and copies a few more but
    leaves the rest where it found them -- the orbit ephemeris, the HRC dead-time file and
    the grating set are never in ``repro/`` at all, and reading them still means reading
    what was downloaded.

    ``repro`` is ``None`` for a family the reprocessing does not write, which skips the
    ``repro/`` directory for it rather than finding the archive's own copy there under an
    archive name.
    """
    candidates = []
    if config.get("products", DEFAULT_CONFIG["products"]) == "repro" and repro is not None:
        candidates.append((chandra_repro_path(obsid, config), repro))
    candidates.append((chandra_archive_path(obsid, config), archive))
    return candidates


def _archive_products(obsid, config, archive, repro=None):
    """
    Every file of one family this run should read, sorted.

    The first root that holds anything wins; the rest are not consulted. An observation
    that was never downloaded gives an empty list rather than raising: whether there is
    anything to reduce is the caller's decision to make, as it is in
    :mod:`heasarc_retrieve_pipeline.xmm`.
    """
    for root, pattern in _product_candidates(obsid, config, archive, repro):
        found = _glob_both_ways(root, pattern)
        if found:
            return found
    return []


def _one_archive_product(obsid, config, archive, what, repro=None):
    """
    The single file of one family this run should read, or ``None``.

    A Chandra observation is one detector in one mode, so each of these families has
    exactly one member. Two is not a tie to break at random -- it means a half-finished
    reprocessing or two archive versions side by side, and reducing the wrong one in
    silence is the worst available outcome.
    """
    found = _archive_products(obsid, config, archive, repro)
    if not found:
        return None
    if len(found) > 1:
        raise ValueError(
            f"observation {chandra_obsid(obsid)} has {len(found)} {what} files, and one "
            f"of them would be reduced in silence: {[os.path.basename(p) for p in found]}"
        )
    return found[0]


def chandra_event_list(obsid, config):
    """
    The level-2 event list this run should reduce, or ``None`` if there is none.

    On the archive route that is the one the archive shipped. On the reprocessing route it
    is ``chandra_repro``'s ``*_repro_evt2.fits``, and the archive's own is not consulted --
    see :func:`_product_candidates`.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``primary/*_evt2.fits[.gz]``, or to ``repro/*_repro_evt2.fits`` on the
        reprocessing route.

    Raises
    ------
    ValueError
        If there is more than one.
    """
    return _one_archive_product(
        obsid,
        config,
        os.path.join("primary", "*_evt2.fits"),
        "evt2",
        repro="*_repro_evt2.fits",
    )


def chandra_aspect_solution(obsid, config):
    """
    The aspect solution, which ``specextract`` and ``axbary`` both need.

    Note it is ``asol1`` and not ``osol1``: the archive writes both, and the one-second
    ``osol1`` under ``secondary/aspect/`` is a different file.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``primary/*_asol1.fits[.gz]``. ``chandra_repro`` copies it in uncompressed,
        and may have applied a boresight correction to it, so on the reprocessing route the
        copy in ``repro/`` is the one to use.
    """
    return _one_archive_product(
        obsid,
        config,
        os.path.join("primary", "*_asol1.fits"),
        "asol1",
        repro="pcadf*_asol1.fits",
    )


def chandra_bad_pixel_file(obsid, config):
    """
    The bad-pixel list, from whichever directory this instrument's lives in.

    **ACIS files it under** ``primary/`` **and HRC under** ``secondary/``. Looking in one
    directory finds it for one instrument and silently misses it for the other, and the
    symptom does not appear until ``specextract`` runs.

    On the reprocessing route the name is asked for as ``*_repro_bpix1.fits`` and not as a
    bare ``*_bpix1.fits``, because ``chandra_repro`` copies the archive's list in beside
    the one it just made: the loose glob matches two files in one directory and the
    one-or-raise guard fires on a perfectly healthy reprocessing.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``{primary,secondary}/*_bpix1.fits[.gz]``, or to
        ``repro/*_repro_bpix1.fits`` on the reprocessing route.
    """
    for directory in ("primary", "secondary"):
        found = _one_archive_product(
            obsid,
            config,
            os.path.join(directory, "*_bpix1.fits"),
            "bpix1",
            repro="*_repro_bpix1.fits",
        )
        if found is not None:
            return found
    return None


def chandra_level1_event_list(obsid, config):
    """
    The level-1 event list, which is what ``chandra_repro`` reprocesses.

    Never read on the reprocessing route's own output: ``chandra_repro`` consumes this
    file and does not copy it, so it is always the download's, and the search path is not
    consulted.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``secondary/*_evt1.fits[.gz]``. ``None`` says this observation was
        downloaded with the archive route's filter, which does not fetch level 1.
    """
    return _one_archive_product(obsid, config, os.path.join("secondary", "*_evt1.fits"), "evt1")


def chandra_dead_time_file(obsid, config):
    """
    The dead-time-factor file, which only HRC writes.

    This is the file the whole timing story rests on: its veto ratio, and not the event
    header, says whether an HRC observation really has the 15.625 us resolution every HRC
    header claims. ACIS has none, and ``None`` is the answer rather than an error.

    ``chandra_repro`` does not copy it, so this is read from the download on both routes.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``primary/*_dtf1.fits[.gz]``, or ``None`` for ACIS.
    """
    return _one_archive_product(obsid, config, os.path.join("primary", "*_dtf1.fits"), "dtf1")


def chandra_orbit_ephemeris(obsid, config):
    """
    The orbit ephemeris ``axbary`` barycentres with.

    Four files of an observation end in ``_eph1.fits.gz`` -- orbit, lunar, solar and
    angles -- and only this one describes where the spacecraft was.

    ``chandra_repro`` does not copy it either, so barycentring reads the download on both
    routes.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``primary/orbitf*_eph1.fits[.gz]``.
    """
    return _one_archive_product(
        obsid, config, os.path.join("primary", "orbitf*_eph1.fits"), "orbit ephemeris"
    )


def chandra_mask_file(obsid, config):
    """
    The detector mask.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``secondary/*_msk1.fits[.gz]``. ``chandra_repro`` copies this one in
        unchanged, so on the reprocessing route it is read from ``repro/`` under the
        archive's own name.
    """
    return _one_archive_product(
        obsid,
        config,
        os.path.join("secondary", "*_msk1.fits"),
        "msk1",
        repro="*_msk1.fits",
    )


def chandra_gti_file(obsid, config):
    """
    The observation's own good-time intervals, as the archive's pipeline found them.

    Named ``flt1``, and usually ``*_std_flt1.fits.gz``. This is the starting point the
    flare screening narrows, not a replacement for it.

    ``chandra_repro`` calls its own **``flt2``**, so the reprocessing route asks for a
    different name rather than the same one in a different place. A run that asked for
    ``flt1`` there would find nothing, fall through to the download, and screen flares
    against good times that belong to the file it is not reducing.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``secondary/*_flt1.fits[.gz]``, or to ``repro/*_repro_flt2.fits`` on the
        reprocessing route.
    """
    return _one_archive_product(
        obsid,
        config,
        os.path.join("secondary", "*_flt1.fits"),
        "flt1",
        repro="*_repro_flt2.fits",
    )


def chandra_grating_spectrum(obsid, config):
    """
    The archive's ready-made grating spectra, or ``None`` where there is no grating.

    Collected, never re-made: by Matteo's ruling of 2026-09-12 gratings are in scope as
    collection only, and ``tgextract`` is never run. This file *is* the spectrum.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``primary/*_pha2.fits[.gz]``.
    """
    return _one_archive_product(obsid, config, os.path.join("primary", "*_pha2.fits"), "pha2")


def chandra_grating_responses(obsid, config):
    """
    The responses belonging to :func:`chandra_grating_spectrum`.

    Twelve ARF/RMF pairs for a HETG observation -- HEG and MEG, orders plus and minus one
    to three -- which is 155 MB of the 180 downloaded for ``2749`` and the price of the
    gratings decision.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    list of str
        Paths to ``primary/responses/*_{arf,rmf}2.fits[.gz]``, sorted; empty where there
        is no grating.
    """
    found = []
    for kind in ("arf", "rmf"):
        found += _archive_products(
            obsid, config, os.path.join("primary", "responses", f"*_{kind}2.fits")
        )
    return sorted(found)


@dataclass(frozen=True)
class ObservationPart:
    """
    One pointing of an observation, and the files that belong to it alone.

    Some Chandra observations were taken in several separate pointings under one obsid --
    ``1411`` is two, 84 days apart. Chandra calls each an OBI, an observation interval.
    The archive merges their events into one level-2 list and ships everything else once
    per part, and each part has to be reduced with its own: its own dead time, its own
    aspect, its own orbit. An ordinary observation is simply one part.

    Attributes
    ----------
    number : int or None
        The part number as the archive writes it, ``2`` for ``hrcf01411_002N006_dtf1``.
        Numbers can skip and need not start at zero -- ``433`` is parts 1, 3 and 4.
        ``None`` only when nothing downloaded carries one.
    tstart, tstop : float or None
        The span of the part's own files, in spacecraft seconds. For an observation of one
        part, the event list's.
    dead_time_file : str or None
        HRC only.
    aspect_solutions : tuple of str
        Every aspect solution of this part. Usually one; ``433``'s first part has three.
    orbit_ephemeris, bad_pixel_file, mask_file, gti_file : str or None
    """

    number: Optional[int]
    tstart: Optional[float]
    tstop: Optional[float]
    dead_time_file: Optional[str] = None
    aspect_solutions: tuple = ()
    orbit_ephemeris: Optional[str] = None
    bad_pixel_file: Optional[str] = None
    mask_file: Optional[str] = None
    gti_file: Optional[str] = None


#: The families a part is made of: ``(field, archive patterns, repro pattern)``, searched
#: the way the single-file getters above search them. The bad-pixel list is looked for in
#: both directories, because ACIS files it under ``primary/`` and HRC under ``secondary/``.
_PART_FAMILIES = (
    ("dead_time_file", (os.path.join("primary", "*_dtf1.fits"),), None),
    ("aspect_solutions", (os.path.join("primary", "*_asol1.fits"),), "pcadf*_asol1.fits"),
    ("orbit_ephemeris", (os.path.join("primary", "orbitf*_eph1.fits"),), None),
    (
        "bad_pixel_file",
        (os.path.join("primary", "*_bpix1.fits"), os.path.join("secondary", "*_bpix1.fits")),
        "*_repro_bpix1.fits",
    ),
    ("mask_file", (os.path.join("secondary", "*_msk1.fits"),), "*_msk1.fits"),
    ("gti_file", (os.path.join("secondary", "*_flt1.fits"),), "*_repro_flt2.fits"),
)

#: Read only for the time it spans: every part has one, on both instruments.
_PART_FIELD_OF_VIEW = os.path.join("primary", "*_fov1.fits")

#: ``hrcf01411_002N006_dtf1`` and ``pcadf01411_002N001_asol1`` both name part 2. The
#: time-named ``pcadf071323369N004_asol1`` and ``orbitf057024064N002_eph1`` name none.
_PART_NUMBER_RE = re.compile(r"^[a-z]+f\d+_(\d{3})N\d{3}_")

#: The processing version, ``N006``, which is what two copies of one file differ by.
_VERSION_RE = re.compile(r"N\d{3}(?=_)")


def _part_number(path):
    """The part number in an archive name, or ``None`` when the name carries none."""
    found = _PART_NUMBER_RE.match(os.path.basename(path))
    return None if found is None else int(found.group(1))


def _family_files(obsid, config, archive_patterns, repro):
    """Every file of one family, from the first directory that holds any."""
    for pattern in archive_patterns:
        found = _archive_products(obsid, config, pattern, repro)
        if found:
            return found
    return []


def _refuse_two_versions(obsid, files):
    """
    Raise when one file is present twice, under two processing versions or compressions.

    Parts legitimately differ in version -- ``108`` ships ``_000N006`` beside
    ``_001N005`` -- so only two copies of the *same* file are refused: the same name once
    the version and the ``.gz`` are taken off.
    """
    copies = {}
    for path in files:
        name = os.path.basename(path)
        name = name[: -len(".gz")] if name.endswith(".gz") else name
        copies.setdefault(_VERSION_RE.sub("N", name), []).append(path)
    for paths in copies.values():
        if len(paths) > 1:
            raise ValueError(
                f"observation {obsid} has {len(paths)} versions of one file, and one of "
                f"them would be reduced in silence: {sorted(os.path.basename(p) for p in paths)}"
            )


def _time_keywords(path):
    """``(TSTART, TSTOP, OBI_NUM)`` from a file's first extension; ``OBI_NUM`` may be None."""
    header = fits.getheader(path, 1)
    obi = header.get("OBI_NUM")
    return float(header["TSTART"]), float(header["TSTOP"]), None if obi is None else int(obi)


def _overlaps(first, second):
    """Whether two ``(start, stop)`` spans share any time. Touching is not sharing."""
    return first[0] < second[1] and second[0] < first[1]


def _at_most_one(obsid, number, field, paths):
    """The single file of a family in one part, ``None``, or an error naming them all."""
    if len(paths) > 1:
        what = "orbit ephemeris" if field == "orbit_ephemeris" else field.replace("_", " ")
        raise ValueError(
            f"part {number} of observation {obsid} has {len(paths)} {what} files, and "
            f"choosing one would be a guess: {sorted(os.path.basename(p) for p in paths)}"
        )
    return paths[0] if paths else None


def chandra_observation_parts(obsid, config):
    """
    Split an observation's companion files into its parts.

    **Files that carry a part number are paired by it**; files that do not are paired by
    the time they cover. That second rule is not optional: orbit files are named by start
    time on every observation, and so are the aspect solutions of the oldest ones --
    ``433`` has five ``pcadf<time>N004_asol1`` files for three parts. Where a time-paired
    file's header has ``OBI_NUM``, it must agree with the part the time put it in.

    **An observation of one part opens nothing new.** Every file belongs to that part, and
    its span is the event list's own ``TSTART`` and ``TSTOP``. This is the path every
    ordinary observation takes, and the companions' headers are never read on it.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``. ``products`` chooses where to look, as it does
        for the single-file getters.

    Returns
    -------
    tuple of ObservationPart
        In part-number order. One, for an ordinary observation.

    Raises
    ------
    ValueError
        If one file of one part is present in two versions; if a part has two orbit files,
        bad-pixel lists, masks, good-time files or dead-time files; or if a time-paired
        file's ``OBI_NUM`` names a different part from the one its time falls in.
    """
    obsid = chandra_obsid(obsid)
    families = {
        field: _family_files(obsid, config, archive, repro)
        for field, archive, repro in _PART_FAMILIES
    }
    fields_of_view = _archive_products(obsid, config, _PART_FIELD_OF_VIEW)
    for files in (*families.values(), fields_of_view):
        _refuse_two_versions(obsid, files)

    numbered = {}
    for files in (*families.values(), fields_of_view):
        for path in files:
            if _part_number(path) is not None:
                numbered.setdefault(_part_number(path), []).append(path)

    if len(numbered) <= 1:
        number = next(iter(numbered), None)
        events = chandra_event_list(obsid, config)
        tstart = tstop = None
        if events is not None:
            header = fits.getheader(events, 1)
            tstart, tstop = header.get("TSTART"), header.get("TSTOP")
        fields = {
            field: (
                tuple(files)
                if field == "aspect_solutions"
                else _at_most_one(obsid, number, field, files)
            )
            for field, files in families.items()
        }
        return (
            ObservationPart(
                number=number,
                tstart=None if tstart is None else float(tstart),
                tstop=None if tstop is None else float(tstop),
                **fields,
            ),
        )

    spans = {}
    for number, paths in numbered.items():
        times = [_time_keywords(path) for path in paths]
        spans[number] = (min(t[0] for t in times), max(t[1] for t in times))

    unnumbered = {
        path: _time_keywords(path)
        for files in families.values()
        for path in files
        if _part_number(path) is None
    }
    paired = set()
    parts = []
    for number in sorted(numbered):
        fields = {}
        for field, files in families.items():
            mine = [path for path in files if _part_number(path) == number]
            for path in files:
                if path not in unnumbered or not _overlaps(unnumbered[path][:2], spans[number]):
                    continue
                header_part = unnumbered[path][2]
                if header_part is not None and header_part != number:
                    raise ValueError(
                        f"{os.path.basename(path)} falls in the time of part {number} of "
                        f"observation {obsid}, and its OBI_NUM says part {header_part}"
                    )
                mine.append(path)
                paired.add(path)
            fields[field] = (
                tuple(sorted(mine))
                if field == "aspect_solutions"
                else _at_most_one(obsid, number, field, mine)
            )
        parts.append(ObservationPart(number, *spans[number], **fields))

    for path in sorted(set(unnumbered) - paired):
        get_logger().warning(
            f"{obsid}: {os.path.basename(path)} covers the time of none of the "
            f"{len(parts)} parts, so no part uses it"
        )
    return tuple(parts)


#: Chandra's mission reference, 1998-01-01 in TT, for an event list that does not say.
CHANDRA_MJDREF = 50814.0


def chandra_parts_warning(obsid, stem, parts, mjdref=CHANDRA_MJDREF):
    """
    What the log, the record and the report page say about an observation in parts.

    In words, because it changes how everything else about the observation is read: how
    many parts there are, when each was taken, how far apart, and what each part's products
    are called.

    Parameters
    ----------
    obsid : str
    stem : str
        The observation's stem, which each part's label is added to.
    parts : sequence of ObservationPart
    mjdref : float, optional
        What the parts' times count from, as the event list's ``MJDREF`` gives it.

    Returns
    -------
    str or None
        ``None`` for an observation of one part.

    Examples
    --------
    >>> parts = (ObservationPart(1, 74117285.6, 74123990.7), ObservationPart(2, 77203909.7, 77206498.4))
    >>> print(chandra_parts_warning("380", "chandra00380_acisi_timed", parts))
    ... # doctest: +NORMALIZE_WHITESPACE
    380 was taken in 2 parts, 35.6 days apart: part 1 on 2000-05-07 to 2000-05-07, whose
    products are named chandra00380_acisi_timed_obi001; part 2 on 2000-06-12 to 2000-06-12,
    whose products are named chandra00380_acisi_timed_obi002. Each part is reduced as an
    observation of its own and nothing is merged across the gap, so a timing search over
    more than one part has to be chosen, not inherited.
    """
    if len(parts) < 2:
        return None

    def date(met):
        return Time(mjdref + met / 86400.0, format="mjd", scale="tt").utc.iso[:10]

    gaps = [
        f"{(later.tstart - earlier.tstop) / 86400.0:.1f}"
        for earlier, later in zip(parts, parts[1:])
    ]
    each = "; ".join(
        f"part {part.number} on {date(part.tstart)} to {date(part.tstop)}, whose products "
        f"are named {stem}_{chandra_part_label(part)}"
        for part in parts
    )
    return (
        f"{obsid} was taken in {len(parts)} parts, {' and '.join(gaps)} days apart: {each}. "
        "Each part is reduced as an observation of its own and nothing is merged across the "
        "gap, so a timing search over more than one part has to be chosen, not inherited."
    )


def _part_summary(part):
    """One part as the diagnostics record carries it: numbers and file names, not paths."""

    def name(path):
        return None if path is None else os.path.basename(path)

    return dict(
        number=part.number,
        tstart=part.tstart,
        tstop=part.tstop,
        dead_time_file=name(part.dead_time_file),
        aspect_solutions=[os.path.basename(path) for path in part.aspect_solutions],
        orbit_ephemeris=name(part.orbit_ephemeris),
        bad_pixel_file=name(part.bad_pixel_file),
        mask_file=name(part.mask_file),
        gti_file=name(part.gti_file),
    )


#: ``READMODE`` as the header spells it, against the label an output file carries.
ACIS_READ_MODES = {"TIMED": "timed", "CONTINUOUS": "cc"}


def chandra_mode_label(header, fast_timing=None):
    """
    The mode field of an output file's stem.

    Three rules, in order.

    A **grating** in the beam names the mode, because a grating observation's products are
    the grating products: ``chandra02749_aciss_hetg_src.pi``.

    Otherwise **ACIS** says its readout mode in ``READMODE``: ``TIMED`` or ``CONTINUOUS``,
    which become ``timed`` and ``cc``. Note the label says nothing about how fast the
    observation actually is -- ``timed`` covers both a 3.2 s full frame and obsid
    ``5644``'s 0.44 s subarray, and the honest number comes from ``TIMEDEL``.

    Otherwise **HRC**, which says nothing usable at all. Every HRC event header reads
    ``DATAMODE = 'OBSERVING'`` and ``TIMEDEL = 1.5625e-05`` whether or not that resolution
    is real, so the caller must have read the dead-time file and must pass the answer in.

    Parameters
    ----------
    header : dict or astropy.io.fits.Header
        An event list header, carrying ``INSTRUME``, ``GRATING`` and -- for ACIS --
        ``READMODE``.
    fast_timing : bool, optional
        For HRC only: whether this observation really has the 15.625 us resolution its
        header claims, as the dead-time file's veto ratio decides it.

    Returns
    -------
    str
        ``"hetg"``, ``"letg"``, ``"timed"``, ``"cc"``, ``"timing"`` or ``"imaging"``.

    Raises
    ------
    ValueError
        For an HRC header with no ``fast_timing`` given, or an ACIS ``READMODE`` this does
        not know. Defaulting the first would label ``17661``, a real fast-timing
        observation, exactly as it labels ``6298``, which is the confusion this module
        exists to prevent.

    Examples
    --------
    >>> chandra_mode_label({"INSTRUME": "ACIS", "GRATING": "HETG", "READMODE": "TIMED"})
    'hetg'
    >>> chandra_mode_label({"INSTRUME": "HRC", "GRATING": "NONE"}, fast_timing=True)
    'timing'
    """
    grating = str(header.get("GRATING", "NONE")).strip().upper()
    if grating in ("HETG", "LETG"):
        return grating.lower()

    if str(header.get("INSTRUME", "")).strip().upper() == "HRC":
        if fast_timing is None:
            raise ValueError(
                "an HRC header cannot say whether its 15.625 us resolution is real, so "
                "fast_timing must be given -- read it off the dead-time file"
            )
        return "timing" if fast_timing else "imaging"

    read_mode = str(header.get("READMODE", "")).strip().upper()
    if read_mode not in ACIS_READ_MODES:
        raise ValueError(f"READMODE {read_mode!r} is neither TIMED nor CONTINUOUS")
    return ACIS_READ_MODES[read_mode]


#: The time resolution the CXC documents for HRC where the wiring error is not
#: recoverable: "about 4 milliseconds". Used only when there is no dead-time file to
#: measure the trigger rate from, and always flagged as the assumption it is.
#:
#: See https://cxc.cfa.harvard.edu/ciao/caveats/hrc_timing.html
HRC_DOCUMENTED_RESOLUTION = 4.0e-3


@dataclass
class DeadTimeFactors:
    """
    What the dead-time-factor file says about on-board vetoing.

    Attributes
    ----------
    veto_ratio : float
        ``VALID_EVT_COUNT / TOTAL_EVT_COUNT``, from medians over the usable rows. One
        means nothing was vetoed; ``6298`` measures 0.296.
    trigger_rate_hz : float
        Front-end triggers per second: median ``TOTAL_EVT_COUNT`` over the sample
        interval.
    sample_interval : float
        Seconds between rows, measured rather than read from the header.
    n_good_rows, n_rows : int
        Rows with every ``STATUS`` bit clear, and rows in the file.
    """

    veto_ratio: float
    trigger_rate_hz: float
    sample_interval: float
    n_good_rows: int
    n_rows: int


@dataclass
class TimeResolution:
    """
    What time resolution an observation can actually support, and why.

    Recorded in the diagnostics for every observation, so that a user reads the honest
    number rather than one off a spec sheet.

    Attributes
    ----------
    seconds : float
        The resolution itself.
    basis : str
        Which branch produced it, as a short key for the diagnostics record.
    reason : str
        The same thing in plain English, for the report page.
    fast_timing : bool or None
        For HRC, whether the wiring error is recoverable and the header's 15.625 us
        stands. ``None`` for ACIS, where the question does not arise. This is what
        :func:`chandra_mode_label` needs to label an HRC observation.
    veto_ratio, trigger_rate_hz : float or None
        Carried through from :class:`DeadTimeFactors` when there was one, so the record
        shows the evidence and not only the conclusion.
    parts : tuple of dict
        For an observation taken in several parts, each part's own answer -- ``number``,
        ``seconds``, ``basis``, ``fast_timing``, ``veto_ratio``, ``trigger_rate_hz`` and
        ``exposure_s`` -- so the record shows what was combined. Empty for one part.
    """

    seconds: float
    basis: str
    reason: str
    fast_timing: Optional[bool] = None
    veto_ratio: Optional[float] = None
    trigger_rate_hz: Optional[float] = None
    parts: tuple = ()


def read_dead_time_factors(path):
    """
    Measure the on-board veto fraction and trigger rate from a ``dtf1`` file.

    The file is 71-109 kB, is already downloaded because HRC rates need it, and samples
    ``TOTAL_EVT_COUNT`` and ``VALID_EVT_COUNT`` every 2.05 s. Their ratio is the veto
    fraction, which is exactly what decides whether the HRC wiring error is recoverable.

    Medians rather than sums, and only rows with every ``STATUS`` bit clear: on the real
    ``6298`` that is 2 392 rows of 2 769.

    Parameters
    ----------
    path : str
        Path to the ``dtf1`` file, gzipped or not.

    Returns
    -------
    DeadTimeFactors

    Raises
    ------
    ValueError
        If no row has a clear ``STATUS``, or if the median trigger count is zero -- which
        would make the derived resolution infinite rather than merely wrong.

    Notes
    -----
    The file's own ``TIMEDEL`` keyword reads 2.0 and is the *sampling* interval's nominal
    value. It is not the event time resolution, and it is not the measured sampling
    interval either, which is 2.05 s. Three different things, one keyword name.
    """
    with fits.open(path) as hdulist:
        table = hdulist["DTF"].data
        status = table["STATUS"]
        clear = status.sum(axis=1) == 0 if np.ndim(status) > 1 else status == 0
        good = table[clear]
        n_rows = len(table)

    if len(good) == 0:
        raise ValueError(f"{path} has no usable rows: every STATUS is flagged")

    times = np.sort(np.asarray(good["TIME"], dtype=float))
    sample_interval = float(np.median(np.diff(times))) if len(times) > 1 else float("nan")
    total = float(np.median(good["TOTAL_EVT_COUNT"]))
    valid = float(np.median(good["VALID_EVT_COUNT"]))

    if total <= 0:
        raise ValueError(f"{path} records no triggers, so no trigger rate can be measured")

    return DeadTimeFactors(
        veto_ratio=valid / total,
        trigger_rate_hz=total / sample_interval,
        sample_interval=sample_interval,
        n_good_rows=len(good),
        n_rows=n_rows,
    )


def chandra_time_resolution(header, dtf=None, config=None):
    """
    What time resolution an observation can actually support, with its reason.

    This is the honest analogue of XMM's extraction-window check: the pipeline states what
    timing the data carry instead of letting a user read 16 us off a spec sheet. Pure
    Python, no CIAO, no network.

    The branches:

    * **ACIS, Timed Exposure** -- the frame time, from ``TIMEDEL``. Read and never
      assumed: obsid ``2749`` measures 2.54104 s and ``5644`` measures **0.44104 s**
      against a nominal 3.2 s, and it was ``5644``'s subarray that let Liu 2024 detect a
      1.37 s pulsation.
    * **ACIS, Continuous Clocking** -- also ``TIMEDEL``, which measures 2.85 ms, with the
      warning that one spatial dimension is gone and source and background overlap in it.
    * **HRC with no dead-time file** -- the documented ~4 ms, flagged as an assumption.
      Never the header's 15.625 us, which would be an unearned claim.
    * **HRC, veto ratio at or above the threshold** -- ``TIMEDEL``, the full 15.625 us:
      every trigger was telemetered, so the wiring error is recoverable.
    * **HRC, veto ratio below it** -- one over the trigger rate.

    **The order of those last two matters and is the point of the function.** Applying the
    rate formula to a ``S_TIMING`` observation gives 16.67 ms, a thousand times worse than
    the truth, so the ratio is tested first.

    Parameters
    ----------
    header : dict or astropy.io.fits.Header
        The event list header: ``INSTRUME``, ``TIMEDEL``, and ``READMODE`` for ACIS.
    dtf : DeadTimeFactors, optional
        What :func:`read_dead_time_factors` found, for HRC. Ignored for ACIS.
    config : dict, optional
        Only ``hrc_veto_ratio_threshold`` is read; defaults to
        :data:`DEFAULT_CONFIG`'s 0.99.

    Returns
    -------
    TimeResolution

    Raises
    ------
    ValueError
        For a missing ``TIMEDEL``, or an ACIS ``READMODE`` this does not know.

    Examples
    --------
    >>> hrc_s = DeadTimeFactors(1.0, 60.0, 2.05, 14586, 14588)
    >>> found = chandra_time_resolution({"INSTRUME": "HRC", "TIMEDEL": 1.5625e-05}, hrc_s)
    >>> found.seconds, found.fast_timing
    (1.5625e-05, True)
    """
    config = DEFAULT_CONFIG if config is None else config
    threshold = config.get("hrc_veto_ratio_threshold", DEFAULT_CONFIG["hrc_veto_ratio_threshold"])
    instrument = str(header.get("INSTRUME", "")).strip().upper()

    timedel = header.get("TIMEDEL")
    if timedel is None and not (instrument == "HRC" and dtf is None):
        raise ValueError("the event header carries no TIMEDEL, and inventing one would be a lie")

    if instrument == "HRC":
        if dtf is None:
            return TimeResolution(
                seconds=HRC_DOCUMENTED_RESOLUTION,
                basis="hrc_documented",
                reason=(
                    "No dead-time file was downloaded, so the on-board veto fraction "
                    "could not be measured. Assuming the CXC's documented ~4 ms for HRC, "
                    "rather than the 15.625 us the header claims: a backplane wiring "
                    "error time-tags each event with the following trigger, and only an "
                    "unvetoed observation can have that undone."
                ),
                fast_timing=False,
            )
        if dtf.veto_ratio >= threshold:
            return TimeResolution(
                seconds=float(timedel),
                basis="hrc_unvetoed",
                reason=(
                    f"{dtf.veto_ratio:.3f} of front-end triggers were telemetered, at or "
                    f"above the {threshold:g} threshold, so no vetoing took place and the "
                    "wiring error's time-tag shift can be undone. The header's "
                    f"{float(timedel) * 1e6:.3f} us therefore stands. This is the "
                    "S_TIMING signature."
                ),
                fast_timing=True,
                veto_ratio=dtf.veto_ratio,
                trigger_rate_hz=dtf.trigger_rate_hz,
            )
        return TimeResolution(
            seconds=1.0 / dtf.trigger_rate_hz,
            basis="hrc_trigger_rate",
            reason=(
                f"Only {dtf.veto_ratio:.3f} of front-end triggers were telemetered, so "
                f"{(1 - dtf.veto_ratio) * 100:.1f} per cent were vetoed on board and the "
                "wiring error's time-tag shift cannot be undone. The resolution is one "
                f"over the trigger rate of {dtf.trigger_rate_hz:.1f} per second, not the "
                f"{float(timedel) * 1e6:.3f} us the header claims."
            ),
            fast_timing=False,
            veto_ratio=dtf.veto_ratio,
            trigger_rate_hz=dtf.trigger_rate_hz,
        )

    if instrument != "ACIS":
        raise ValueError(f"INSTRUME {instrument!r} is neither ACIS nor HRC")

    read_mode = str(header.get("READMODE", "")).strip().upper()
    if read_mode == "TIMED":
        return TimeResolution(
            seconds=float(timedel),
            basis="acis_frame_time",
            reason=(
                f"ACIS Timed Exposure reads out every {float(timedel):.5f} s, and that "
                "frame time is the finest timing the events carry. It is read from the "
                "header rather than assumed: a subarray runs far faster than the nominal "
                "3.2 s."
            ),
        )
    if read_mode == "CONTINUOUS":
        return TimeResolution(
            seconds=float(timedel),
            basis="acis_continuous_clocking",
            reason=(
                f"ACIS Continuous Clocking reads out a row every {float(timedel) * 1e3:.2f} "
                "ms. One spatial dimension is destroyed to buy that, so source and "
                "background overlap along the collapsed axis and cannot be separated by "
                "position."
            ),
        )
    raise ValueError(f"READMODE {read_mode!r} is neither TIMED nor CONTINUOUS")


def combine_dead_time_factors(factors, exposures):
    """
    Several parts' dead-time evidence as one, each part weighted by its exposure.

    Parameters
    ----------
    factors : sequence of DeadTimeFactors
    exposures : sequence of float or None
        Seconds of good time per part. Where any is unknown, or they sum to nothing, every
        part weighs the same.

    Returns
    -------
    DeadTimeFactors
        The veto ratio, trigger rate and sample interval are weighted means; the row counts
        are totals.

    Examples
    --------
    >>> one = DeadTimeFactors(0.2, 200.0, 2.05, 10, 10)
    >>> two = DeadTimeFactors(0.3, 100.0, 2.05, 10, 12)
    >>> round(combine_dead_time_factors([one, two], [3000.0, 1000.0]).veto_ratio, 3)
    0.225
    """
    weights = np.array([np.nan if one is None else float(one) for one in exposures])
    if not np.all(np.isfinite(weights)) or weights.sum() <= 0:
        weights = np.ones(len(factors))

    def mean(values):
        return float(np.average(np.asarray(values, dtype=float), weights=weights))

    return DeadTimeFactors(
        veto_ratio=mean([one.veto_ratio for one in factors]),
        trigger_rate_hz=mean([one.trigger_rate_hz for one in factors]),
        sample_interval=mean([one.sample_interval for one in factors]),
        n_good_rows=sum(one.n_good_rows for one in factors),
        n_rows=sum(one.n_rows for one in factors),
    )


def chandra_parts_time_resolution(header, evidence, config=None):
    """
    One time resolution for an observation taken in several parts.

    Every part is first answered on its own, by :func:`chandra_time_resolution`, and the
    answers are kept. Then:

    * **ACIS** -- the parts were checked to share their readout by
      :func:`chandra_check_part_configurations`, so they share one answer.
    * **HRC, every part on the same side of the veto threshold** -- the dead-time evidence
      is combined, weighted by each part's exposure, and answered once. On obsid ``1411``
      that is a veto ratio of 0.227 and 4.93 ms, between its parts' 4.82 and 5.18 ms.
    * **HRC, parts on opposite sides**, or a part with no dead-time file beside one with
      -- the coarser answer. An average here is a trap: 500 ks unvetoed and 1 ks vetoed
      average above the threshold and would claim 15.625 us for events of which some are
      good to 5 ms.

    Parameters
    ----------
    header : dict or astropy.io.fits.Header
        The merged event list's header, as for :func:`chandra_time_resolution`.
    evidence : sequence of tuple
        ``(part number, DeadTimeFactors or None, exposure in seconds or None)``, one per
        part. See :func:`chandra_part_exposure`.
    config : dict, optional
        Only ``hrc_veto_ratio_threshold`` is read.

    Returns
    -------
    TimeResolution
        For one part, exactly what :func:`chandra_time_resolution` returns, and nothing
        about exposure is needed.
    """
    evidence = list(evidence)
    if len(evidence) == 1:
        return chandra_time_resolution(header, evidence[0][1], config)

    answers = [chandra_time_resolution(header, dtf, config) for _, dtf, _ in evidence]
    parts = tuple(
        dict(
            number=number,
            seconds=answer.seconds,
            basis=answer.basis,
            fast_timing=answer.fast_timing,
            veto_ratio=answer.veto_ratio,
            trigger_rate_hz=answer.trigger_rate_hz,
            exposure_s=exposure,
        )
        for (number, _, exposure), answer in zip(evidence, answers)
    )
    count = len(evidence)
    by_part = "; ".join(f"part {part['number']}: {part['seconds'] * 1e3:.3g} ms" for part in parts)

    def answered(answer, reason, **changes):
        fields = dict(
            seconds=answer.seconds,
            basis=answer.basis,
            fast_timing=answer.fast_timing,
            veto_ratio=answer.veto_ratio,
            trigger_rate_hz=answer.trigger_rate_hz,
        )
        fields.update(changes)
        return TimeResolution(reason=reason, parts=parts, **fields)

    if str(header.get("INSTRUME", "")).strip().upper() != "HRC":
        return answered(
            answers[0],
            f"All {count} parts were read out the same way, so they share one answer. "
            f"{answers[0].reason}",
        )

    dtfs = [dtf for _, dtf, _ in evidence]
    if all(dtf is None for dtf in dtfs):
        return answered(
            answers[0], f"None of the {count} parts has a dead-time file. {answers[0].reason}"
        )

    if any(dtf is None for dtf in dtfs) or len({one.fast_timing for one in answers}) > 1:
        coarsest = max(answers, key=lambda answer: answer.seconds)
        known = [(dtf, exposure) for _, dtf, exposure in evidence if dtf is not None]
        return answered(
            coarsest,
            f"The {count} parts of this observation do not agree on whether on-board "
            f"vetoing took place ({by_part}), so the coarser resolution, "
            f"part {parts[answers.index(coarsest)]['number']}'s "
            f"{coarsest.seconds * 1e3:.3g} ms, is the one that holds for the observation as "
            "a whole. An average would claim a resolution some of its events do not have.",
            basis="hrc_parts_disagree",
            fast_timing=False,
            veto_ratio=combine_dead_time_factors(*zip(*known)).veto_ratio,
        )

    combined = chandra_time_resolution(
        header,
        combine_dead_time_factors(dtfs, [exposure for _, _, exposure in evidence]),
        config,
    )
    return answered(
        combined,
        f"Combined over {count} parts, weighted by exposure ({by_part}). {combined.reason}",
    )


def chandra_part_exposure(part):
    """
    How much good time one part holds, which is what its evidence is weighted by.

    Parameters
    ----------
    part : ObservationPart

    Returns
    -------
    float or None
        The part's own good-time intervals, summed; failing those, the span its files
        cover; failing that, ``None``, which :func:`combine_dead_time_factors` reads as
        "weigh every part the same".
    """
    if part.gti_file is not None:
        gti = read_observation_gti(part.gti_file)
        if gti is not None and len(gti):
            return float(np.sum(gti[:, 1] - gti[:, 0]))
    if part.tstart is not None and part.tstop is not None:
        return float(part.tstop - part.tstart)
    return None


def _time_resolution_evidence(parts):
    """``(number, dead-time factors, exposure)`` per part; exposure only when there are several."""
    several = len(parts) > 1
    return [
        (
            part.number,
            None if part.dead_time_file is None else read_dead_time_factors(part.dead_time_file),
            chandra_part_exposure(part) if several else None,
        )
        for part in parts
    ]


#: What every part of an observation must share for one time resolution and one file stem
#: to describe them all: the readout mode, the frame time, and the chips -- or, for HRC,
#: the detector.
PART_CONFIGURATION_KEYWORDS = ("READMODE", "TIMEDEL", "DETNAM")


def chandra_check_part_configurations(header, parts):
    """
    Refuse an observation whose parts were not taken the same way.

    Each part's good-time file repeats the readout keywords -- measured on obsid ``380``,
    whose mask and field-of-view files do too -- and they are compared with each other and
    with the merged event list. ``FIRSTROW`` is deliberately not among them: the mask file
    uses that name for something else, 3 against the event list's 1 on ``380``, and a
    subarray already shows in ``TIMEDEL``.

    An observation of one part is not checked, and nothing is opened.

    Parameters
    ----------
    header : dict or astropy.io.fits.Header
        The merged event list's header.
    parts : sequence of ObservationPart

    Raises
    ------
    ValueError
        If any of :data:`PART_CONFIGURATION_KEYWORDS` differs anywhere. One time resolution
        and one file stem cannot describe both, and reducing them as one would be wrong in
        silence.
    """
    if len(parts) < 2:
        return

    rows = [("the event list", header)]
    for part in parts:
        source = part.gti_file or part.mask_file
        if source is not None:
            rows.append((f"part {part.number}", fits.getheader(source, 1)))

    for keyword in PART_CONFIGURATION_KEYWORDS:
        seen = {
            label: (
                round(float(found[keyword]), 9)
                if keyword == "TIMEDEL"
                else str(found[keyword]).strip().upper()
            )
            for label, found in rows
            if found.get(keyword) is not None
        }
        if len(set(seen.values())) > 1:
            raise ValueError(
                f"the parts of this observation were not taken the same way: {keyword} is "
                + ", ".join(f"{value} in {label}" for label, value in seen.items())
                + ". One time resolution and one file stem cannot describe them, so the "
                "observation is refused rather than reduced as if it were one."
            )


#: The CALDB current on the CXC's conda channel on 2026-09-12.
#:
#: Used only to say how stale an archive product's calibration is. It is a diagnostic and
#: never a route decision: with archive level-2 as the default, the reduction reports the
#: staleness and lets the user ask for ``products="repro"`` if they care. Bump it when the
#: channel moves, or the report merely becomes less useful rather than wrong.
CURRENT_CALDB_VERSION = "4.12.4"


def caldb_version_tuple(version):
    """
    A CALDB version as integers, for comparing.

    Compared as text, ``"4.9.4" > "4.12.4"``, because ``9`` sorts after ``1``. That would
    call the oldest products in the archive up to date, so versions are never compared as
    strings.

    Parameters
    ----------
    version : str or None
        As ``CALDBVER`` spells it: ``"4.9.4"``, ``"4.12.4"``, occasionally with a letter.

    Returns
    -------
    tuple of int or None
        ``None`` when there is nothing readable, which is a diagnostic that says so
        rather than a failed reduction.

    Examples
    --------
    >>> caldb_version_tuple("4.9.4") < caldb_version_tuple("4.12.4")
    True
    """
    if not version:
        return None
    parts = []
    for piece in str(version).split("."):
        digits = "".join(takewhile(str.isdigit, piece))
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts) or None


@dataclass(frozen=True)
class Observation:
    """
    One Chandra observation, and the files that hold it.

    A Chandra observation is **one detector in one mode**, so unlike XMM's ``Exposure``
    there is exactly one of these per OBSID and no fan-out: no ``(instrument, expid,
    mode)`` key, and nothing downstream has to loop.

    Attributes
    ----------
    obsid : str
        Unpadded, as :func:`chandra_obsid` gives it.
    detector : str
        ``"acisi"``, ``"aciss"``, ``"hrci"`` or ``"hrcs"``.
    grating : str
        ``"NONE"``, ``"HETG"`` or ``"LETG"``, as the header spells it.
    mode : str
        The label :func:`chandra_mode_label` chose, which is what the file stem carries.
    time_resolution : TimeResolution
        What the data can actually support, with its reason. For HRC this is what decided
        ``mode``.
    chips : tuple of int
        CCDs read out, empty for HRC. See :func:`chandra_chips`.
    event_list : str
        The level-2 event list. Every other path may be ``None``; this one may not, and an
        observation without it is not an ``Observation`` at all.
    aspect_solution, bad_pixel_file, mask_file, gti_file : str or None
        Companion products. All four are ``None`` for an observation of several parts,
        where each part has its own in ``parts``: see :attr:`aspect_solutions` and
        :func:`chandra_part_observation`.
    dead_time_file : str or None
        HRC only, and ``None`` for ACIS is normal rather than missing. ``None`` too for an
        observation of several parts, where each part's is in ``parts``.
    orbit_ephemeris : str or None
        What ``axbary`` barycentres with. ``None`` for an observation of several parts,
        each of which has its own in ``parts``.
    grating_spectrum : str or None
    grating_responses : tuple of str
        The archive's ready-made grating products, collected and never re-made.
    caldb_version, ascds_version : str or None
        What the archive's own reduction was made with.
    data_mode : str or None
        ``DATAMODE``, as the header spells it. ``FAINT``, ``VFAINT`` and ``GRADED`` for
        ACIS; ``OBSERVING`` for HRC. It is not the mode in the file stem and is not used
        to choose anything -- it is carried because ``GRADED`` changes what a spectrum is
        worth, and the report page should say so rather than leave it to be discovered.
    active_rows : tuple or None
        ``(FIRSTROW, NROWS)`` for ACIS -- the rows of each CCD that were actually clocked
        out. This is the subarray, and it is why obsid ``5644`` reads out every 0.44 s
        instead of every 3.2 s: 128 rows of 1024. ``None`` for HRC, and for an ACIS header
        that does not say, which means a full frame.
    sky_pixel_arcsec : float or None
        What one sky pixel is on the sky, as the event list declares it: 0.492 arcseconds
        for ACIS and 0.1318 for HRC. Left ``None``, it is filled in from ``detector``.
    parts : tuple of ObservationPart
        The pointings the observation was taken in, each with its own companion files --
        see :func:`chandra_observation_parts`. One for an ordinary observation; empty only
        for an ``Observation`` built by hand.
    part : ObservationPart or None
        Set when this ``Observation`` is one part of an observation taken in several, as
        :func:`chandra_part_observation` makes it; the part's label then ends the stem.
    """

    obsid: str
    detector: str
    grating: str
    mode: str
    time_resolution: TimeResolution
    chips: tuple
    event_list: str
    aspect_solution: Optional[str] = None
    bad_pixel_file: Optional[str] = None
    mask_file: Optional[str] = None
    gti_file: Optional[str] = None
    dead_time_file: Optional[str] = None
    orbit_ephemeris: Optional[str] = None
    grating_spectrum: Optional[str] = None
    grating_responses: tuple = ()
    caldb_version: Optional[str] = None
    ascds_version: Optional[str] = None
    data_mode: Optional[str] = None
    active_rows: Optional[tuple] = None
    sky_pixel_arcsec: Optional[float] = None
    parts: tuple = ()
    part: Optional[ObservationPart] = None

    def __post_init__(self):
        if self.sky_pixel_arcsec is None:
            object.__setattr__(self, "sky_pixel_arcsec", nominal_sky_pixel_arcsec(self.detector))

    @property
    def stem(self):
        """What every output file of this observation is named from."""
        stem = chandra_file_stem(self.obsid, self.detector, self.mode)
        return stem if self.part is None else f"{stem}_{chandra_part_label(self.part)}"

    @property
    def aspect_solutions(self):
        """
        Every aspect solution the merged event list was made with, in time order.

        The one ``aspect_solution`` when there is one. Otherwise every part's own, one after
        another -- ``433`` has five for three parts, three of them in its first -- which
        is also what one part taken as its own observation holds.
        """
        if self.aspect_solution is not None:
            return (self.aspect_solution,)
        return tuple(path for part in self.parts for path in part.aspect_solutions)

    @property
    def is_continuous_clocking(self):
        """
        Whether the readout collapsed a spatial dimension.

        Asked of the time resolution rather than of ``mode``, and that is not a detail:
        ``mode`` is the label in the file stem, and a grating in the beam takes that name
        for itself -- an HETG observation read out in Continuous Clocking is labelled
        ``hetg``, with no trace of ``cc`` in it. The time resolution's basis is derived
        from ``READMODE`` and cannot be shadowed.
        """
        return self.time_resolution.basis == "acis_continuous_clocking"

    @property
    def caldb_is_stale(self):
        """
        Whether the archive's reduction predates the current CALDB.

        ``None`` when either version is unreadable. Reported, never acted on.
        """
        made_with = caldb_version_tuple(self.caldb_version)
        current = caldb_version_tuple(CURRENT_CALDB_VERSION)
        if made_with is None or current is None:
            return None
        return made_with < current


def chandra_archive_front_end(obsid, config, rec=None):
    """
    Read one observation off the level-2 products, whichever route made them.

    The default route, and **also the reader the reprocessing route uses**: the getters
    above are route-aware, so :func:`chandra_repro_front_end` runs the task and then calls
    this to read the result. No CIAO is involved here. The archive's ``primary/`` products
    are what CIAO would produce, so this reads their headers, finds the companions and
    works out what the data can support.

    The one place it does real work is the time resolution, and for HRC that is not in the
    header: :func:`read_dead_time_factors` measures the on-board veto fraction, and its
    answer is what separates a genuine 15.625 us observation from one 280 times coarser.
    An HRC observation whose dead-time file was not downloaded still reads -- degraded to
    the documented ~4 ms, and saying so.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``. ``hrc_veto_ratio_threshold`` is read.
    rec : StepRecord, optional
        Where to record what was found, including how stale the calibration is.

    Returns
    -------
    Observation or None
        ``None`` when the directory holds no level-2 event list. Whether that means
        ``NO_SCIENCE_DATA`` is the caller's decision, as it is for XMM.
    """
    obsid = chandra_obsid(obsid)
    events = chandra_event_list(obsid, config)
    if events is None:
        return None

    with fits.open(events) as hdulist:
        header = hdulist[1].header

    parts = chandra_observation_parts(obsid, config)
    chandra_check_part_configurations(header, parts)
    resolution = chandra_parts_time_resolution(header, _time_resolution_evidence(parts), config)
    dtf_path = parts[0].dead_time_file if len(parts) == 1 else None

    observation = Observation(
        obsid=obsid,
        detector=chandra_detector(header),
        grating=str(header.get("GRATING", "NONE")).strip().upper(),
        mode=chandra_mode_label(header, fast_timing=resolution.fast_timing),
        time_resolution=resolution,
        chips=tuple(chandra_chips(header)),
        event_list=events,
        aspect_solution=chandra_aspect_solution(obsid, config) if len(parts) <= 1 else None,
        bad_pixel_file=chandra_bad_pixel_file(obsid, config) if len(parts) <= 1 else None,
        mask_file=chandra_mask_file(obsid, config) if len(parts) <= 1 else None,
        gti_file=chandra_gti_file(obsid, config) if len(parts) <= 1 else None,
        dead_time_file=dtf_path,
        orbit_ephemeris=chandra_orbit_ephemeris(obsid, config) if len(parts) <= 1 else None,
        grating_spectrum=chandra_grating_spectrum(obsid, config),
        grating_responses=tuple(chandra_grating_responses(obsid, config)),
        caldb_version=_keyword(header, "CALDBVER"),
        ascds_version=_keyword(header, "ASCDSVER"),
        data_mode=_keyword(header, "DATAMODE"),
        active_rows=_active_rows(header),
        sky_pixel_arcsec=chandra_sky_pixel_arcsec(header),
        parts=parts,
    )

    warning = chandra_parts_warning(
        obsid, observation.stem, parts, float(header.get("MJDREF", CHANDRA_MJDREF))
    )
    if warning is not None:
        get_logger().warning(warning)

    if rec is not None:
        rec.value(
            warnings=[] if warning is None else [warning],
            detector=observation.detector,
            grating=observation.grating,
            mode=observation.mode,
            chips=list(observation.chips),
            stem=observation.stem,
            time_resolution_s=resolution.seconds,
            time_resolution_basis=resolution.basis,
            time_resolution_reason=resolution.reason,
            hrc_veto_ratio=resolution.veto_ratio,
            hrc_trigger_rate_hz=resolution.trigger_rate_hz,
            time_resolution_parts=list(resolution.parts),
            caldb_version=observation.caldb_version,
            caldb_current=CURRENT_CALDB_VERSION,
            caldb_is_stale=observation.caldb_is_stale,
            ascds_version=observation.ascds_version,
            data_mode=observation.data_mode,
            has_dead_time_file=all(part.dead_time_file is not None for part in parts),
            n_grating_responses=len(observation.grating_responses),
            sky_pixel_arcsec=observation.sky_pixel_arcsec,
            n_parts=len(parts),
            parts=[_part_summary(part) for part in parts],
        )

    return observation


#: Products ``chandra_repro`` re-makes, against the ones it merely copies beside them.
#:
#: Measured on obsid ``5644`` on 2026-09-12. The twelve files it left were::
#:
#:     acisf05644_repro_evt2.fits      <- new: the level-2 event list
#:     acisf05644_repro_bpix1.fits     <- new: bad pixels, with afterglows re-found
#:     acisf05644_repro_flt2.fits      <- new: the good-time intervals, and note flt2
#:     acisf05644_repro_fov1.fits      <- new: the field of view
#:     acisf05644_000N004_bpix1.fits   <- copied, and the archive's own name
#:     acisf05644_000N004_fov1.fits    <- copied
#:     acisf05644_000N004_msk1.fits    <- copied
#:     acisf05644_000N004_mtl1.fits    <- copied
#:     acisf05644_000N004_stat1.fits   <- copied
#:     acisf240626566N004_pbk0.fits    <- copied
#:     pcadf05644_000N001_asol1.fits   <- copied, uncompressed
#:     acisf05644_asol1.lis            <- written for its own use
#:
#: Two things in that list are traps and both are handled in the getters above. The
#: copies mean a bare ``*_bpix1.fits`` glob matches **two** files in one directory, so the
#: reprocessing route asks for ``*_repro_bpix1.fits`` and gets the new one; and the new
#: good-time file is ``flt2``, not the ``flt1`` the archive ships, so the same getter asks
#: for a different name on each route.
#:
#: What is *not* there matters as much: no orbit ephemeris, no dead-time file, no grating
#: spectrum or responses. Those are read from the download, which is why the search path
#: has two roots and not one.
REPRO_EVENT_LIST_PATTERN = "*_repro_evt2.fits"


def chandra_repro_front_end(obsid, config, rec=None, env=None, log_to=None):
    """
    Re-run the archive's pipeline with ``chandra_repro``, then read what it wrote.

    The route behind ``config["products"] = "repro"``. It exists for the observations the
    archive's own products are too old for -- the calibration has moved, or the level-2
    file was made by a CIAO the reduction no longer trusts -- and for the ones that have no
    level-2 product at all.

    ``set_ardlib=no`` is not a detail. ``chandra_repro`` would otherwise write this
    observation's bad-pixel list into whichever ``ardlib.par`` it can reach, and
    :func:`heasarc_retrieve_pipeline.ciao.ciao_environment` already gives every observation
    a private one; letting the task do it as well means the file is written twice from two
    directions. See that module's docstring.

    **Nothing is renamed afterwards.** The reprocessed products keep ``chandra_repro``'s
    own names, against the output-naming rule and for the reason the grating products keep
    theirs: the task cross-references them in headers this pipeline did not write. Its
    names carry the obsid already, and :func:`chandra_repro_path` puts them in a directory
    that carries it too.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path`` and ``out_data_path``.
    rec : StepRecord, optional
        Where to record what was reprocessed, and what of the observation the reprocessing
        did not touch.
    env : dict, optional
        Environment for the task; :func:`~heasarc_retrieve_pipeline.ciao.ciao_environment`
        by default.
    log_to : str, optional
        Where to write the task's output.

    Returns
    -------
    Observation or None
        ``None`` when nothing at all was downloaded, which is the caller's cue for
        ``NO_SCIENCE_DATA``. Reading the reprocessed products is
        :func:`chandra_archive_front_end`'s job on this route as on the other one.

    Raises
    ------
    FileNotFoundError
        If the observation was downloaded but holds no level-1 event list, so there is
        nothing to reprocess. That is the archive route's download filter, which does not
        fetch level 1 -- an unrecoverable mismatch between the filter a run downloaded
        with and the route it is now reducing on, and silently falling back to the
        archive's level-2 file would answer a configuration error with the wrong data.
    RuntimeError
        If ``chandra_repro`` returns cleanly and leaves no event list.
    """
    from . import ciao

    obsid = chandra_obsid(obsid)
    indir = chandra_archive_path(obsid, config)
    outdir = chandra_repro_path(obsid, config)

    if not os.path.isdir(indir) or not os.listdir(indir):
        return None

    if chandra_level1_event_list(obsid, config) is None:
        raise FileNotFoundError(
            f"{obsid}: products='repro' was asked for and no level-1 event list was "
            f"downloaded to {indir}. The archive route's download filter does not fetch "
            f"one; re-download with products='repro' set."
        )

    # chandra_repro creates the last component of outdir and refuses to create any above
    # it, so it is made here. An existing empty directory it accepts even with clobber=no.
    os.makedirs(outdir, exist_ok=True)

    ciao.run(
        "chandra_repro",
        produces=[],  # the names are chandra_repro's own; the event list is checked below
        indir=indir,
        outdir=outdir,
        set_ardlib="no",
        clobber="yes",
        env=env if env is not None else ciao.ciao_environment(obsid, config),
        log_to=log_to,
    )

    reprocessed = _glob_both_ways(outdir, REPRO_EVENT_LIST_PATTERN)
    if not reprocessed:
        raise RuntimeError(
            f"{obsid}: chandra_repro returned cleanly and wrote no {REPRO_EVENT_LIST_PATTERN} "
            f"into {outdir}. A zero return code proves nothing; see ciao.run."
        )

    observation = chandra_archive_front_end(obsid, config, rec=rec)

    if rec is not None:
        rec.value(
            repro_directory=outdir,
            repro_event_list=observation.event_list if observation else None,
            reprocessed_products=sorted(
                os.path.basename(path) for path in _glob_both_ways(outdir, "*_repro_*.fits")
            ),
            # chandra_repro leaves CALDBVER at the value the archive's file carried -- on
            # 5644 it still read 4.9.2 after a run with CALDB 4.12.4 installed -- so on this
            # route the header's calibration version is not evidence of anything, and
            # ASCDSVER is the keyword that moves.
            caldb_version_is_from_the_archive=True,
        )

    return observation


def _active_rows(header):
    """
    ``(FIRSTROW, NROWS)`` from an event header, or ``None`` when it does not say.

    The subarray, and the reason a Timed Exposure observation can be fast: obsid ``5644``
    clocks out 128 rows starting at 449, which is what makes its frame time 0.44 s rather
    than 3.2 s. HRC has no such keywords, and neither does an ACIS full frame in some
    processing versions -- both come back ``None``, which downstream reads as 1 to 1024.

    Examples
    --------
    >>> _active_rows({"FIRSTROW": 449, "NROWS": 128})
    (449, 128)
    >>> _active_rows({"DETNAM": "HRC-I"}) is None
    True
    """
    first, nrows = header.get("FIRSTROW"), header.get("NROWS")
    if first is None or nrows is None:
        return None
    return (int(first), int(nrows))


def _keyword(header, name):
    """A header keyword as a stripped string, or ``None`` when it is absent or blank."""
    value = header.get(name)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


#: One ACIS sky pixel, in arcseconds: an 8192 x 8192 sky plane, ``TCDLT`` 1.3667e-4 degrees.
ACIS_SKY_PIXEL_ARCSEC = 0.492

#: One HRC sky pixel, in arcseconds. The sky plane has the detector's own resolution, so an
#: HRC-I plane is 32768 pixels across and an HRC-S one 65536, both at ``TCDLT`` 3.6611e-5
#: degrees -- measured on obsids ``8189`` and ``23460``. It is not the ACIS value, and
#: assuming it was put every HRC radius reported in arcseconds out by a factor of 3.7.
HRC_SKY_PIXEL_ARCSEC = 0.1318


def nominal_sky_pixel_arcsec(instrument):
    """
    The sky pixel scale an instrument's files normally carry.

    Examples
    --------
    >>> nominal_sky_pixel_arcsec("hrci")
    0.1318
    >>> nominal_sky_pixel_arcsec("ACIS")
    0.492
    """
    return (
        HRC_SKY_PIXEL_ARCSEC if str(instrument).upper().startswith("HRC") else ACIS_SKY_PIXEL_ARCSEC
    )


def chandra_sky_pixel_arcsec(header):
    """
    The sky pixel scale an event list declares, in arcseconds.

    Read off the ``x`` column's ``TCDLT``, which is what every coordinate in the file is
    measured in. A header that does not say falls back on :func:`nominal_sky_pixel_arcsec`
    for its ``INSTRUME``.

    Parameters
    ----------
    header : astropy.io.fits.Header
        The events extension's header.

    Returns
    -------
    float
    """
    for key in header:
        if key.startswith("TTYPE") and str(header[key]).strip().lower() == "x":
            increment = header.get(f"TCDLT{key[len('TTYPE') :]}")
            if increment:
                return abs(float(increment)) * 3600.0
    return nominal_sky_pixel_arcsec(header.get("INSTRUME", "ACIS"))


def arcsec_to_sky_pixels(arcsec, pixel_arcsec):
    """
    An angle on the sky, in Chandra sky pixels of ``pixel_arcsec`` arcseconds.

    Examples
    --------
    >>> arcsec_to_sky_pixels(0.984, 0.492)
    2.0
    """
    return arcsec / pixel_arcsec


def sky_pixels_to_arcsec(pixels, pixel_arcsec):
    """
    Chandra sky pixels of ``pixel_arcsec`` arcseconds, as an angle on the sky.

    Examples
    --------
    >>> sky_pixels_to_arcsec(2.0, 0.492)
    0.984
    """
    return pixels * pixel_arcsec


def circle_region(x, y, radius_arcsec, pixel_arcsec):
    """
    A circle, as CIAO's Data Model spells it.

    The shape alone, with no column system in front of it -- see :func:`sky_filter`. Kept
    apart so the same text can go into a region file, where naming a column would be
    wrong, and onto a file name, where it is required.

    Examples
    --------
    >>> circle_region(4100.38, 4131.82, 0.984, 0.492)
    'circle(4100.3800,4131.8200,2.0000)'
    """
    return f"circle({x:.4f},{y:.4f},{arcsec_to_sky_pixels(radius_arcsec, pixel_arcsec):.4f})"


def annulus_region(x, y, inner_arcsec, outer_arcsec, pixel_arcsec):
    """
    An annulus, as CIAO's Data Model spells it.

    Examples
    --------
    >>> annulus_region(4100.0, 4131.0, 0.984, 1.968, 0.492)
    'annulus(4100.0000,4131.0000,2.0000,4.0000)'
    """
    inner = arcsec_to_sky_pixels(inner_arcsec, pixel_arcsec)
    outer = arcsec_to_sky_pixels(outer_arcsec, pixel_arcsec)
    return f"annulus({x:.4f},{y:.4f},{inner:.4f},{outer:.4f})"


def sky_filter(region):
    """
    A shape, as a Data Model filter on the sky columns.

    Appended to a file name, this is what ``dmcopy`` and ``dmextract`` cut with. The
    brackets and parentheses are exactly why :func:`heasarc_retrieve_pipeline.ciao.run`
    never goes through a shell.

    Examples
    --------
    >>> sky_filter(circle_region(4100.38, 4131.82, 0.984, 0.492))
    '[sky=circle(4100.3800,4131.8200,2.0000)]'
    """
    return f"[sky={region}]"


def chipx_filter(spans):
    """
    A Data Model filter selecting ranges of detector columns.

    Continuous Clocking's regions, and the reason they exist: the readout collapses one
    spatial dimension, so the only coordinate that still separates source from background
    is ``chipx``. Several ranges go into one filter, which is how a background strip on
    each side of the source is selected in a single pass.

    Parameters
    ----------
    spans : sequence of (int, int)
        Inclusive first and last column of each range.

    Examples
    --------
    >>> chipx_filter([(100, 106)])
    '[chipx=100:106]'
    >>> chipx_filter([(80, 96), (110, 126)])
    '[chipx=80:96,110:126]'
    """
    return "[chipx=" + ",".join(f"{first}:{last}" for first, last in spans) + "]"


def parse_pget(text, names):
    """
    Read back what ``pget`` printed, one value to a line.

    ``dmcoords`` does not answer on standard output: it writes its results into its own
    parameter file, and ``pget dmcoords x y`` is the tool that reads them out again. The
    values come back in the order they were asked for and with nothing to label them, so
    the count is checked -- a missing line would otherwise shift every later name onto the
    wrong number and hand back a chip identifier as a sky coordinate, silently.

    Parameters
    ----------
    text : str
        What ``pget`` printed.
    names : sequence of str
        The parameters asked for, in the order they were asked for.

    Returns
    -------
    dict
        Name to float.

    Raises
    ------
    ValueError
        If the number of values does not match the number of names.

    Examples
    --------
    >>> parse_pget("4100.4\\n4131.8\\n", ("x", "y"))
    {'x': 4100.4, 'y': 4131.8}
    """
    values = [line.strip() for line in text.splitlines() if line.strip()]
    if len(values) != len(names):
        raise ValueError(
            f"pget was asked for {len(names)} values and printed {len(values)}: "
            f"{text!r}. Pairing them up would put the wrong number under every later name."
        )
    return {name: float(value) for name, value in zip(names, values)}


@dataclass(frozen=True)
class SourcePosition:
    """
    Where the source sits, in every coordinate system the reduction needs.

    Attributes
    ----------
    x, y : float
        Sky coordinates, in the 8192 x 8192 sky plane. What a circular region is drawn in.
    chip_id : int
        Which CCD or HRC segment the source landed on. ``specextract`` and ``pileup_map``
        both need it: a spectrum is built per chip, and pile-up is a per-frame quantity.
    chipx, chipy : float
        Position on that chip. ``chipx`` is Continuous Clocking's only surviving
        coordinate.
    theta_arcmin : float
        Off-axis angle. Carried because it is what makes the PSF radius vary, so a report
        that quotes a radius without it says nothing about whether the radius is sensible.
    """

    x: float
    y: float
    chip_id: int
    chipx: float
    chipy: float
    theta_arcmin: float


#: What ``dmcoords`` is asked for, in the order :func:`parse_pget` reads them back.
_DMCOORDS_ANSWERS = ("x", "y", "chip_id", "chipx", "chipy", "theta")


def chandra_source_position(observation, ra, dec, env=None, log_to=None):
    """
    Convert the position asked for into sky and chip coordinates.

    **The position is the one the caller gave, never the header's.** That rule is Matteo's
    for XMM and it is sharper here than anywhere else in this pipeline: obsid ``5644``'s
    ``OBJECT`` is M82 X-1, the pulsation that Liu 2024 published is M82 X-2's, and the two
    are 4.63 arcseconds apart. An implementation that extracted at ``RA_TARG`` would find
    nothing and would look like it had worked.

    ``dmcoords`` is used rather than ``psfsize_srcs``, which also reports a sky position,
    for one reason: ``dmcoords`` is a compiled Data Model tool that every installation
    has, and ``psfsize_srcs`` is a contributed Python script. The position must not depend
    on the more fragile of the two -- and where a radius is configured outright,
    ``psfsize_srcs`` is not run at all.

    Parameters
    ----------
    observation : Observation
        Its ``event_list`` sets the coordinate frame, and its ``aspect_solutions`` refine
        it. An observation with no aspect solution still converts, off the header alone.
        One taken in several parts passes every part's, as a CIAO stack -- ``dmcoords``
        accepts ``asolfile="a,b"`` -- because one sky position has to come out for the
        whole merged event list. An event list that carries the averaged aspect keywords
        ``DY_AVG``, ``DZ_AVG`` and ``DTH_AVG``, as ``1411``'s does, is converted with those
        and ``dmcoords`` ignores the files; they are passed all the same, so the answer
        does not depend on which kind of event list the archive happened to write.
    ra, dec : float
        Source position in degrees.
    env : dict, optional
        From :func:`heasarc_retrieve_pipeline.ciao.ciao_environment`. It matters here for
        a second reason beyond ``ardlib.par``: ``dmcoords`` *answers* through its
        parameter file, so two observations sharing one ``PFILES`` could read each other's
        position.
    log_to : str, optional
        File the task's output goes to.

    Returns
    -------
    SourcePosition
    """
    from . import ciao

    parameters = dict(
        infile=observation.event_list, option="cel", ra=float(ra), dec=float(dec), celfmt="deg"
    )
    if observation.aspect_solutions:
        parameters["asolfile"] = ",".join(observation.aspect_solutions)

    ciao.run("dmcoords", produces=[], env=env, log_to=log_to, **parameters)
    answer = ciao.run(
        "pget",
        args=("dmcoords",) + _DMCOORDS_ANSWERS,
        produces=[],
        capture=True,
        env=env,
    )
    found = parse_pget(answer.stdout, _DMCOORDS_ANSWERS)

    return SourcePosition(
        x=found["x"],
        y=found["y"],
        chip_id=int(found["chip_id"]),
        chipx=found["chipx"],
        chipy=found["chipy"],
        theta_arcmin=found["theta"],
    )


@dataclass(frozen=True)
class PsfSize:
    """
    What ``psfsize_srcs`` measured at the source's off-axis angle.

    Attributes
    ----------
    radius_arcsec : float
        Radius enclosing ``psf_ecf`` of the counts at ``psf_energy_kev``.
    """

    radius_arcsec: float


def read_psf_size(path, pixel_arcsec):
    """
    Read the region file ``psfsize_srcs`` wrote.

    The conversion to arcseconds happens once, here: the tool writes ``R`` in sky pixels
    and every radius in this module's configuration is an angle. The file carries no scale
    of its own, so the observation's has to be passed in.

    Only ``R`` is read. The file also carries ``NEAR_CHIP_EDGE``, and that column is
    **not** trusted: ``psfsize_srcs`` bounds a subarray's rows at ``NROWS - 1 - edge``
    where the bound is ``FIRSTROW + NROWS - 1 - edge``, so on any subarray the upper bound
    falls below the lower one and every position is flagged. Measured on obsid ``5644``
    (``FIRSTROW = 449``, ``NROWS = 128``, dither margin 32): the tool's window is 481 to
    95, which nothing can be inside. :func:`chandra_chip_edge` does the check instead, and
    answers with a distance rather than a flag.

    Parameters
    ----------
    path : str
        The region file.
    pixel_arcsec : float
        The observation's sky pixel scale, :attr:`Observation.sky_pixel_arcsec`.

    Returns
    -------
    PsfSize

    Raises
    ------
    ValueError
        If the file holds no row, which is what a position off the detector produces.
    """
    with fits.open(path) as hdulist:
        table = hdulist[1].data
        if table is None or len(table) == 0:
            raise ValueError(
                f"{path} holds no source: psfsize_srcs found nothing at that position, "
                "which normally means it falls outside the detector."
            )
        radius = float(table["R"][0])

    return PsfSize(radius_arcsec=sky_pixels_to_arcsec(radius, pixel_arcsec))


def chandra_psf_radius(observation, config, ra, dec, outfile, env=None, log_to=None):
    """
    Ask ``psfsize_srcs`` how big this source's point spread function is.

    Chandra's PSF grows from about one arcsecond on-axis to over ten at eight arcminutes
    off-axis, which is why this is measured per observation rather than configured once.
    Obsid ``5644`` measures 0.830 arcseconds at M82 X-2's position, 0.29 arcminutes
    off-axis, for ``ecf=0.9`` at 1 keV.

    Parameters
    ----------
    observation : Observation
    config : dict
        ``psf_ecf`` and ``psf_energy_kev`` are read.
    ra, dec : float
        Source position in degrees.
    outfile : str
        Where the region file goes. Kept rather than thrown away: it carries the
        off-axis angle and the chip-edge warning as well as the radius.
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    PsfSize
    """
    from . import ciao

    os.makedirs(os.path.dirname(os.path.abspath(outfile)), exist_ok=True)
    ciao.run(
        "psfsize_srcs",
        produces=outfile,
        env=env,
        log_to=log_to,
        infile=observation.event_list,
        # A space between the two numbers is rejected: psfsize_srcs wants a comma, a plus
        # or a minus, and reads a bare space as a malformed sexagesimal.
        pos=f"{float(ra)},{float(dec)}",
        outfile=outfile,
        energy=config["psf_energy_kev"],
        ecf=config["psf_ecf"],
        clobber=True,
    )
    return read_psf_size(outfile, observation.sky_pixel_arcsec)


#: How far an ACIS source has to be from the edge of its active window before a circular
#: region is safe, in chip pixels.
#:
#: It is the dither pattern's amplitude, which is what ``psfsize_srcs`` uses and what
#: matters: Chandra dithers during an observation, so a source a few pixels from an edge
#: spends part of the exposure off the chip altogether and the region collects a fraction
#: of the counts it was sized for.
ACIS_DITHER_MARGIN_PIX = 32

#: Columns and rows on one ACIS CCD.
ACIS_CHIP_PIXELS = 1024


@dataclass(frozen=True)
class ChipEdge:
    """
    How close the source sits to the edge of the active detector area.

    Reported, never acted on -- the same ruling as pile-up. A source near an edge is still
    reduced; what changes is that the report says the enclosed fraction is not the one that
    was asked for.

    Attributes
    ----------
    margin_pix : float or None
        Distance to the nearest edge of the active window, in chip pixels. ``None`` for
        HRC, where the question is not asked.
    near_edge : bool or None
        Whether that distance is inside the dither margin.
    window : tuple or None
        ``(first row, last row)`` of the active window. A full frame is ``(1, 1024)``; a
        subarray is narrower, and obsid ``5644``'s is ``(449, 576)``.
    """

    margin_pix: Optional[float] = None
    near_edge: Optional[bool] = None
    window: Optional[tuple] = None


def chandra_chip_edge(observation, position, margin_pix=ACIS_DITHER_MARGIN_PIX):
    """
    How far the source sits from the edge of the active detector area.

    Done here rather than read off ``psfsize_srcs``' ``NEAR_CHIP_EDGE``, which is wrong on
    every ACIS subarray -- see :func:`read_psf_size`. It is also more useful as a distance
    than as a flag: 48 pixels of clearance and 5 pixels of clearance are both "not near the
    edge" to a boolean.

    HRC is not checked, which is the same choice ``psfsize_srcs`` makes. Its microchannel
    plate has no CCD edges, its segments are read out differently, and a position near one
    is not the failure mode a chip gap is.

    Parameters
    ----------
    observation : Observation
        ``detector`` decides whether the question applies, and ``active_rows`` gives the
        window. An ACIS observation with no ``active_rows`` recorded is treated as a full
        frame, which is what a header with no ``FIRSTROW`` means.
    position : SourcePosition
    margin_pix : float, optional
        How much clearance counts as enough. Defaults to the dither amplitude.

    Returns
    -------
    ChipEdge

    Examples
    --------
    >>> position = SourcePosition(0.0, 0.0, 7, 226.3, 496.95, 0.29)
    >>> edge = chandra_chip_edge(_FullFrame(), position)
    >>> round(edge.margin_pix, 1), edge.near_edge
    (225.3, False)
    """
    if not observation.detector.startswith("acis"):
        return ChipEdge()

    first, nrows = observation.active_rows or (1, ACIS_CHIP_PIXELS)
    last = first + nrows - 1
    distances = (
        position.chipx - 1,
        ACIS_CHIP_PIXELS - position.chipx,
        position.chipy - first,
        last - position.chipy,
    )
    closest = float(min(distances))
    return ChipEdge(margin_pix=closest, near_edge=closest < margin_pix, window=(first, last))


class _FullFrame:
    """A stand-in for the doctest of :func:`chandra_chip_edge`, which needs only two
    attributes of an observation and not a whole event list to build one from."""

    detector = "aciss"
    active_rows = None


@dataclass(frozen=True)
class ExtractionRegions:
    """
    Where the source is, where the background is, and how both were arrived at.

    Attributes
    ----------
    source, background : str
        Data Model filters, ready to append to a file name.
    radius_arcsec : float
        The source radius. ``None`` in Continuous Clocking, which has no circle.
    background_inner_arcsec, background_outer_arcsec : float or None
        The annulus, for the record.
    basis : str
        ``"psfsize_srcs"`` or ``"configured"`` -- where the radius came from.
    chip_edge : ChipEdge
        How much clearance the source has, from :func:`chandra_chip_edge`.
    reason : str
        Plain English, for the report page.
    """

    source: str
    background: str
    radius_arcsec: Optional[float] = None
    background_inner_arcsec: Optional[float] = None
    background_outer_arcsec: Optional[float] = None
    basis: str = "configured"
    chip_edge: ChipEdge = ChipEdge()
    reason: str = ""


def chandra_extraction_regions(
    position,
    radius_arcsec,
    config,
    continuous_clocking=False,
    basis="configured",
    chip_edge=None,
    *,
    pixel_arcsec,
):
    """
    The source and background selections for a point source.

    Two shapes, and which one applies is decided by the readout rather than by the
    detector. **Imaging** -- every ACIS Timed Exposure observation and all of HRC -- gets
    a circle with an annulus around it, the same reasoning as XMM's: the background varies
    across the field of view and a concentric ring is the closest sample there is, and
    ``bkg_inner_factor`` is what clears the wings of the point spread function.

    **Continuous Clocking** gets strips of ``chipx`` instead. The readout collapses one
    spatial dimension into the time axis, so a circle drawn on the sky selects a smear and
    not a source, and ``chipx`` is the only coordinate that still separates the two. This
    is the direct analogue of XMM Timing's ``RAWX`` strips -- and unlike them it has never
    been run on real data, so its defaults are a starting guess and its record says so.

    Parameters
    ----------
    position : SourcePosition
    radius_arcsec : float
        The source radius, from :func:`chandra_psf_radius` or from the configuration.
    config : dict
        ``bkg_inner_factor``, ``bkg_outer_factor``, and for Continuous Clocking
        ``cc_source_halfwidth_pix`` and ``cc_background_pix``.
    continuous_clocking : bool, optional
        Whether the readout collapsed a spatial dimension.
    basis : str, optional
        Where ``radius_arcsec`` came from, for the record.
    chip_edge : ChipEdge, optional
        How much clearance the source has, for the record.
    pixel_arcsec : float
        The observation's sky pixel scale, which the circles are drawn in.

    Returns
    -------
    ExtractionRegions
    """
    if continuous_clocking:
        half = int(config["cc_source_halfwidth_pix"])
        inner, outer = (int(value) for value in config["cc_background_pix"])
        centre = int(round(position.chipx))
        return ExtractionRegions(
            source=chipx_filter([(centre - half, centre + half)]),
            background=chipx_filter(
                [(centre - outer, centre - inner), (centre + inner, centre + outer)]
            ),
            basis=basis,
            chip_edge=chip_edge or ChipEdge(),
            reason=(
                f"Continuous Clocking collapsed one spatial dimension into the time axis, "
                f"so the regions are strips of chipx around column {centre} rather than "
                f"circles on the sky. Source and background overlap along the collapsed "
                f"axis and cannot be separated by position, so the background strips "
                f"carry some of the source. These widths have not been tested on real "
                f"data -- see open item 6 of the Chandra plan."
            ),
        )

    inner = radius_arcsec * config["bkg_inner_factor"]
    outer = radius_arcsec * config["bkg_outer_factor"]
    chip_edge = chip_edge or ChipEdge()
    edge = (
        f" The source sits {chip_edge.margin_pix:.0f} chip pixels from the edge of the "
        f"active area, inside the {ACIS_DITHER_MARGIN_PIX}-pixel dither amplitude, so it "
        "spends part of the exposure off the detector and the circle collects less than "
        "the enclosed fraction it was sized for."
        if chip_edge.near_edge
        else ""
    )
    return ExtractionRegions(
        source=sky_filter(circle_region(position.x, position.y, radius_arcsec, pixel_arcsec)),
        background=sky_filter(annulus_region(position.x, position.y, inner, outer, pixel_arcsec)),
        radius_arcsec=radius_arcsec,
        background_inner_arcsec=inner,
        background_outer_arcsec=outer,
        basis=basis,
        chip_edge=chip_edge,
        reason=(
            f"A {radius_arcsec:.2f} arcsecond circle at {position.theta_arcmin:.2f} "
            f"arcminutes off axis, with the background taken from an annulus of "
            f"{inner:.2f} to {outer:.2f} arcseconds around it." + edge
        ),
    )


def chandra_source_regions(observation, config, ra, dec, rec=None, env=None, log_to=None):
    """
    Work out where to extract this observation's source and background from.

    The step the reduction calls: convert the position, size the region, and record what
    was done and why.

    ``src_radius_arcsec`` in the configuration overrides the measurement. Where it is
    ``None`` -- the default -- ``psfsize_srcs`` is asked, which is the right default
    because a fixed radius is wrong at both ends of Chandra's field of view.

    Parameters
    ----------
    observation : Observation
    config : dict
        A complete configuration.
    ra, dec : float
        Source position in degrees.
    rec : StepRecord, optional
    env : dict, optional
    log_to : str, optional
        File ``dmcoords``' output goes to. ``psfsize_srcs`` gets its own beside it.

    Returns
    -------
    tuple
        ``(SourcePosition, ExtractionRegions)``.
    """
    rec = rec or no_record()

    position = chandra_source_position(observation, ra, dec, env=env, log_to=log_to)

    edge = chandra_chip_edge(observation, position)

    configured = config.get("src_radius_arcsec")
    if configured is not None:
        size = PsfSize(radius_arcsec=float(configured))
        basis = "configured"
    else:
        size = chandra_psf_radius(
            observation,
            config,
            ra,
            dec,
            os.path.join(
                chandra_pipeline_output_path(observation.obsid, config),
                f"{observation.stem}_psf.reg",
            ),
            env=env,
            log_to=None if log_to is None else log_to.replace("dmcoords", "psfsize_srcs"),
        )
        basis = "psfsize_srcs"

    regions = chandra_extraction_regions(
        position,
        size.radius_arcsec,
        config,
        continuous_clocking=observation.is_continuous_clocking,
        basis=basis,
        chip_edge=edge,
        pixel_arcsec=observation.sky_pixel_arcsec,
    )

    rec.value(
        ra=float(ra),
        dec=float(dec),
        sky_x=position.x,
        sky_y=position.y,
        chip_id=position.chip_id,
        chipx=position.chipx,
        chipy=position.chipy,
        theta_arcmin=position.theta_arcmin,
        source_region=regions.source,
        background_region=regions.background,
        radius_arcsec=regions.radius_arcsec,
        background_inner_arcsec=regions.background_inner_arcsec,
        background_outer_arcsec=regions.background_outer_arcsec,
        radius_basis=regions.basis,
        chip_edge_margin_pix=edge.margin_pix,
        near_chip_edge=edge.near_edge,
        active_rows=list(edge.window) if edge.window else None,
        reason=regions.reason,
    )
    get_logger().info(f"{observation.obsid}: {regions.reason}")
    return position, regions


#: Where ``dmextract opt=ltc1`` puts the light curve.
CHANDRA_LIGHTCURVE_EXTENSION = "LIGHTCURVE"


def chandra_flare_curve_filter(observation, position, regions, config):
    """
    The Data Model filter the background light curve is extracted through.

    **Not the background annulus.** That ring is a few arcseconds across and holds far too
    few counts to see a flare in. What is wanted is as much of the detector as can be had
    with the source kept out of it, which for ACIS imaging is the source's own chip minus
    the source circle.

    The source is cut out with region algebra, ``field()-circle(...)``, and not with the
    Data Model's ``exclude``. That is not a style choice: an ``[exclude ...]`` alongside
    any other filter is refused outright with *"cannot mix EXCLUDE and FILTER"*, so the
    chip filter and the cut-out cannot both be written that way. Measured against a real
    ``dmextract`` on 2026-09-12.

    Three things differ by configuration.

    * **ACIS** is banded in energy, because the particle background dominates outside the
      band and adds noise to a measurement that is about counting. **HRC** is not: it has
      no usable energy resolution, which is also why it gets no spectrum.
    * **ACIS** is cut to the source's chip. **HRC** is not -- its plate is one piece.
    * **Continuous Clocking** uses the background strips themselves. A circle on the sky
      selects a smear there, so there is no source region to subtract from a field.

    Parameters
    ----------
    observation : Observation
    position : SourcePosition
    regions : ExtractionRegions
    config : dict
        ``flare_energy_ev`` and ``flare_bin_seconds`` are read.

    Returns
    -------
    str
        A filter to append to the event list's name.
    """
    if observation.is_continuous_clocking:
        parts = [regions.background]
    else:
        parts = []
        if observation.detector.startswith("acis"):
            band = config.get("flare_energy_ev")
            if band is not None:
                parts.append(f"[energy={band[0]:g}:{band[1]:g}]")
            parts.append(f"[ccd_id={position.chip_id}]")
        # regions.source is "[sky=<shape>]"; the shape alone is what field() subtracts.
        shape = regions.source[len("[sky=") : -1]
        parts.append(f"[sky=field()-{shape}]")

    parts.append(f"[bin time=::{config['flare_bin_seconds']}]")
    return "".join(parts)


@dataclass(frozen=True)
class FlareLightCurve:
    """
    The background time series a flare cut is made on.

    Attributes
    ----------
    time : numpy.ndarray
        Bin centres, in the mission time of the event list.
    rate : numpy.ndarray
        Livetime-corrected count rate, ``NaN`` where the bin had no exposure.
    rate_error : numpy.ndarray or None
        Recorded for the figure, not used in the arithmetic.
    cadence : float
        Bin width.
    tstart, tstop : float
        What the curve covers.
    """

    time: np.ndarray
    rate: np.ndarray
    rate_error: Optional[np.ndarray]
    cadence: float
    tstart: float
    tstop: float


def read_chandra_lightcurve(path):
    """
    Read what ``dmextract opt=ltc1`` wrote.

    One thing has to be undone on the way in. ``dmextract`` emits a bin for every interval
    between ``TSTART`` and ``TSTOP``, the ones inside the observation's good times and the
    ones outside them alike, and writes the outside ones with zero exposure and a rate of
    zero. Obsid ``5644`` opens with three: its first good time starts 1 729 s after
    ``TSTART``. Read at face value they are the quietest bins in the observation, and they
    would pull the quiescent level down and the threshold with it. They become ``NaN``
    here, which is what the pipeline's interval utilities already understand.

    Parameters
    ----------
    path : str
        The light curve file.

    Returns
    -------
    FlareLightCurve
    """
    with fits.open(path) as hdulist:
        table = hdulist[CHANDRA_LIGHTCURVE_EXTENSION]
        header = table.header
        time = np.asarray(table.data["TIME"], dtype=float)
        rate = np.asarray(table.data["COUNT_RATE"], dtype=float)
        exposure = np.asarray(table.data["EXPOSURE"], dtype=float)
        names = table.data.columns.names
        error = np.asarray(table.data["STAT_ERR"], dtype=float) if "STAT_ERR" in names else None

    rate = np.where(exposure > 0, rate, np.nan)

    cadence = header.get("TIMEDEL")
    if cadence is None:
        cadence = float(np.median(np.diff(time))) if time.size > 1 else 0.0

    return FlareLightCurve(
        time=time,
        rate=rate,
        rate_error=error,
        cadence=float(cadence),
        tstart=float(header.get("TSTART", time[0] - cadence / 2 if time.size else 0.0)),
        tstop=float(header.get("TSTOP", time[-1] + cadence / 2 if time.size else 0.0)),
    )


def read_observation_gti(path):
    """
    The good time intervals an event list already carries.

    Found by class rather than by name, and that matters: ACIS names the block after the
    chip it belongs to -- obsid ``5644``'s is ``GTI7`` -- so looking for an extension
    called ``GTI`` finds nothing at all. Where several blocks exist, one per chip, they are
    merged: the flare cut is applied to the whole file and cannot be per chip.

    Parameters
    ----------
    path : str
        An event list.

    Returns
    -------
    numpy.ndarray or None
        Shape ``(N, 2)``, sorted and disjoint, or ``None`` when the file carries none.
    """
    intervals = []
    with fits.open(path) as hdulist:
        for hdu in hdulist[1:]:
            names = getattr(getattr(hdu, "columns", None), "names", []) or []
            if "START" in names and "STOP" in names:
                intervals.append(
                    np.column_stack(
                        [
                            np.asarray(hdu.data["START"], dtype=float),
                            np.asarray(hdu.data["STOP"], dtype=float),
                        ]
                    )
                )

    if not intervals:
        return None
    return merge_intervals(np.vstack(intervals))


@dataclass(frozen=True)
class FlareThreshold:
    """
    The rate above which a bin counts as flaring, and the evidence for it.

    Attributes
    ----------
    threshold : float or None
        ``None`` when there were too few usable bins to measure anything, which is not a
        failure -- it means the observation is kept whole and the record says why.
    level, scatter : float or None
        The quiescent rate and its standard deviation, after clipping.
    n_bins_used : int
        How many bins the two above were measured from.
    reason : str
        Plain English, for the report page.
    """

    threshold: Optional[float]
    level: Optional[float] = None
    scatter: Optional[float] = None
    n_bins_used: int = 0
    reason: str = ""


def chandra_flare_threshold(rate, config):
    """
    The rate above which this observation counts as flaring.

    The quiescent level is measured with the flaring bins clipped out, which is what
    CIAO's own ``lc_sigma_clip`` does and for the reason that makes it necessary: a plain
    mean and standard deviation over a curve containing a flare are both raised *by* the
    flare, so a big one lifts its own threshold above itself and is kept. Clipping first
    breaks that circle.

    **No fixed rate appears anywhere here**, and that is the same lesson XMM's flare step
    learnt the hard way -- there, the SAS cookbook's 0.35 counts/s turned out to be wrong
    by a factor of a hundred for the curves PPS actually writes. A Chandra background rate
    depends on the chip, the subarray, the energy band and the epoch, so the only honest
    threshold is one measured from the observation in hand.

    Parameters
    ----------
    rate : numpy.ndarray
        Count rates, ``NaN`` where a bin had no exposure.
    config : dict
        ``flare_sigma`` and ``flare_min_bins`` are read.

    Returns
    -------
    FlareThreshold
    """
    sigma = config.get("flare_sigma", DEFAULT_CONFIG["flare_sigma"])
    minimum = config.get("flare_min_bins", DEFAULT_CONFIG["flare_min_bins"])

    usable = np.asarray(rate, dtype=float)
    usable = usable[np.isfinite(usable)]
    if usable.size < minimum:
        return FlareThreshold(
            threshold=None,
            n_bins_used=int(usable.size),
            reason=(
                f"The light curve has {usable.size} usable bins, fewer than the "
                f"{minimum} a quiescent level can be measured from. The observation is "
                "kept whole rather than screened against a number that means nothing."
            ),
        )

    level, scatter = _clipped_level(usable, sigma)
    # A threshold equal to the level would flag every bin above it, which on a flat curve
    # is half of them. Poisson noise on the quiescent level is the floor below which the
    # scatter cannot be believed, and it keeps a short or rounded curve from cutting itself
    # to pieces.
    floor = np.sqrt(max(level, 0.0) / max(config["flare_bin_seconds"], 1.0))
    threshold = level + sigma * max(scatter, floor, np.finfo(float).eps)

    return FlareThreshold(
        threshold=float(threshold),
        level=float(level),
        scatter=float(scatter),
        n_bins_used=int(usable.size),
        reason=(
            f"The quiescent background is {level:.4g} counts/s with a scatter of "
            f"{scatter:.3g}, measured from {usable.size} bins with the flaring ones "
            f"clipped out. A bin counts as flaring above {threshold:.4g} counts/s, "
            f"{sigma:g} sigma up."
        ),
    )


def _clipped_level(values, sigma, iterations=5):
    """
    The mean and standard deviation of the quiet part of a curve.

    Sigma clipping by hand rather than through ``astropy.stats``: five passes, each
    dropping what lies more than ``sigma`` standard deviations from the current mean. It
    is four lines, it has no options to get wrong, and it stops as soon as a pass drops
    nothing.
    """
    kept = np.asarray(values, dtype=float)
    for _ in range(iterations):
        mean, scatter = float(np.mean(kept)), float(np.std(kept))
        if scatter <= 0:
            break
        inside = np.abs(kept - mean) <= sigma * scatter
        if inside.all() or not inside.any():
            break
        kept = kept[inside]
    return float(np.mean(kept)), float(np.std(kept))


def chandra_flare_gti(observation, config, lightcurve, rec=None):
    """
    The stretches of one observation the background was quiet enough to keep.

    Pure Python over a curve CIAO has already made: the thresholding, the intervals and the
    intersection with the observation's own good times are all
    :mod:`heasarc_retrieve_pipeline.utils` functions that NuSTAR and XMM already use.

    **This step is deliberately reluctant.** Chandra's background flares matter far less
    than XMM's for the bright sources this pipeline is aimed at, and the failure mode that
    actually costs something is not a missed flare but a cut that eats a good observation
    -- a variable source leaking into the background region looks exactly like a flare. So
    a screening that wants more than ``flare_max_removed_fraction`` of the exposure is
    reported and *not applied*, and too short a curve to measure is likewise left alone.
    Both of those are recorded with their numbers, never silent.

    Parameters
    ----------
    observation : Observation
        Its ``event_list`` supplies the good times the result is intersected with.
    config : dict
        A complete configuration.
    lightcurve : str
        The file :func:`chandra_flare_curve_filter` was extracted into.
    rec : StepRecord, optional

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``. Never ``None``: an observation that cannot be screened is kept
        whole, which is its own good time intervals unchanged.
    """
    rec = rec or no_record()
    logger = get_logger()

    curve = read_chandra_lightcurve(lightcurve)
    whole = read_observation_gti(observation.event_list)
    if whole is None:
        whole = np.array([[curve.tstart, curve.tstop]], dtype=float)
    before = float(np.sum(whole[:, 1] - whole[:, 0]))

    found = chandra_flare_threshold(curve.rate, config)
    if found.threshold is None:
        logger.info(f"{observation.obsid}: not screening for flares. {found.reason}")
        rec.value(
            applied=False,
            reason=found.reason,
            n_bins_used=found.n_bins_used,
            exposure_before=before,
            exposure_after=before,
            removed_fraction=0.0,
        )
        return whole

    flaring = intervals_above_threshold(
        curve.time, curve.rate, found.threshold, cadence=curve.cadence
    )
    # Bounded by whichever of the curve and the observation reaches further, so that a
    # stretch of the observation the curve does not cover is *kept*. Real ``dmextract``
    # curves run from TSTART to TSTOP and so contain the good times outright; where one
    # does not, the honest reading is "not measured", and not measured must not mean cut.
    quiet = good_intervals(
        flaring,
        min(curve.tstart, float(whole[0, 0])),
        max(curve.tstop, float(whole[-1, 1])),
    )
    # Intersected rather than merely clipped, so that the exposure recorded here and the
    # exposure the cleaned file ends up with are the same number. ``dmcopy`` intersects
    # too -- verified on real data -- and two derivations of one answer is two answers
    # waiting to disagree.
    gti = intersect_intervals(quiet, whole) if quiet.size else np.empty((0, 2))
    after = float(np.sum(gti[:, 1] - gti[:, 0])) if gti.size else 0.0
    removed = 1.0 - after / before if before > 0 else 0.0

    limit = config["flare_max_removed_fraction"]
    applied = removed <= limit
    if not applied:
        reason = (
            f"Screening at {found.threshold:.4g} counts/s would remove {removed:.0%} of "
            f"the exposure, more than the {limit:.0%} this mission allows. At that size "
            "a variable source leaking into the background region is a likelier "
            "explanation than a flare, so the observation is kept whole and the cut is "
            "reported instead of made."
        )
        logger.warning(f"{observation.obsid}: {reason}")
        gti, after, removed = whole, before, 0.0
    else:
        reason = found.reason
        if removed > config["flare_warn_fraction"]:
            logger.warning(
                f"{observation.obsid}: flare screening removed {removed:.0%} of the "
                f"exposure ({before - after:.0f} s of {before:.0f} s)"
            )
        else:
            logger.info(
                f"{observation.obsid}: flare screening kept {after:.0f} s of {before:.0f} s"
            )

    rec.value(
        applied=bool(applied),
        reason=reason,
        threshold=found.threshold,
        quiescent_rate=found.level,
        quiescent_scatter=found.scatter,
        n_bins_used=found.n_bins_used,
        bin_seconds=curve.cadence,
        light_curve=os.path.basename(lightcurve),
        exposure_before=before,
        exposure_after=after,
        removed_fraction=removed,
        n_intervals_kept=len(gti),
    )
    arrays = dict(lc_time=curve.time, lc_rate=curve.rate, gti_before=whole, gti_after=gti)
    if curve.rate_error is not None:
        arrays["lc_rate_err"] = curve.rate_error
    rec.array(**arrays)
    return gti


def chandra_flare_lightcurve_path(observation, config):
    """Where this observation's background light curve goes."""
    return os.path.join(
        chandra_pipeline_output_path(observation.obsid, config), f"{observation.stem}_bkg_lc.fits"
    )


def chandra_flare_gti_path(observation, config):
    """Where the good time intervals the flare cut arrived at go."""
    return os.path.join(
        chandra_pipeline_output_path(observation.obsid, config), f"{observation.stem}_flare.gti"
    )


def chandra_cleaned_event_list_path(observation, config):
    """Where the screened event list goes."""
    return os.path.join(
        chandra_pipeline_output_path(observation.obsid, config), f"{observation.stem}_cl.evt"
    )


def chandra_flare_lightcurve(observation, position, regions, config, env=None, log_to=None):
    """
    Extract the background light curve the flare cut is made on.

    Parameters
    ----------
    observation : Observation
    position : SourcePosition
    regions : ExtractionRegions
    config : dict
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    str
        The light curve file.
    """
    from . import ciao

    outfile = chandra_flare_lightcurve_path(observation, config)
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    ciao.run(
        "dmextract",
        produces=outfile,
        env=env,
        log_to=log_to,
        infile=observation.event_list
        + chandra_flare_curve_filter(observation, position, regions, config),
        outfile=outfile,
        opt="ltc1",
        clobber=True,
    )
    return outfile


def write_gti_file(path, gti):
    """
    Write good time intervals where CIAO's Data Model can read them.

    ``dmcopy`` takes a good time interval table by name, as ``evt2.fits[@flare.gti]``, and
    intersects it with the intervals the file already carries -- verified against a real
    ``dmcopy`` on obsid ``5644``, where a deliberately over-wide table left ``ONTIME``
    exactly as it was. Writing the intervals :func:`chandra_flare_gti` already computed,
    rather than having CIAO derive them a second time, keeps the intervals that get
    recorded on the report and the intervals that get applied to the events the same
    intervals.

    A near-copy of ``xmm.write_gti_file``, and left as one for the reason the whole CIAO
    runner is: the two write for different readers, and tying them together at the one
    place where the file formats might diverge would be a poor trade.

    Parameters
    ----------
    path : str
        File to write. Overwritten if it exists.
    gti : numpy.ndarray
        Shape ``(N, 2)``.

    Returns
    -------
    str
        The path written.
    """
    gti = np.atleast_2d(np.asarray(gti, dtype=float)).reshape(-1, 2)
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", unit="s", array=gti[:, 0]),
            fits.Column(name="STOP", format="D", unit="s", array=gti[:, 1]),
        ],
        name="GTI",
    )
    hdu.header["HDUCLASS"] = ("OGIP", "File conforms to OGIP standards")
    hdu.header["HDUCLAS1"] = ("GTI", "Extension contains good time intervals")
    hdu.header["HDUCLAS2"] = ("STANDARD", "Standard good time intervals")
    path = str(path)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return path


def chandra_clean_event_list(observation, config, gti, rec=None, env=None, log_to=None):
    """
    Write the screened event list every later step reads.

    The whole field, not the source region: pile-up is measured on a chip, a spectrum
    needs its own background, and the source cut-out happens later and from this file.

    Parameters
    ----------
    observation : Observation
    config : dict
    gti : numpy.ndarray
        From :func:`chandra_flare_gti`.
    rec : StepRecord, optional
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    str
        The cleaned event list.
    """
    from . import ciao

    rec = rec or no_record()
    outfile = chandra_cleaned_event_list_path(observation, config)
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    gti_file = write_gti_file(chandra_flare_gti_path(observation, config), gti)
    ciao.run(
        "dmcopy",
        produces=outfile,
        env=env,
        log_to=log_to,
        infile=f"{observation.event_list}[@{gti_file}]",
        outfile=outfile,
        clobber=True,
    )

    rec.value(
        cleaned_event_list=os.path.basename(outfile),
        gti_file=os.path.basename(gti_file),
    )
    get_logger().info(f"{observation.obsid}: cleaned events in {os.path.basename(outfile)}")
    return outfile


#: ``axbary``'s reference frame, and with it the ephemeris.
#:
#: The parameter admits exactly two values -- ``FK5``, which is DE200, and ``ICRS``, which
#: is **DE405**. There is no DE430, so Chandra is the one mission in this pipeline not on
#: the ephemeris every other one uses, and that break was measured rather than accepted:
#: over obsid ``6298``'s own span and position, geocentric so that only the ephemerides
#: differ, DE430 minus DE405 is a **constant +0.377 microseconds**, varying by 0.0016 us
#: across a two-hour observation. It cannot distort a pulse profile, a period or a
#: periodogram *within* an observation at any Chandra time resolution; it survives only as
#: an absolute phase offset against DE430 times from another mission, where against M82
#: X-2's 1.37 s spin it is 2.7e-7 in phase.
#:
#: HEASOFT ``barycorr`` is not an alternative. It has no Chandra orbit reader at all --
#: ``hdaxbary``'s only ones are ``xtescorbit``, ``nicerscorbit`` and ``swiftscorbit`` --
#: and on a real Chandra event list with its own orbit file it dies with "no bracketing
#: sample found", with the orbit file demonstrably not at fault. Measured 2026-09-12; see
#: ``docs/chandra_integration_plan.md``.
BARYCENTRE_REFFRAME = "ICRS"

#: What ``TIMESYS`` must read after a successful correction.
BARYCENTRED_TIMESYS = "TDB"

#: What DE405 costs against the pipeline's DE430, in microseconds. Constant, measured.
DE405_MINUS_DE430_US = 0.377


def _barycentred_time_keywords(path):
    """The three keywords that say whether, and how, a file was barycentred."""
    with fits.open(path) as hdulist:
        header = hdulist[1].header
        return (
            _keyword(header, "TIMESYS"),
            _keyword(header, "TIMEREF"),
            _keyword(header, "PLEPHEM"),
        )


def chandra_barycenter(
    observation, config, events, ra="NONE", dec="NONE", rec=None, env=None, log_to=None
):
    """
    Write a barycentred copy of the cleaned event list.

    Converting arrival times from the spacecraft to the solar system barycentre is what
    makes a coherent timing search possible at all, and Chandra makes the point more
    sharply than any other mission here: it reaches 125 000 km from Earth, so the
    correction to the geocentre drifts about 2.1 s across a 75 ks observation -- 1.6 cycles
    of M82 X-2's spin -- and the spacecraft-to-geocentre term adds another 0.129 s on top
    of that. Barycentring to the geocentre alone would smear a tenth of a cycle.

    **The correction is made to the position asked for, never to the one in the header.**
    This is Matteo's rule for XMM, and obsid ``5644`` is the strongest case for it in the
    whole pipeline: its ``OBJECT`` is M82 X-1, the pulsation Liu 2024 published belongs to
    M82 X-2, and the two sit 4.63 arcseconds apart. Told nothing, ``axbary`` would correct
    to the header's target and the signal would not be there.

    ``axbary`` writes a new file rather than editing in place, so the original stays on
    spacecraft time -- which matters, because an event list whose times are silently no
    longer spacecraft times is a trap for every later step.

    **The aspect solution is deliberately not barycentred alongside it.** The CXC's thread
    says to do that, and it is right for their workflow and wrong for this one: here the
    spectra, the responses and the pile-up map are all built from the *uncorrected* cleaned
    list, and nothing pairs the barycentred file with an aspect solution. Barycentring one
    anyway would write a 17 MB copy per observation that no step reads. If a later step
    ever does pair the two, this is the line to revisit.

    Parameters
    ----------
    observation : Observation
        ``orbit_ephemeris`` is what the correction is computed from. Without one there is
        nothing to correct with, and that is recorded rather than raised.
    config : dict
    events : str
        The cleaned event list, on spacecraft time.
    ra, dec : float or str, optional
        Source position in degrees. Anything that is not a pair of numbers -- the default
        ``"NONE"`` -- leaves ``axbary`` to read the position out of the header, loudly.
    rec : StepRecord, optional
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    str or None
        The barycentred file, or ``None`` when there was no orbit ephemeris.

    Raises
    ------
    ValueError
        If ``axbary`` returns success and the output is not on barycentric time, which
        would otherwise leave a file that looks corrected and is not.
    """
    from . import ciao
    from .barycenter import barycentered_file_name

    rec = rec or no_record()
    logger = get_logger()

    if observation.orbit_ephemeris is None:
        reason = (
            f"{observation.obsid} has no orbit ephemeris, so its times cannot be corrected "
            "to the barycentre. axbary needs the spacecraft's own position, and Chandra is "
            "far enough from Earth that assuming the geocentre would smear a pulse profile."
        )
        logger.warning(reason)
        rec.value(barycentered=False, reason=reason)
        return None

    output = barycentered_file_name(events)

    position = _source_coordinates(ra, dec)
    at_position = {}
    if position is None:
        logger.warning(
            f"{observation.obsid}: no source position was given, so "
            f"{os.path.basename(output)} is barycentred to the target in its own header -- "
            "the pointing, which is not the source. On obsid 5644 those two are 4.63 "
            "arcseconds and one published pulsation apart."
        )
    else:
        at_position = dict(ra=position[0], dec=position[1])

    ciao.run(
        "axbary",
        produces=output,
        env=env,
        log_to=log_to,
        infile=events,
        orbitfile=observation.orbit_ephemeris,
        outfile=output,
        refframe=BARYCENTRE_REFFRAME,
        clobber=True,
        **at_position,
    )

    timesys, timeref, ephemeris = _barycentred_time_keywords(output)
    if timesys != BARYCENTRED_TIMESYS:
        raise ValueError(
            f"axbary returned success but left {os.path.basename(output)} on "
            f"{timesys or 'no'} time rather than {BARYCENTRED_TIMESYS}."
        )

    rec.value(
        barycentered=True,
        barycentered_file=os.path.basename(output),
        orbit_ephemeris=os.path.basename(observation.orbit_ephemeris),
        refframe=BARYCENTRE_REFFRAME,
        timesys=timesys,
        timeref=timeref,
        ephemeris=ephemeris,
        de405_minus_de430_us=DE405_MINUS_DE430_US,
        srcra=None if position is None else position[0],
        srcdec=None if position is None else position[1],
        position_from="argument" if position is not None else "header",
        reason=(
            f"Corrected to the barycentre with axbary at refframe={BARYCENTRE_REFFRAME}, "
            f"which is {ephemeris}. Every other mission in this pipeline uses DE430, and "
            f"axbary offers no such option; the difference is a constant "
            f"{DE405_MINUS_DE430_US} microseconds, so it cannot affect anything measured "
            "within this observation."
        ),
    )
    logger.info(
        f"{observation.obsid}: barycentred to {os.path.basename(output)} "
        f"with {ephemeris} at the position asked for"
    )
    return output


def _source_coordinates(ra, dec):
    """
    ``(ra, dec)`` as numbers, or ``None`` when no position was given.

    ``process_chandra_obsid`` defaults both to the string ``"NONE"``, the way every
    mission's entry point does. Twin of ``xmm._source_coordinates``.

    Examples
    --------
    >>> _source_coordinates("148.96267", 69.67931)
    (148.96267, 69.67931)
    >>> _source_coordinates("NONE", "NONE") is None
    True
    """
    try:
        return float(ra), float(dec)
    except (TypeError, ValueError):
        return None


def chandra_barycentered_source_events(
    observation, config, barycentered, regions, rec=None, env=None, log_to=None
):
    """
    Cut the source region out of the barycentred event list.

    This is the file a timing analysis actually reads, and neither of the two it sits
    between is: the barycentred list is the whole field, and the cleaned list is still on
    spacecraft time.

    Cutting the region out of the corrected list, rather than correcting a source list a
    second time, is the same choice XMM made: ``axbary`` runs once per observation, so the
    two files cannot then disagree about the position they were corrected to.

    Parameters
    ----------
    observation : Observation
    config : dict
    barycentered : str or None
        From :func:`chandra_barycenter`. ``None`` means there is nothing to cut from.
    regions : ExtractionRegions
    rec : StepRecord, optional
        Shares the ``barycenter`` step's record, so one record holds the correction, the
        position it was made to and the file cut from it.
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    str or None
    """
    from . import ciao
    from .barycenter import barycentered_file_name

    rec = rec or no_record()
    if barycentered is None:
        rec.value(barycentered_source_file=None)
        return None

    output = barycentered_file_name(
        os.path.join(
            chandra_pipeline_output_path(observation.obsid, config),
            f"{observation.stem}_src.evt",
        )
    )
    os.makedirs(os.path.dirname(output), exist_ok=True)
    ciao.run(
        "dmcopy",
        produces=output,
        env=env,
        log_to=log_to,
        infile=barycentered + regions.source,
        outfile=output,
        clobber=True,
    )

    rec.value(barycentered_source_file=os.path.basename(output), source_region=regions.source)
    get_logger().info(
        f"{observation.obsid}: barycentred source events in {os.path.basename(output)}"
    )
    return output


def chandra_part_label(part):
    """
    What names one part in a file name.

    Examples
    --------
    >>> chandra_part_label(ObservationPart(2, 0.0, 1.0))
    'obi002'
    """
    return f"obi{part.number:03d}"


def chandra_part_observation(observation, part, config, env=None, log_to=None):
    """
    One part of an observation, as an observation of its own.

    Matteo's ruling of 2026-09-13, and the CXC's answer too: ``splitobs`` separates the
    parts so that each "can then be processed as if they were separate observations". Here
    that holds from the source region on. Each part has its own aspect solutions, mask,
    bad-pixel list, good-time file and orbit ephemeris, and ``specextract`` takes one mask
    file per observation, so a spectrum of the merged list would have to pick one part's.

    The part's events are cut out of the merged event list by its own times, into
    ``<stem>_obiNNN_evt2.fits``. **The cut's** ``TSTART`` **and** ``TSTOP`` **are then
    rewritten to the part's.** ``dmcopy``'s time filter trims the good-time blocks, the
    exposure and the events, and leaves those two at the merged list's; ``dmextract`` bins
    a light curve over them, and on ``380``'s second part that was 15 447 bins of 200 s, 7
    with any exposure. On ``1411``, 84 days apart, it would be 36 000.

    The time resolution, and with it the mode in the stem, is the part's own, from its own
    dead-time file: ``1411``'s second part is 5.18 ms, where the combination in the front
    end's record is 4.93.

    Parameters
    ----------
    observation : Observation
        The whole observation, as the front end read it.
    part : ObservationPart
        One of its ``parts``.
    config : dict
        ``out_data_path`` is where the cut goes; ``hrc_veto_ratio_threshold`` is read.
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    Observation
        With ``part`` set, ``parts`` holding that part alone, and the part's companion
        files in the single fields -- except ``aspect_solution``, which stays ``None``
        because a part can have several: :attr:`Observation.aspect_solutions` has them.
    """
    from . import ciao

    header = fits.getheader(observation.event_list, 1)
    dtf = None if part.dead_time_file is None else read_dead_time_factors(part.dead_time_file)
    resolution = chandra_time_resolution(header, dtf, config)
    one = dataclasses.replace(
        observation,
        mode=chandra_mode_label(header, fast_timing=resolution.fast_timing),
        time_resolution=resolution,
        aspect_solution=None,
        bad_pixel_file=part.bad_pixel_file,
        mask_file=part.mask_file,
        gti_file=part.gti_file,
        dead_time_file=part.dead_time_file,
        orbit_ephemeris=part.orbit_ephemeris,
        parts=(part,),
        part=part,
    )

    output = os.path.join(
        chandra_pipeline_output_path(observation.obsid, config), f"{one.stem}_evt2.fits"
    )
    os.makedirs(os.path.dirname(output), exist_ok=True)
    ciao.run(
        "dmcopy",
        produces=output,
        env=env,
        log_to=log_to,
        infile=f"{observation.event_list}[time={part.tstart!r}:{part.tstop!r}]",
        outfile=output,
        clobber=True,
    )
    with fits.open(output, mode="update") as hdulist:
        hdulist[1].header["TSTART"] = part.tstart
        hdulist[1].header["TSTOP"] = part.tstop

    get_logger().info(
        f"{observation.obsid}: part {part.number} cut into {os.path.basename(output)}, "
        f"time resolution {resolution.seconds} s"
    )
    return dataclasses.replace(one, event_list=output)


#: How hard :func:`chandra_compress_barycentered_events` compresses. Measured on obsid
#: ``8505``'s 319 MB list: level 1 leaves 244 MB in 6 s, level 6 leaves 240 MB in 15 s.
#: HRC event lists are mostly incompressible numbers, so the extra effort buys nothing.
BARYCENTRED_GZIP_LEVEL = 1


def chandra_compress_barycentered_events(barycentered, rec=None):
    """
    Gzip the whole-field barycentred list, once the source has been cut out of it.

    Nothing in the reduction reads it again: the timing analysis reads the source cut, and
    every other step reads the cleaned list. It is kept rather than deleted because a
    different region -- another source in the field -- can still be cut from it, and
    CIAO reads a gzipped event list directly.

    The file is written under a temporary name and renamed at the end, so an interrupted
    run cannot leave a truncated ``.gz`` that looks finished.

    Parameters
    ----------
    barycentered : str or None
        From :func:`chandra_barycenter`. ``None`` means there is nothing to compress.
    rec : StepRecord, optional
        The ``barycenter`` step's record, whose ``barycentered_file`` is renamed to match.

    Returns
    -------
    str or None
        The compressed file.
    """
    if barycentered is None:
        return None

    rec = rec or no_record()
    output = barycentered + ".gz"
    partial = output + ".part"
    with (
        open(barycentered, "rb") as source,
        gzip.open(partial, "wb", compresslevel=BARYCENTRED_GZIP_LEVEL) as target,
    ):
        shutil.copyfileobj(source, target, length=1 << 20)
    os.replace(partial, output)
    os.remove(barycentered)

    rec.value(barycentered_file=os.path.basename(output))
    return output


#: The CXC's conversion from counts per ACIS frame to pile-up fraction, from
#: `ahelp pileup_map <https://cxc.harvard.edu/ciao/ahelp/pileup_map.html>`_, with the
#: origin added because an empty pixel is not piled. The three tabulated points lie
#: exactly on ``fraction = counts_per_frame / 2``, so the interpolation is a straight
#: line; the table is kept in the source anyway, because the *tool's* table is the
#: authority and a future CIAO may revise it.
PILEUP_TABLE = ((0.0, 0.0), (0.02, 0.01), (0.10, 0.05), (0.20, 0.10))


def pileup_fraction(counts_per_frame):
    """
    The pile-up fraction the CXC's table implies for a counts-per-frame value.

    Returns ``None`` above the last tabulated point rather than extrapolating. That is not
    caution for its own sake: a severely piled source *craters*, as the tool's own help
    says -- two photons in one frame are telemetered as one event of twice the energy, or
    rejected outright -- so past some rate the counts per frame stop rising and start
    falling. A straight line drawn through the crater would report less pile-up for a
    worse source, which is the one answer that would actively mislead.

    Parameters
    ----------
    counts_per_frame : float

    Returns
    -------
    float or None

    Examples
    --------
    >>> pileup_fraction(0.02)
    0.01
    >>> round(pileup_fraction(0.06), 4)
    0.03
    >>> pileup_fraction(0.5) is None
    True
    """
    counts = float(counts_per_frame)
    if counts > PILEUP_TABLE[-1][0]:
        return None
    rates = [point[0] for point in PILEUP_TABLE]
    fractions = [point[1] for point in PILEUP_TABLE]
    return float(np.interp(counts, rates, fractions))


def sky_to_image_pixel(header, x, y):
    """
    Where a sky position falls in a CIAO image's own array.

    A binned CIAO image is a crop of the sky plane, and its array indices are not sky
    coordinates: the corner is wherever the binning started. The transform is in the
    header as ``image = LTM * sky + LTV``, which is CIAO's own convention and the only
    thing that makes a position drawn on the sky mean anything in the array.

    Parameters
    ----------
    header : astropy.io.fits.Header
    x, y : float
        Sky pixel coordinates.

    Returns
    -------
    tuple of float
        Zero-based ``(column, row)`` into the array.
    """
    ltm1 = float(header.get("LTM1_1", 1.0))
    ltm2 = float(header.get("LTM2_2", 1.0))
    ltv1 = float(header.get("LTV1", 0.0))
    ltv2 = float(header.get("LTV2", 0.0))
    return (ltm1 * float(x) + ltv1 - 0.5, ltm2 * float(y) + ltv2 - 0.5)


@dataclass(frozen=True)
class PileUpCounts:
    """
    What the pile-up map says inside the source region.

    Attributes
    ----------
    peak_counts_per_frame : float
        The worst pixel.
    percentile_counts_per_frame : float
        The configured percentile of the pixels, which is the number to quote: the peak is
        one pixel and therefore noisy, and the mean is dragged down by the region's empty
        edge.
    percentile : float
    pixels : int
        How many map pixels the circle covered.
    """

    peak_counts_per_frame: float
    percentile_counts_per_frame: float
    percentile: float
    pixels: int


def read_pileup_map(path, x, y, radius_arcsec, percentile, *, pixel_arcsec):
    """
    Read the counts per frame inside the source circle.

    The circle is applied here rather than by the Data Model on purpose. Filtering an
    *image* with ``[sky=circle(...)]`` crops to the bounding box and zeroes what falls
    outside it, so the zeroes would join the sample and drag the percentile down. Selecting
    the pixels through the header's own transform keeps the sample to the circle.

    Parameters
    ----------
    path : str
        The ``pileup_map`` output.
    x, y : float
        The source, in sky pixels.
    radius_arcsec : float
    percentile : float
    pixel_arcsec : float
        The observation's sky pixel scale.

    Returns
    -------
    PileUpCounts or None
        ``None`` when the circle falls off the image, which means the map was made of the
        wrong chip and no number should be reported.
    """
    with fits.open(path) as opened:
        hdu = next(one for one in opened if one.data is not None and one.data.ndim == 2)
        data = np.asarray(hdu.data, dtype=float)
        header = hdu.header

    column, row = sky_to_image_pixel(header, x, y)
    radius_pixels = arcsec_to_sky_pixels(radius_arcsec, pixel_arcsec) * abs(
        float(header.get("LTM1_1", 1.0))
    )
    rows, columns = np.indices(data.shape)
    inside = (columns - column) ** 2 + (rows - row) ** 2 <= radius_pixels**2
    if not inside.any():
        return None

    values = data[inside]
    return PileUpCounts(
        peak_counts_per_frame=float(values.max()),
        percentile_counts_per_frame=float(np.percentile(values, percentile)),
        percentile=float(percentile),
        pixels=int(values.size),
    )


def chandra_pileup_applies(observation):
    """
    Whether pile-up can be measured for this observation at all.

    Parameters
    ----------
    observation : Observation

    Returns
    -------
    tuple
        ``(applies, reason)``. ``reason`` is empty when it applies and plain English when
        it does not, because "no pile-up measurement" on a report page is a question and
        the answer should be on the same line.
    """
    if not observation.detector.startswith("acis"):
        return False, (
            "HRC counts photons one at a time rather than in frames, so there is no frame "
            "for two photons to land in and nothing for pileup_map to measure."
        )
    if observation.is_continuous_clocking:
        return False, (
            "Continuous Clocking clocks the chip out without ever integrating a frame, so "
            "counts per frame is not a quantity this observation has."
        )
    return True, ""


@dataclass(frozen=True)
class PileUp:
    """
    What this observation's pile-up is, and what was done about it: nothing.

    Attributes
    ----------
    counts : PileUpCounts or None
    fraction, peak_fraction : float or None
        What the CXC's table makes of the percentile and the peak. ``None`` means the
        counts per frame are off the top of the table -- see :func:`pileup_fraction`.
    frame_time : float or None
        The frame the counts are per, in seconds. Worth reporting next to the fraction:
        the CXC's "counts per second" column assumes 3.2 s, and a subarray observation
        like obsid ``5644`` at 0.44 s tolerates seven times the count rate for the same
        pile-up.
    applies : bool
    reason : str
    """

    counts: Optional[PileUpCounts] = None
    fraction: Optional[float] = None
    peak_fraction: Optional[float] = None
    frame_time: Optional[float] = None
    applies: bool = True
    reason: str = ""


def chandra_pileup_image_path(observation, config):
    """Where the single-pixel counts image of the source chip goes."""
    return os.path.join(
        chandra_pipeline_output_path(observation.obsid, config), f"{observation.stem}_chipimg.fits"
    )


def chandra_pileup_map_path(observation, config):
    """Where the counts-per-frame map goes."""
    return os.path.join(
        chandra_pipeline_output_path(observation.obsid, config), f"{observation.stem}_pileup.fits"
    )


def _pileup_image_filter(observation, position, config):
    """
    The Data Model filter that makes ``pileup_map``'s input.

    Three things, each from the tool's own help. **One chip**, because a dropped frame on
    another CCD corrupts the whole image and dropped frames are commonest exactly where
    pile-up is. **Binned by one**, because the algorithm is defined per detector pixel and
    does not work otherwise. **No energy filter**, because pile-up is two photons
    telemetered as one event at their summed energy -- cut on energy and the evidence goes
    with it. Grade and status filtering the level-2 list already carries, and that is what
    the help asks for.

    The image is a square of ``pileup_image_pixels`` around the source rather than the whole
    chip. The help recommends about 2048 on a side; the crater a severely piled source
    makes is tens of pixels across, so a window this size holds the crater and its halo
    with room to spare, and it keeps the image the same size whatever the observation.
    """
    half = int(config["pileup_image_pixels"]) // 2
    x0, x1 = int(round(position.x)) - half, int(round(position.x)) + half
    y0, y1 = int(round(position.y)) - half, int(round(position.y)) + half
    return f"[ccd_id={position.chip_id}][bin x={x0}:{x1}:1,y={y0}:{y1}:1]"


def chandra_pileup(
    observation, config, cleaned, position, regions, rec=None, env=None, log_to=None
):
    """
    Measure ACIS pile-up, and report it without correcting it.

    For the bright X-ray binaries this pipeline is aimed at, pile-up is not a corner case
    to flag but the normal condition of an ACIS imaging observation -- a source at
    0.07 counts per second is already 10% piled at a 3.2 s frame. There is no correction
    here and there is not going to be one: ``jdpileup``, annulus surgery and readout-streak
    extraction are all analysis decisions with a scientist's judgement in them, and this
    pipeline's job is to say what the number is so that the decision can be made.

    Parameters
    ----------
    observation : Observation
    config : dict
        ``pileup_percentile`` and ``pileup_image_pixels``.
    cleaned : str or None
        The screened event list. ``None`` means there is nothing to measure.
    position : SourcePosition
        Which chip to make the image of, and where in it to read the answer.
    regions : ExtractionRegions
        For the source radius; the circle is applied to the map, not to the events.
    rec : StepRecord, optional
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    PileUp
        Always a ``PileUp``, never ``None``: "not measured, and here is why" is a result
        the report page needs as much as a number.
    """
    from . import ciao

    rec = rec or no_record()
    applies, why = chandra_pileup_applies(observation)
    if not applies or cleaned is None:
        reason = why or "There is no cleaned event list to measure pile-up on."
        rec.value(pileup_measured=False, pileup_reason=reason)
        get_logger().info(f"{observation.obsid}: no pile-up measurement -- {reason}")
        return PileUp(applies=applies, reason=reason)

    image = chandra_pileup_image_path(observation, config)
    os.makedirs(os.path.dirname(image), exist_ok=True)
    ciao.run(
        "dmcopy",
        produces=image,
        env=env,
        log_to=log_to,
        infile=cleaned + _pileup_image_filter(observation, position, config),
        outfile=image,
        clobber=True,
    )

    mapped = chandra_pileup_map_path(observation, config)
    ciao.run(
        "pileup_map",
        produces=mapped,
        env=env,
        log_to=log_to,
        infile=image,
        outfile=mapped,
        clobber=True,
    )

    percentile = float(config["pileup_percentile"])
    counts = read_pileup_map(
        mapped,
        position.x,
        position.y,
        regions.radius_arcsec,
        percentile,
        pixel_arcsec=observation.sky_pixel_arcsec,
    )
    if counts is None:
        reason = (
            "The source circle fell outside the pile-up map, which means the map was made "
            "of the wrong chip. No number is reported rather than a wrong one."
        )
        rec.value(pileup_measured=False, pileup_reason=reason)
        return PileUp(applies=True, reason=reason)

    fraction = pileup_fraction(counts.percentile_counts_per_frame)
    peak_fraction = pileup_fraction(counts.peak_counts_per_frame)
    frame_time = observation.time_resolution.seconds
    said = (
        f"more than {PILEUP_TABLE[-1][1]:.0%}, off the top of the CXC's table"
        if fraction is None
        else f"about {fraction:.1%}"
    )
    reason = (
        f"The {percentile:.0f}th percentile of the counts per frame inside the source "
        f"circle is {counts.percentile_counts_per_frame:.3f}, over {counts.pixels} map "
        f"pixels, and the worst pixel is {counts.peak_counts_per_frame:.3f}. By the CXC's "
        f"table that is {said} pile-up at a {frame_time:.5f} second frame. It is measured "
        f"and reported, never corrected."
    )
    rec.value(
        pileup_measured=True,
        pileup_reason=reason,
        pileup_percentile=percentile,
        pileup_counts_per_frame=counts.percentile_counts_per_frame,
        pileup_peak_counts_per_frame=counts.peak_counts_per_frame,
        pileup_fraction=fraction,
        pileup_peak_fraction=peak_fraction,
        pileup_pixels=counts.pixels,
        pileup_map_file=os.path.basename(mapped),
        frame_time_s=frame_time,
    )
    get_logger().info(f"{observation.obsid}: {reason}")
    return PileUp(
        counts=counts,
        fraction=fraction,
        peak_fraction=peak_fraction,
        frame_time=frame_time,
        applies=True,
        reason=reason,
    )


#: What a Chandra spectrum is worth plotting over, in keV. ACIS is calibrated from about
#: 0.3 keV and has very little effective area past 8, so a wider band is empty axis.
#: ``report.spectrum_figure`` falls back on NuSTAR's 3-79 keV without this.
CHANDRA_SPECTRUM_BAND_KEV = (0.3, 8.0)


@dataclass(frozen=True)
class SpectrumProducts:
    """
    The files one observation's spectrum is made of.

    Attributes
    ----------
    source, background : str
        The two spectra. ``BACKSCAL`` holds each region's area, so the ratio the fit needs
        is in the files rather than in a note somewhere.
    arf : str
        Effective area.
    corrected_arf : str
        The same, times the fraction of the point spread function the extraction circle
        actually caught. This -- not ``arf`` -- is what ``ANCRFILE`` points at, because the
        circle is sized from the PSF and therefore always misses some of the source.
    rmf : str
        Redistribution matrix.
    background_arf, background_rmf : str
        The background's own responses. ``bkgresp=yes`` makes them; they matter when the
        background is fitted rather than subtracted.
    grouped : str
        The source spectrum binned for fitting, and the file to open in XSPEC: it carries
        ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE``, so the rest follow it.
    """

    source: str
    background: str
    arf: str
    corrected_arf: str
    rmf: str
    background_arf: str
    background_rmf: str
    grouped: str


def chandra_spectrum_route(observation):
    """
    Which of the three things this observation's spectrum is.

    Parameters
    ----------
    observation : Observation

    Returns
    -------
    tuple
        ``(route, reason)``. ``route`` is ``"specextract"``, ``"collect"`` or ``"none"``;
        ``reason`` is empty for the first and plain English for the other two.
    """
    if observation.grating in ("HETG", "LETG"):
        return "collect", (
            f"{observation.grating} is in the beam, so the spectrum is the dispersed one "
            f"the archive already made. The pha2 and its responses are collected; "
            f"tgextract is not run and specextract would extract the zeroth order only."
        )
    if not observation.detector.startswith("acis"):
        return "none", (
            "HRC has almost no energy resolution -- its pulse height says roughly whether "
            "a photon was soft or hard and no more -- so there is no spectrum to extract. "
            "An HRC observation is a timing and imaging instrument in this pipeline."
        )
    return "specextract", ""


def chandra_spectrum_paths(observation, config):
    """
    Where one observation's spectral products go, and what they are called.

    The names are ``specextract``'s, and that is deliberate. It builds every output from
    ``outroot`` and writes those names into the spectrum's ``BACKFILE``, ``RESPFILE`` and
    ``ANCRFILE``, so renaming afterwards means rewriting three cards in two files and
    keeping them in step forever. Choosing ``outroot`` so that its own convention lands on
    the plan's ``<stem>_src.pi`` costs nothing and removes the whole problem. The one
    oddity it buys is ``_src_bkg.pi`` for the background, which is ugly and correct.

    Parameters
    ----------
    observation : Observation
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    SpectrumProducts
        Paths under ``<out_data_path>/<OBSID>/products``.
    """
    root = os.path.join(
        chandra_product_output_path(observation.obsid, config), f"{observation.stem}_src"
    )
    return SpectrumProducts(
        source=f"{root}.pi",
        background=f"{root}_bkg.pi",
        arf=f"{root}.arf",
        corrected_arf=f"{root}.corr.arf",
        rmf=f"{root}.rmf",
        background_arf=f"{root}_bkg.arf",
        background_rmf=f"{root}_bkg.rmf",
        grouped=f"{root}_grp.pi",
    )


def read_chandra_spectrum(spectrum, rmf):
    """
    One spectrum as a drawable curve, for the report page.

    See :func:`heasarc_retrieve_pipeline.utils.read_pha_spectrum`, which does the work and
    is shared with XMM.

    Parameters
    ----------
    spectrum, rmf : str

    Returns
    -------
    dict or None
    """
    return read_pha_spectrum(spectrum, rmf)


#: What ``DATAMODE`` costs a spectrum, where it costs anything. ``GRADED`` is the one that
#: matters and obsid ``5644`` is in it: ACIS telemetered a grade and a summed pulse height
#: per event and discarded the 3x3 pixel island, so nothing downstream can recompute the
#: charge-transfer-inefficiency correction or apply the VFAINT background cleaning. The
#: spectrum extracts without a word of complaint and is worth less than a ``FAINT`` one,
#: which is exactly the kind of thing a report page exists to say out loud.
SPECTRUM_DATA_MODE_CAVEATS = {
    "GRADED": (
        "DATAMODE is GRADED: ACIS sent down a grade and a summed pulse height per event "
        "and threw away the pixel island, so the CTI correction cannot be recomputed and "
        "the VFAINT background cleaning is not available. The spectrum is real and its "
        "energy scale is coarser than a FAINT observation's."
    )
}


def chandra_collect_grating_products(observation, config, rec=None):
    """
    Copy the archive's grating spectrum and responses into the products directory.

    Collection, never extraction: by Matteo's ruling of 2026-09-12 the archive's ``pha2``
    *is* the spectrum of a grating observation and ``tgextract`` is never run. Running
    ``specextract`` instead would silently extract the zeroth order -- a real spectrum, of
    the wrong thing.

    **The archive's names are kept**, against the output-naming rule. A ``pha2`` and its
    responses -- twelve of them for HETG -- are cross-referenced by name and by row order
    in ways this pipeline did not create and cannot check, so renaming risks breaking a
    set it did not make. The archive's names carry the obsid already, which is what the
    rule was for.

    Parameters
    ----------
    observation : Observation
    config : dict
        Must contain ``out_data_path``.
    rec : StepRecord, optional

    Returns
    -------
    list of str
        The copies, spectrum first.
    """
    rec = rec or no_record()
    products = chandra_product_output_path(observation.obsid, config)
    os.makedirs(products, exist_ok=True)

    sources = [observation.grating_spectrum] if observation.grating_spectrum else []
    sources += list(observation.grating_responses)

    collected = []
    for source in sources:
        destination = os.path.join(products, os.path.basename(source))
        if os.path.abspath(source) != os.path.abspath(destination):
            shutil.copy2(source, destination)
        collected.append(destination)

    _, reason = chandra_spectrum_route(observation)
    rec.value(
        spectrum_route="collect",
        spectrum_reason=reason,
        grating=observation.grating,
        n_grating_files=len(collected),
        grating_files=[os.path.basename(one) for one in collected],
    )
    get_logger().info(
        f"{observation.obsid}: collected {len(collected)} {observation.grating} files; "
        f"tgextract was not run"
    )
    return collected


def chandra_calculate_spectra(
    observation, config, cleaned, regions, rec=None, env=None, log_to=None
):
    """
    Extract the source and background spectra with their responses.

    ``specextract`` is CIAO's metatask and the direct analogue of XMM's ``especget``: it
    runs ``dmextract`` for both spectra, ``mkarf`` and ``arfcorr`` for the effective area
    and its aperture correction, and ``mkacisrmf`` for the redistribution matrix, and it
    writes the names of the companions into the source spectrum's header.

    **Unweighted responses, aperture corrected.** ``weight=no`` makes the response at the
    source's own position instead of averaging it over the region, which is what a point
    source wants; ``correctpsf=yes`` then scales the effective area by the fraction of the
    point spread function the circle actually caught. The second is not optional here,
    because step 6 sizes that circle *from* the PSF and so always leaves some of the source
    outside it -- without the correction every fitted normalisation would be low by the
    encircled-energy fraction.

    **Grouping is a separate ``dmgroup`` call.** ``specextract``'s own ``grouptype`` and
    ``binspec`` did nothing whatever in CIAO 4.18.0 -- measured on obsid ``5644``, the
    spectrum came back with ``GROUPING = 0``, no ``GROUPING`` column and no warning of any
    kind. Calling ``dmgroup`` is explicit, keeps ``BACKFILE``, ``RESPFILE`` and
    ``ANCRFILE`` on the way through, and does not depend on a contributed script's
    conventions.

    Parameters
    ----------
    observation : Observation
    config : dict
        ``spectrum_min_counts``, ``spectrum_weight``, ``spectrum_correct_psf``.
    cleaned : str or None
        The screened event list. ``None`` means there is nothing to extract from.
    regions : ExtractionRegions
        From :func:`chandra_source_regions`.
    rec : StepRecord, optional
    env : dict, optional
    log_to : str, optional

    Returns
    -------
    SpectrumProducts or None
        ``None`` when this observation gets no spectrum of its own -- HRC, a grating
        observation, or no cleaned list. The record says which.
    """
    from . import ciao

    rec = rec or no_record()
    route, reason = chandra_spectrum_route(observation)

    if route == "collect":
        chandra_collect_grating_products(observation, config, rec=rec)
        return None
    if route == "none" or cleaned is None:
        reason = reason or "There is no cleaned event list to extract a spectrum from."
        rec.value(spectrum_route="none", spectrum_reason=reason)
        get_logger().info(f"{observation.obsid}: no spectrum -- {reason}")
        return None

    paths = chandra_spectrum_paths(observation, config)
    products = os.path.dirname(paths.source)
    os.makedirs(products, exist_ok=True)
    root = paths.source[: -len(".pi")]

    get_logger().info(
        f"{observation.obsid}: extracting the spectrum from {regions.source}. "
        f"mkacisrmf is the slow part of this."
    )
    ciao.run(
        "specextract",
        produces=[paths.source, paths.background, paths.arf, paths.rmf],
        env=env,
        log_to=log_to,
        infile=cleaned + regions.source,
        outroot=root,
        bkgfile=cleaned + regions.background,
        asp=",".join(observation.aspect_solutions),
        mskfile=observation.mask_file or "",
        badpixfile=observation.bad_pixel_file or "",
        bkgresp="yes",
        weight="yes" if config["spectrum_weight"] else "no",
        correctpsf="yes" if config["spectrum_correct_psf"] else "no",
        grouptype="NONE",
        binspec="NONE",
        clobber=True,
    )

    ciao.run(
        "dmgroup",
        produces=paths.grouped,
        env=env,
        log_to=log_to,
        infile=paths.source,
        outfile=paths.grouped,
        grouptype="NUM_CTS",
        grouptypeval=config["spectrum_min_counts"],
        binspec="",
        xcolumn="CHANNEL",
        ycolumn="COUNTS",
        clobber=True,
    )

    caveat = SPECTRUM_DATA_MODE_CAVEATS.get((observation.data_mode or "").strip().upper(), "")
    for which, path in (("src", paths.source), ("bkg", paths.background)):
        curve = read_chandra_spectrum(path, paths.rmf)
        if curve is not None:
            # ``spec_<stem>_<src|bkg>_<...>`` is report.spectrum_figure's convention, and
            # following it is what makes the spectra draw.
            rec.array(
                **{f"spec_{observation.stem}_{which}_{key}": value for key, value in curve.items()}
            )

    rec.value(
        spectrum_route="specextract",
        spectrum_reason="",
        spectrum_caveat=caveat,
        data_mode=observation.data_mode,
        source_spectrum=os.path.basename(paths.source),
        background_spectrum=os.path.basename(paths.background),
        arf=os.path.basename(paths.arf),
        corrected_arf=os.path.basename(paths.corrected_arf),
        rmf=os.path.basename(paths.rmf),
        grouped_spectrum=os.path.basename(paths.grouped),
        source_region=regions.source,
        background_region=regions.background,
        min_counts=config["spectrum_min_counts"],
        weighted=bool(config["spectrum_weight"]),
        psf_corrected=bool(config["spectrum_correct_psf"]),
        energy_band=list(CHANDRA_SPECTRUM_BAND_KEV),
    )
    if caveat:
        get_logger().warning(f"{observation.obsid}: {caveat}")
    return paths


#: Why an observation reduced without a position stops where it does.
NO_POSITION_REASON = (
    "no source position was given, and every step past the front end is built from one: "
    "the extraction regions, the background curve the flares are cut on (the source is "
    "cut out of it), the pile-up measurement, the spectrum, and the barycentring, which "
    "is only as good as the position it is made at"
)


def _chandra_reduce(obsid, observation, part, key, config, ra, dec, diagnostics, env):
    """
    Everything after the front end, for one observation or for one part of one.

    ``part`` is ``None`` for an ordinary observation, whose records and tool logs keep the
    names they always had. For a part, the observation is first cut down to it by
    :func:`chandra_part_observation`, and every record is keyed and every tool log suffixed
    with its label, ``obi000``, so that the parts cannot overwrite each other.
    """
    suffix = f"_{key}" if key else ""

    def log(tool):
        return tool_log_file(f"{tool}{suffix}", obsid, config)

    with record_step(diagnostics, obsid, "source_region", key=key) as rec:
        rec.value(ra=ra, dec=dec)
        if part is not None:
            rec.value(part=part.number)
            observation = chandra_part_observation(
                observation, part, config, env=env, log_to=log("dmcopy_part")
            )
        position, regions = chandra_source_regions(
            observation, config, ra, dec, rec=rec, env=env, log_to=log("psfsize_srcs")
        )

    with record_step(diagnostics, obsid, "flare_filtering", key=key) as rec:
        lightcurve = chandra_flare_lightcurve(
            observation, position, regions, config, env=env, log_to=log("dmextract")
        )
        gti = chandra_flare_gti(observation, config, lightcurve, rec=rec)

    with record_step(diagnostics, obsid, "clean_event_list", key=key) as rec:
        cleaned = chandra_clean_event_list(
            observation, config, gti, rec=rec, env=env, log_to=log("dmcopy")
        )

    with record_step(diagnostics, obsid, "pileup_check", key=key) as rec:
        chandra_pileup(
            observation,
            config,
            cleaned,
            position,
            regions,
            rec=rec,
            env=env,
            log_to=log("pileup_map"),
        )

    with record_step(diagnostics, obsid, "barycenter", key=key) as rec:
        barycentered = chandra_barycenter(
            observation, config, cleaned, ra=ra, dec=dec, rec=rec, env=env, log_to=log("axbary")
        )
        if barycentered is not None:
            chandra_barycentered_source_events(
                observation,
                config,
                barycentered,
                regions,
                rec=rec,
                env=env,
                log_to=log("dmcopy_src_bary"),
            )
            chandra_compress_barycentered_events(barycentered, rec=rec)

    with record_step(diagnostics, obsid, "calculate_spectra", key=key) as rec:
        chandra_calculate_spectra(
            observation, config, cleaned, regions, rec=rec, env=env, log_to=log("specextract")
        )


@flow(flow_run_name="chandra_{obsid}")
def process_chandra_obsid(obsid, config=None, ra="NONE", dec="NONE", flags=None):
    """
    Reduce one Chandra observation end to end.

    One detector in one mode, so unlike XMM there is no loop over exposures: the front end
    reads the observation, and then regions, flare screening, a cleaned event list, a
    pile-up measurement, barycentring and the spectrum follow once each.

    The order is forced in one place and chosen in another. The regions come **first**,
    before any screening, because the flare curve is measured on the source's chip with
    the source cut out of it. Barycentring comes **before** the spectrum, because timing
    is what this pipeline is judged on and ``specextract`` is by far the slowest task in
    it; an observation whose spectrum fails should still have its barycentred events.

    Like XMM, and unlike NuSTAR, ``ra`` and ``dec`` are used as given and never
    overridden -- see :func:`chandra_source_position`.

    **An observation taken in several parts is reduced one part at a time**, each as an
    observation of its own from the source region on -- see
    :func:`chandra_part_observation`. Every record is keyed by the part's label and every
    output named with it. As XMM does with its exposures, a part that fails is recorded and
    the others are still reduced; only when every part fails does the observation.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict, optional
        Pipeline configuration; :data:`DEFAULT_CONFIG` where it says nothing.
        ``products`` picks the front end.
    ra, dec : float or str, optional
        Source position in degrees. Without one the front end still runs and records what
        the observation is, and every later step is recorded as skipped with the reason.
    flags : dict, optional
        Accepted for the signature every mission's entry point shares. Nothing reads it.

    Returns
    -------
    str or None
        :data:`heasarc_retrieve_pipeline.utils.NO_SCIENCE_DATA` when the observation holds
        no event list the chosen route can reduce, and ``None`` otherwise.
    """
    from . import ciao

    config = chandra_config(config)
    obsid = chandra_obsid(obsid)
    logger = get_logger()
    logger.info(f"Processing Chandra observation {obsid} on the {config['products']} route")

    for directory in (
        chandra_pipeline_output_path(obsid, config),
        chandra_product_output_path(obsid, config),
    ):
        os.makedirs(directory, exist_ok=True)
    diagnostics = diagnostics_path(obsid, config)

    with record_step(diagnostics, obsid, "chandra_front_end") as rec:
        if config["products"] == "repro":
            observation = chandra_repro_front_end(
                obsid, config, rec=rec, log_to=tool_log_file("chandra_repro", obsid, config)
            )
        else:
            observation = chandra_archive_front_end(obsid, config, rec=rec)

    if observation is None:
        # Not a failure. A downloaded Chandra directory can hold nothing a route reduces.
        logger.warning(f"{obsid} holds no event list to reduce. Nothing to do.")
        return NO_SCIENCE_DATA

    logger.info(
        f"{obsid}: {observation.detector} {observation.mode}, grating {observation.grating}, "
        f"time resolution {observation.time_resolution.seconds} s"
    )

    if _source_coordinates(ra, dec) is None:
        with record_step(diagnostics, obsid, "source_region") as rec:
            rec.skip(NO_POSITION_REASON)
        logger.warning(f"{obsid}: {NO_POSITION_REASON}.")
        return None

    env = ciao.ciao_environment(obsid, config)

    parts = getattr(observation, "parts", ())
    units = [(chandra_part_label(part), part) for part in parts] if len(parts) > 1 else [("", None)]
    failed = {}
    for key, part in units:
        try:
            _chandra_reduce(obsid, observation, part, key, config, ra, dec, diagnostics, env)
        except Exception as error:
            if part is None:
                raise
            # XMM's rule for its exposures: the step that failed has recorded why under this
            # part's key, and the parts that worked are kept.
            failed[key] = error
            logger.error(
                f"{obsid}: part {part.number} could not be reduced and is left out of this "
                f"observation's products: {type(error).__name__}: {error}"
            )

    if failed and len(failed) == len(units):
        last = list(failed.values())[-1]
        raise RuntimeError(
            f"no part of {obsid} could be reduced; the last failure was "
            f"{type(last).__name__}: {last}"
        ) from last
    if failed:
        logger.warning(
            f"{obsid}: reduced {len(units) - len(failed)} of {len(units)} parts. "
            f"{', '.join(sorted(failed))} failed; the diagnostics say why."
        )

    logger.info(f"Finished processing Chandra observation {obsid}")
    return None
