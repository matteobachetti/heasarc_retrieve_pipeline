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

import glob
import os
from dataclasses import dataclass
from itertools import takewhile
from typing import Optional

import numpy as np
from astropy.io import fits

from .diagnostics import no_record
from .utils import (
    get_logger,
    good_intervals,
    intersect_intervals,
    intervals_above_threshold,
    merge_intervals,
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


def _archive_products(obsid, config, pattern):
    """
    Every downloaded file matching a glob, sorted, gzipped or not.

    The archive gzips its products and ``chandra_repro`` does not, so each pattern is
    tried both ways. An observation that was never downloaded gives an empty list rather
    than raising: whether there is anything to reduce is the caller's decision to make,
    as it is in :mod:`heasarc_retrieve_pipeline.xmm`.
    """
    root = chandra_archive_path(obsid, config)
    found = set()
    for suffix in ("", ".gz"):
        found.update(glob.glob(os.path.join(root, pattern + suffix)))
    return sorted(found)


def _one_archive_product(obsid, config, pattern, what):
    """
    The single downloaded file matching a glob, or ``None``.

    A Chandra observation is one detector in one mode, so each of these families has
    exactly one member. Two is not a tie to break at random -- it means a half-finished
    reprocessing or two archive versions side by side, and reducing the wrong one in
    silence is the worst available outcome.
    """
    found = _archive_products(obsid, config, pattern)
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
    The archive's level-2 event list, or ``None`` if the observation has none.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``primary/*_evt2.fits[.gz]``.

    Raises
    ------
    ValueError
        If there is more than one.
    """
    return _one_archive_product(obsid, config, os.path.join("primary", "*_evt2.fits"), "evt2")


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
        Path to ``primary/*_asol1.fits[.gz]``.
    """
    return _one_archive_product(obsid, config, os.path.join("primary", "*_asol1.fits"), "asol1")


def chandra_bad_pixel_file(obsid, config):
    """
    The bad-pixel list, from whichever directory this instrument's lives in.

    **ACIS files it under** ``primary/`` **and HRC under** ``secondary/``. Looking in one
    directory finds it for one instrument and silently misses it for the other, and the
    symptom does not appear until ``specextract`` runs.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``{primary,secondary}/*_bpix1.fits[.gz]``.
    """
    for directory in ("primary", "secondary"):
        found = _one_archive_product(
            obsid, config, os.path.join(directory, "*_bpix1.fits"), "bpix1"
        )
        if found is not None:
            return found
    return None


def chandra_dead_time_file(obsid, config):
    """
    The dead-time-factor file, which only HRC writes.

    This is the file the whole timing story rests on: its veto ratio, and not the event
    header, says whether an HRC observation really has the 15.625 us resolution every HRC
    header claims. ACIS has none, and ``None`` is the answer rather than an error.

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
        Path to ``secondary/*_msk1.fits[.gz]``.
    """
    return _one_archive_product(obsid, config, os.path.join("secondary", "*_msk1.fits"), "msk1")


def chandra_gti_file(obsid, config):
    """
    The observation's own good-time intervals, as the archive's pipeline found them.

    Named ``flt1``, and usually ``*_std_flt1.fits.gz``. This is the starting point the
    flare screening narrows, not a replacement for it.

    Parameters
    ----------
    obsid : int or str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str or None
        Path to ``secondary/*_flt1.fits[.gz]``.
    """
    return _one_archive_product(obsid, config, os.path.join("secondary", "*_flt1.fits"), "flt1")


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
    """

    seconds: float
    basis: str
    reason: str
    fast_timing: Optional[bool] = None
    veto_ratio: Optional[float] = None
    trigger_rate_hz: Optional[float] = None


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
        Companion products.
    dead_time_file : str or None
        HRC only, and ``None`` for ACIS is normal rather than missing.
    orbit_ephemeris : str or None
        What ``axbary`` barycentres with.
    grating_spectrum : str or None
    grating_responses : tuple of str
        The archive's ready-made grating products, collected and never re-made.
    caldb_version, ascds_version : str or None
        What the archive's own reduction was made with.
    active_rows : tuple or None
        ``(FIRSTROW, NROWS)`` for ACIS -- the rows of each CCD that were actually clocked
        out. This is the subarray, and it is why obsid ``5644`` reads out every 0.44 s
        instead of every 3.2 s: 128 rows of 1024. ``None`` for HRC, and for an ACIS header
        that does not say, which means a full frame.
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
    active_rows: Optional[tuple] = None

    @property
    def stem(self):
        """What every output file of this observation is named from."""
        return chandra_file_stem(self.obsid, self.detector, self.mode)

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
    Read one observation off the archive's own level-2 products.

    The default route. No CIAO is involved: the archive's ``primary/`` products are what
    CIAO would produce, so this reads their headers, finds the companions and works out
    what the data can support.

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

    dtf_path = chandra_dead_time_file(obsid, config)
    dtf = read_dead_time_factors(dtf_path) if dtf_path is not None else None
    resolution = chandra_time_resolution(header, dtf, config)

    observation = Observation(
        obsid=obsid,
        detector=chandra_detector(header),
        grating=str(header.get("GRATING", "NONE")).strip().upper(),
        mode=chandra_mode_label(header, fast_timing=resolution.fast_timing),
        time_resolution=resolution,
        chips=tuple(chandra_chips(header)),
        event_list=events,
        aspect_solution=chandra_aspect_solution(obsid, config),
        bad_pixel_file=chandra_bad_pixel_file(obsid, config),
        mask_file=chandra_mask_file(obsid, config),
        gti_file=chandra_gti_file(obsid, config),
        dead_time_file=dtf_path,
        orbit_ephemeris=chandra_orbit_ephemeris(obsid, config),
        grating_spectrum=chandra_grating_spectrum(obsid, config),
        grating_responses=tuple(chandra_grating_responses(obsid, config)),
        caldb_version=_keyword(header, "CALDBVER"),
        ascds_version=_keyword(header, "ASCDSVER"),
        active_rows=_active_rows(header),
    )

    if rec is not None:
        rec.value(
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
            caldb_version=observation.caldb_version,
            caldb_current=CURRENT_CALDB_VERSION,
            caldb_is_stale=observation.caldb_is_stale,
            ascds_version=observation.ascds_version,
            has_dead_time_file=dtf_path is not None,
            n_grating_responses=len(observation.grating_responses),
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


#: One Chandra sky pixel, in arcseconds. ``dmcoords`` reports it as the sky pixel scale on
#: every observation, ACIS and HRC alike: the two detectors have very different physical
#: pixels, but both are projected onto the same 8192 x 8192 sky plane.
SKY_PIXEL_ARCSEC = 0.492


def arcsec_to_sky_pixels(arcsec):
    """
    An angle on the sky, in Chandra sky pixels.

    Examples
    --------
    >>> arcsec_to_sky_pixels(0.984)
    2.0
    """
    return arcsec / SKY_PIXEL_ARCSEC


def sky_pixels_to_arcsec(pixels):
    """
    Chandra sky pixels, as an angle on the sky.

    Examples
    --------
    >>> sky_pixels_to_arcsec(2.0)
    0.984
    """
    return pixels * SKY_PIXEL_ARCSEC


def circle_region(x, y, radius_arcsec):
    """
    A circle, as CIAO's Data Model spells it.

    The shape alone, with no column system in front of it -- see :func:`sky_filter`. Kept
    apart so the same text can go into a region file, where naming a column would be
    wrong, and onto a file name, where it is required.

    Examples
    --------
    >>> circle_region(4100.38, 4131.82, 0.984)
    'circle(4100.3800,4131.8200,2.0000)'
    """
    return f"circle({x:.4f},{y:.4f},{arcsec_to_sky_pixels(radius_arcsec):.4f})"


def annulus_region(x, y, inner_arcsec, outer_arcsec):
    """
    An annulus, as CIAO's Data Model spells it.

    Examples
    --------
    >>> annulus_region(4100.0, 4131.0, 0.984, 1.968)
    'annulus(4100.0000,4131.0000,2.0000,4.0000)'
    """
    inner = arcsec_to_sky_pixels(inner_arcsec)
    outer = arcsec_to_sky_pixels(outer_arcsec)
    return f"annulus({x:.4f},{y:.4f},{inner:.4f},{outer:.4f})"


def sky_filter(region):
    """
    A shape, as a Data Model filter on the sky columns.

    Appended to a file name, this is what ``dmcopy`` and ``dmextract`` cut with. The
    brackets and parentheses are exactly why :func:`heasarc_retrieve_pipeline.ciao.run`
    never goes through a shell.

    Examples
    --------
    >>> sky_filter(circle_region(4100.38, 4131.82, 0.984))
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
        Its ``event_list`` sets the coordinate frame, and its ``aspect_solution`` refines
        it. An observation with no aspect solution still converts, off the header alone.
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
    if observation.aspect_solution is not None:
        parameters["asolfile"] = observation.aspect_solution

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


def read_psf_size(path):
    """
    Read the region file ``psfsize_srcs`` wrote.

    The conversion to arcseconds happens once, here: the tool writes ``R`` in sky pixels
    and every radius in this module's configuration is an angle.

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

    return PsfSize(radius_arcsec=sky_pixels_to_arcsec(radius))


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
    return read_psf_size(outfile)


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
        source=sky_filter(circle_region(position.x, position.y, radius_arcsec)),
        background=sky_filter(annulus_region(position.x, position.y, inner, outer)),
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


def read_pileup_map(path, x, y, radius_arcsec, percentile):
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
    radius_pixels = arcsec_to_sky_pixels(radius_arcsec) * abs(float(header.get("LTM1_1", 1.0)))
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
    counts = read_pileup_map(mapped, position.x, position.y, regions.radius_arcsec, percentile)
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
