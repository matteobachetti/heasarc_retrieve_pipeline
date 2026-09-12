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
from typing import Optional

import numpy as np
from astropy.io import fits

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
    "hrc_veto_ratio_threshold": 0.99,
    "pileup_percentile": 90.0,
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
