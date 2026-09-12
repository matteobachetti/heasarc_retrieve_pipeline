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

#: The pipeline's defaults for a Chandra run.
#:
#: ``products`` chooses the route: ``"archive"`` reads the level-2 products the archive
#: already holds, ``"repro"`` runs ``chandra_repro`` over the level-1 telemetry. The
#: archive is the default by Matteo's ruling of 2026-09-12, mirroring XMM's PPS/ODF split
#: -- with ``CALDBVER`` reported as a staleness diagnostic rather than used to decide the
#: route.
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
