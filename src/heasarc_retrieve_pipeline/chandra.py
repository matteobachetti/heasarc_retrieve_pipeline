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

#: The pipeline's defaults for a Chandra run.
#:
#: ``products`` chooses the route: ``"archive"`` reads the level-2 products the archive
#: already holds, ``"repro"`` runs ``chandra_repro`` over the level-1 telemetry. The
#: archive is the default by Matteo's ruling of 2026-09-12, mirroring XMM's PPS/ODF split
#: -- with ``CALDBVER`` reported as a staleness diagnostic rather than used to decide the
#: route.
DEFAULT_CONFIG = {
    "out_data_path": "./",
    "input_data_path": "./",
    "products": "archive",
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
