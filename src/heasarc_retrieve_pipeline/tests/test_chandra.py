"""
Offline tests for the Chandra reduction.

No CIAO is needed for any of these. Unlike SAS, CIAO *can* be installed from conda, but
Matteo ruled on 2026-09-12 that continuous integration stays offline and stubbed anyway,
so everything decidable from a file name, a path or a configuration is decided in pure
Python and tested here.

The file names are real. All three observation directories were listed in full from the
public S3 mirror of the HEASARC archive on 2026-09-12, and the three observations are the
ones ``docs/chandra_integration_plan.md`` measured: they span HRC-I, HRC-S in its
fast-timing mode, and ACIS-S behind a transmission grating.
"""

import re

import pytest

from heasarc_retrieve_pipeline import chandra


# ``6298``: HRC-I, no grating. 28 files and 51.0 MB in the archive; the nine below are
# 15.4 MB of it. This is the observation that shows the bad-pixel trap -- its ``bpix1``
# is under ``secondary/``.
HRC_I_6298_KEPT = [
    "oif.fits",
    "primary/hrcf06298N006_evt2.fits.gz",
    "primary/hrcf06298_000N006_dtf1.fits.gz",
    "primary/hrcf06298_000N006_fov1.fits.gz",
    "primary/orbitf235397100N001_eph1.fits.gz",
    "primary/pcadf06298_000N001_asol1.fits.gz",
    "secondary/hrcf06298_000N006_bpix1.fits.gz",
    "secondary/hrcf06298_000N006_msk1.fits.gz",
    "secondary/hrcf06298_000N006_std_flt1.fits.gz",
]

HRC_I_6298_DROPPED = [
    # The raw level-1 event list, 22.6 MB. The archive route reduces level 2 and never
    # looks at it; the reprocessing route needs it.
    "secondary/hrcf06298_000N006_evt1.fits.gz",
    # The verification-and-validation report: 10.4 MB of 51, for human eyes only, and the
    # single biggest saving on every observation.
    "secondary/axaff06298N006_VV001_vvref2.pdf.gz",
    "axaff06298N006_VV001_vv2.pdf",
    # Pictures, including two that share a stem with files that are kept.
    "primary/hrcf06298N006_cntr_img2.fits.gz",
    "primary/hrcf06298N006_cntr_img2.jpg",
    "primary/hrcf06298N006_full_img2.fits.gz",
    "primary/hrcf06298N006_full_img2.jpg",
    # Near-misses, and the reason every pattern is anchored on the whole name. An
    # ``osol1`` is not an ``asol1``; a ``std_dtfstat1`` is not a ``dtf1``; the lunar and
    # solar ephemerides end in ``_eph1`` exactly as the orbit one does.
    "secondary/aspect/pcadf235694823N004_osol1.fits.gz",
    "secondary/aspect/pcadf235701383N004_osol1.fits.gz",
    "secondary/hrcf06298_000N006_std_dtfstat1.fits.gz",
    "secondary/ephem/lunarf235397100N001_eph1.fits.gz",
    "secondary/ephem/solarf235397100N001_eph1.fits.gz",
    "secondary/ephem/anglesf06298_000N004_eph1.fits.gz",
    # Aspect housekeeping and the mission timeline, which nothing here reads.
    "secondary/aspect/pcadf235697045N004_aqual1.fits.gz",
    "secondary/aspect/pcadf235697049N004_adat71.fits.gz",
    "secondary/hrcf06298_000N006_mtl1.fits.gz",
    "00README",
]

# ``17661``: HRC-S in ``S_TIMING``. 35 files and 264.5 MB; the nine below are 88.7 MB.
# Same shape as ``6298`` -- which is the point. Nothing in the listing distinguishes the
# one observation that really has 15.625 us resolution from the one that does not.
HRC_S_17661_KEPT = [
    "oif.fits",
    "primary/hrcf17661N002_evt2.fits.gz",
    "primary/hrcf17661_001N002_dtf1.fits.gz",
    "primary/hrcf17661_001N002_fov1.fits.gz",
    "primary/orbitf548856303N001_eph1.fits.gz",
    "primary/pcadf17661_001N001_asol1.fits.gz",
    "secondary/hrcf17661_001N002_bpix1.fits.gz",
    "secondary/hrcf17661_001N002_msk1.fits.gz",
    "secondary/hrcf17661_001N002_std_flt1.fits.gz",
]

HRC_S_17661_DROPPED = [
    "secondary/hrcf17661_001N002_evt1.fits.gz",
    "secondary/axaff17661N002_VV001_vvref2.pdf.gz",  # 59.9 MB of 264.5
    "axaff17661N002_VV001_vv2.pdf",
    "primary/hrcf17661N002_cntr_img2.fits.gz",
    "primary/hrcf17661N002_cntr_img2.jpg",
    "primary/hrcf17661N002_full_img2.fits.gz",
    "primary/hrcf17661N002_full_img2.jpg",
    "secondary/aspect/pcadf548892511N002_osol1.fits.gz",
    "secondary/aspect/pcadf548893662N002_adat71.fits.gz",
    "secondary/aspect/pcadf548893659N002_aqual1.fits.gz",
    "secondary/hrcf17661_001N002_std_dtfstat1.fits.gz",
    "secondary/ephem/lunarf548856303N001_eph1.fits.gz",
    "secondary/ephem/solarf548856303N001_eph1.fits.gz",
    "secondary/ephem/anglesf17661_001N002_eph1.fits.gz",
    "secondary/hrcf17661_001N002_mtl1.fits.gz",
    "00README",
]

# ``2749``: ACIS-S behind HETG. 63 files and 458.5 MB; the 33 below are 180.2 MB, of
# which 155 MB is ``responses/`` alone. That is the price of the gratings decision, and
# it is the right price: those 24 files *are* the spectra, already made.
ACIS_HETG_2749_KEPT = [
    "oif.fits",
    "primary/acisf02749N004_evt2.fits.gz",
    "primary/acisf02749N004_pha2.fits.gz",
    "primary/acisf02749_000N004_bpix1.fits.gz",
    "primary/acisf02749_000N004_fov1.fits.gz",
    "primary/orbitf136814700N001_eph1.fits.gz",
    "primary/pcadf02749_000N001_asol1.fits.gz",
    "primary/responses/acisf02749N004_Hm1_arf2.fits.gz",
    "primary/responses/acisf02749N004_Hm1_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Hm2_arf2.fits.gz",
    "primary/responses/acisf02749N004_Hm2_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Hm3_arf2.fits.gz",
    "primary/responses/acisf02749N004_Hm3_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Hp1_arf2.fits.gz",
    "primary/responses/acisf02749N004_Hp1_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Hp2_arf2.fits.gz",
    "primary/responses/acisf02749N004_Hp2_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Hp3_arf2.fits.gz",
    "primary/responses/acisf02749N004_Hp3_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Mm1_arf2.fits.gz",
    "primary/responses/acisf02749N004_Mm1_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Mm2_arf2.fits.gz",
    "primary/responses/acisf02749N004_Mm2_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Mm3_arf2.fits.gz",
    "primary/responses/acisf02749N004_Mm3_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Mp1_arf2.fits.gz",
    "primary/responses/acisf02749N004_Mp1_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Mp2_arf2.fits.gz",
    "primary/responses/acisf02749N004_Mp2_rmf2.fits.gz",
    "primary/responses/acisf02749N004_Mp3_arf2.fits.gz",
    "primary/responses/acisf02749N004_Mp3_rmf2.fits.gz",
    "secondary/acisf02749_000N004_flt1.fits.gz",
    "secondary/acisf02749_000N004_msk1.fits.gz",
]

ACIS_HETG_2749_DROPPED = [
    # 165 MB of level-1 telemetry across two files.
    "secondary/acisf02749_000N004_evt1.fits.gz",
    "secondary/acisf02749_000N004_evt1a.fits.gz",
    "secondary/axaff02749N003_VV001_vvref2.pdf.gz",  # 101.7 MB of 458.5
    "axaff02749N003_VV001_vv2.pdf",
    "primary/acisf02749N004_cntr_img2.jpg",
    "primary/acisf02749N004_full_img2.jpg",
    # ACIS bias maps and the parameter block: only the reprocessing route wants these.
    "secondary/acisf136980412N004_0_bias0.fits.gz",
    "secondary/acisf136980412N004_5_bias0.fits.gz",
    "secondary/acisf136981522N004_pbk0.fits.gz",
    "secondary/acisf02749_000N004_stat1.fits.gz",
    "secondary/acisf02749_000N004_mtl1.fits.gz",
    "secondary/aspect/pcadf136975249N004_osol1.fits.gz",
    "secondary/aspect/pcadf136981952N004_aqual1.fits.gz",
    "secondary/ephem/lunarf136814700N001_eph1.fits.gz",
    "secondary/ephem/solarf136814700N001_eph1.fits.gz",
    "secondary/ephem/anglesf02749_000N004_eph1.fits.gz",
    "00README",
]

#: The three listings, keyed by the OBSID and the directory the archive files them under,
#: which is the OBSID's last digit.
OBSERVATIONS = {
    "6298": ("8", HRC_I_6298_KEPT, HRC_I_6298_DROPPED),
    "17661": ("1", HRC_S_17661_KEPT, HRC_S_17661_DROPPED),
    "2749": ("9", ACIS_HETG_2749_KEPT, ACIS_HETG_2749_DROPPED),
}


def what_a_filter_keeps(arguments, obsid):
    """
    Run a download filter over a recorded listing, the way a transport would.

    Both transports match against the whole remote name, not the basename: an HTTPS URL
    for one, a bucket key for the other. They are spelled differently and the filter has
    to work on either, so both are tried here and the answers must agree. This is
    ``test_xmm.py``'s helper of the same name, over Chandra's archive paths.
    """
    include = arguments.get("re_include", "")
    exclude = arguments.get("re_exclude", "")
    include = re.compile(include) if include else None
    exclude = re.compile(exclude) if exclude else None

    digit, kept, dropped = OBSERVATIONS[obsid]
    entries = sorted(kept + dropped)

    answers = {}
    for flavour, base in [
        ("https", f"https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/{digit}/{obsid}/"),
        ("s3", f"chandra/data/byobsid/{digit}/{obsid}/"),
    ]:
        answers[flavour] = [
            entry
            for entry in entries
            if (include is None or include.search(base + entry))
            and not (exclude is not None and exclude.search(base + entry))
        ]
    assert answers["https"] == answers["s3"], "the filter reads an S3 key and a URL differently"
    return answers["s3"]


class TestTheArchiveRoute:
    """
    What the default route downloads: the archive's own level-2 products.

    Measured on 2026-09-12 at 30%, 34% and 39% of the three directories.
    """

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_it_keeps_exactly_what_was_measured(self, obsid):
        _, kept, _ = OBSERVATIONS[obsid]

        assert what_a_filter_keeps(chandra.chandra_download_filter({}), obsid) == sorted(kept)

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_the_bad_pixel_file_survives_for_both_instruments(self, obsid):
        """
        The trap, and the reason there is a test for it.

        ACIS files its ``bpix1`` under ``primary/`` and HRC under ``secondary/``. A
        filter anchored on ``primary/`` alone drops it for every HRC observation, and the
        symptom does not appear until ``specextract`` runs, hours later.
        """
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), obsid)

        assert len([name for name in kept if "_bpix1." in name]) == 1

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_the_orbit_ephemeris_comes_but_the_lunar_and_solar_ones_do_not(self, obsid):
        """
        The one family that is genuinely ambiguous by name.

        Four files end in ``_eph1.fits.gz`` -- orbit, lunar, solar and angles -- and only
        the orbit one barycentres anything. Two independent things exclude the other
        three: the ``orbitf`` prefix, and the ``primary/`` anchor, since the rest are
        filed under ``secondary/ephem/``. Dropping either alone still passes; dropping
        both fails this test, which is what makes the redundancy worth keeping.
        """
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), obsid)

        assert [name for name in kept if name.endswith("_eph1.fits.gz")] == [
            name for name in kept if "/orbitf" in name
        ]

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_the_aspect_solution_comes_but_not_the_one_second_solution(self, obsid):
        """
        ``asol1`` is what ``specextract`` wants. ``osol1`` differs from it by one letter,
        which is alarming to read but not to match: the pattern is the literal
        ``_asol1.fits.gz``, and the ``osol1`` files are under ``secondary/aspect/``
        besides. This is a regression guard against widening either, not a demonstration
        that the current pattern is at risk.
        """
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), obsid)

        assert len([name for name in kept if name.endswith("_asol1.fits.gz")]) == 1
        assert not [name for name in kept if "osol1" in name]

    def test_the_dead_time_file_comes_for_hrc_but_its_statistics_file_does_not(self):
        """
        ``dtf1`` is the discriminator the whole timing story rests on. ``std_dtfstat1``
        is a different file whose name contains those four letters -- again a near-miss
        to the eye rather than to the pattern, and again worth pinning.
        """
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), "6298")

        assert "primary/hrcf06298_000N006_dtf1.fits.gz" in kept
        assert not [name for name in kept if "dtfstat" in name]

    def test_acis_has_no_dead_time_file_to_keep(self):
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), "2749")

        assert not [name for name in kept if "_dtf1." in name]

    def test_the_grating_products_come_whole(self):
        """
        Gratings are collected, never re-extracted: the ``pha2`` and the twelve
        ARF/RMF pairs are the spectra, and this is the only place they come from.
        """
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), "2749")

        assert len([name for name in kept if "/responses/" in name]) == 24
        assert len([name for name in kept if name.endswith("_pha2.fits.gz")]) == 1

    @pytest.mark.parametrize("obsid", ["6298", "17661"])
    def test_an_ungrating_observation_brings_no_grating_products(self, obsid):
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), obsid)

        assert not [name for name in kept if "/responses/" in name or "_pha2." in name]

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_the_verification_report_is_left_at_the_archive(self, obsid):
        """60 MB of ``17661`` and 102 MB of ``2749``, and no reduction reads a word of it."""
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), obsid)

        assert not [name for name in kept if name.endswith((".pdf", ".pdf.gz"))]

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_no_level_one_telemetry_is_downloaded(self, obsid):
        """The archive route reduces the archive's level 2. Level 1 is 165 MB on ``2749``."""
        kept = what_a_filter_keeps(chandra.chandra_download_filter({}), obsid)

        assert not [name for name in kept if "_evt1" in name or "_bias0." in name]


class TestTheReprocessingRoute:
    """
    What ``products="repro"`` downloads: everything ``chandra_repro`` could read.

    It excludes rather than includes, because ``chandra_repro`` is given a directory and
    decides for itself what in it to use. Measured at 79%, 77% and 78% of the three.
    """

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_it_keeps_the_level_one_products_the_archive_route_skips(self, obsid):
        kept = what_a_filter_keeps(chandra.chandra_download_filter({"products": "repro"}), obsid)

        assert [name for name in kept if "_evt1" in name] == sorted(
            name for name in OBSERVATIONS[obsid][2] if "_evt1" in name
        )

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_it_still_leaves_the_report_and_the_pictures_behind(self, obsid):
        """The only exclusions, and between them they are over a fifth of a directory."""
        kept = what_a_filter_keeps(chandra.chandra_download_filter({"products": "repro"}), obsid)

        assert not [
            name
            for name in kept
            if name.endswith((".pdf", ".pdf.gz", ".jpg")) or name.endswith("_img2.fits.gz")
        ]

    @pytest.mark.parametrize("obsid", sorted(OBSERVATIONS))
    def test_it_keeps_everything_else(self, obsid):
        digit, kept_files, dropped = OBSERVATIONS[obsid]
        kept = what_a_filter_keeps(chandra.chandra_download_filter({"products": "repro"}), obsid)

        pictures_and_reports = {
            name
            for name in kept_files + dropped
            if name.endswith((".pdf", ".pdf.gz", ".jpg", "_img2.fits.gz"))
        }
        assert set(kept) == set(kept_files + dropped) - pictures_and_reports


class TestTheFilterItself:
    def test_the_default_route_is_the_archive(self):
        """An empty config and an explicit ``"archive"`` must agree."""
        assert chandra.chandra_download_filter({}) == chandra.chandra_download_filter(
            {"products": "archive"}
        )

    @pytest.mark.parametrize("products", ["archive", "repro"])
    def test_it_names_only_arguments_the_downloader_takes(self, products):
        """``core.mission_download_filter`` rejects anything else, and loudly."""
        assert set(chandra.chandra_download_filter({"products": products})) <= {
            "re_include",
            "re_exclude",
        }

    def test_an_unknown_route_is_an_error(self):
        """Falling back to "download everything" would answer a typo with half a gigabyte."""
        with pytest.raises(ValueError, match="archive"):
            chandra.chandra_download_filter({"products": "Archive "})
