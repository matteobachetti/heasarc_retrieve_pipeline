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

import json
import os
import re
from types import SimpleNamespace

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import chandra, ciao
from heasarc_retrieve_pipeline.diagnostics import record_step


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

# ``5644``: ACIS-S on chip 7 alone, Timed Exposure on a 128-row subarray at 0.44104 s,
# and the observation Liu 2024 detected M82 X-2's 1.37 s pulsation in. Listed from S3 on
# 2026-09-12 and downloaded through the pipeline's own filter on the same day: 8 files and
# 27.6 MB of the archive's 39 files and 220.9 MB. Not one of the plan's three measured
# configurations -- it is the known-answer test, and its subarray is why it is fast.
ACIS_S_5644_KEPT = [
    "oif.fits",
    "primary/acisf05644N004_evt2.fits.gz",
    "primary/acisf05644_000N004_bpix1.fits.gz",
    "primary/acisf05644_000N004_fov1.fits.gz",
    "primary/orbitf240581100N001_eph1.fits.gz",
    "primary/pcadf05644_000N001_asol1.fits.gz",
    "secondary/acisf05644_000N004_flt1.fits.gz",
    "secondary/acisf05644_000N004_msk1.fits.gz",
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


class TestNamingAnObservation:
    """
    The padding asymmetry, which is the one thing here that is easy to get wrong.

    ``chanmaster.obsid`` is an integer and the archive files an observation under its
    unpadded decimal form -- ``byobsid/8/6298/`` -- so that is what the download directory
    is called and what the pipeline's own directories must match. But the archive's *file*
    names are zero-padded to five digits (``hrcf06298``, ``acisf02749``), and output files
    leave the tree that gives them context, so their stems pad too.
    """

    @pytest.mark.parametrize("given", [6298, "6298", "06298", "0006298"])
    def test_a_directory_is_named_the_way_the_archive_files_it(self, given):
        assert chandra.chandra_obsid(given) == "6298"

    @pytest.mark.parametrize("given", [6298, "6298", "06298"])
    def test_a_file_stem_pads_to_five_digits(self, given):
        assert chandra.chandra_padded_obsid(given) == "06298"

    def test_an_obsid_too_long_to_pad_is_left_alone(self):
        """Chandra has not reached six digits, but truncating one would be worse."""
        assert chandra.chandra_padded_obsid(123456) == "123456"

    @pytest.mark.parametrize("given", ["", "abc", "62 98", "../6298", None])
    def test_something_that_is_not_an_obsid_is_an_error(self, given):
        with pytest.raises((ValueError, TypeError)):
            chandra.chandra_obsid(given)

    def test_the_stem_names_the_observation_the_detector_and_the_mode(self):
        """The rule Matteo set for XMM on 2026-09-08: every file names its own OBSID."""
        assert chandra.chandra_file_stem(6298, "hrci", "imaging") == "chandra06298_hrci_imaging"
        assert chandra.chandra_file_stem("2749", "aciss", "hetg") == "chandra02749_aciss_hetg"

    def test_the_longest_stem_leaves_room_in_a_fits_card(self):
        """
        ``BACKFILE`` and friends are 80-character cards, and FTOOLS truncate at that.
        The longest stem a real observation can produce is well inside it.
        """
        stem = chandra.chandra_file_stem(99999, "aciss", "continuous_clocking")

        assert len(stem + "_bkg.pi") < 60


class TestWhichAcisConfigurationAnObservationIs:
    """
    ACIS-I or ACIS-S, which the chip set cannot answer and ``SIM_Z`` can.

    ``DETNAM`` names the chips that were switched on, not the configuration. Measured on
    2026-09-12 over 150 randomly chosen archived ACIS observations: **50 of them had both
    chip 3 (I3) and chip 7 (S3) on**, and those 50 span both configurations, so no rule
    written on the chip set can decide. ``SIM_Z`` -- where the Science Instrument Module
    was parked -- separates them with an 18.1 mm gap and no overlap:

    ============ ========================= =========================
    Catalogue    ``SIM_Z`` range (75 each) Median (= nominal aimpoint)
    ============ ========================= =========================
    ``ACIS-I``   -238.274 .. -214.099      -233.587
    ``ACIS-S``   -195.973 .. -182.134      -190.143
    ============ ========================= =========================
    """

    @pytest.mark.parametrize(
        "detnam, sim_z, expected",
        [
            # The four observations the plan measures, with their real header values.
            ("ACIS-456789", -187.125, "aciss"),  # 2749, ACIS-S + HETG
            ("ACIS-7", -190.140, "aciss"),  # 5644, the known-answer subarray
            ("ACIS-01236", -225.783, "acisi"),  # 14022, ACIS-I
            ("ACIS-012378", -233.587, "acisi"),  # 62520, ACIS-I at the nominal aimpoint
            # The two chip sets that defeat every chip rule, with the SIM_Z that decides.
            ("ACIS-235678", -190.133, "aciss"),  # ACIS-S, yet chip 3 is on
            ("ACIS-012367", -233.587, "acisi"),  # ACIS-I, yet chip 7 is on
            # The extremes of the measured ranges, either side of the gap.
            ("ACIS-0123", -214.099, "acisi"),
            ("ACIS-56789", -195.973, "aciss"),
            ("ACIS-0123", -238.274, "acisi"),
            ("ACIS-567", -182.134, "aciss"),
        ],
    )
    def test_the_sim_position_decides(self, detnam, sim_z, expected):
        header = {"INSTRUME": "ACIS", "DETNAM": detnam, "SIM_Z": sim_z}

        assert chandra.chandra_detector(header) == expected

    def test_the_chip_set_alone_would_get_a_third_of_the_archive_wrong(self):
        """
        Both configurations below have chips 3 and 7 on. A rule reading either chip
        answers the same for both, and one of the answers is wrong.
        """
        acis_i = {"INSTRUME": "ACIS", "DETNAM": "ACIS-012367", "SIM_Z": -233.587}
        acis_s = {"INSTRUME": "ACIS", "DETNAM": "ACIS-235678", "SIM_Z": -190.133}

        assert chandra.chandra_detector(acis_i) != chandra.chandra_detector(acis_s)

    @pytest.mark.parametrize("detnam, expected", [("HRC-I", "hrci"), ("HRC-S", "hrcs")])
    def test_hrc_says_so_in_its_own_detnam_and_needs_no_sim_position(self, detnam, expected):
        """``DETNAM`` is the configuration itself for HRC, so ``SIM_Z`` is never consulted."""
        header = {"INSTRUME": "HRC", "DETNAM": detnam}

        assert chandra.chandra_detector(header) == expected

    def test_an_acis_header_without_a_sim_position_is_an_error(self):
        """Guessing would put a wrong detector into every output file name."""
        with pytest.raises(ValueError, match="SIM_Z"):
            chandra.chandra_detector({"INSTRUME": "ACIS", "DETNAM": "ACIS-01236"})

    def test_an_unknown_instrument_is_an_error(self):
        with pytest.raises(ValueError, match="INSTRUME"):
            chandra.chandra_detector({"INSTRUME": "EPIC", "DETNAM": "PN"})


def a_downloaded_observation(tmp_path, obsid="6298", names=None):
    """Lay the named archive files out where a download would have put them."""
    names = HRC_I_6298_KEPT if names is None else names
    config = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}
    for name in names:
        path = tmp_path / chandra.chandra_obsid(obsid) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
    return config


class TestWhereEverythingGoes:
    def test_the_download_directory_is_the_archives_own_spelling(self, tmp_path):
        """
        The download lands in ``<input>/6298`` because that is the last component of the
        archive prefix. Looking in ``<input>/06298`` would find an empty directory and
        report an observation with no science data.
        """
        config = a_downloaded_observation(tmp_path)

        assert chandra.chandra_archive_path("06298", config) == str(tmp_path / "6298")

    def test_the_output_tree_uses_the_names_the_report_already_knows(self, tmp_path):
        """
        ``event_cl`` and ``products`` are NuSTAR's names, kept so that
        ``report.OBSERVATION_SUBDIRECTORIES`` finds a Chandra tree untaught -- the same
        reason :mod:`heasarc_retrieve_pipeline.xmm` kept them.
        """
        config = {"out_data_path": str(tmp_path)}

        assert chandra.chandra_base_output_path(6298, config) == str(tmp_path / "6298")
        assert chandra.chandra_pipeline_output_path(6298, config) == str(
            tmp_path / "6298" / "event_cl"
        )
        assert chandra.chandra_product_output_path(6298, config) == str(
            tmp_path / "6298" / "products"
        )


class TestFindingTheProductsOfAnObservation:
    def test_it_finds_every_family_of_an_hrc_observation(self, tmp_path):
        config = a_downloaded_observation(tmp_path, "6298", HRC_I_6298_KEPT)

        def base(found):
            return None if found is None else os.path.basename(found)

        assert base(chandra.chandra_event_list("6298", config)) == "hrcf06298N006_evt2.fits.gz"
        assert base(chandra.chandra_aspect_solution("6298", config)) == (
            "pcadf06298_000N001_asol1.fits.gz"
        )
        assert base(chandra.chandra_bad_pixel_file("6298", config)) == (
            "hrcf06298_000N006_bpix1.fits.gz"
        )
        assert base(chandra.chandra_dead_time_file("6298", config)) == (
            "hrcf06298_000N006_dtf1.fits.gz"
        )
        assert base(chandra.chandra_orbit_ephemeris("6298", config)) == (
            "orbitf235397100N001_eph1.fits.gz"
        )
        assert base(chandra.chandra_mask_file("6298", config)) == "hrcf06298_000N006_msk1.fits.gz"

    def test_it_finds_the_bad_pixel_file_of_an_acis_observation_in_the_other_directory(
        self, tmp_path
    ):
        """
        The trap again, one layer up: ACIS files ``bpix1`` under ``primary/`` and HRC
        under ``secondary/``. The finder must look in both, whichever instrument it has.
        """
        config = a_downloaded_observation(tmp_path, "2749", ACIS_HETG_2749_KEPT)

        found = chandra.chandra_bad_pixel_file("2749", config)

        assert os.path.basename(found) == "acisf02749_000N004_bpix1.fits.gz"
        assert os.path.dirname(found).endswith("primary")

    def test_acis_has_no_dead_time_file_and_that_is_not_an_error(self, tmp_path):
        """Only HRC writes one, so ``None`` is the answer, not an exception."""
        config = a_downloaded_observation(tmp_path, "2749", ACIS_HETG_2749_KEPT)

        assert chandra.chandra_dead_time_file("2749", config) is None

    def test_it_collects_the_grating_products_whole(self, tmp_path):
        """Twelve ARF/RMF pairs and one ``pha2``: these are the spectra, already made."""
        config = a_downloaded_observation(tmp_path, "2749", ACIS_HETG_2749_KEPT)

        assert os.path.basename(chandra.chandra_grating_spectrum("2749", config)) == (
            "acisf02749N004_pha2.fits.gz"
        )
        assert len(chandra.chandra_grating_responses("2749", config)) == 24

    def test_an_observation_without_gratings_collects_none(self, tmp_path):
        config = a_downloaded_observation(tmp_path, "6298", HRC_I_6298_KEPT)

        assert chandra.chandra_grating_spectrum("6298", config) is None
        assert chandra.chandra_grating_responses("6298", config) == []

    def test_an_observation_never_downloaded_finds_nothing_rather_than_raising(self, tmp_path):
        """Whether there is anything to reduce is the caller's decision, as for XMM."""
        config = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}

        assert chandra.chandra_event_list("6298", config) is None
        assert chandra.chandra_grating_responses("6298", config) == []

    def test_an_ungzipped_product_is_found_too(self, tmp_path):
        """``chandra_repro`` writes plain FITS; the archive gzips. Both are products."""
        config = a_downloaded_observation(tmp_path, "6298", ["primary/hrcf06298N006_evt2.fits"])

        assert os.path.basename(chandra.chandra_event_list("6298", config)) == (
            "hrcf06298N006_evt2.fits"
        )

    def test_two_event_lists_are_an_error_rather_than_a_coin_toss(self, tmp_path):
        """
        A Chandra observation is one detector in one mode and so has one level-2 event
        list. Two means something is wrong -- a half-finished reprocessing, most likely --
        and picking one at random would reduce the wrong data in silence.
        """
        config = a_downloaded_observation(
            tmp_path,
            "6298",
            ["primary/hrcf06298N006_evt2.fits.gz", "primary/hrcf06298N005_evt2.fits.gz"],
        )

        with pytest.raises(ValueError, match="evt2"):
            chandra.chandra_event_list("6298", config)


class TestWhichChipsWereReadOut:
    """
    ``DETNAM`` is a chip list, and the digits are chip identifiers.

    Chips 0-3 are the ACIS-I array (I0-I3) and chips 4-9 are the ACIS-S array (S0-S5).
    The aimpoint is I3 -- chip 3 -- for ACIS-I and S3 -- chip 7 -- for ACIS-S. An
    observation routinely switches on chips from both arrays: ``ACIS-012367`` is the
    whole ACIS-I array read out with S2 and S3 alongside it. Step 6 needs the list to
    know which chip the source lands on.
    """

    @pytest.mark.parametrize(
        "detnam, chips",
        [
            ("ACIS-012367", [0, 1, 2, 3, 6, 7]),  # the ACIS-I array plus S2 and S3
            ("ACIS-456789", [4, 5, 6, 7, 8, 9]),  # the whole ACIS-S array
            ("ACIS-0123", [0, 1, 2, 3]),  # the ACIS-I array alone
            ("ACIS-7", [7]),  # one chip: obsid 5644's subarray
            ("ACIS-235678", [2, 3, 5, 6, 7, 8]),
        ],
    )
    def test_it_reads_the_chip_list_off_detnam(self, detnam, chips):
        assert chandra.chandra_chips({"INSTRUME": "ACIS", "DETNAM": detnam}) == chips

    @pytest.mark.parametrize("detnam", ["HRC-I", "HRC-S"])
    def test_hrc_has_no_chips(self, detnam):
        """HRC is a microchannel plate, not a CCD array, so the answer is empty."""
        assert chandra.chandra_chips({"INSTRUME": "HRC", "DETNAM": detnam}) == []

    def test_the_aimpoint_chip_is_named_for_each_configuration(self):
        """I3 for ACIS-I and S3 for ACIS-S, which is where an on-axis source lands."""
        assert chandra.ACIS_AIMPOINT_CHIP == {"acisi": 3, "aciss": 7}

    def test_both_aimpoint_chips_can_be_on_at_once(self):
        """
        Which is the whole reason ``chandra_detector`` reads ``SIM_Z``: this chip list
        contains both aimpoints, so it cannot say which one the telescope was focused on.
        """
        chips = chandra.chandra_chips({"INSTRUME": "ACIS", "DETNAM": "ACIS-012367"})

        assert set(chandra.ACIS_AIMPOINT_CHIP.values()) <= set(chips)


class TestTheModeLabelInAFileName:
    """
    The third field of an output stem, and the one the header cannot always supply.

    A grating in the beam names the mode, because a grating observation's products *are*
    the grating products. Otherwise ACIS says its readout mode in ``READMODE``. HRC says
    nothing usable at all -- every HRC header reads ``DATAMODE = 'OBSERVING'`` whether or
    not the observation has real 15.625 us resolution -- so the caller settles that from
    the dead-time file and passes the answer in.
    """

    @pytest.mark.parametrize("grating, expected", [("HETG", "hetg"), ("LETG", "letg")])
    def test_a_grating_names_the_mode(self, grating, expected):
        header = {"INSTRUME": "ACIS", "GRATING": grating, "READMODE": "TIMED"}

        assert chandra.chandra_mode_label(header) == expected

    def test_a_grating_names_it_for_hrc_too(self, tmp_path):
        header = {"INSTRUME": "HRC", "GRATING": "LETG"}

        assert chandra.chandra_mode_label(header) == "letg"

    def test_timed_exposure_is_timed(self):
        """Obsid 2749's mode, and 85% of the archive's."""
        header = {"INSTRUME": "ACIS", "GRATING": "NONE", "READMODE": "TIMED"}

        assert chandra.chandra_mode_label(header) == "timed"

    def test_continuous_clocking_is_cc(self):
        """Measured on obsid 31917: ``READMODE = 'CONTINUOUS'``, and TIMEDEL 2.85 ms."""
        header = {"INSTRUME": "ACIS", "GRATING": "NONE", "READMODE": "CONTINUOUS"}

        assert chandra.chandra_mode_label(header) == "cc"

    @pytest.mark.parametrize("fast_timing, expected", [(True, "timing"), (False, "imaging")])
    def test_hrc_is_labelled_from_the_answer_it_is_given(self, fast_timing, expected):
        """
        ``17661`` and ``6298`` have identical headers and differ by a factor of 280 in
        real time resolution. Only the dead-time file tells them apart, so only a caller
        that has read it can label them.
        """
        header = {"INSTRUME": "HRC", "GRATING": "NONE", "DATAMODE": "OBSERVING"}

        assert chandra.chandra_mode_label(header, fast_timing=fast_timing) == expected

    def test_an_hrc_header_with_no_answer_supplied_is_an_error(self):
        """
        Defaulting to "imaging" would label ``17661`` -- a real ``S_TIMING`` observation
        -- exactly as it labels ``6298``, which is the confusion this module exists to
        prevent.
        """
        header = {"INSTRUME": "HRC", "GRATING": "NONE", "DATAMODE": "OBSERVING"}

        with pytest.raises(ValueError, match="fast_timing"):
            chandra.chandra_mode_label(header)

    def test_an_acis_header_with_an_unknown_readmode_is_an_error(self):
        header = {"INSTRUME": "ACIS", "GRATING": "NONE", "READMODE": "SOMETHING_NEW"}

        with pytest.raises(ValueError, match="READMODE"):
            chandra.chandra_mode_label(header)

    def test_the_label_is_always_usable_in_a_file_stem(self):
        """Whatever it returns has to survive ``chandra_file_stem``'s validation."""
        for header, kwargs in [
            ({"INSTRUME": "ACIS", "GRATING": "HETG", "READMODE": "TIMED"}, {}),
            ({"INSTRUME": "ACIS", "GRATING": "NONE", "READMODE": "CONTINUOUS"}, {}),
            ({"INSTRUME": "HRC", "GRATING": "NONE"}, {"fast_timing": True}),
        ]:
            label = chandra.chandra_mode_label(header, **kwargs)

            assert chandra.chandra_file_stem(6298, "hrci", label).endswith(label)


def a_dead_time_file(path, total, valid, sample_interval=2.05, n=200, bad_rows=0):
    """
    A ``dtf1`` file with the real column structure, and values chosen by the caller.

    The columns and their formats are copied from the real files: ``STATUS`` is ``8X``,
    eight bits, and a row counts only when every one of them is zero.
    """
    n_good = n - bad_rows
    status = np.zeros((n, 8), dtype=bool)
    status[n_good:, 0] = True
    columns = fits.ColDefs(
        [
            fits.Column("TIME", "1D", "s", array=np.arange(n) * sample_interval + 1.0e8),
            fits.Column("DTF", "1D", array=np.full(n, 0.9)),
            fits.Column("DTF_ERR", "1D", array=np.full(n, 0.01)),
            fits.Column("PROC_EVT_COUNT", "1J", "count", array=np.full(n, valid)),
            fits.Column("TOTAL_EVT_COUNT", "1J", "count", array=np.full(n, total)),
            fits.Column("VALID_EVT_COUNT", "1J", "count", array=np.full(n, valid)),
            fits.Column("STATUS", "8X", array=status),
        ]
    )
    hdu = fits.BinTableHDU.from_columns(columns, name="DTF")
    # The real files carry this, and it is the sampling interval of the table -- NOT the
    # event time resolution. Putting it here keeps the test honest about the confusion.
    hdu.header["TIMEDEL"] = 2.0
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


class TestWhatTimeResolutionTheDataSupports:
    """
    The part of Chandra a naive reduction gets wrong.

    Every HRC event header reads ``TIMEDEL = 1.5625e-05`` whether or not that resolution
    is real. A backplane wiring error latches the event time on every front-end *trigger*
    rather than every telemetered *event*, so each event carries the following trigger's
    time; where on-board vetoing threw the intervening triggers away, the shift cannot be
    undone and the resolution is about one over the trigger rate. ``S_TIMING`` disables
    all vetoing, so there the shift is recoverable and the full 15.625 us stands.

    The discriminator is the dead-time file's veto ratio, and it is already on disk.
    """

    def test_no_vetoing_means_the_header_is_right_and_the_rate_formula_is_not(self):
        """
        The whole point of the function, and the first test written.

        Obsid ``17661`` is HRC-S ``S_TIMING``: ``VALID/TOTAL = 1.000`` at 60.0 triggers
        per second. Applying ``1 / rate`` to it gives 16.67 ms -- **a thousand times
        worse than the truth**. The ratio has to be tested first.
        """
        header = {"INSTRUME": "HRC", "DETNAM": "HRC-S", "TIMEDEL": 1.5625e-05}
        dtf = chandra.DeadTimeFactors(
            veto_ratio=1.0,
            trigger_rate_hz=60.0,
            sample_interval=2.05,
            n_good_rows=14586,
            n_rows=14588,
        )

        found = chandra.chandra_time_resolution(header, dtf)

        assert found.seconds == pytest.approx(1.5625e-05)
        assert found.seconds != pytest.approx(1 / 60.0)
        assert found.fast_timing is True

    def test_heavy_vetoing_gives_the_documented_four_milliseconds(self):
        """
        Obsid ``6298`` is HRC-I: ``VALID/TOTAL = 0.296`` at 228.8 triggers per second,
        which is 4.37 ms -- and the CXC documents "about 4 milliseconds" for exactly this.
        The header claims 15.625 us, so believing it would be wrong by a factor of 280.
        """
        header = {"INSTRUME": "HRC", "DETNAM": "HRC-I", "TIMEDEL": 1.5625e-05}
        dtf = chandra.DeadTimeFactors(
            veto_ratio=0.2964,
            trigger_rate_hz=228.78,
            sample_interval=2.05,
            n_good_rows=2392,
            n_rows=2769,
        )

        found = chandra.chandra_time_resolution(header, dtf)

        assert found.seconds == pytest.approx(4.371e-3, rel=1e-3)
        assert found.seconds / 1.5625e-05 == pytest.approx(280, rel=0.02)
        assert found.fast_timing is False

    def test_the_two_hrc_observations_have_identical_headers(self):
        """
        The reason the dead-time file is consulted at all: nothing in the event header
        separates a real 15.625 us observation from one 280 times coarser.
        """
        header = {"INSTRUME": "HRC", "DETNAM": "HRC-I", "TIMEDEL": 1.5625e-05}
        timing = chandra.DeadTimeFactors(1.0, 60.0, 2.05, 14586, 14588)
        imaging = chandra.DeadTimeFactors(0.2964, 228.78, 2.05, 2392, 2769)

        assert chandra.chandra_time_resolution(header, timing).seconds != pytest.approx(
            chandra.chandra_time_resolution(header, imaging).seconds
        )

    def test_hrc_without_a_dead_time_file_assumes_the_documented_value_and_says_so(self):
        """Never the header's 15.625 us, which would be an unearned claim."""
        header = {"INSTRUME": "HRC", "DETNAM": "HRC-I", "TIMEDEL": 1.5625e-05}

        found = chandra.chandra_time_resolution(header, None)

        assert found.seconds == pytest.approx(chandra.HRC_DOCUMENTED_RESOLUTION)
        assert found.fast_timing is False
        assert "assum" in found.reason.lower()

    @pytest.mark.parametrize("ratio, expected_fast", [(0.989, False), (0.99, True), (1.0, True)])
    def test_the_threshold_is_where_the_configuration_puts_it(self, ratio, expected_fast):
        header = {"INSTRUME": "HRC", "DETNAM": "HRC-S", "TIMEDEL": 1.5625e-05}
        dtf = chandra.DeadTimeFactors(ratio, 100.0, 2.05, 100, 100)

        found = chandra.chandra_time_resolution(header, dtf)

        assert found.fast_timing is expected_fast

    def test_a_stricter_threshold_can_be_configured(self):
        """
        0.99 was chosen from two observations, one at 1.000 and one at 0.296. The gap is
        enormous so almost any threshold works, but the distribution across the ~1669
        HRC-S observations is unmeasured -- so it stays a knob.
        """
        header = {"INSTRUME": "HRC", "DETNAM": "HRC-S", "TIMEDEL": 1.5625e-05}
        dtf = chandra.DeadTimeFactors(0.995, 100.0, 2.05, 100, 100)

        strict = chandra.chandra_time_resolution(header, dtf, {"hrc_veto_ratio_threshold": 0.999})

        assert strict.fast_timing is False

    def test_acis_timed_exposure_is_the_frame_time_in_the_header(self):
        """Obsid 2749: 2.54104 s, and honest about it."""
        header = {"INSTRUME": "ACIS", "READMODE": "TIMED", "TIMEDEL": 2.54104}

        found = chandra.chandra_time_resolution(header)

        assert found.seconds == pytest.approx(2.54104)
        assert found.fast_timing is None

    def test_an_acis_subarray_is_read_from_the_header_and_not_assumed_to_be_slow(self):
        """
        Obsid ``5644``, the known-answer test: ``TIMEDEL = 0.44104``, not the nominal
        3.2 s. Nyquist period 0.88 s, so M82 X-2's 1.37 s spin is sampled 3.1 times a
        cycle -- which is how Liu 2024 detected it. Assuming 3.2 s would have discarded
        the one M82 observation with a published detection.
        """
        header = {"INSTRUME": "ACIS", "READMODE": "TIMED", "TIMEDEL": 0.44104}

        found = chandra.chandra_time_resolution(header)

        assert found.seconds == pytest.approx(0.44104)
        assert 2 * found.seconds < 1.345

    def test_continuous_clocking_is_milliseconds_and_warns_about_the_lost_dimension(self):
        """Measured on obsid 31917: ``TIMEDEL = 0.00285``, i.e. 2.85 ms."""
        header = {"INSTRUME": "ACIS", "READMODE": "CONTINUOUS", "TIMEDEL": 0.00285}

        found = chandra.chandra_time_resolution(header)

        assert found.seconds == pytest.approx(0.00285)
        assert "spatial" in found.reason.lower()

    def test_every_branch_says_why_in_plain_english(self):
        """The reason is recorded in the diagnostics for every observation."""
        cases = [
            ({"INSTRUME": "ACIS", "READMODE": "TIMED", "TIMEDEL": 3.14104}, None),
            ({"INSTRUME": "ACIS", "READMODE": "CONTINUOUS", "TIMEDEL": 0.00285}, None),
            ({"INSTRUME": "HRC", "TIMEDEL": 1.5625e-05}, None),
            (
                {"INSTRUME": "HRC", "TIMEDEL": 1.5625e-05},
                chandra.DeadTimeFactors(1.0, 60.0, 2.05, 9, 9),
            ),
            (
                {"INSTRUME": "HRC", "TIMEDEL": 1.5625e-05},
                chandra.DeadTimeFactors(0.2964, 228.78, 2.05, 9, 9),
            ),
        ]
        seen = set()
        for header, dtf in cases:
            found = chandra.chandra_time_resolution(header, dtf)

            assert found.seconds > 0
            assert len(found.reason.split()) >= 5
            seen.add(found.basis)
        assert len(seen) == len(cases), "each branch must be distinguishable in the record"

    def test_an_acis_readmode_it_does_not_know_is_an_error(self):
        header = {"INSTRUME": "ACIS", "READMODE": "SOMETHING_NEW", "TIMEDEL": 3.2}

        with pytest.raises(ValueError, match="READMODE"):
            chandra.chandra_time_resolution(header)

    def test_a_header_with_no_timedel_is_an_error(self):
        """Every branch but the HRC fallback needs it, and inventing one would be a lie."""
        with pytest.raises(ValueError, match="TIMEDEL"):
            chandra.chandra_time_resolution({"INSTRUME": "ACIS", "READMODE": "TIMED"})


class TestReadingTheDeadTimeFile:
    def test_it_reproduces_the_two_measured_observations(self, tmp_path):
        """
        The real medians, from the real files, read on 2026-09-12: ``6298`` gives
        469 total and 139 valid per 2.05 s sample, ``17661`` gives 123 and 123.
        """
        hrc_i = a_dead_time_file(tmp_path / "i.fits", total=469, valid=139)
        hrc_s = a_dead_time_file(tmp_path / "s.fits", total=123, valid=123)

        assert chandra.read_dead_time_factors(hrc_i).veto_ratio == pytest.approx(0.2964, rel=1e-3)
        assert chandra.read_dead_time_factors(hrc_i).trigger_rate_hz == pytest.approx(
            228.78, rel=1e-3
        )
        assert chandra.read_dead_time_factors(hrc_s).veto_ratio == pytest.approx(1.0)
        assert chandra.read_dead_time_factors(hrc_s).trigger_rate_hz == pytest.approx(60.0)

    def test_rows_with_a_status_bit_set_are_left_out(self, tmp_path):
        """
        ``STATUS`` is eight bits and a row counts only when every one is zero. On the
        real ``6298`` that drops 377 rows of 2769.
        """
        path = a_dead_time_file(tmp_path / "d.fits", total=469, valid=139, n=100, bad_rows=40)

        found = chandra.read_dead_time_factors(path)

        assert (found.n_good_rows, found.n_rows) == (60, 100)

    def test_the_sample_interval_is_measured_not_taken_from_the_header(self, tmp_path):
        """
        The file's own ``TIMEDEL`` says 2.0 and the rows are 2.05 s apart. That keyword
        is the sampling interval's nominal value -- and it is *not* the event time
        resolution either, which is the confusion this whole module is about.
        """
        path = a_dead_time_file(tmp_path / "d.fits", total=469, valid=139, sample_interval=2.05)

        assert chandra.read_dead_time_factors(path).sample_interval == pytest.approx(2.05)

    def test_a_file_with_no_usable_rows_is_an_error(self, tmp_path):
        path = a_dead_time_file(tmp_path / "d.fits", total=469, valid=139, n=10, bad_rows=10)

        with pytest.raises(ValueError, match="no usable rows"):
            chandra.read_dead_time_factors(path)

    def test_a_file_with_no_triggers_is_an_error(self, tmp_path):
        """Dividing by a zero trigger rate would give an infinite time resolution."""
        path = a_dead_time_file(tmp_path / "d.fits", total=0, valid=0)

        with pytest.raises(ValueError, match="no triggers"):
            chandra.read_dead_time_factors(path)


def an_event_file(path, **keywords):
    """A level-2 event list carrying the header keywords the front end reads."""
    header = {
        "INSTRUME": "HRC",
        "DETNAM": "HRC-I",
        "GRATING": "NONE",
        "DATAMODE": "OBSERVING",
        "TIMEDEL": 1.5625e-05,
        "TIMESYS": "TT",
        "MJDREF": 50814.0,
        "ASCDSVER": "10.10",
        "CALDBVER": "4.9.5",
    }
    header.update(keywords)
    hdu = fits.BinTableHDU.from_columns(
        [fits.Column("time", "1D", array=np.arange(10.0))], name="EVENTS"
    )
    for key, value in header.items():
        if value is not None:
            hdu.header[key] = value
    path.parent.mkdir(parents=True, exist_ok=True)
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


def an_archive_observation(tmp_path, obsid="6298", names=None, dtf=(469, 139), **keywords):
    """
    A downloaded observation with a real event list and a real dead-time file.

    ``a_downloaded_observation`` writes empty placeholders, which is all the path finders
    need. Anything that opens a file needs more than that, so the two files the front end
    reads are written properly here. ``dtf`` is ``(TOTAL_EVT_COUNT, VALID_EVT_COUNT)``,
    defaulting to obsid ``6298``'s real medians -- a heavily vetoed HRC-I observation.
    """
    names = HRC_I_6298_KEPT if names is None else names
    config = a_downloaded_observation(tmp_path, obsid, names)
    for name in names:
        path = tmp_path / chandra.chandra_obsid(obsid) / name
        if name.endswith("_evt2.fits.gz"):
            path.unlink()
            an_event_file(path.with_suffix(""), **keywords)
        elif name.endswith("_dtf1.fits.gz") and dtf is not None:
            path.unlink()
            a_dead_time_file(path.with_suffix(""), total=dtf[0], valid=dtf[1])
    return config


class TestComparingCalibrationVersions:
    """
    The archive's level-2 products were made with the CALDB of their day, and with the
    archive route as the default that becomes a number to report rather than a reason to
    reprocess. Obsid ``2749`` says 4.9.4; the CALDB current on 2026-09-12 was 4.12.4.
    """

    def test_a_version_is_compared_as_numbers_and_not_as_a_string(self):
        """
        The trap: as text, ``"4.9.4" > "4.12.4"`` because ``9`` sorts after ``1``. A
        string comparison would call the oldest products in the archive up to date.
        """
        assert "4.9.4" > "4.12.4"  # the wrong answer, stated so the test explains itself

        assert chandra.caldb_version_tuple("4.9.4") < chandra.caldb_version_tuple("4.12.4")

    @pytest.mark.parametrize(
        "version, expected",
        [("4.9.4", (4, 9, 4)), ("4.12.4", (4, 12, 4)), ("4.9", (4, 9)), ("4.11.1b", (4, 11, 1))],
    )
    def test_it_reads_the_versions_the_archive_actually_writes(self, version, expected):
        assert chandra.caldb_version_tuple(version) == expected

    @pytest.mark.parametrize("version", [None, "", "unknown"])
    def test_an_unreadable_version_is_none_rather_than_an_error(self, version):
        """A missing CALDBVER is a diagnostic that says so, not a failed reduction."""
        assert chandra.caldb_version_tuple(version) is None


class TestReadingAnObservationOffTheArchive:
    def test_it_reads_an_hrc_observation_whole(self, tmp_path):
        config = an_archive_observation(tmp_path, "6298")

        found = chandra.chandra_archive_front_end("6298", config)

        assert found.obsid == "6298"
        assert found.detector == "hrci"
        assert found.grating == "NONE"
        assert os.path.basename(found.event_list).endswith("_evt2.fits")
        assert found.dead_time_file is not None
        assert found.orbit_ephemeris is not None
        assert found.chips == ()

    def test_it_reads_an_acis_grating_observation_whole(self, tmp_path):
        config = an_archive_observation(
            tmp_path,
            "2749",
            ACIS_HETG_2749_KEPT,
            INSTRUME="ACIS",
            DETNAM="ACIS-456789",
            GRATING="HETG",
            READMODE="TIMED",
            DATAMODE="FAINT",
            TIMEDEL=2.54104,
            SIM_Z=-187.125,
            CALDBVER="4.9.4",
        )

        found = chandra.chandra_archive_front_end("2749", config)

        assert found.detector == "aciss"
        assert found.mode == "hetg"
        assert found.chips == (4, 5, 6, 7, 8, 9)
        assert found.dead_time_file is None
        assert len(found.grating_responses) == 24
        assert found.time_resolution.seconds == pytest.approx(2.54104)

    def test_the_stem_of_an_observation_names_it_fully(self, tmp_path):
        config = an_archive_observation(tmp_path, "6298")

        found = chandra.chandra_archive_front_end("6298", config)

        assert found.stem == "chandra06298_hrci_imaging"

    def test_a_fast_timing_hrc_observation_is_labelled_timing(self, tmp_path):
        """
        The two HRC observations have identical headers, so only the dead-time file can
        make these two stems differ -- and it does.
        """
        config = an_archive_observation(
            tmp_path, "17661", HRC_S_17661_KEPT, dtf=(123, 123), DETNAM="HRC-S"
        )

        found = chandra.chandra_archive_front_end("17661", config)

        assert found.stem == "chandra17661_hrcs_timing"
        assert found.time_resolution.seconds == pytest.approx(1.5625e-05)
        assert found.time_resolution.fast_timing is True

    def test_an_observation_with_no_event_list_is_none(self, tmp_path):
        """``NO_SCIENCE_DATA`` is the caller's decision to make; this just reports."""
        config = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}

        assert chandra.chandra_archive_front_end("6298", config) is None

    def test_it_records_how_stale_the_calibration_is(self, tmp_path):
        config = an_archive_observation(
            tmp_path,
            "2749",
            ACIS_HETG_2749_KEPT,
            INSTRUME="ACIS",
            DETNAM="ACIS-456789",
            GRATING="HETG",
            READMODE="TIMED",
            TIMEDEL=2.54104,
            SIM_Z=-187.125,
            CALDBVER="4.9.4",
            ASCDSVER="10.9.4",
        )
        directory = str(tmp_path / "diag")

        with record_step(directory, "2749", "chandra_front_end") as rec:
            found = chandra.chandra_archive_front_end("2749", config, rec=rec)

        record = json.load(open(os.path.join(directory, "chandra_front_end.json")))

        assert found.caldb_version == "4.9.4"
        assert record["values"]["caldb_version"] == "4.9.4"
        assert record["values"]["caldb_is_stale"] is True
        assert record["values"]["detector"] == "aciss"
        assert record["values"]["time_resolution_s"] == pytest.approx(2.54104)
        assert record["values"]["time_resolution_basis"] == "acis_frame_time"

    def test_a_current_calibration_is_not_called_stale(self, tmp_path):
        config = an_archive_observation(tmp_path, "6298", CALDBVER=chandra.CURRENT_CALDB_VERSION)
        directory = str(tmp_path / "diag")

        with record_step(directory, "6298", "chandra_front_end") as rec:
            chandra.chandra_archive_front_end("6298", config, rec=rec)

        record = json.load(open(os.path.join(directory, "chandra_front_end.json")))

        assert record["values"]["caldb_is_stale"] is False

    def test_it_works_without_a_record(self, tmp_path):
        """Called from a test, or from a context with no output directory."""
        config = an_archive_observation(tmp_path, "6298")

        assert chandra.chandra_archive_front_end("6298", config) is not None

    def test_an_hrc_observation_without_its_dead_time_file_still_reads(self, tmp_path):
        """
        Degraded, not failed: the resolution falls back to the documented ~4 ms and says
        so. The observation is still reducible.
        """
        names = [n for n in HRC_I_6298_KEPT if "_dtf1." not in n]
        config = an_archive_observation(tmp_path, "6298", names)

        found = chandra.chandra_archive_front_end("6298", config)

        assert found.time_resolution.basis == "hrc_documented"
        assert found.mode == "imaging"


class TestAnglesAndSkyPixels:
    def test_a_sky_pixel_is_the_0_492_arcseconds_dmcoords_reports(self):
        assert chandra.SKY_PIXEL_ARCSEC == 0.492

    def test_the_two_conversions_undo_each_other(self):
        assert chandra.sky_pixels_to_arcsec(chandra.arcsec_to_sky_pixels(3.7)) == pytest.approx(3.7)


class TestHowARegionIsSpelt:
    """
    CIAO's Data Model, not SAS. A region is a bare shape and the filter that carries it
    names the column system, so the two are kept apart: the shape is what goes into a
    region file, and ``[sky=...]`` is what goes onto a file name.
    """

    def test_a_circle_is_centre_and_radius_in_sky_pixels(self):
        assert (
            chandra.circle_region(4100.38, 4131.82, 0.984) == "circle(4100.3800,4131.8200,2.0000)"
        )

    def test_an_annulus_carries_both_radii(self):
        assert chandra.annulus_region(4100.0, 4131.0, 0.984, 1.968) == (
            "annulus(4100.0000,4131.0000,2.0000,4.0000)"
        )

    def test_a_sky_filter_is_the_shape_with_its_column_system(self):
        assert chandra.sky_filter("circle(1.0,2.0,3.0)") == "[sky=circle(1.0,2.0,3.0)]"

    def test_a_chip_strip_is_a_range_of_columns(self):
        assert chandra.chipx_filter([(100, 106)]) == "[chipx=100:106]"

    def test_two_strips_are_one_filter(self):
        """Continuous Clocking's background is a strip on each side, and the Data Model
        takes both in one filter rather than needing two passes over the file."""
        assert chandra.chipx_filter([(80, 96), (110, 126)]) == "[chipx=80:96,110:126]"


class TestReadingWhatATaskWorkedOut:
    def test_pget_answers_one_value_to_a_line_in_the_order_asked(self):
        said = "4100.380855260638\n4131.817158105547\n7\n"

        assert chandra.parse_pget(said, ("x", "y", "chip_id")) == {
            "x": 4100.380855260638,
            "y": 4131.817158105547,
            "chip_id": 7.0,
        }

    def test_too_few_values_raises_rather_than_pairing_them_up_wrongly(self):
        """
        The failure this guards against is silent and bad: one missing line would shift
        every later name onto the wrong number, and a source position would come back as
        a chip identifier without anything going wrong visibly.
        """
        with pytest.raises(ValueError, match="3 values"):
            chandra.parse_pget("4100.4\n4131.8\n", ("x", "y", "chip_id"))

    def test_blank_lines_are_not_values(self):
        assert chandra.parse_pget("\n4100.4\n\n4131.8\n\n", ("x", "y")) == {
            "x": 4100.4,
            "y": 4131.8,
        }


def a_psf_region_file(path, radius_pixels=1.6874055297, near_chip_edge=False):
    """
    What ``psfsize_srcs`` leaves behind, with the columns it really writes.

    The numbers are obsid ``5644``'s, measured on 2026-09-12 at M82 X-2's position with
    ``energy=1.0`` and ``ecf=0.9``: 1.687 sky pixels, which is 0.830 arcseconds.
    """
    columns = [
        fits.Column("SHAPE", "6A", array=np.array(["circle"])),
        fits.Column("X", "1D", array=np.array([4100.3808552606])),
        fits.Column("Y", "1D", array=np.array([4131.8171581055])),
        fits.Column("R", "1D", array=np.array([radius_pixels])),
        fits.Column("THETA", "1D", array=np.array([0.29134389680617])),
        fits.Column("CHIP_ID", "1J", array=np.array([7])),
        fits.Column("NEAR_CHIP_EDGE", "1L", array=np.array([near_chip_edge])),
    ]
    fits.BinTableHDU.from_columns(columns, name="REGION").writeto(path, overwrite=True)
    return str(path)


class TestReadingThePsfSize:
    def test_the_radius_comes_back_in_arcseconds(self, tmp_path):
        """``psfsize_srcs`` writes ``R`` in sky pixels; everything in this module's
        configuration is in arcseconds, so the conversion happens once, here."""
        path = a_psf_region_file(tmp_path / "psf.reg")

        size = chandra.read_psf_size(path)

        assert size.radius_arcsec == pytest.approx(0.830, abs=0.001)

    def test_only_the_radius_is_read_off_the_file(self, tmp_path):
        """
        The file also carries ``NEAR_CHIP_EDGE``, and that column is not trusted -- see
        :class:`TestHowCloseTheSourceIsToAnEdge`. Reading it would put a warning that is
        wrong on every subarray observation into every subarray observation's report.
        """
        path = a_psf_region_file(tmp_path / "psf.reg", near_chip_edge=True)

        assert not hasattr(chandra.read_psf_size(path), "near_chip_edge")

    def test_an_empty_region_file_says_the_position_is_not_on_the_detector(self, tmp_path):
        path = tmp_path / "psf.reg"
        fits.BinTableHDU.from_columns(
            [fits.Column("R", "1D", array=np.array([]))], name="REGION"
        ).writeto(path, overwrite=True)

        with pytest.raises(ValueError, match="no source"):
            chandra.read_psf_size(str(path))


class TestSizingTheExtractionRegions:
    def _a_position(self, **overrides):
        values = dict(x=4100.38, y=4131.82, chip_id=7, chipx=226.3, chipy=496.95, theta_arcmin=0.29)
        values.update(overrides)
        return chandra.SourcePosition(**values)

    def test_the_source_is_a_circle_at_the_position_asked_for(self, tmp_path):
        regions = chandra.chandra_extraction_regions(
            self._a_position(), 0.984, dict(chandra.DEFAULT_CONFIG), continuous_clocking=False
        )

        assert regions.source == "[sky=circle(4100.3800,4131.8200,2.0000)]"

    def test_the_background_is_an_annulus_scaled_from_the_source_radius(self, tmp_path):
        config = dict(chandra.DEFAULT_CONFIG, bkg_inner_factor=1.5, bkg_outer_factor=3.0)

        regions = chandra.chandra_extraction_regions(
            self._a_position(), 0.984, config, continuous_clocking=False
        )

        assert regions.background == "[sky=annulus(4100.3800,4131.8200,3.0000,6.0000)]"
        assert regions.background_inner_arcsec == pytest.approx(1.476)
        assert regions.background_outer_arcsec == pytest.approx(2.952)

    def test_continuous_clocking_gets_strips_in_the_surviving_coordinate(self):
        """
        Continuous Clocking collapses one spatial dimension, so a circle on the sky
        selects a smear rather than a source. The surviving coordinate is ``chipx``, and
        the regions are the direct analogue of XMM Timing's ``RAWX`` strips.
        """
        config = dict(chandra.DEFAULT_CONFIG, cc_source_halfwidth_pix=3, cc_background_pix=(10, 30))

        regions = chandra.chandra_extraction_regions(
            self._a_position(chipx=226.3), 0.984, config, continuous_clocking=True
        )

        assert regions.source == "[chipx=223:229]"
        assert regions.background == "[chipx=196:216,236:256]"

    def test_a_continuous_clocking_background_is_flagged_as_overlapping_the_source(self):
        regions = chandra.chandra_extraction_regions(
            self._a_position(), 0.984, dict(chandra.DEFAULT_CONFIG), continuous_clocking=True
        )

        assert "collapsed" in regions.reason

    def test_a_source_near_an_edge_says_so_in_plain_english(self):
        regions = chandra.chandra_extraction_regions(
            self._a_position(),
            0.984,
            dict(chandra.DEFAULT_CONFIG),
            continuous_clocking=False,
            chip_edge=chandra.ChipEdge(margin_pix=27.2, near_edge=True, window=(449, 576)),
        )

        assert "27 chip pixels" in regions.reason
        assert "dither" in regions.reason

    def test_a_source_with_room_around_it_says_nothing_about_edges(self):
        regions = chandra.chandra_extraction_regions(
            self._a_position(),
            0.984,
            dict(chandra.DEFAULT_CONFIG),
            continuous_clocking=False,
            chip_edge=chandra.ChipEdge(margin_pix=47.9, near_edge=False, window=(449, 576)),
        )

        assert "dither" not in regions.reason

    def test_the_basis_says_where_the_radius_came_from(self):
        regions = chandra.chandra_extraction_regions(
            self._a_position(),
            0.984,
            dict(chandra.DEFAULT_CONFIG),
            continuous_clocking=False,
            basis="psfsize_srcs",
        )

        assert regions.basis == "psfsize_srcs"


class TestWhetherTheReadoutCollapsedADimension:
    """
    The question is asked of the time resolution and not of ``mode``, because a grating
    takes the mode label for itself: an HETG observation read out in Continuous Clocking
    is labelled ``hetg``, and testing ``mode == "cc"`` would give it sky circles over a
    smear.
    """

    def _an_observation(self, basis, mode):
        return chandra.Observation(
            obsid="2749",
            detector="aciss",
            grating="HETG",
            mode=mode,
            time_resolution=chandra.TimeResolution(seconds=2.85e-3, basis=basis, reason=""),
            chips=(7,),
            event_list="evt2.fits",
        )

    def test_a_grating_observation_in_continuous_clocking_is_still_collapsed(self):
        assert self._an_observation("acis_continuous_clocking", "hetg").is_continuous_clocking

    def test_a_timed_exposure_is_not(self):
        assert not self._an_observation("acis_frame_time", "timed").is_continuous_clocking

    def test_neither_is_hrc(self):
        assert not self._an_observation("hrc_trigger_rate", "imaging").is_continuous_clocking


@pytest.fixture
def stub_ciao_tasks(monkeypatch):
    """
    A CIAO whose tasks do what the real ones do to the file system, and nothing else.

    ``dmcoords`` answers through its parameter file, so the stub has ``pget`` print obsid
    ``5644``'s real numbers; ``psfsize_srcs`` writes a region file holding that
    observation's real 1.687-pixel radius.
    """
    calls = []

    def fake_run(name, *, produces, args=(), capture=False, **kwargs):
        calls.append((name, args, kwargs))
        if name == "psfsize_srcs":
            a_psf_region_file(kwargs["outfile"])
        if name == "pget":
            said = "4100.380855260638\n4131.817158105547\n7\n226.298\n496.950\n0.29134\n"
            return SimpleNamespace(stdout=said)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(ciao, "run", fake_run)
    return calls


class TestWorkingOutWhereToExtract:
    def _an_observation(self, tmp_path):
        return chandra.Observation(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "acisf05644N004_evt2.fits.gz"),
            aspect_solution=str(tmp_path / "pcadf05644_000N001_asol1.fits.gz"),
        )

    def test_the_position_asked_for_is_the_position_converted(self, tmp_path, stub_ciao_tasks):
        """
        The sharpest case in the whole module. Obsid ``5644``'s ``OBJECT`` is M82 X-1;
        the published pulsation is M82 X-2's, 4.63 arcseconds away. Reducing at the
        header's target would find nothing and would look like it had worked.
        """
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        chandra.chandra_source_regions(self._an_observation(tmp_path), config, 148.96267, 69.67931)

        dmcoords = [call for call in stub_ciao_tasks if call[0] == "dmcoords"][0]
        assert dmcoords[2]["ra"] == 148.96267
        assert dmcoords[2]["dec"] == 69.67931

    def test_the_aspect_solution_is_passed_when_there_is_one(self, tmp_path, stub_ciao_tasks):
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        chandra.chandra_source_regions(self._an_observation(tmp_path), config, 148.96, 69.68)

        dmcoords = [call for call in stub_ciao_tasks if call[0] == "dmcoords"][0]
        assert "asolfile" in dmcoords[2]

    def test_an_observation_without_one_still_converts(self, tmp_path, stub_ciao_tasks):
        observation = self._an_observation(tmp_path)
        observation = chandra.Observation(**{**observation.__dict__, "aspect_solution": None})
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        chandra.chandra_source_regions(observation, config, 148.96, 69.68)

        dmcoords = [call for call in stub_ciao_tasks if call[0] == "dmcoords"][0]
        assert "asolfile" not in dmcoords[2]

    def test_by_default_the_radius_is_measured_rather_than_assumed(self, tmp_path, stub_ciao_tasks):
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        _, regions = chandra.chandra_source_regions(
            self._an_observation(tmp_path), config, 148.96, 69.68
        )

        assert regions.basis == "psfsize_srcs"
        assert regions.radius_arcsec == pytest.approx(0.830, abs=0.001)

    def test_a_configured_radius_skips_the_measurement_entirely(self, tmp_path, stub_ciao_tasks):
        """Not merely overridden: ``psfsize_srcs`` is a contributed Python script, and a
        run that does not need it should not depend on it being installed and working."""
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path), src_radius_arcsec=2.0)

        _, regions = chandra.chandra_source_regions(
            self._an_observation(tmp_path), config, 148.96, 69.68
        )

        assert regions.radius_arcsec == 2.0
        assert regions.basis == "configured"
        assert [call[0] for call in stub_ciao_tasks] == ["dmcoords", "pget"]

    def test_what_was_done_and_why_is_recorded(self, tmp_path, stub_ciao_tasks):
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))
        directory = tmp_path / "diagnostics"

        with record_step(str(directory), "5644", "source_region") as rec:
            chandra.chandra_source_regions(
                self._an_observation(tmp_path), config, 148.96267, 69.67931, rec=rec
            )

        written = json.loads(next(directory.glob("*source_region*.json")).read_text())
        assert written["values"]["chip_id"] == 7
        assert written["values"]["radius_basis"] == "psfsize_srcs"
        assert written["values"]["source_region"].startswith("[sky=circle(")
        assert "off axis" in written["values"]["reason"]


class TestHowCloseTheSourceIsToAnEdge:
    """
    Our own check, and the reason it is ours is a bug in ``psfsize_srcs``. It bounds a
    subarray's rows at ``NROWS - 1 - edge`` where the bound is
    ``FIRSTROW + NROWS - 1 - edge``, so on any subarray the upper bound falls *below* the
    lower one and every position is flagged. Obsid ``5644`` has ``FIRSTROW = 449`` and
    ``NROWS = 128``, so the tool's window is 481 to 95 -- which nothing can be inside, and
    the real window is 481 to 544.

    Both numbers here were measured against a real CIAO on 2026-09-12: ``5644`` has 48
    pixels of clearance and ``8190`` has 27, and the tool calls both of them near an edge.
    """

    def _an_observation(self, detector="aciss", active_rows=(449, 128)):
        return chandra.Observation(
            obsid="5644",
            detector=detector,
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list="evt2.fits",
            active_rows=active_rows,
        )

    def _at(self, chipx, chipy):
        return chandra.SourcePosition(0.0, 0.0, 7, chipx, chipy, 0.3)

    def test_the_window_is_the_subarray_and_not_the_whole_chip(self):
        edge = chandra.chandra_chip_edge(self._an_observation(), self._at(226.3, 496.95))

        assert edge.window == (449, 576)

    def test_a_source_well_inside_the_subarray_is_not_near_an_edge(self):
        """Obsid ``5644``. ``psfsize_srcs`` says it is; it has 48 pixels of clearance."""
        edge = chandra.chandra_chip_edge(self._an_observation(), self._at(226.3, 496.95))

        assert edge.margin_pix == pytest.approx(47.95, abs=0.01)
        assert edge.near_edge is False

    def test_a_source_inside_the_dither_amplitude_is(self):
        """Obsid ``8190``, whose source sits 27 pixels from the first clocked row."""
        edge = chandra.chandra_chip_edge(self._an_observation(), self._at(654.6, 476.22))

        assert edge.margin_pix == pytest.approx(27.22, abs=0.01)
        assert edge.near_edge is True

    def test_a_full_frame_is_assumed_when_the_header_does_not_say(self):
        edge = chandra.chandra_chip_edge(self._an_observation(active_rows=None), self._at(500, 500))

        assert edge.window == (1, 1024)
        assert edge.near_edge is False

    def test_the_columns_are_checked_as_well_as_the_rows(self):
        edge = chandra.chandra_chip_edge(self._an_observation(), self._at(4.0, 500.0))

        assert edge.margin_pix == pytest.approx(3.0)
        assert edge.near_edge is True

    def test_hrc_is_not_asked(self):
        """The same choice ``psfsize_srcs`` makes. A microchannel plate has no CCD edges,
        and a position near a segment boundary is not the failure mode a chip gap is."""
        edge = chandra.chandra_chip_edge(self._an_observation(detector="hrci"), self._at(100, 100))

        assert edge.margin_pix is None
        assert edge.near_edge is None


class TestTheSubarrayInTheHeader:
    def test_it_is_read_off_a_real_observation_s_keywords(self, tmp_path):
        """Obsid ``5644``'s own, measured 2026-09-12: 128 rows from row 449, which is why
        its frame time is 0.44 s and not 3.2 s."""
        config = an_archive_observation(
            tmp_path,
            obsid="5644",
            names=ACIS_S_5644_KEPT,
            dtf=None,
            INSTRUME="ACIS",
            DETNAM="ACIS-7",
            READMODE="TIMED",
            TIMEDEL=0.44104,
            SIM_Z=-190.14006604987,
            FIRSTROW=449,
            NROWS=128,
        )

        assert chandra.chandra_archive_front_end("5644", config).active_rows == (449, 128)

    def test_an_hrc_observation_has_none(self, tmp_path):
        config = an_archive_observation(tmp_path)

        assert chandra.chandra_archive_front_end("6298", config).active_rows is None


class TestWhereTheFlareCurveIsMeasured:
    """
    Not the background annulus. That ring is a few arcseconds across and holds far too few
    counts to see a flare in; the curve wants as much of the detector as can be had while
    keeping the source out of it.
    """

    def _an_observation(self, detector="aciss", basis="acis_frame_time", mode="timed"):
        return chandra.Observation(
            obsid="5644",
            detector=detector,
            grating="NONE",
            mode=mode,
            time_resolution=chandra.TimeResolution(0.44104, basis, ""),
            chips=(7,),
            event_list="evt2.fits",
        )

    def _regions(self, **overrides):
        values = dict(
            source="[sky=circle(4100.3809,4131.8172,1.6874)]",
            background="[sky=annulus(4100.3809,4131.8172,2.5311,5.0622)]",
            radius_arcsec=0.83,
        )
        values.update(overrides)
        return chandra.ExtractionRegions(**values)

    def _position(self):
        return chandra.SourcePosition(4100.38, 4131.82, 7, 226.3, 496.95, 0.29)

    def test_acis_takes_the_source_chip_with_the_source_cut_out(self):
        found = chandra.chandra_flare_curve_filter(
            self._an_observation(), self._position(), self._regions(), dict(chandra.DEFAULT_CONFIG)
        )

        assert "[ccd_id=7]" in found
        assert "field()-circle(4100.3809,4131.8172,1.6874)" in found

    def test_the_source_is_cut_out_with_region_algebra_and_not_with_exclude(self):
        """
        ``exclude`` is the obvious spelling and the Data Model refuses it: an ``[exclude
        ...]`` alongside any other filter fails with "cannot mix EXCLUDE and FILTER", so
        the chip filter and the cut-out cannot both be written that way. ``field()-shape``
        says the same thing inside one filter, and composes.
        """
        found = chandra.chandra_flare_curve_filter(
            self._an_observation(), self._position(), self._regions(), dict(chandra.DEFAULT_CONFIG)
        )

        assert "exclude" not in found

    def test_acis_is_banded_in_energy(self):
        config = dict(chandra.DEFAULT_CONFIG, flare_energy_ev=(500, 7000))

        found = chandra.chandra_flare_curve_filter(
            self._an_observation(), self._position(), self._regions(), config
        )

        assert "[energy=500:7000]" in found

    def test_hrc_is_not_banded_and_is_not_cut_to_one_segment(self):
        """HRC has no usable energy resolution to band on, and its plate is one piece."""
        config = dict(chandra.DEFAULT_CONFIG, flare_energy_ev=(500, 7000))

        found = chandra.chandra_flare_curve_filter(
            self._an_observation(detector="hrci", basis="hrc_trigger_rate", mode="imaging"),
            self._position(),
            self._regions(),
            config,
        )

        assert "energy" not in found
        assert "ccd_id" not in found

    def test_continuous_clocking_uses_its_own_background_strips(self):
        """A circle on the sky selects a smear in Continuous Clocking, so there is nothing
        to cut out of a field; the strips already are the background."""
        observation = self._an_observation(basis="acis_continuous_clocking", mode="cc")
        regions = self._regions(source="[chipx=223:229]", background="[chipx=196:216,236:256]")

        found = chandra.chandra_flare_curve_filter(
            observation, self._position(), regions, dict(chandra.DEFAULT_CONFIG)
        )

        assert "[chipx=196:216,236:256]" in found
        assert "field()" not in found

    def test_the_bin_width_is_the_last_thing_in_the_filter(self):
        config = dict(chandra.DEFAULT_CONFIG, flare_bin_seconds=200.0)

        found = chandra.chandra_flare_curve_filter(
            self._an_observation(), self._position(), self._regions(), config
        )

        assert found.endswith("[bin time=::200.0]")


def a_chandra_lightcurve(path, rate, exposure=None, cadence=500.0, tstart=0.0):
    """A ``dmextract opt=ltc1`` light curve, with the columns the real tool writes."""
    rate = np.asarray(rate, dtype=float)
    exposure = np.full(rate.size, cadence) if exposure is None else np.asarray(exposure, float)
    time = tstart + cadence * (np.arange(rate.size) + 0.5)
    columns = [
        fits.Column("TIME", "1D", array=time),
        fits.Column("COUNT_RATE", "1D", array=rate),
        fits.Column("COUNTS", "1J", array=np.round(rate * exposure).astype(int)),
        fits.Column("STAT_ERR", "1D", array=np.sqrt(np.abs(rate) / cadence)),
        fits.Column("EXPOSURE", "1D", array=exposure),
    ]
    hdu = fits.BinTableHDU.from_columns(columns, name="LIGHTCURVE")
    hdu.header["TIMEDEL"] = cadence
    hdu.header["TSTART"] = tstart
    hdu.header["TSTOP"] = tstart + cadence * rate.size
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


class TestReadingTheFlareCurve:
    def test_a_bin_with_no_exposure_is_not_a_quiet_bin(self, tmp_path):
        """
        ``dmextract`` emits every bin between ``TSTART`` and ``TSTOP``, including the ones
        that fall in the gaps of the observation's own good times, and writes them with
        zero exposure and zero rate. Obsid ``5644`` opens with three of them: its first
        good time starts 1 729 s after ``TSTART``. Read as rates they are the quietest
        bins in the observation, and they would pull the quiescent level down and the
        threshold with it.
        """
        path = a_chandra_lightcurve(
            tmp_path / "lc.fits", rate=[0.0, 0.0, 2.3, 2.5, 2.4], exposure=[0, 0, 500, 500, 500]
        )

        curve = chandra.read_chandra_lightcurve(path)

        assert np.isnan(curve.rate[:2]).all()
        assert curve.rate[2] == pytest.approx(2.3)

    def test_the_cadence_comes_off_the_header(self, tmp_path):
        path = a_chandra_lightcurve(tmp_path / "lc.fits", rate=[1.0, 1.0], cadence=200.0)

        assert chandra.read_chandra_lightcurve(path).cadence == 200.0


class TestDecidingWhatCountsAsAFlare:
    def test_the_level_is_measured_from_the_quiet_bins_and_not_from_all_of_them(self):
        """
        A handful of flaring bins would drag a plain mean and, worse, inflate the standard
        deviation, so a big flare raises its own threshold above itself and is kept. The
        quiescent level is sigma-clipped first, which is what CIAO's own ``lc_sigma_clip``
        does.
        """
        quiet = np.full(100, 2.0)
        rate = np.concatenate([quiet, np.full(10, 50.0)])

        found = chandra.chandra_flare_threshold(rate, dict(chandra.DEFAULT_CONFIG, flare_sigma=3.0))

        assert found.level == pytest.approx(2.0, abs=0.01)
        assert found.threshold < 10.0

    def test_bins_with_no_exposure_take_no_part(self):
        rate = np.array([np.nan, np.nan, 2.0, 2.1, 1.9, 2.0])

        found = chandra.chandra_flare_threshold(
            rate, dict(chandra.DEFAULT_CONFIG, flare_min_bins=4)
        )

        assert found.n_bins_used == 4
        assert found.level == pytest.approx(2.0, abs=0.1)

    def test_a_perfectly_flat_curve_still_gives_a_threshold_above_itself(self):
        """Zero scatter is not a real light curve, but a short one can round to it, and a
        threshold equal to the level would flag every bin."""
        found = chandra.chandra_flare_threshold(np.full(50, 2.0), dict(chandra.DEFAULT_CONFIG))

        assert found.threshold > 2.0

    def test_too_few_usable_bins_is_no_threshold_rather_than_a_meaningless_one(self):
        found = chandra.chandra_flare_threshold(
            np.array([2.0, 2.1, np.nan]), dict(chandra.DEFAULT_CONFIG)
        )

        assert found.threshold is None
        assert "bins" in found.reason


def an_event_file_with_gti(path, gti, tstart=None, tstop=None):
    """An event list carrying good time intervals, as every Chandra level-2 file does."""
    gti = np.atleast_2d(np.asarray(gti, dtype=float))
    events = fits.BinTableHDU.from_columns(
        [fits.Column("time", "1D", array=np.linspace(gti[0][0], gti[-1][1], 20))], name="EVENTS"
    )
    events.header["TSTART"] = gti[0][0] if tstart is None else tstart
    events.header["TSTOP"] = gti[-1][1] if tstop is None else tstop
    good = fits.BinTableHDU.from_columns(
        [
            fits.Column("START", "1D", array=gti[:, 0]),
            fits.Column("STOP", "1D", array=gti[:, 1]),
        ],
        name="GTI7",
    )
    fits.HDUList([fits.PrimaryHDU(), events, good]).writeto(path, overwrite=True)
    return str(path)


class TestReadingTheObservationsOwnGoodTimes:
    def test_the_gti_block_is_found_whatever_the_chip_number_names_it(self, tmp_path):
        """ACIS names the block after the chip -- ``GTI7`` on obsid ``5644`` -- so looking
        for an extension called ``GTI`` finds nothing at all."""
        path = an_event_file_with_gti(tmp_path / "evt.fits", [[100.0, 200.0], [300.0, 400.0]])

        assert chandra.read_observation_gti(path).tolist() == [[100.0, 200.0], [300.0, 400.0]]

    def test_an_event_list_without_one_says_so_rather_than_inventing_times(self, tmp_path):
        path = tmp_path / "evt.fits"
        hdu = fits.BinTableHDU.from_columns(
            [fits.Column("time", "1D", array=np.arange(5.0))], name="EVENTS"
        )
        fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)

        assert chandra.read_observation_gti(str(path)) is None


class TestScreeningTheFlares:
    def _observation(self, tmp_path, gti=((1000.0, 51000.0),)):
        events = an_event_file_with_gti(tmp_path / "evt2.fits", list(gti), tstart=0.0)
        return chandra.Observation(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=events,
        )

    def test_a_quiet_observation_is_kept_whole(self, tmp_path):
        curve = a_chandra_lightcurve(
            tmp_path / "lc.fits", rate=np.full(100, 2.0) + np.arange(100) % 3 * 0.01, cadence=500.0
        )

        gti = chandra.chandra_flare_gti(
            self._observation(tmp_path), dict(chandra.DEFAULT_CONFIG), curve
        )

        assert gti.tolist() == [[1000.0, 51000.0]]

    def test_a_flare_is_cut_out_of_the_middle(self, tmp_path):
        rate = np.full(100, 2.0)
        rate[50:55] = 40.0
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=rate, cadence=500.0)

        gti = chandra.chandra_flare_gti(
            self._observation(tmp_path), dict(chandra.DEFAULT_CONFIG), curve
        )

        assert len(gti) == 2
        assert gti[0][1] == pytest.approx(25000.0)
        assert gti[1][0] == pytest.approx(27500.0)

    def test_the_result_never_reaches_outside_the_observations_own_good_times(self, tmp_path):
        """
        The light curve runs from ``TSTART``, and the observation's good times start
        later -- 1 729 s later on obsid ``5644``. Writing back the wider interval would
        make the recorded exposure larger than the file's, and the two numbers that are
        supposed to describe the same thing would disagree.
        """
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=np.full(120, 2.0), cadence=500.0)

        gti = chandra.chandra_flare_gti(
            self._observation(tmp_path, gti=((1000.0, 51000.0),)),
            dict(chandra.DEFAULT_CONFIG),
            curve,
        )

        assert gti[0][0] == 1000.0
        assert gti[-1][1] == 51000.0

    def test_a_stretch_the_curve_does_not_cover_is_kept_rather_than_cut(self, tmp_path):
        """Not measured must not mean cut. A real ``dmextract`` curve runs from ``TSTART``
        to ``TSTOP`` and so contains the good times outright, but a short one must not
        quietly take the uncovered exposure with it."""
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=np.full(40, 2.0), cadence=500.0)

        gti = chandra.chandra_flare_gti(
            self._observation(tmp_path), dict(chandra.DEFAULT_CONFIG), curve
        )

        assert gti.tolist() == [[1000.0, 51000.0]]

    def test_a_gap_in_the_observations_good_times_survives_the_screening(self, tmp_path):
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=np.full(100, 2.0), cadence=500.0)

        gti = chandra.chandra_flare_gti(
            self._observation(tmp_path, gti=((1000.0, 20000.0), (30000.0, 49000.0))),
            dict(chandra.DEFAULT_CONFIG),
            curve,
        )

        assert gti.tolist() == [[1000.0, 20000.0], [30000.0, 49000.0]]

    def test_a_cut_that_would_take_too_much_is_refused_and_the_observation_kept(self, tmp_path):
        """
        Matteo's rule for this step: the default must be gentle and must never throw away
        a good observation. A screening that wants half the exposure is far more likely to
        be a variable source leaking into the background region than a two-hour flare, so
        it is reported and not applied.
        """
        rate = np.full(100, 2.0)
        rate[:50] = 40.0
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=rate, cadence=500.0)

        gti = chandra.chandra_flare_gti(
            self._observation(tmp_path), dict(chandra.DEFAULT_CONFIG), curve
        )

        assert gti.tolist() == [[1000.0, 51000.0]]

    def test_what_was_cut_and_why_is_recorded(self, tmp_path):
        rate = np.full(100, 2.0)
        rate[50:55] = 40.0
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=rate, cadence=500.0)
        directory = tmp_path / "diagnostics"

        with record_step(str(directory), "5644", "flare_filtering") as rec:
            chandra.chandra_flare_gti(
                self._observation(tmp_path), dict(chandra.DEFAULT_CONFIG), curve, rec=rec
            )

        written = json.loads(next(directory.glob("*flare_filtering*.json")).read_text())
        assert written["values"]["applied"] is True
        assert written["values"]["exposure_after"] < written["values"]["exposure_before"]
        assert written["values"]["threshold"] > 2.0

    def test_a_curve_too_short_to_measure_leaves_the_observation_alone(self, tmp_path):
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=[2.0, 2.1], cadence=500.0)
        directory = tmp_path / "diagnostics"

        with record_step(str(directory), "5644", "flare_filtering") as rec:
            gti = chandra.chandra_flare_gti(
                self._observation(tmp_path), dict(chandra.DEFAULT_CONFIG), curve, rec=rec
            )

        assert gti.tolist() == [[1000.0, 51000.0]]
        written = json.loads(next(directory.glob("*flare_filtering*.json")).read_text())
        assert written["values"]["applied"] is False


class TestWritingTheCleanedEventList:
    def _observation(self, tmp_path):
        return chandra.Observation(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "acisf05644N004_evt2.fits.gz"),
        )

    def test_the_good_times_reach_dmcopy_as_a_file_and_not_as_a_time_filter(
        self, tmp_path, stub_ciao_tasks
    ):
        """
        ``[@file]`` rather than ``[time=a:b,c:d]``. A written table has no length limit, and
        ``dmcopy`` intersects it with the intervals the file already carries instead of
        replacing them -- verified against a real ``dmcopy`` on obsid ``5644``.
        """
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))
        gti = np.array([[1000.0, 20000.0], [25000.0, 51000.0]])

        chandra.chandra_clean_event_list(self._observation(tmp_path), config, gti)

        infile = [call for call in stub_ciao_tasks if call[0] == "dmcopy"][0][2]["infile"]
        assert infile.endswith(
            "[@" + str(tmp_path / "5644/event_cl/chandra05644_aciss_timed_flare.gti") + "]"
        )

    def test_the_intervals_written_are_the_intervals_computed(self, tmp_path, stub_ciao_tasks):
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))
        gti = np.array([[1000.0, 20000.0], [25000.0, 51000.0]])

        chandra.chandra_clean_event_list(self._observation(tmp_path), config, gti)

        written = fits.open(tmp_path / "5644/event_cl/chandra05644_aciss_timed_flare.gti")[1].data
        assert written["START"].tolist() == [1000.0, 25000.0]
        assert written["STOP"].tolist() == [20000.0, 51000.0]

    def test_the_output_names_the_observation_it_belongs_to(self, tmp_path, stub_ciao_tasks):
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        found = chandra.chandra_clean_event_list(
            self._observation(tmp_path), config, np.array([[0.0, 1.0]])
        )

        assert os.path.basename(found) == "chandra05644_aciss_timed_cl.evt"


def a_barycentred_file(path, timesys="TDB"):
    """What ``axbary`` leaves behind: the same events, on barycentric time."""
    hdu = fits.BinTableHDU.from_columns(
        [fits.Column("time", "1D", array=np.arange(10.0))], name="EVENTS"
    )
    hdu.header["TIMESYS"] = timesys
    hdu.header["TIMEREF"] = "SOLARSYSTEM"
    hdu.header["PLEPHEM"] = "JPL-DE405"
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


@pytest.fixture
def stub_axbary(monkeypatch):
    """A CIAO whose ``axbary`` writes a barycentred file and whose ``dmcopy`` writes a cut."""
    calls = []

    def fake_run(name, *, produces, args=(), capture=False, **kwargs):
        calls.append((name, kwargs))
        if name == "axbary":
            a_barycentred_file(kwargs["outfile"])
        if name == "dmcopy":
            os.makedirs(os.path.dirname(kwargs["outfile"]), exist_ok=True)
            a_barycentred_file(kwargs["outfile"])
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(ciao, "run", fake_run)
    return calls


class TestBarycentringWithAxbary:
    def _observation(self, tmp_path, orbit="primary/orbitf240581100N001_eph1.fits.gz"):
        return chandra.Observation(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "chandra05644_aciss_timed_cl.evt"),
            orbit_ephemeris=None if orbit is None else str(tmp_path / orbit),
        )

    def _config(self, tmp_path):
        return dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

    def test_the_correction_is_made_to_the_position_asked_for(self, tmp_path, stub_axbary):
        """
        The sharpest case in the pipeline. Obsid ``5644``'s header target is M82 X-1 and
        the published pulsation is M82 X-2's, 4.63 arcseconds away; ``axbary`` told nothing
        would correct to the wrong one of the two.
        """
        chandra.chandra_barycenter(
            self._observation(tmp_path),
            self._config(tmp_path),
            str(tmp_path / "cl.evt"),
            148.96267,
            69.67931,
        )

        axbary = [call for call in stub_axbary if call[0] == "axbary"][0][1]
        assert (axbary["ra"], axbary["dec"]) == (148.96267, 69.67931)

    def test_without_a_position_the_header_is_left_to_speak(self, tmp_path, stub_axbary):
        chandra.chandra_barycenter(
            self._observation(tmp_path), self._config(tmp_path), str(tmp_path / "cl.evt")
        )

        axbary = [call for call in stub_axbary if call[0] == "axbary"][0][1]
        assert "ra" not in axbary and "dec" not in axbary

    def test_the_reference_frame_is_the_only_one_that_is_de405(self, tmp_path, stub_axbary):
        chandra.chandra_barycenter(
            self._observation(tmp_path), self._config(tmp_path), str(tmp_path / "cl.evt")
        )

        assert [c for c in stub_axbary if c[0] == "axbary"][0][1]["refframe"] == "ICRS"

    def test_the_original_is_not_edited_in_place(self, tmp_path, stub_axbary):
        found = chandra.chandra_barycenter(
            self._observation(tmp_path), self._config(tmp_path), str(tmp_path / "cl.evt")
        )

        assert found == str(tmp_path / "cl_bary.evt")

    def test_an_observation_with_no_orbit_ephemeris_records_why_and_does_not_raise(
        self, tmp_path, stub_axbary
    ):
        directory = tmp_path / "diagnostics"

        with record_step(str(directory), "5644", "barycenter") as rec:
            found = chandra.chandra_barycenter(
                self._observation(tmp_path, orbit=None),
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                rec=rec,
            )

        assert found is None
        written = json.loads(next(directory.glob("*barycenter*.json")).read_text())
        assert written["values"]["barycentered"] is False
        assert "orbit ephemeris" in written["values"]["reason"]

    def test_a_file_that_did_not_come_back_on_barycentric_time_raises(self, tmp_path, monkeypatch):
        """A zero return code from ``axbary`` is not evidence that anything was corrected,
        and a file that looks corrected and is not would poison every period it produced."""

        def fake_run(name, *, produces, args=(), capture=False, **kwargs):
            if name == "axbary":
                a_barycentred_file(kwargs["outfile"], timesys="TT")
            return SimpleNamespace(stdout="")

        monkeypatch.setattr(ciao, "run", fake_run)

        with pytest.raises(ValueError, match="TT time rather than TDB"):
            chandra.chandra_barycenter(
                self._observation(tmp_path), self._config(tmp_path), str(tmp_path / "cl.evt")
            )

    def test_the_ephemeris_and_what_it_costs_are_recorded(self, tmp_path, stub_axbary):
        """The DE405 break is never invisible: it is in every observation's record, with
        the 0.377 microseconds it costs against the pipeline's DE430."""
        directory = tmp_path / "diagnostics"

        with record_step(str(directory), "5644", "barycenter") as rec:
            chandra.chandra_barycenter(
                self._observation(tmp_path),
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                148.96267,
                69.67931,
                rec=rec,
            )

        values = json.loads(next(directory.glob("*barycenter*.json")).read_text())["values"]
        assert values["ephemeris"] == "JPL-DE405"
        assert values["refframe"] == "ICRS"
        assert values["de405_minus_de430_us"] == 0.377
        assert values["position_from"] == "argument"

    def test_the_source_is_cut_out_of_the_corrected_list_and_not_corrected_twice(
        self, tmp_path, stub_axbary
    ):
        config = self._config(tmp_path)
        regions = chandra.ExtractionRegions(
            source="[sky=circle(4100.3809,4131.8172,1.6874)]", background="[sky=annulus(1,2,3,4)]"
        )

        found = chandra.chandra_barycentered_source_events(
            self._observation(tmp_path), config, str(tmp_path / "cl_bary.evt"), regions
        )

        dmcopy = [call for call in stub_axbary if call[0] == "dmcopy"][0][1]
        assert dmcopy["infile"] == str(tmp_path / "cl_bary.evt") + regions.source
        assert os.path.basename(found) == "chandra05644_aciss_timed_src_bary.evt"

    def test_with_nothing_corrected_there_is_nothing_to_cut(self, tmp_path, stub_axbary):
        regions = chandra.ExtractionRegions(source="[sky=circle(1,2,3)]", background="")

        found = chandra.chandra_barycentered_source_events(
            self._observation(tmp_path), self._config(tmp_path), None, regions
        )

        assert found is None
        assert stub_axbary == []
