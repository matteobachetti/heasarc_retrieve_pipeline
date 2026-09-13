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

import dataclasses
import json
import os
import pathlib
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

    def test_an_hrc_detnam_naming_neither_detector_is_an_error(self):
        with pytest.raises(ValueError, match="HRC-X"):
            chandra.chandra_detector({"INSTRUME": "HRC", "DETNAM": "HRC-X"})


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


# ``1411``: HRC-I on M82, taken in two parts 84 days apart under one obsid. Listed from the
# S3 mirror on 2026-09-13, with every time read off the real file headers the same day.
# The archive merges the parts into one level-2 event list and ships every other product
# once per part, with the part number -- ``000``, ``002`` -- in the name.
_1411_PART_000 = (57471875.357874, 57509598.434235)
_1411_PART_002 = (64767109.146926, 64787079.222651)
HRC_I_1411_TIMES = {
    "primary/hrcf01411N006_evt2.fits.gz": (57471875.357874, 64787079.222651),
    "primary/hrcf01411_000N006_dtf1.fits.gz": _1411_PART_000,
    "primary/hrcf01411_000N006_fov1.fits.gz": _1411_PART_000,
    "primary/hrcf01411_002N006_dtf1.fits.gz": _1411_PART_002,
    "primary/hrcf01411_002N006_fov1.fits.gz": _1411_PART_002,
    "primary/orbitf057024064N002_eph1.fits.gz": (57024064.184, 58838464.184),
    "primary/orbitf064281664N002_eph1.fits.gz": (64281664.184, 66096064.184),
    "primary/pcadf01411_000N001_asol1.fits.gz": (57472330.71414, 57508816.871706, {"OBI_NUM": 0}),
    "primary/pcadf01411_002N001_asol1.fits.gz": (64768580.278229, 64786525.210131, {"OBI_NUM": 2}),
    "secondary/hrcf01411_000N006_bpix1.fits.gz": _1411_PART_000,
    "secondary/hrcf01411_000N006_msk1.fits.gz": _1411_PART_000,
    "secondary/hrcf01411_000N006_std_flt1.fits.gz": _1411_PART_000,
    "secondary/hrcf01411_002N006_bpix1.fits.gz": _1411_PART_002,
    "secondary/hrcf01411_002N006_msk1.fits.gz": _1411_PART_002,
    "secondary/hrcf01411_002N006_std_flt1.fits.gz": _1411_PART_002,
}

# ``433``: ACIS-S with HETG, in three parts. The names are real, listed on 2026-09-13; the
# times are invented, in the right order, because the observation was not downloaded. It is
# the case that breaks pairing by name: the parts are numbered 001, 003 and 004, and its
# five aspect solutions carry a start time where the part number would be.
_433_PART_001 = (71323000.0, 71450000.0)
_433_PART_003 = (76842000.0, 76900000.0)
_433_PART_004 = (83242000.0, 83300000.0)
ACIS_433_TIMES = {
    "primary/acisf00433N008_evt2.fits.gz": (71323000.0, 83300000.0),
    "primary/acisf00433_001N005_bpix1.fits.gz": _433_PART_001,
    "primary/acisf00433_001N005_fov1.fits.gz": _433_PART_001,
    "primary/acisf00433_003N006_bpix1.fits.gz": _433_PART_003,
    "primary/acisf00433_003N006_fov1.fits.gz": _433_PART_003,
    "primary/acisf00433_004N004_bpix1.fits.gz": _433_PART_004,
    "primary/acisf00433_004N004_fov1.fits.gz": _433_PART_004,
    "primary/orbitf070977900N001_eph1.fits.gz": (70977900.0, 72792300.0),
    "primary/orbitf076766700N001_eph1.fits.gz": (76766700.0, 78581100.0),
    "primary/orbitf082987500N001_eph1.fits.gz": (82987500.0, 84801900.0),
    "primary/pcadf071323369N004_asol1.fits.gz": (71323369.0, 71391700.0, {"OBI_NUM": 1}),
    "primary/pcadf071391777N004_asol1.fits.gz": (71391777.0, 71419600.0, {"OBI_NUM": 1}),
    "primary/pcadf071419624N004_asol1.fits.gz": (71419624.0, 71449000.0, {"OBI_NUM": 1}),
    "primary/pcadf076842088N006_asol1.fits.gz": (76842088.0, 76899000.0, {"OBI_NUM": 3}),
    "primary/pcadf083242295N004_asol1.fits.gz": (83242295.0, 83299000.0, {"OBI_NUM": 4}),
    "secondary/acisf00433_001N005_flt1.fits.gz": _433_PART_001,
    "secondary/acisf00433_001N005_msk1.fits.gz": _433_PART_001,
    "secondary/acisf00433_003N006_flt1.fits.gz": _433_PART_003,
    "secondary/acisf00433_003N006_msk1.fits.gz": _433_PART_003,
    "secondary/acisf00433_004N004_flt1.fits.gz": _433_PART_004,
    "secondary/acisf00433_004N004_msk1.fits.gz": _433_PART_004,
}


def a_timed_observation(tmp_path, obsid, times):
    """
    A download whose every file carries the ``TSTART`` and ``TSTOP`` a real one would.

    ``times`` maps each archive name to ``(tstart, tstop)``, or to ``(tstart, tstop,
    header)`` for extra keywords. Pairing the parts of an observation by time reads these,
    so the empty placeholders of :func:`a_downloaded_observation` will not do.
    """
    config = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}
    for name, spec in times.items():
        tstart, tstop, extra = (tuple(spec) + ({},))[:3]
        path = tmp_path / chandra.chandra_obsid(obsid) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if "_evt2." in name:
            an_event_file(path, TSTART=tstart, TSTOP=tstop, **extra)
            continue
        hdu = fits.BinTableHDU.from_columns([fits.Column("TIME", "1D", array=[tstart])])
        for key, value in dict(TSTART=tstart, TSTOP=tstop, **extra).items():
            hdu.header[key] = value
        fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return config


def _names(paths):
    return [os.path.basename(path) for path in paths]


class TestSplittingAnObservationIntoItsParts:
    """
    Some Chandra observations were taken in several separate pointings under one obsid.
    Chandra calls each an OBI; this module calls it a *part*. The archive merges the parts'
    events into one level-2 list and ships the rest -- dead time, aspect, orbit, bad
    pixels, mask, good times -- once per part, and each part has to be reduced with its
    own.
    """

    def parts(self, tmp_path, obsid, times):
        return chandra.chandra_observation_parts(obsid, a_timed_observation(tmp_path, obsid, times))

    def test_an_ordinary_observation_is_one_part_and_opens_nothing_new(self, tmp_path):
        """
        No second code path: a single-pointing observation is one part. Its companions are
        empty placeholders here, so the test also proves that none of them is opened --
        the part's time range is the event list's own.
        """
        config = an_archive_observation(tmp_path, "6298", TSTART=235397500.0, TSTOP=235420000.0)

        (part,) = chandra.chandra_observation_parts("6298", config)

        assert part.number == 0
        assert (part.tstart, part.tstop) == (235397500.0, 235420000.0)
        assert os.path.basename(part.dead_time_file) == "hrcf06298_000N006_dtf1.fits"
        assert _names(part.aspect_solutions) == ["pcadf06298_000N001_asol1.fits.gz"]
        assert os.path.basename(part.orbit_ephemeris) == "orbitf235397100N001_eph1.fits.gz"
        assert os.path.basename(part.bad_pixel_file) == "hrcf06298_000N006_bpix1.fits.gz"
        assert os.path.basename(part.mask_file) == "hrcf06298_000N006_msk1.fits.gz"
        assert os.path.basename(part.gti_file) == "hrcf06298_000N006_std_flt1.fits.gz"

    def test_obsid_1411_is_two_parts_each_with_its_own_files(self, tmp_path):
        first, second = self.parts(tmp_path, "1411", HRC_I_1411_TIMES)

        assert (first.number, second.number) == (0, 2)
        assert os.path.basename(first.dead_time_file) == "hrcf01411_000N006_dtf1.fits.gz"
        assert os.path.basename(second.dead_time_file) == "hrcf01411_002N006_dtf1.fits.gz"
        assert _names(second.aspect_solutions) == ["pcadf01411_002N001_asol1.fits.gz"]
        assert os.path.basename(second.bad_pixel_file) == "hrcf01411_002N006_bpix1.fits.gz"
        assert os.path.basename(second.mask_file) == "hrcf01411_002N006_msk1.fits.gz"
        assert os.path.basename(second.gti_file) == "hrcf01411_002N006_std_flt1.fits.gz"

    def test_an_orbit_file_carries_no_part_number_and_is_paired_by_time(self, tmp_path):
        first, second = self.parts(tmp_path, "1411", HRC_I_1411_TIMES)

        assert os.path.basename(first.orbit_ephemeris) == "orbitf057024064N002_eph1.fits.gz"
        assert os.path.basename(second.orbit_ephemeris) == "orbitf064281664N002_eph1.fits.gz"

    def test_each_part_spans_its_own_files_and_the_gap_is_84_days(self, tmp_path):
        first, second = self.parts(tmp_path, "1411", HRC_I_1411_TIMES)

        assert (first.tstart, first.tstop) == pytest.approx(_1411_PART_000)
        assert (second.tstart, second.tstop) == pytest.approx(_1411_PART_002)
        assert (second.tstart - first.tstop) / 86400 == pytest.approx(84.0, abs=0.1)

    def test_part_numbers_can_skip_and_aspect_solutions_are_paired_by_time(self, tmp_path):
        """Obsid 433: parts 001, 003 and 004, and five aspect solutions named by time."""
        first, second, third = self.parts(tmp_path, "433", ACIS_433_TIMES)

        assert (first.number, second.number, third.number) == (1, 3, 4)
        assert _names(first.aspect_solutions) == [
            "pcadf071323369N004_asol1.fits.gz",
            "pcadf071391777N004_asol1.fits.gz",
            "pcadf071419624N004_asol1.fits.gz",
        ]
        assert _names(second.aspect_solutions) == ["pcadf076842088N006_asol1.fits.gz"]
        assert _names(third.aspect_solutions) == ["pcadf083242295N004_asol1.fits.gz"]
        assert os.path.basename(third.orbit_ephemeris) == "orbitf082987500N001_eph1.fits.gz"

    def test_an_aspect_solution_whose_header_names_another_part_is_an_error(self, tmp_path):
        """The time says one part and ``OBI_NUM`` says another: neither is believed."""
        times = dict(ACIS_433_TIMES)
        times["primary/pcadf076842088N006_asol1.fits.gz"] = (76842088.0, 76899000.0, {"OBI_NUM": 4})

        with pytest.raises(ValueError, match="OBI_NUM"):
            self.parts(tmp_path, "433", times)

    def test_parts_may_come_from_different_processing_versions(self, tmp_path):
        """Obsid 108 really ships ``_000N006`` beside ``_001N005``. That is not a conflict."""
        times = {
            name.replace("_002N006_", "_002N005_"): spec for name, spec in HRC_I_1411_TIMES.items()
        }

        first, second = self.parts(tmp_path, "1411", times)

        assert os.path.basename(second.dead_time_file) == "hrcf01411_002N005_dtf1.fits.gz"

    def test_two_versions_of_one_part_are_an_error(self, tmp_path):
        times = dict(HRC_I_1411_TIMES)
        times["primary/hrcf01411_000N005_dtf1.fits.gz"] = _1411_PART_000

        with pytest.raises(ValueError, match="hrcf01411_000N005_dtf1.*hrcf01411_000N006_dtf1"):
            self.parts(tmp_path, "1411", times)

    def test_two_versions_of_one_time_named_aspect_solution_are_an_error(self, tmp_path):
        times = dict(ACIS_433_TIMES)
        times["primary/pcadf071323369N005_asol1.fits.gz"] = (71323369.0, 71391700.0, {"OBI_NUM": 1})

        with pytest.raises(ValueError, match="pcadf071323369N00"):
            self.parts(tmp_path, "433", times)

    def test_a_part_no_orbit_file_covers_has_none(self, tmp_path):
        """Recorded rather than raised, as it is for an ordinary observation."""
        times = dict(HRC_I_1411_TIMES)
        del times["primary/orbitf064281664N002_eph1.fits.gz"]

        first, second = self.parts(tmp_path, "1411", times)

        assert first.orbit_ephemeris is not None
        assert second.orbit_ephemeris is None

    def test_a_part_two_orbit_files_cover_is_an_error(self, tmp_path):
        """``axbary`` takes one orbit file, so choosing between two would be a guess."""
        times = dict(HRC_I_1411_TIMES)
        times["primary/orbitf057400000N002_eph1.fits.gz"] = (57400000.0, 59214400.0)

        with pytest.raises(ValueError, match="orbit"):
            self.parts(tmp_path, "1411", times)

    def test_the_reprocessing_route_s_unnumbered_products_belong_to_the_one_part(self, tmp_path):
        """``chandra_repro`` names its bad pixels and good times without a part number."""
        config = a_reprocessed_observation(tmp_path)

        (part,) = chandra.chandra_observation_parts("5644", config)

        assert os.path.basename(part.bad_pixel_file) == "acisf05644_repro_bpix1.fits"
        assert os.path.basename(part.gti_file) == "acisf05644_repro_flt2.fits"

    def test_the_front_end_records_every_part(self, tmp_path):
        config = an_archive_observation(tmp_path, "6298", TSTART=1.0e8, TSTOP=1.1e8)
        directory = tmp_path / "diag"

        with record_step(str(directory), "6298", "chandra_front_end") as rec:
            found = chandra.chandra_archive_front_end("6298", config, rec=rec)

        values = json.loads((directory / "chandra_front_end.json").read_text())["values"]
        assert len(found.parts) == 1
        assert values["n_parts"] == 1
        assert values["parts"][0]["number"] == 0
        assert values["parts"][0]["tstart"] == pytest.approx(1.0e8)
        assert values["parts"][0]["dead_time_file"] == "hrcf06298_000N006_dtf1.fits"


@pytest.fixture
def stub_dmcopy_by_time(monkeypatch):
    """A CIAO whose ``dmcopy`` copies the file named before the filter, header and all."""
    calls = []

    def fake_run(name, *, produces, args=(), capture=False, **kwargs):
        calls.append((name, kwargs))
        if name == "dmcopy":
            with fits.open(kwargs["infile"].split("[")[0]) as hdulist:
                hdulist.writeto(kwargs["outfile"], overwrite=True)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(ciao, "run", fake_run)
    return calls


class TestTakingOnePartAsItsOwnObservation:
    """
    Matteo's ruling of 2026-09-13: each part of an observation is reduced as an observation
    of its own. ``specextract`` takes one mask file per observation, and ``splitobs`` -- the
    CXC's own answer -- hands each part to ``chandra_repro`` as if it were separate.
    """

    def _380(self, tmp_path):
        config = a_timed_observation(tmp_path, "380", ACIS_380_TIMES)
        return config, chandra.chandra_archive_front_end("380", config)

    def test_the_front_end_reads_an_observation_in_parts_without_choosing_a_file(self, tmp_path):
        _, observation = self._380(tmp_path)

        assert len(observation.parts) == 2
        assert observation.part is None
        assert observation.bad_pixel_file is None
        assert observation.mask_file is None
        assert observation.gti_file is None
        assert observation.orbit_ephemeris is None

    def test_the_front_end_warns_that_the_observation_is_in_parts(self, tmp_path):
        """
        Loudly, and in words: how many parts, when each was taken, how far apart, and
        what its products are called. ``380``'s parts are 35.6 days apart.
        """
        config = a_timed_observation(tmp_path, "380", ACIS_380_TIMES)
        directory = tmp_path / "diag"

        with record_step(str(directory), "380", "chandra_front_end") as rec:
            chandra.chandra_archive_front_end("380", config, rec=rec)

        values = json.loads((directory / "chandra_front_end.json").read_text())["values"]
        (warning,) = values["warnings"]
        assert "2 parts" in warning
        assert "35.6 days apart" in warning
        assert "2000-05-07" in warning and "2000-06-12" in warning
        assert "_obi001" in warning and "_obi002" in warning

    def test_an_ordinary_observation_warns_of_nothing(self, tmp_path):
        config = an_archive_observation(tmp_path, "6298")
        directory = tmp_path / "diag"

        with record_step(str(directory), "6298", "chandra_front_end") as rec:
            chandra.chandra_archive_front_end("6298", config, rec=rec)

        values = json.loads((directory / "chandra_front_end.json").read_text())["values"]
        assert values["warnings"] == []

    def test_a_part_is_named_for_its_number(self, tmp_path, stub_dmcopy_by_time):
        config, observation = self._380(tmp_path)

        one = chandra.chandra_part_observation(observation, observation.parts[1], config)

        assert one.stem == observation.stem + "_obi002"
        assert one.part == observation.parts[1]
        assert one.parts == (observation.parts[1],)

    def test_a_part_s_events_are_cut_from_the_merged_list_by_its_own_times(
        self, tmp_path, stub_dmcopy_by_time
    ):
        config, observation = self._380(tmp_path)

        one = chandra.chandra_part_observation(observation, observation.parts[1], config)

        (dmcopy,) = [kwargs for name, kwargs in stub_dmcopy_by_time if name == "dmcopy"]
        assert dmcopy["infile"] == (
            f"{observation.event_list}[time=77203909.743536:77206498.381131]"
        )
        assert one.event_list == os.path.join(
            chandra.chandra_pipeline_output_path("380", config), f"{one.stem}_evt2.fits"
        )

    def test_a_part_s_event_list_starts_and_stops_when_the_part_does(
        self, tmp_path, stub_dmcopy_by_time
    ):
        """
        ``dmcopy``'s time filter trims the good-time blocks, ``EXPOSURE`` and the events,
        and leaves ``TSTART`` and ``TSTOP`` at the merged list's. ``dmextract`` then bins the
        flare curve over the header's range: on ``380``'s second part, 15 447 bins of 200 s,
        7 of them with any exposure. Measured with CIAO on 2026-09-13.
        """
        config, observation = self._380(tmp_path)

        one = chandra.chandra_part_observation(observation, observation.parts[1], config)

        header = fits.getheader(one.event_list, 1)
        assert (header["TSTART"], header["TSTOP"]) == (77203909.743536, 77206498.381131)

    def test_a_part_s_event_list_says_where_that_part_pointed(self, tmp_path, stub_dmcopy_by_time):
        """
        The merged list has no ``RA_PNT``, ``DEC_PNT`` or ``ROLL_PNT``: ``380``'s parts were
        rolled 251.6 and 282.8 degrees. Every file of a part has its own, and without them
        ``psfsize_srcs`` stops with "Input keyword list is missing" -- measured on the real
        ``380`` through ``core`` on 2026-09-13, on both parts.
        """
        pointing = {
            "RA_PNT": 148.8495527153,
            "DEC_PNT": 69.628018652338,
            "ROLL_PNT": 282.83742905797,
        }
        times = dict(ACIS_380_TIMES)
        for name in (
            "secondary/acisf00380_002N006_flt1.fits.gz",
            "secondary/acisf00380_002N006_msk1.fits.gz",
        ):
            times[name] = (*_380_PART_002[:2], dict(_380_CONFIGURATION, **pointing))
        config = a_timed_observation(tmp_path, "380", times)
        observation = chandra.chandra_archive_front_end("380", config)

        one = chandra.chandra_part_observation(observation, observation.parts[1], config)

        header = fits.getheader(one.event_list, 1)
        assert {key: header.get(key) for key in pointing} == pointing

    def test_a_part_s_event_list_names_its_part(self, tmp_path, stub_dmcopy_by_time):
        """
        CIAO keeps a list of the obsids taken in parts, and ``380`` is on it: ``specextract``
        refuses an event list of one without ``OBI_NUM`` -- "For multi-OBI datasets like
        380 the obi argument must be set" -- and the merged list has none. Measured on the
        real ``380`` through ``core`` on 2026-09-13, on both parts.
        """
        config, observation = self._380(tmp_path)

        one = chandra.chandra_part_observation(observation, observation.parts[1], config)

        assert fits.getheader(one.event_list, 1)["OBI_NUM"] == 2

    def test_a_part_carries_its_own_companion_files(self, tmp_path, stub_dmcopy_by_time):
        config, observation = self._380(tmp_path)

        one = chandra.chandra_part_observation(observation, observation.parts[1], config)

        assert _names([one.bad_pixel_file, one.mask_file, one.gti_file, one.orbit_ephemeris]) == [
            "acisf00380_002N006_bpix1.fits.gz",
            "acisf00380_002N006_msk1.fits.gz",
            "acisf00380_002N006_flt1.fits.gz",
            "orbitf077025900N001_eph1.fits.gz",
        ]
        assert _names(one.aspect_solutions) == ["pcadf00380_002N001_asol1.fits.gz"]

    def test_an_acis_part_reads_out_at_its_frame_time(self, tmp_path, stub_dmcopy_by_time):
        config, observation = self._380(tmp_path)

        one = chandra.chandra_part_observation(observation, observation.parts[0], config)

        assert one.time_resolution.seconds == 3.24104
        assert one.mode == observation.mode

    def test_an_hrc_part_has_its_own_time_resolution_and_not_the_combined_one(
        self, tmp_path, stub_dmcopy_by_time
    ):
        """``1411``'s second part: 5.18 ms, where the two parts together answer 4.93."""
        a_dead_time_file(tmp_path / "d000.fits", 425, 95)
        a_dead_time_file(tmp_path / "d002.fits", 396, 93)
        parts = (
            chandra.ObservationPart(0, *_1411_PART_000, dead_time_file=str(tmp_path / "d000.fits")),
            chandra.ObservationPart(2, *_1411_PART_002, dead_time_file=str(tmp_path / "d002.fits")),
        )
        observation = chandra.Observation(
            obsid="1411",
            detector="hrci",
            grating="NONE",
            mode="imaging",
            time_resolution=chandra.TimeResolution(4.934e-3, "hrc_trigger_rate", ""),
            chips=(0,),
            event_list=an_event_file(
                tmp_path / "hrcf01411N006_evt2.fits", TSTART=5.7e7, TSTOP=6.5e7
            ),
            parts=parts,
        )
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        one = chandra.chandra_part_observation(observation, parts[1], config)

        assert one.time_resolution.seconds == pytest.approx(2.05 / 396)
        assert one.time_resolution.fast_timing is False
        assert one.mode == "imaging"
        assert one.dead_time_file == str(tmp_path / "d002.fits")


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


# ``1411``'s two dead-time files, measured on 2026-09-13: medians of 425 triggers and 95
# telemetered events per 2.05 s sample in part 000, 396 and 93 in part 002. Both heavily
# vetoed, at slightly different rates. The exposures are each part's good time.
_1411_DTF_000 = chandra.DeadTimeFactors(95 / 425, 425 / 2.05, 2.05, 15233, 17737)
_1411_DTF_002 = chandra.DeadTimeFactors(93 / 396, 396 / 2.05, 2.05, 7405, 8690)
_1411_GOOD_TIME = (36275.00755862892, 17722.250644013286)

# ``380``: M82 on ACIS-I, in two parts about 36 days apart. Names, times and keywords read
# off the real files on 2026-09-13. Each part's good-time, mask and field-of-view files
# repeat the readout configuration, which is what lets the parts be compared.
_380_CONFIGURATION = {
    "READMODE": "TIMED",
    "DATAMODE": "VFAINT",
    "TIMEDEL": 3.24104,
    "DETNAM": "ACIS-012367",
}
_380_PART_001 = (74117285.617538, 74123990.655284, _380_CONFIGURATION)
_380_PART_002 = (77203909.743536, 77206498.381131, _380_CONFIGURATION)
ACIS_380_TIMES = {
    "primary/acisf00380N007_evt2.fits.gz": (
        74117285.617538,
        77206498.381131,
        dict(_380_CONFIGURATION, INSTRUME="ACIS", SIM_Z=-233.58743446083),
    ),
    "primary/acisf00380_001N005_bpix1.fits.gz": _380_PART_001,
    "primary/acisf00380_001N005_fov1.fits.gz": _380_PART_001,
    "primary/acisf00380_002N006_bpix1.fits.gz": _380_PART_002,
    "primary/acisf00380_002N006_fov1.fits.gz": _380_PART_002,
    "primary/orbitf073742700N001_eph1.fits.gz": (73742700.184, 75427200.184),
    "primary/orbitf077025900N001_eph1.fits.gz": (77025900.184, 78710400.184),
    "primary/pcadf00380_001N001_asol1.fits.gz": (74117601.31755, 74122686.855236, {"OBI_NUM": 1}),
    "primary/pcadf00380_002N001_asol1.fits.gz": (77204795.087318, 77206184.21862, {"OBI_NUM": 2}),
    "secondary/acisf00380_001N005_flt1.fits.gz": _380_PART_001,
    "secondary/acisf00380_001N005_msk1.fits.gz": _380_PART_001,
    "secondary/acisf00380_002N006_flt1.fits.gz": _380_PART_002,
    "secondary/acisf00380_002N006_msk1.fits.gz": _380_PART_002,
}

_HRC_HEADER = {"INSTRUME": "HRC", "DETNAM": "HRC-I", "TIMEDEL": 1.5625e-05}


class TestTimeResolutionOverSeveralParts:
    """
    A multi-part observation gets one time resolution, one mode label and one file stem,
    so its parts' evidence has to be combined -- and combined so that no part is claimed
    to be better than it is.
    """

    def test_one_part_is_exactly_the_answer_for_an_ordinary_observation(self):
        """No second code path: a single part changes nothing, and reads no exposure."""
        dtf = chandra.DeadTimeFactors(0.2964, 228.78, 2.05, 2392, 2769)

        combined = chandra.chandra_parts_time_resolution(_HRC_HEADER, [(0, dtf, None)])

        assert combined == chandra.chandra_time_resolution(_HRC_HEADER, dtf)

    def test_1411_combines_its_two_vetoed_parts_weighted_by_exposure(self):
        evidence = [
            (0, _1411_DTF_000, _1411_GOOD_TIME[0]),
            (2, _1411_DTF_002, _1411_GOOD_TIME[1]),
        ]

        found = chandra.chandra_parts_time_resolution(_HRC_HEADER, evidence)

        assert found.veto_ratio == pytest.approx(0.22724, rel=1e-3)
        assert found.trigger_rate_hz == pytest.approx(202.68, rel=1e-3)
        assert found.seconds == pytest.approx(4.934e-3, rel=1e-3)
        assert found.fast_timing is False
        assert "weighted by exposure" in found.reason

    def test_every_part_s_own_answer_is_kept_beside_the_combination(self):
        evidence = [
            (0, _1411_DTF_000, _1411_GOOD_TIME[0]),
            (2, _1411_DTF_002, _1411_GOOD_TIME[1]),
        ]

        found = chandra.chandra_parts_time_resolution(_HRC_HEADER, evidence)

        assert [part["number"] for part in found.parts] == [0, 2]
        assert found.parts[0]["seconds"] == pytest.approx(4.8235e-3, rel=1e-3)
        assert found.parts[1]["seconds"] == pytest.approx(5.1768e-3, rel=1e-3)
        assert found.parts[1]["exposure_s"] == pytest.approx(17722.25)

    def test_parts_on_opposite_sides_of_the_threshold_take_the_coarser_resolution(self):
        """
        The trap an average walks into: 500 ks unvetoed and 1 ks vetoed average to a veto
        ratio of 0.9986, above the threshold, which would claim 15.625 us for events of
        which some are only good to 5 ms.
        """
        unvetoed = chandra.DeadTimeFactors(1.0, 60.0, 2.05, 100, 100)
        vetoed = chandra.DeadTimeFactors(0.3, 200.0, 2.05, 100, 100)
        evidence = [(0, unvetoed, 500_000.0), (1, vetoed, 1_000.0)]
        naive = chandra.combine_dead_time_factors([unvetoed, vetoed], [500_000.0, 1_000.0])
        assert chandra.chandra_time_resolution(_HRC_HEADER, naive).fast_timing is True

        found = chandra.chandra_parts_time_resolution(_HRC_HEADER, evidence)

        assert found.seconds == pytest.approx(5.0e-3)
        assert found.fast_timing is False
        assert found.basis == "hrc_parts_disagree"
        assert "coarser" in found.reason

    def test_two_unvetoed_parts_keep_the_header_s_resolution(self):
        evidence = [
            (0, chandra.DeadTimeFactors(1.0, 60.0, 2.05, 100, 100), 10_000.0),
            (1, chandra.DeadTimeFactors(0.995, 62.0, 2.05, 100, 100), 20_000.0),
        ]

        found = chandra.chandra_parts_time_resolution(_HRC_HEADER, evidence)

        assert found.seconds == pytest.approx(1.5625e-05)
        assert found.fast_timing is True

    def test_a_part_without_its_dead_time_file_cannot_be_called_unvetoed(self):
        """That part falls back on the documented 4 ms, and the coarser answer stands."""
        evidence = [
            (0, chandra.DeadTimeFactors(1.0, 60.0, 2.05, 100, 100), 10_000.0),
            (1, None, 1.0),
        ]

        found = chandra.chandra_parts_time_resolution(_HRC_HEADER, evidence)

        assert found.seconds == pytest.approx(chandra.HRC_DOCUMENTED_RESOLUTION)
        assert found.fast_timing is False

    def test_acis_parts_share_the_frame_time_they_were_checked_to_share(self):
        header = {"INSTRUME": "ACIS", "READMODE": "TIMED", "TIMEDEL": 3.24104}

        found = chandra.chandra_parts_time_resolution(
            header, [(1, None, 3813.0), (2, None, 1184.0)]
        )

        assert found.seconds == pytest.approx(3.24104)
        assert "2 parts" in found.reason

    def test_equal_weights_stand_in_when_no_exposure_is_known(self):
        combined = chandra.combine_dead_time_factors([_1411_DTF_000, _1411_DTF_002], [None, None])

        assert combined.veto_ratio == pytest.approx((95 / 425 + 93 / 396) / 2)

    def test_a_part_s_exposure_is_its_good_time(self, tmp_path):
        gti = chandra.write_gti_file(
            tmp_path / "flt1.fits", np.array([[57472541.4797728, 57508816.48733143]])
        )
        part = chandra.ObservationPart(0, 57471875.357874, 57509598.434235, gti_file=gti)

        assert chandra.chandra_part_exposure(part) == pytest.approx(_1411_GOOD_TIME[0])

    def test_without_good_times_a_part_s_exposure_is_its_span(self):
        part = chandra.ObservationPart(0, 100.0, 350.0)

        assert chandra.chandra_part_exposure(part) == pytest.approx(250.0)

    def test_the_front_end_records_what_each_part_answered(self, tmp_path):
        """Empty for one part: there is nothing combined to show."""
        config = an_archive_observation(tmp_path, "6298")
        directory = tmp_path / "diag"

        with record_step(str(directory), "6298", "chandra_front_end") as rec:
            chandra.chandra_archive_front_end("6298", config, rec=rec)

        values = json.loads((directory / "chandra_front_end.json").read_text())["values"]
        assert values["time_resolution_parts"] == []
        assert values["has_dead_time_file"] is True


class TestPartsMustBeTakenTheSameWay:
    def parts_of(self, tmp_path, times):
        config = a_timed_observation(tmp_path, "380", times)
        header = fits.getheader(chandra.chandra_event_list("380", config), 1)
        return header, chandra.chandra_observation_parts("380", config)

    def test_380_s_two_parts_were_read_out_identically(self, tmp_path):
        header, parts = self.parts_of(tmp_path, ACIS_380_TIMES)

        chandra.chandra_check_part_configurations(header, parts)

    @pytest.mark.parametrize(
        "keyword, value",
        [("READMODE", "CONTINUOUS"), ("TIMEDEL", 0.44104), ("DETNAM", "ACIS-7")],
    )
    def test_a_part_read_out_differently_is_refused(self, tmp_path, keyword, value):
        """Mode, frame time and chips: one resolution and one stem cannot describe both."""
        different = (*_380_PART_002[:2], dict(_380_CONFIGURATION, **{keyword: value}))
        times = dict(ACIS_380_TIMES)
        times["secondary/acisf00380_002N006_flt1.fits.gz"] = different
        header, parts = self.parts_of(tmp_path, times)

        with pytest.raises(ValueError, match=f"{keyword}.*part 2"):
            chandra.chandra_check_part_configurations(header, parts)

    def test_hrc_parts_on_different_detectors_are_refused(self, tmp_path):
        times = {
            name: (*spec[:2], {"DETNAM": "HRC-I"}) if "_evt2." not in name else spec
            for name, spec in HRC_I_1411_TIMES.items()
            if "_asol1" not in name
        }
        times["secondary/hrcf01411_002N006_std_flt1.fits.gz"] = (
            *_1411_PART_002,
            {"DETNAM": "HRC-S"},
        )
        config = a_timed_observation(tmp_path, "1411", times)
        header = fits.getheader(chandra.chandra_event_list("1411", config), 1)
        parts = chandra.chandra_observation_parts("1411", config)

        with pytest.raises(ValueError, match="DETNAM"):
            chandra.chandra_check_part_configurations(header, parts)

    def test_an_hrc_good_time_file_s_own_timedel_is_not_a_difference(self, tmp_path):
        """
        Measured on the real ``1411``: both parts' ``std_flt1`` say ``TIMEDEL`` 0.25625 s,
        the sampling of the filter, while the event list says 1.5625e-05 s. The parts agree
        with each other, and comparing either with the event list compares two quantities.
        """
        times = {
            name: (
                (*spec[:2], {"DETNAM": "HRC-I", "TIMEDEL": 0.2562500089407})
                if "_flt1" in name
                else spec
            )
            for name, spec in HRC_I_1411_TIMES.items()
            if "_asol1" not in name
        }
        config = a_timed_observation(tmp_path, "1411", times)
        header = fits.getheader(chandra.chandra_event_list("1411", config), 1)

        chandra.chandra_check_part_configurations(
            header, chandra.chandra_observation_parts("1411", config)
        )

    def test_an_ordinary_observation_opens_nothing(self, tmp_path):
        """Its companions are empty placeholders, so opening one would raise."""
        config = an_archive_observation(tmp_path, "6298")
        header = fits.getheader(chandra.chandra_event_list("6298", config), 1)

        chandra.chandra_check_part_configurations(
            header, chandra.chandra_observation_parts("6298", config)
        )


def an_event_file(path, sky_pixel_deg=None, **keywords):
    """
    A level-2 event list carrying the header keywords the front end reads.

    ``sky_pixel_deg`` adds an ``x`` column with that ``TCDLT``, the way the archive's
    files carry the sky pixel scale.
    """
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
    columns = [fits.Column("time", "1D", array=np.arange(10.0))]
    if sky_pixel_deg is not None:
        columns.append(fits.Column("x", "1E", array=np.zeros(10), coord_inc=sky_pixel_deg))
    hdu = fits.BinTableHDU.from_columns(columns, name="EVENTS")
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
    def test_the_sky_pixel_scale_is_the_event_list_s_own_and_is_recorded(self, tmp_path):
        config = an_archive_observation(tmp_path, "6298", sky_pixel_deg=-3.6611111111111e-05)
        directory = tmp_path / "diagnostics"

        with record_step(str(directory), "6298", "chandra_front_end") as rec:
            found = chandra.chandra_archive_front_end("6298", config, rec=rec)

        assert found.sky_pixel_arcsec == pytest.approx(0.1318)
        written = json.loads(next(directory.glob("*chandra_front_end*.json")).read_text())
        assert written["values"]["sky_pixel_arcsec"] == pytest.approx(0.1318)

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

    def test_an_unreadable_calibration_version_is_called_neither(self):
        """``None``, not ``False``: an unknown calibration is not a current one."""
        observation = chandra.Observation(
            obsid="6298",
            detector="hrci",
            grating="NONE",
            mode="imaging",
            time_resolution=chandra.TimeResolution(4.37e-3, "hrc_trigger_rate", ""),
            chips=(),
            event_list="evt2.fits",
            caldb_version="unknown",
        )

        assert observation.caldb_is_stale is None

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
    """
    An ACIS sky pixel is 0.492 arcseconds and an HRC one is 0.1318. This used to be one
    constant, the ACIS value, and every HRC radius reported in arcseconds came out 3.7
    times too large.
    """

    @staticmethod
    def _a_header(tcdlt, instrument):
        return fits.Header(
            [("INSTRUME", instrument), ("TTYPE1", "time"), ("TTYPE2", "x"), ("TCDLT2", tcdlt)]
        )

    def test_an_hrc_scale_is_read_off_the_sky_column(self):
        """Obsid ``8189``'s and ``23460``'s own ``TCDLT`` for ``x``."""
        header = self._a_header(-3.6611111111111e-05, "HRC")

        assert chandra.chandra_sky_pixel_arcsec(header) == pytest.approx(0.1318)

    def test_an_acis_scale_is_read_off_the_sky_column(self):
        header = self._a_header(-1.3666666666667e-04, "ACIS")

        assert chandra.chandra_sky_pixel_arcsec(header) == pytest.approx(0.492)

    def test_the_column_is_believed_over_the_instrument(self):
        header = self._a_header(-1.3666666666667e-04, "HRC")

        assert chandra.chandra_sky_pixel_arcsec(header) == pytest.approx(0.492)

    def test_a_header_without_the_column_falls_back_on_the_instrument(self):
        hrc = fits.Header([("INSTRUME", "HRC")])
        acis = fits.Header([("INSTRUME", "ACIS")])

        assert chandra.chandra_sky_pixel_arcsec(hrc) == chandra.HRC_SKY_PIXEL_ARCSEC
        assert chandra.chandra_sky_pixel_arcsec(acis) == chandra.ACIS_SKY_PIXEL_ARCSEC

    def test_an_observation_built_without_one_takes_its_detector_s(self, tmp_path):
        fields = dict(
            obsid="1",
            grating="NONE",
            mode="imaging",
            time_resolution=chandra.TimeResolution(1.5625e-05, "hrc_imaging", ""),
            chips=(),
            event_list=str(tmp_path / "evt2.fits"),
        )

        hrc = chandra.Observation(detector="hrci", **fields)
        acis = chandra.Observation(detector="aciss", **fields)

        assert hrc.sky_pixel_arcsec == chandra.HRC_SKY_PIXEL_ARCSEC
        assert acis.sky_pixel_arcsec == chandra.ACIS_SKY_PIXEL_ARCSEC

    def test_the_conversion_uses_the_scale_it_is_given(self):
        assert chandra.arcsec_to_sky_pixels(0.492, 0.1318) == pytest.approx(3.733, abs=0.001)

    def test_the_two_conversions_undo_each_other(self):
        pixels = chandra.arcsec_to_sky_pixels(3.7, 0.1318)

        assert chandra.sky_pixels_to_arcsec(pixels, 0.1318) == pytest.approx(3.7)


class TestHowARegionIsSpelt:
    """
    CIAO's Data Model, not SAS. A region is a bare shape and the filter that carries it
    names the column system, so the two are kept apart: the shape is what goes into a
    region file, and ``[sky=...]`` is what goes onto a file name.
    """

    def test_a_circle_is_centre_and_radius_in_sky_pixels(self):
        assert (
            chandra.circle_region(4100.38, 4131.82, 0.984, 0.492)
            == "circle(4100.3800,4131.8200,2.0000)"
        )

    def test_an_hrc_circle_is_measured_in_hrc_pixels(self):
        assert (
            chandra.circle_region(16384.5, 16384.5, 0.2636, 0.1318)
            == "circle(16384.5000,16384.5000,2.0000)"
        )

    def test_an_annulus_carries_both_radii(self):
        assert chandra.annulus_region(4100.0, 4131.0, 0.984, 1.968, 0.492) == (
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

        size = chandra.read_psf_size(path, chandra.ACIS_SKY_PIXEL_ARCSEC)

        assert size.radius_arcsec == pytest.approx(0.830, abs=0.001)

    def test_an_hrc_radius_is_converted_at_the_hrc_scale(self, tmp_path):
        """The file carries no scale of its own, so the observation's has to be passed."""
        path = a_psf_region_file(tmp_path / "psf.reg", radius_pixels=6.3)

        size = chandra.read_psf_size(path, chandra.HRC_SKY_PIXEL_ARCSEC)

        assert size.radius_arcsec == pytest.approx(0.830, abs=0.001)

    def test_only_the_radius_is_read_off_the_file(self, tmp_path):
        """
        The file also carries ``NEAR_CHIP_EDGE``, and that column is not trusted -- see
        :class:`TestHowCloseTheSourceIsToAnEdge`. Reading it would put a warning that is
        wrong on every subarray observation into every subarray observation's report.
        """
        path = a_psf_region_file(tmp_path / "psf.reg", near_chip_edge=True)

        assert not hasattr(chandra.read_psf_size(path, 0.492), "near_chip_edge")

    def test_an_empty_region_file_says_the_position_is_not_on_the_detector(self, tmp_path):
        path = tmp_path / "psf.reg"
        fits.BinTableHDU.from_columns(
            [fits.Column("R", "1D", array=np.array([]))], name="REGION"
        ).writeto(path, overwrite=True)

        with pytest.raises(ValueError, match="no source"):
            chandra.read_psf_size(str(path), 0.492)


class TestSizingTheExtractionRegions:
    def _a_position(self, **overrides):
        values = dict(x=4100.38, y=4131.82, chip_id=7, chipx=226.3, chipy=496.95, theta_arcmin=0.29)
        values.update(overrides)
        return chandra.SourcePosition(**values)

    def test_the_source_is_a_circle_at_the_position_asked_for(self, tmp_path):
        regions = chandra.chandra_extraction_regions(
            self._a_position(),
            0.984,
            dict(chandra.DEFAULT_CONFIG),
            continuous_clocking=False,
            pixel_arcsec=0.492,
        )

        assert regions.source == "[sky=circle(4100.3800,4131.8200,2.0000)]"

    def test_the_background_is_an_annulus_scaled_from_the_source_radius(self, tmp_path):
        config = dict(chandra.DEFAULT_CONFIG, bkg_inner_factor=1.5, bkg_outer_factor=3.0)

        regions = chandra.chandra_extraction_regions(
            self._a_position(), 0.984, config, continuous_clocking=False, pixel_arcsec=0.492
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
            self._a_position(chipx=226.3),
            0.984,
            config,
            continuous_clocking=True,
            pixel_arcsec=0.492,
        )

        assert regions.source == "[chipx=223:229]"
        assert regions.background == "[chipx=196:216,236:256]"

    def test_a_continuous_clocking_background_is_flagged_as_overlapping_the_source(self):
        regions = chandra.chandra_extraction_regions(
            self._a_position(),
            0.984,
            dict(chandra.DEFAULT_CONFIG),
            continuous_clocking=True,
            pixel_arcsec=0.492,
        )

        assert "collapsed" in regions.reason

    def test_a_source_near_an_edge_says_so_in_plain_english(self):
        regions = chandra.chandra_extraction_regions(
            self._a_position(),
            0.984,
            dict(chandra.DEFAULT_CONFIG),
            continuous_clocking=False,
            pixel_arcsec=0.492,
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
            pixel_arcsec=0.492,
            chip_edge=chandra.ChipEdge(margin_pix=47.9, near_edge=False, window=(449, 576)),
        )

        assert "dither" not in regions.reason

    def test_the_basis_says_where_the_radius_came_from(self):
        regions = chandra.chandra_extraction_regions(
            self._a_position(),
            0.984,
            dict(chandra.DEFAULT_CONFIG),
            continuous_clocking=False,
            pixel_arcsec=0.492,
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

    def _in_parts(self, tmp_path):
        """``1411``'s shape: part 0 with one aspect solution, part 2 with two."""
        parts = (
            chandra.ObservationPart(
                0, 57471875.4, 57509598.4, aspect_solutions=(str(tmp_path / "a0.fits"),)
            ),
            chandra.ObservationPart(
                2,
                64767109.1,
                64787079.2,
                aspect_solutions=(str(tmp_path / "a2.fits"), str(tmp_path / "b2.fits")),
            ),
        )
        observation = self._an_observation(tmp_path)
        return chandra.Observation(
            **{**observation.__dict__, "aspect_solution": None, "parts": parts}
        )

    def test_every_part_s_aspect_solution_is_stacked_for_dmcoords(self, tmp_path, stub_ciao_tasks):
        """
        One sky position has to come out for the whole merged event list, so ``dmcoords``
        is given the aspect solutions of every part, as a CIAO stack, in time order.
        """
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        chandra.chandra_source_regions(self._in_parts(tmp_path), config, 148.96, 69.68)

        dmcoords = [call for call in stub_ciao_tasks if call[0] == "dmcoords"][0]
        assert dmcoords[2]["asolfile"] == ",".join(
            str(tmp_path / name) for name in ("a0.fits", "a2.fits", "b2.fits")
        )

    def test_the_aspect_solutions_of_an_observation_in_parts_are_all_of_them(self, tmp_path):
        assert _names(self._in_parts(tmp_path).aspect_solutions) == [
            "a0.fits",
            "a2.fits",
            "b2.fits",
        ]

    def test_the_aspect_solutions_of_an_ordinary_observation_are_its_one(self, tmp_path):
        observation = self._an_observation(tmp_path)

        assert observation.aspect_solutions == (observation.aspect_solution,)

    def test_no_aspect_solution_is_no_aspect_solutions(self, tmp_path):
        observation = self._an_observation(tmp_path)
        observation = chandra.Observation(**{**observation.__dict__, "aspect_solution": None})

        assert observation.aspect_solutions == ()

    def test_by_default_the_radius_is_measured_rather_than_assumed(self, tmp_path, stub_ciao_tasks):
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        _, regions = chandra.chandra_source_regions(
            self._an_observation(tmp_path), config, 148.96, 69.68
        )

        assert regions.basis == "psfsize_srcs"
        assert regions.radius_arcsec == pytest.approx(0.830, abs=0.001)

    def test_an_hrc_circle_is_the_size_psfsize_srcs_measured(self, tmp_path, stub_ciao_tasks):
        """
        The tool answers in sky pixels and the circle is drawn in sky pixels, so the circle
        is the tool's answer whatever the scale. What the scale changes is the number of
        arcseconds reported, and at the ACIS scale an HRC one was 3.7 times too large.
        """
        observation = chandra.Observation(
            **{
                **self._an_observation(tmp_path).__dict__,
                "detector": "hrci",
                "chips": (),
                "sky_pixel_arcsec": chandra.HRC_SKY_PIXEL_ARCSEC,
            },
        )
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))

        _, regions = chandra.chandra_source_regions(observation, config, 148.96, 69.68)

        assert regions.source.endswith(",1.6874)]")
        assert regions.radius_arcsec == pytest.approx(1.6874055297 * 0.1318)

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

    def test_the_curve_is_extracted_through_that_filter_into_a_named_file(
        self, tmp_path, stub_ciao_tasks
    ):
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path))
        observation = self._an_observation()

        found = chandra.chandra_flare_lightcurve(
            observation, self._position(), self._regions(), config
        )

        name, _, kwargs = stub_ciao_tasks[-1]
        assert name == "dmextract"
        assert kwargs["opt"] == "ltc1"
        assert kwargs["infile"] == "evt2.fits" + chandra.chandra_flare_curve_filter(
            observation, self._position(), self._regions(), config
        )
        assert found == kwargs["outfile"]
        assert found == str(tmp_path / "5644/event_cl/chandra05644_aciss_timed_bkg_lc.fits")


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

        The limit is lowered below a real 5% cut rather than a curve built to flare for half
        the observation, because such a curve cannot reach this branch at all: half the
        bins at 40 counts/s raise the clipped scatter to 19, the threshold to 78, and
        nothing is flagged. An earlier version of this test did exactly that and passed
        without ever refusing anything -- measured on 2026-09-12.
        """
        rate = np.full(100, 2.0)
        rate[50:55] = 40.0
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=rate, cadence=500.0)
        config = dict(chandra.DEFAULT_CONFIG, flare_max_removed_fraction=0.04)
        directory = tmp_path / "diagnostics"

        with record_step(str(directory), "5644", "flare_filtering") as rec:
            gti = chandra.chandra_flare_gti(self._observation(tmp_path), config, curve, rec=rec)

        assert gti.tolist() == [[1000.0, 51000.0]]
        written = json.loads(next(directory.glob("*flare_filtering*.json")).read_text())
        assert written["values"]["applied"] is False
        assert written["values"]["removed_fraction"] == 0.0
        assert written["values"]["exposure_after"] == written["values"]["exposure_before"]
        assert "would remove 5%" in written["values"]["reason"]

    def test_a_large_cut_within_the_limit_is_made_and_warned_about(self, tmp_path, caplog):
        rate = np.full(100, 2.0)
        rate[50:55] = 40.0
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=rate, cadence=500.0)
        config = dict(chandra.DEFAULT_CONFIG, flare_warn_fraction=0.01)

        with caplog.at_level("WARNING"):
            gti = chandra.chandra_flare_gti(self._observation(tmp_path), config, curve)

        assert len(gti) == 2
        assert "flare screening removed 5%" in caplog.text

    def test_an_event_list_without_good_times_is_bounded_by_the_curve(self, tmp_path):
        """No GTI block to intersect with, so the curve's own span stands in for it."""
        events = tmp_path / "no_gti_evt2.fits"
        hdu = fits.BinTableHDU.from_columns(
            [fits.Column("time", "1D", array=np.arange(5.0))], name="EVENTS"
        )
        fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(events, overwrite=True)
        observation = dataclasses.replace(self._observation(tmp_path), event_list=str(events))
        curve = a_chandra_lightcurve(tmp_path / "lc.fits", rate=np.full(100, 2.0), cadence=500.0)

        gti = chandra.chandra_flare_gti(observation, dict(chandra.DEFAULT_CONFIG), curve)

        assert gti.tolist() == [[0.0, 50000.0]]

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


class TestCompressingTheBarycentredList:
    """
    The whole-field barycentred list is read once, to cut the source out of it, and kept
    compressed after that. HRC event lists do not compress well -- obsid ``8505``'s went
    from 319 to 244 MB -- but a batch of them is still most of the disk a run takes.
    """

    def test_the_compressed_file_holds_the_same_events(self, tmp_path):
        original = a_barycentred_file(tmp_path / "cl_bary.evt")
        with fits.open(original) as hdulist:
            expected = hdulist["EVENTS"].data["time"].copy()

        found = chandra.chandra_compress_barycentered_events(original)

        assert found == original + ".gz"
        with fits.open(found) as hdulist:
            np.testing.assert_array_equal(hdulist["EVENTS"].data["time"], expected)
            assert hdulist["EVENTS"].header["TIMESYS"] == "TDB"

    def test_only_the_compressed_file_is_left(self, tmp_path):
        original = a_barycentred_file(tmp_path / "cl_bary.evt")

        chandra.chandra_compress_barycentered_events(original)

        assert sorted(path.name for path in tmp_path.iterdir()) == ["cl_bary.evt.gz"]

    def test_the_record_names_the_file_that_is_left(self, tmp_path):
        directory = tmp_path / "diagnostics"
        original = a_barycentred_file(tmp_path / "cl_bary.evt")

        with record_step(str(directory), "5644", "barycenter") as rec:
            rec.value(barycentered_file="cl_bary.evt")
            chandra.chandra_compress_barycentered_events(original, rec=rec)

        values = json.loads(next(directory.glob("*barycenter*.json")).read_text())["values"]
        assert values["barycentered_file"] == "cl_bary.evt.gz"

    def test_with_nothing_corrected_there_is_nothing_to_compress(self):
        assert chandra.chandra_compress_barycentered_events(None) is None


def a_pileup_map(path, values, x0=4000, y0=4500):
    """
    A counts-per-frame image the way ``pileup_map`` writes one.

    CIAO images carry the sky-to-image transform in ``LTM``/``LTV``, and everything that
    reads one back has to go through it: the array is a crop of the sky plane and its
    corner is not the sky origin. ``values`` is indexed ``[row, column]``, i.e. ``[y, x]``.
    """
    data = np.asarray(values, dtype=np.float32)
    header = fits.Header()
    header["LTM1_1"] = 1.0
    header["LTM2_2"] = 1.0
    header["LTV1"] = 0.5 - x0
    header["LTV2"] = 0.5 - y0
    header["MTYPE1"] = "sky"
    header["MFORM1"] = "x,y"
    fits.PrimaryHDU(data=data, header=header).writeto(path, overwrite=True)
    return path


class TestTheCxcPileUpTable:
    def test_the_three_tabulated_points_come_back_as_tabulated(self):
        assert chandra.pileup_fraction(0.02) == pytest.approx(0.01)
        assert chandra.pileup_fraction(0.10) == pytest.approx(0.05)
        assert chandra.pileup_fraction(0.20) == pytest.approx(0.10)

    def test_between_the_points_it_interpolates(self):
        assert chandra.pileup_fraction(0.06) == pytest.approx(0.03)
        assert chandra.pileup_fraction(0.01) == pytest.approx(0.005)

    def test_an_empty_pixel_is_not_piled(self):
        assert chandra.pileup_fraction(0.0) == 0.0

    def test_above_the_table_it_refuses_to_extrapolate(self):
        """
        The relation saturates -- a severely piled source craters, and the counts per
        frame stop growing with the true rate. Extrapolating the straight line past the
        last tabulated point would read that crater as *less* pile-up. ``None`` means
        "worse than the table goes", which is the only honest answer.
        """
        assert chandra.pileup_fraction(0.25) is None
        assert chandra.pileup_fraction(3.0) is None


class TestWhereAPixelOfTheMapIs:
    def test_a_sky_position_is_found_through_the_images_own_transform(self, tmp_path):
        path = a_pileup_map(str(tmp_path / "map.fits"), np.zeros((4, 4)), x0=4000, y0=4500)
        header = fits.getheader(path)

        assert chandra.sky_to_image_pixel(header, 4000.0, 4500.0) == pytest.approx((0.0, 0.0))
        assert chandra.sky_to_image_pixel(header, 4002.0, 4501.0) == pytest.approx((2.0, 1.0))

    def test_a_binned_image_scales_as_well_as_shifts(self, tmp_path):
        path = str(tmp_path / "map.fits")
        a_pileup_map(path, np.zeros((4, 4)))
        with fits.open(path, mode="update") as opened:
            opened[0].header["LTM1_1"] = 0.5
            opened[0].header["LTM2_2"] = 0.5
            opened[0].header["LTV1"] = 0.5 - 4000 * 0.5
            opened[0].header["LTV2"] = 0.5 - 4500 * 0.5
        header = fits.getheader(path)

        assert chandra.sky_to_image_pixel(header, 4004.0, 4500.0) == pytest.approx((2.0, 0.0))


class TestReadingThePileUpMap:
    def _a_map(self, tmp_path):
        values = np.zeros((9, 9))
        values[4, 4] = 0.30
        values[4, 5] = 0.10
        values[5, 4] = 0.06
        values[3, 4] = 0.02
        values[0, 0] = 9.99
        return a_pileup_map(str(tmp_path / "pileup.fits"), values, x0=4000, y0=4500)

    def test_only_the_pixels_inside_the_circle_are_read(self, tmp_path):
        """The bright corner pixel is 5.7 pixels away and must not be seen: a pile-up
        measurement is of the source, and any other source on the chip is not it."""
        radius = chandra.sky_pixels_to_arcsec(1.0, 0.492)
        found = chandra.read_pileup_map(
            self._a_map(tmp_path), 4004.0, 4504.0, radius, 90.0, pixel_arcsec=0.492
        )

        assert found.peak_counts_per_frame == pytest.approx(0.30)
        assert found.pixels == 5

    def test_the_percentile_is_taken_over_those_pixels(self, tmp_path):
        radius = chandra.sky_pixels_to_arcsec(1.0, 0.492)
        found = chandra.read_pileup_map(
            self._a_map(tmp_path), 4004.0, 4504.0, radius, 90.0, pixel_arcsec=0.492
        )

        assert found.percentile_counts_per_frame == pytest.approx(
            np.percentile([0.30, 0.10, 0.06, 0.02, 0.0], 90.0)
        )

    def test_the_radius_is_converted_at_the_scale_it_is_given(self, tmp_path):
        found = chandra.read_pileup_map(
            self._a_map(tmp_path), 4004.0, 4504.0, 2.0, 90.0, pixel_arcsec=2.0
        )

        assert found.pixels == 5

    def test_a_radius_that_lands_on_nothing_is_not_a_measurement(self, tmp_path):
        radius = chandra.sky_pixels_to_arcsec(1.0, 0.492)
        found = chandra.read_pileup_map(
            self._a_map(tmp_path), 9000.0, 9000.0, radius, 90.0, pixel_arcsec=0.492
        )

        assert found is None


class TestWhetherPileUpCanBeMeasuredAtAll:
    def _observation(self, tmp_path, **kwargs):
        fields = dict(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "evt2.fits"),
        )
        fields.update(kwargs)
        return chandra.Observation(**fields)

    def test_acis_timed_exposure_is_the_case_it_was_written_for(self, tmp_path):
        applies, why = chandra.chandra_pileup_applies(self._observation(tmp_path))

        assert applies is True
        assert why == ""

    def test_hrc_has_no_frames_to_pile_into(self, tmp_path):
        observation = self._observation(
            tmp_path,
            detector="hrci",
            mode="imaging",
            chips=(),
            time_resolution=chandra.TimeResolution(1.5625e-05, "hrc_imaging", ""),
        )

        applies, why = chandra.chandra_pileup_applies(observation)

        assert applies is False
        assert "frame" in why

    def test_continuous_clocking_does_not_have_frames_in_the_same_sense(self, tmp_path):
        observation = self._observation(
            tmp_path,
            mode="cc",
            time_resolution=chandra.TimeResolution(0.00285, "acis_continuous_clocking", ""),
        )

        applies, why = chandra.chandra_pileup_applies(observation)

        assert applies is False
        assert "Continuous Clocking" in why


@pytest.fixture
def stub_pileup_tasks(monkeypatch):
    """A ``dmcopy`` that writes a counts image and a ``pileup_map`` that writes a map."""
    calls = []

    def fake_run(name, *, produces, args=(), capture=False, **kwargs):
        calls.append((name, kwargs))
        if name == "dmcopy":
            a_pileup_map(kwargs["outfile"], np.zeros((9, 9)), x0=4096, y0=4096)
        if name == "pileup_map":
            values = np.zeros((9, 9))
            values[4, 4] = 0.30
            values[4, 5] = values[5, 4] = values[3, 4] = 0.10
            a_pileup_map(kwargs["outfile"], values, x0=4096, y0=4096)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(ciao, "run", fake_run)
    return calls


class TestMeasuringPileUp:
    def _observation(self, tmp_path, **kwargs):
        fields = dict(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "evt2.fits"),
        )
        fields.update(kwargs)
        return chandra.Observation(**fields)

    def _config(self, tmp_path):
        config = dict(chandra.DEFAULT_CONFIG)
        # ``out_data_path``, which is what the path helpers read. This used to set an
        # ``outdir`` key nothing reads, so every test in the class wrote its chip image and
        # pile-up map under ``./5644/event_cl`` wherever pytest happened to be started.
        config["out_data_path"] = str(tmp_path / "out")
        return config

    def _position(self):
        return chandra.SourcePosition(
            x=4100.0, y=4100.0, chip_id=7, chipx=226.3, chipy=497.0, theta_arcmin=0.29
        )

    def _regions(self):
        return chandra.ExtractionRegions(
            source="[sky=circle(4100,4100,4)]",
            background="[sky=annulus(4100,4100,8,16)]",
            radius_arcsec=chandra.sky_pixels_to_arcsec(1.0, 0.492),
        )

    def test_a_map_that_misses_the_source_reports_no_number_rather_than_a_wrong_one(
        self, tmp_path, monkeypatch
    ):
        def fake_run(name, *, produces, args=(), capture=False, **kwargs):
            a_pileup_map(kwargs["outfile"], np.full((9, 9), 0.3), x0=9000, y0=9000)
            return SimpleNamespace(stdout="")

        monkeypatch.setattr(ciao, "run", fake_run)
        directory = tmp_path / "diag"

        with record_step(str(directory), "5644", "pileup_check") as rec:
            found = chandra.chandra_pileup(
                self._observation(tmp_path),
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                self._position(),
                self._regions(),
                rec=rec,
            )

        assert found.applies is True
        assert found.fraction is None
        assert "wrong chip" in found.reason
        values = json.loads(next(directory.glob("*pileup_check*.json")).read_text())["values"]
        assert values["pileup_measured"] is False

    def test_the_map_is_made_of_the_source_chip_at_single_pixel_binning(
        self, tmp_path, stub_pileup_tasks
    ):
        """
        The tool's own help asks for both: binned by one, or the algorithm does not work,
        and one chip at a time, or dropped frames on another chip corrupt the answer.
        """
        chandra.chandra_pileup(
            self._observation(tmp_path),
            self._config(tmp_path),
            str(tmp_path / "cl.evt"),
            self._position(),
            self._regions(),
        )

        dmcopy = [call for call in stub_pileup_tasks if call[0] == "dmcopy"][0][1]
        assert "[ccd_id=7]" in dmcopy["infile"]
        assert re.search(r"\[bin x=\d+:\d+:1,y=\d+:\d+:1\]", dmcopy["infile"])

    def test_no_energy_filter_is_applied(self, tmp_path, stub_pileup_tasks):
        """Pile-up pushes two photons' energies into one event, so an energy cut throws
        away exactly the events that are the evidence for it."""
        chandra.chandra_pileup(
            self._observation(tmp_path),
            self._config(tmp_path),
            str(tmp_path / "cl.evt"),
            self._position(),
            self._regions(),
        )

        dmcopy = [call for call in stub_pileup_tasks if call[0] == "dmcopy"][0][1]
        assert "energy" not in dmcopy["infile"].split("cl.evt")[1]

    def test_the_map_is_made_from_the_image_and_measured_where_the_source_is(
        self, tmp_path, stub_pileup_tasks
    ):
        found = chandra.chandra_pileup(
            self._observation(tmp_path),
            self._config(tmp_path),
            str(tmp_path / "cl.evt"),
            self._position(),
            self._regions(),
        )

        image = [call for call in stub_pileup_tasks if call[0] == "dmcopy"][0][1]["outfile"]
        mapped = [call for call in stub_pileup_tasks if call[0] == "pileup_map"][0][1]
        assert mapped["infile"] == image
        assert found.counts.peak_counts_per_frame == pytest.approx(0.30)
        assert found.counts.pixels == 5

    def test_the_fraction_is_the_cxc_tables_and_nothing_is_corrected(
        self, tmp_path, stub_pileup_tasks
    ):
        found = chandra.chandra_pileup(
            self._observation(tmp_path),
            self._config(tmp_path),
            str(tmp_path / "cl.evt"),
            self._position(),
            self._regions(),
        )

        assert found.peak_fraction is None
        assert found.fraction == pytest.approx(chandra.pileup_fraction(found.counts.percentile))

    def test_what_it_records(self, tmp_path, stub_pileup_tasks):
        directory = tmp_path / "diag"
        with record_step(str(directory), "5644", "pileup") as rec:
            chandra.chandra_pileup(
                self._observation(tmp_path),
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                self._position(),
                self._regions(),
                rec=rec,
            )

        values = json.loads(next(directory.glob("*pileup*.json")).read_text())["values"]
        assert values["pileup_measured"] is True
        assert values["pileup_percentile"] == 90.0
        assert values["frame_time_s"] == pytest.approx(0.44104)
        assert "pileup_map_file" in values

    def test_hrc_is_skipped_and_says_why(self, tmp_path, stub_pileup_tasks):
        observation = self._observation(
            tmp_path,
            detector="hrci",
            mode="imaging",
            chips=(),
            time_resolution=chandra.TimeResolution(1.5625e-05, "hrc_imaging", ""),
        )
        directory = tmp_path / "diag"

        with record_step(str(directory), "5644", "pileup") as rec:
            found = chandra.chandra_pileup(
                observation,
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                self._position(),
                self._regions(),
                rec=rec,
            )

        values = json.loads(next(directory.glob("*pileup*.json")).read_text())["values"]
        assert found.applies is False
        assert found.counts is None
        assert values["pileup_measured"] is False
        assert "frame" in values["pileup_reason"]
        assert stub_pileup_tasks == []

    def test_with_no_cleaned_list_there_is_nothing_to_measure(self, tmp_path, stub_pileup_tasks):
        found = chandra.chandra_pileup(
            self._observation(tmp_path),
            self._config(tmp_path),
            None,
            self._position(),
            self._regions(),
        )

        assert found.counts is None
        assert stub_pileup_tasks == []


def a_pha_spectrum(path, counts, exposure=1000.0, backfile="none", respfile="x.rmf"):
    """A PHA spectrum of the shape ``specextract`` writes."""
    channels = np.arange(1, len(counts) + 1, dtype=np.int16)
    columns = fits.ColDefs(
        [
            fits.Column(name="CHANNEL", format="I", array=channels),
            fits.Column(name="COUNTS", format="J", array=np.asarray(counts, dtype=np.int32)),
        ]
    )
    hdu = fits.BinTableHDU.from_columns(columns, name="SPECTRUM")
    hdu.header["EXPOSURE"] = exposure
    hdu.header["BACKFILE"] = backfile
    hdu.header["RESPFILE"] = respfile
    hdu.header["TOTCTS"] = int(np.sum(counts))
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return path


def an_rmf_with_ebounds(path, n_channels):
    """Just the ``EBOUNDS`` an energy scale is read from, at 10 eV per channel."""
    low = 0.3 + 0.01 * np.arange(n_channels)
    columns = fits.ColDefs(
        [
            fits.Column(name="CHANNEL", format="I", array=np.arange(1, n_channels + 1)),
            fits.Column(name="E_MIN", format="E", array=low),
            fits.Column(name="E_MAX", format="E", array=low + 0.01),
        ]
    )
    hdu = fits.BinTableHDU.from_columns(columns, name="EBOUNDS")
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return path


class TestWhichSpectrumRouteAnObservationTakes:
    def _observation(self, tmp_path, **kwargs):
        fields = dict(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "evt2.fits"),
        )
        fields.update(kwargs)
        return chandra.Observation(**fields)

    def test_acis_with_no_grating_is_extracted(self, tmp_path):
        route, why = chandra.chandra_spectrum_route(self._observation(tmp_path))

        assert route == "specextract"
        assert why == ""

    def test_a_grating_observation_is_collected_and_never_re_extracted(self, tmp_path):
        """Matteo's ruling of 2026-09-12: the archive's ``pha2`` *is* the spectrum, and
        ``tgextract`` is never run."""
        route, why = chandra.chandra_spectrum_route(
            self._observation(tmp_path, grating="HETG", mode="hetg")
        )

        assert route == "collect"
        assert "HETG" in why

    def test_hrc_has_no_useful_spectrum_and_says_so(self, tmp_path):
        route, why = chandra.chandra_spectrum_route(
            self._observation(
                tmp_path,
                detector="hrcs",
                mode="imaging",
                chips=(),
                time_resolution=chandra.TimeResolution(1.5625e-05, "hrc_imaging", ""),
            )
        )

        assert route == "none"
        assert "energy resolution" in why

    def test_continuous_clocking_is_still_extracted(self, tmp_path):
        """One spatial dimension is gone, but the energies are not, and the strips in
        step 6 are exactly the source and background ``specextract`` needs."""
        route, why = chandra.chandra_spectrum_route(
            self._observation(
                tmp_path,
                mode="cc",
                time_resolution=chandra.TimeResolution(0.00285, "acis_continuous_clocking", ""),
            )
        )

        assert route == "specextract"


class TestWhatTheSpectraAreCalled:
    def _observation(self, tmp_path):
        return chandra.Observation(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "evt2.fits"),
        )

    def test_the_names_are_specextracts_own_so_that_nothing_has_to_be_renamed(self, tmp_path):
        """
        ``specextract`` writes its own names from ``outroot`` and writes those names into
        ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE``. Renaming afterwards would mean
        rewriting three header cards in two files to match -- so the stem is chosen to
        make ``specextract``'s own names the ones the plan asks for.
        """
        config = dict(chandra.DEFAULT_CONFIG)
        config["out_data_path"] = str(tmp_path)

        paths = chandra.chandra_spectrum_paths(self._observation(tmp_path), config)

        assert os.path.basename(paths.source) == "chandra05644_aciss_timed_src.pi"
        assert os.path.basename(paths.background) == "chandra05644_aciss_timed_src_bkg.pi"
        assert os.path.basename(paths.arf) == "chandra05644_aciss_timed_src.arf"
        assert os.path.basename(paths.rmf) == "chandra05644_aciss_timed_src.rmf"
        assert os.path.basename(paths.corrected_arf) == "chandra05644_aciss_timed_src.corr.arf"
        assert os.path.basename(paths.grouped) == "chandra05644_aciss_timed_src_grp.pi"

    def test_every_name_fits_in_a_fits_header_card(self, tmp_path):
        """80 characters is the limit, and these names are written into other files."""
        config = dict(chandra.DEFAULT_CONFIG)
        config["out_data_path"] = str(tmp_path)

        paths = chandra.chandra_spectrum_paths(self._observation(tmp_path), config)

        for path in vars(paths).values():
            assert len(os.path.basename(path)) < 60


class TestReadingASpectrumBack:
    def test_the_energy_scale_comes_from_the_response(self, tmp_path):
        spectrum = a_pha_spectrum(str(tmp_path / "src.pi"), [10, 20, 30], exposure=100.0)
        rmf = an_rmf_with_ebounds(str(tmp_path / "src.rmf"), 3)

        found = chandra.read_chandra_spectrum(spectrum, rmf)

        assert found["energy"] == pytest.approx([0.305, 0.315, 0.325])
        assert found["rate"] == pytest.approx([10.0, 20.0, 30.0], rel=1e-4)

    def test_a_spectrum_that_cannot_be_drawn_is_not_a_failed_extraction(self, tmp_path):
        assert chandra.read_chandra_spectrum(str(tmp_path / "gone.pi"), str(tmp_path)) is None


@pytest.fixture
def stub_specextract(monkeypatch):
    """A ``specextract`` that writes the seven files it really writes, and a ``dmgroup``
    that copies its input."""
    calls = []

    def fake_run(name, *, produces, args=(), capture=False, **kwargs):
        calls.append((name, kwargs))
        if name == "specextract":
            root = kwargs["outroot"]
            an_rmf_with_ebounds(f"{root}.rmf", 4)
            an_rmf_with_ebounds(f"{root}_bkg.rmf", 4)
            for suffix in (".arf", ".corr.arf", "_bkg.arf"):
                an_rmf_with_ebounds(f"{root}{suffix}", 4)
            a_pha_spectrum(f"{root}.pi", [4, 3, 2, 1], respfile=f"{os.path.basename(root)}.rmf")
            a_pha_spectrum(f"{root}_bkg.pi", [1, 1, 1, 1])
        if name == "dmgroup":
            a_pha_spectrum(kwargs["outfile"], [4, 3, 2, 1])
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(ciao, "run", fake_run)
    return calls


class TestExtractingAnAcisSpectrum:
    def _observation(self, tmp_path, **kwargs):
        fields = dict(
            obsid="5644",
            detector="aciss",
            grating="NONE",
            mode="timed",
            time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "evt2.fits"),
            aspect_solution=str(tmp_path / "asol1.fits"),
            bad_pixel_file=str(tmp_path / "bpix1.fits"),
            mask_file=str(tmp_path / "msk1.fits"),
            data_mode="FAINT",
        )
        fields.update(kwargs)
        return chandra.Observation(**fields)

    def _config(self, tmp_path):
        config = dict(chandra.DEFAULT_CONFIG)
        config["out_data_path"] = str(tmp_path / "out")
        return config

    def _regions(self):
        return chandra.ExtractionRegions(
            source="[sky=circle(4100,4131,1.69)]",
            background="[sky=annulus(4100,4131,5.06,16.87)]",
            radius_arcsec=0.83,
        )

    def test_the_source_and_background_are_the_regions_step_six_chose(
        self, tmp_path, stub_specextract
    ):
        chandra.chandra_calculate_spectra(
            self._observation(tmp_path),
            self._config(tmp_path),
            str(tmp_path / "cl.evt"),
            self._regions(),
        )

        call = [one for one in stub_specextract if one[0] == "specextract"][0][1]
        assert call["infile"].endswith("[sky=circle(4100,4131,1.69)]")
        assert call["bkgfile"].endswith("[sky=annulus(4100,4131,5.06,16.87)]")

    def test_the_responses_are_unweighted_and_aperture_corrected(self, tmp_path, stub_specextract):
        """
        A point source wants ``weight=no``, which makes the response at the source's own
        position rather than averaged over the region. And because the circle is sized
        from the PSF it always misses some of the source, so ``correctpsf=yes`` is what
        makes the normalisation of a fit mean anything.
        """
        chandra.chandra_calculate_spectra(
            self._observation(tmp_path),
            self._config(tmp_path),
            str(tmp_path / "cl.evt"),
            self._regions(),
        )

        call = [one for one in stub_specextract if one[0] == "specextract"][0][1]
        assert call["weight"] == "no"
        assert call["correctpsf"] == "yes"

    def test_the_companion_files_are_handed_over(self, tmp_path, stub_specextract):
        observation = self._observation(tmp_path)

        chandra.chandra_calculate_spectra(
            observation, self._config(tmp_path), str(tmp_path / "cl.evt"), self._regions()
        )

        call = [one for one in stub_specextract if one[0] == "specextract"][0][1]
        assert call["asp"] == observation.aspect_solution
        assert call["mskfile"] == observation.mask_file
        assert call["badpixfile"] == observation.bad_pixel_file

    def test_every_aspect_solution_of_a_part_is_handed_over(self, tmp_path, stub_specextract):
        """
        A part can have several -- ``433``'s first has three -- and ``specextract`` takes
        "one or more aspect solution files" per observation, as a stack.
        """
        part = chandra.ObservationPart(
            1, 1.0, 2.0, aspect_solutions=(str(tmp_path / "a.fits"), str(tmp_path / "b.fits"))
        )
        observation = self._observation(tmp_path, aspect_solution=None, parts=(part,), part=part)

        chandra.chandra_calculate_spectra(
            observation, self._config(tmp_path), str(tmp_path / "cl.evt"), self._regions()
        )

        call = [one for one in stub_specextract if one[0] == "specextract"][0][1]
        assert call["asp"] == f"{tmp_path / 'a.fits'},{tmp_path / 'b.fits'}"

    def test_grouping_is_a_separate_call_and_not_specextracts(self, tmp_path, stub_specextract):
        """
        ``specextract``'s own ``grouptype``/``binspec`` did nothing at all in CIAO 4.18.0
        -- the spectrum came back with ``GROUPING = 0`` and no ``GROUPING`` column, with
        no warning. ``dmgroup`` called separately does the job, keeps ``BACKFILE``,
        ``RESPFILE`` and ``ANCRFILE``, and does not depend on a script's conventions.
        """
        config = self._config(tmp_path)
        chandra.chandra_calculate_spectra(
            self._observation(tmp_path), config, str(tmp_path / "cl.evt"), self._regions()
        )

        extract = [one for one in stub_specextract if one[0] == "specextract"][0][1]
        group = [one for one in stub_specextract if one[0] == "dmgroup"][0][1]
        assert extract["grouptype"] == "NONE"
        assert group["grouptypeval"] == config["spectrum_min_counts"]
        assert group["outfile"].endswith("_src_grp.pi")

    def test_what_it_returns_and_records(self, tmp_path, stub_specextract):
        directory = tmp_path / "diag"

        with record_step(str(directory), "5644", "spectra") as rec:
            found = chandra.chandra_calculate_spectra(
                self._observation(tmp_path),
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                self._regions(),
                rec=rec,
            )

        values = json.loads(next(directory.glob("*spectra*.json")).read_text())["values"]
        assert os.path.basename(found.grouped) == "chandra05644_aciss_timed_src_grp.pi"
        assert values["source_spectrum"] == "chandra05644_aciss_timed_src.pi"
        assert values["grouped_spectrum"] == "chandra05644_aciss_timed_src_grp.pi"
        assert values["energy_band"] == list(chandra.CHANDRA_SPECTRUM_BAND_KEV)
        assert values["min_counts"] == 15

    def test_a_graded_observation_says_what_that_costs(self, tmp_path, stub_specextract):
        """
        Obsid ``5644`` is ``DATAMODE = GRADED``: ACIS telemetered a grade and a summed
        pulse height per event and threw the pixel values away. The spectrum extracts
        without complaint, and it is worth less than a ``FAINT`` one -- no CTI correction
        can be recomputed and no VFAINT background cleaning is possible. Saying so on the
        page is the whole of this pipeline's job here.
        """
        directory = tmp_path / "diag"

        with record_step(str(directory), "5644", "spectra") as rec:
            chandra.chandra_calculate_spectra(
                self._observation(tmp_path, data_mode="GRADED"),
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                self._regions(),
                rec=rec,
            )

        values = json.loads(next(directory.glob("*spectra*.json")).read_text())["values"]
        assert values["data_mode"] == "GRADED"
        assert "GRADED" in values["spectrum_caveat"]

    def test_a_faint_observation_has_no_caveat(self, tmp_path, stub_specextract):
        directory = tmp_path / "diag"

        with record_step(str(directory), "5644", "spectra") as rec:
            chandra.chandra_calculate_spectra(
                self._observation(tmp_path),
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                self._regions(),
                rec=rec,
            )

        values = json.loads(next(directory.glob("*spectra*.json")).read_text())["values"]
        assert values["spectrum_caveat"] == ""

    def test_hrc_extracts_nothing_and_says_why(self, tmp_path, stub_specextract):
        observation = self._observation(
            tmp_path,
            detector="hrcs",
            mode="imaging",
            chips=(),
            time_resolution=chandra.TimeResolution(1.5625e-05, "hrc_imaging", ""),
            data_mode="OBSERVING",
        )
        directory = tmp_path / "diag"

        with record_step(str(directory), "5644", "spectra") as rec:
            found = chandra.chandra_calculate_spectra(
                observation,
                self._config(tmp_path),
                str(tmp_path / "cl.evt"),
                self._regions(),
                rec=rec,
            )

        values = json.loads(next(directory.glob("*spectra*.json")).read_text())["values"]
        assert found is None
        assert "energy resolution" in values["spectrum_reason"]
        assert stub_specextract == []

    def test_with_no_cleaned_list_nothing_is_extracted(self, tmp_path, stub_specextract):
        found = chandra.chandra_calculate_spectra(
            self._observation(tmp_path), self._config(tmp_path), None, self._regions()
        )

        assert found is None
        assert stub_specextract == []


class TestCollectingTheGratingProducts:
    def _observation(self, tmp_path):
        archive = tmp_path / "archive"
        archive.mkdir()
        pha2 = archive / "acisf02749N004_pha2.fits.gz"
        pha2.write_bytes(b"spectrum")
        responses = [archive / "acisf02749N004HEG_-1_arf2.fits.gz"]
        responses[0].write_bytes(b"response")
        return chandra.Observation(
            obsid="2749",
            detector="aciss",
            grating="HETG",
            mode="hetg",
            time_resolution=chandra.TimeResolution(2.54104, "acis_frame_time", ""),
            chips=(7,),
            event_list=str(tmp_path / "evt2.fits"),
            grating_spectrum=str(pha2),
            grating_responses=tuple(str(one) for one in responses),
        )

    def test_the_archives_own_files_are_copied_and_nothing_is_run(self, tmp_path):
        config = dict(chandra.DEFAULT_CONFIG)
        config["out_data_path"] = str(tmp_path / "out")

        found = chandra.chandra_collect_grating_products(self._observation(tmp_path), config)

        assert [os.path.basename(one) for one in found] == [
            "acisf02749N004_pha2.fits.gz",
            "acisf02749N004HEG_-1_arf2.fits.gz",
        ]
        assert all(os.path.exists(one) for one in found)

    def test_the_archives_names_are_kept(self, tmp_path):
        """
        Against the output-naming rule, and on purpose. A ``pha2`` and its dozen responses
        are cross-referenced by name and by order in ways this pipeline does not control,
        so renaming them risks breaking a set it did not make. The archive's names already
        carry the obsid, which is what the rule is for.
        """
        config = dict(chandra.DEFAULT_CONFIG)
        config["out_data_path"] = str(tmp_path / "out")

        found = chandra.chandra_collect_grating_products(self._observation(tmp_path), config)

        assert all("02749" in os.path.basename(one) for one in found)

    def test_what_it_records(self, tmp_path):
        config = dict(chandra.DEFAULT_CONFIG)
        config["out_data_path"] = str(tmp_path / "out")
        directory = tmp_path / "diag"

        with record_step(str(directory), "2749", "spectra") as rec:
            chandra.chandra_collect_grating_products(self._observation(tmp_path), config, rec=rec)

        values = json.loads(next(directory.glob("*spectra*.json")).read_text())["values"]
        assert values["grating"] == "HETG"
        assert values["n_grating_files"] == 2
        assert "tgextract" in values["spectrum_reason"]

    def test_the_spectrum_step_hands_a_grating_observation_here_and_runs_nothing(
        self, tmp_path, monkeypatch
    ):
        def refuse(name, **kwargs):
            raise AssertionError(f"a grating observation ran {name}")

        monkeypatch.setattr(ciao, "run", refuse)
        config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path / "out"))

        found = chandra.chandra_calculate_spectra(
            self._observation(tmp_path), config, str(tmp_path / "cl.evt"), regions=None
        )

        assert found is None
        assert list((tmp_path / "out").glob("**/acisf02749N004_pha2.fits.gz"))


#: What ``chandra_repro`` left in ``repro/`` on obsid ``5644``, measured on 2026-09-12.
#:
#: The exact listing, because two of its features are what the reprocessing route has to
#: get right: the archive's own ``bpix1`` and ``fov1`` are copied in beside the newly made
#: ones, so a loose glob finds two of each; and the new good-time file is ``flt2``, not the
#: ``flt1`` the archive ships.
REPRO_5644_WROTE = [
    "acisf05644_000N004_bpix1.fits",
    "acisf05644_000N004_fov1.fits",
    "acisf05644_000N004_msk1.fits",
    "acisf05644_000N004_mtl1.fits",
    "acisf05644_000N004_stat1.fits",
    "acisf05644_asol1.lis",
    "acisf05644_repro_bpix1.fits",
    "acisf05644_repro_evt2.fits",
    "acisf05644_repro_flt2.fits",
    "acisf05644_repro_fov1.fits",
    "acisf240626566N004_pbk0.fits",
    "pcadf05644_000N001_asol1.fits",
]

#: Enough of a level-1 download for the reprocessing route to accept the observation.
ACIS_5644_LEVEL1 = [
    "oif.fits",
    "primary/acisf05644N004_evt2.fits.gz",
    "primary/acisf05644_000N004_bpix1.fits.gz",
    "primary/orbitf240581100N001_eph1.fits.gz",
    "primary/pcadf05644_000N001_asol1.fits.gz",
    "secondary/acisf05644_000N004_evt1.fits.gz",
    "secondary/acisf05644_000N004_flt1.fits.gz",
    "secondary/acisf05644_000N004_msk1.fits.gz",
]


def a_reprocessed_observation(tmp_path, obsid="5644", names=None, **keywords):
    """
    A download plus a ``repro/`` directory holding what ``chandra_repro`` really leaves.

    The event list in ``repro/`` is a real file so that the front end can open it; the
    rest are placeholders, which is all the path finders need. ``keywords`` go into the
    reprocessed event list's header.
    """
    config = a_downloaded_observation(tmp_path, obsid, ACIS_5644_LEVEL1)
    config["products"] = "repro"
    repro = pathlib.Path(chandra.chandra_repro_path(obsid, config))
    repro.mkdir(parents=True, exist_ok=True)
    for name in REPRO_5644_WROTE if names is None else names:
        if name == "acisf05644_repro_evt2.fits":
            an_event_file(repro / name, **keywords)
        else:
            (repro / name).write_bytes(b"")
    return config


@pytest.fixture
def stub_chandra_repro(monkeypatch):
    """A ``chandra_repro`` that writes what the real one wrote on obsid ``5644``."""
    calls = []

    def fake_run(name, *, produces, args=(), capture=False, **kwargs):
        calls.append((name, kwargs))
        if name == "chandra_repro":
            repro = pathlib.Path(kwargs["outdir"])
            for written in REPRO_5644_WROTE:
                if written == "acisf05644_repro_evt2.fits":
                    an_event_file(
                        repro / written,
                        INSTRUME="ACIS",
                        DETNAM="ACIS-7",
                        READMODE="TIMED",
                        DATAMODE="GRADED",
                        TIMEDEL=0.44104,
                        SIM_Z=-190.1,
                        ASCDSVER="CIAO 4.18.0",
                    )
                else:
                    (repro / written).write_bytes(b"")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(ciao, "run", fake_run)
    return calls


class TestWhereTheReprocessingGoes:
    def test_it_has_its_own_directory_beside_the_others(self, tmp_path):
        """
        ``chandra_repro`` writes a dozen files of its own choosing, so they get a
        directory rather than being mixed in with the cleaned lists in ``event_cl``.
        """
        config = {"out_data_path": str(tmp_path)}

        assert chandra.chandra_repro_path(5644, config) == str(tmp_path / "5644" / "repro")

    def test_it_is_the_unpadded_obsid_like_every_other_output(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        assert chandra.chandra_repro_path("05644", config) == str(tmp_path / "5644" / "repro")


class TestWhichProductsTheReprocessingRouteReads:
    """
    The reprocessing route reads two directories, not one, and the plan said one.

    ``chandra_repro`` re-makes four products, copies a handful more, and leaves the rest
    where it found them. So each family is looked for in ``repro/`` first and in the
    download second, and the families the task never writes skip ``repro/`` altogether.
    """

    def test_the_event_list_is_the_reprocessed_one(self, tmp_path):
        config = a_reprocessed_observation(tmp_path)

        found = chandra.chandra_event_list("5644", config)

        assert os.path.basename(found) == "acisf05644_repro_evt2.fits"

    def test_the_archive_route_ignores_the_repro_directory_entirely(self, tmp_path):
        """
        The same tree read with ``products="archive"`` gives the archive's own file. The
        two routes can share one download without either seeing the other's products.
        """
        config = a_reprocessed_observation(tmp_path)
        config["products"] = "archive"

        found = chandra.chandra_event_list("5644", config)

        assert os.path.basename(found) == "acisf05644N004_evt2.fits.gz"

    def test_the_bad_pixel_list_is_the_new_one_and_not_the_copied_one(self, tmp_path):
        """
        Both are in ``repro/``. A bare ``*_bpix1.fits`` glob matches two files, and the
        one-or-raise guard would fire on a healthy reprocessing.
        """
        config = a_reprocessed_observation(tmp_path)

        found = chandra.chandra_bad_pixel_file("5644", config)

        assert os.path.basename(found) == "acisf05644_repro_bpix1.fits"

    def test_the_good_times_come_from_flt2_and_not_from_flt1(self, tmp_path):
        """
        ``chandra_repro`` calls its own good-time file ``flt2``. Asking for ``flt1`` there
        finds nothing, falls through to the download, and screens flares against the good
        times of the file that is not being reduced.
        """
        config = a_reprocessed_observation(tmp_path)

        found = chandra.chandra_gti_file("5644", config)

        assert os.path.basename(found) == "acisf05644_repro_flt2.fits"

    def test_the_mask_is_the_copy_in_the_repro_directory(self, tmp_path):
        config = a_reprocessed_observation(tmp_path)

        found = chandra.chandra_mask_file("5644", config)

        assert os.path.dirname(found) == chandra.chandra_repro_path("5644", config)

    def test_the_aspect_solution_is_the_copy_the_boresight_may_have_moved(self, tmp_path):
        config = a_reprocessed_observation(tmp_path)

        found = chandra.chandra_aspect_solution("5644", config)

        assert os.path.dirname(found) == chandra.chandra_repro_path("5644", config)

    def test_the_orbit_ephemeris_falls_through_to_the_download(self, tmp_path):
        """``chandra_repro`` does not copy it, so barycentring reads the download."""
        config = a_reprocessed_observation(tmp_path)

        found = chandra.chandra_orbit_ephemeris("5644", config)

        assert os.path.dirname(found).endswith(os.path.join("5644", "primary"))

    def test_a_family_absent_from_both_directories_is_still_none(self, tmp_path):
        """ACIS has no dead-time file, and the search path must not turn that into an error."""
        config = a_reprocessed_observation(tmp_path)

        assert chandra.chandra_dead_time_file("5644", config) is None

    def test_two_files_in_the_winning_directory_still_raise(self, tmp_path):
        """
        The search path decides *where* to look, and changes nothing about what an
        ambiguous answer means once it has looked.
        """
        config = a_reprocessed_observation(tmp_path)
        repro = pathlib.Path(chandra.chandra_repro_path("5644", config))
        (repro / "acisf05644_repro_evt2.fits").rename(repro / "a_repro_evt2.fits")
        (repro / "b_repro_evt2.fits").write_bytes(b"")

        with pytest.raises(ValueError, match="2 evt2 files"):
            chandra.chandra_event_list("5644", config)


class TestFindingTheLevelOneEventList:
    def test_it_is_in_the_download_and_never_in_the_repro_directory(self, tmp_path):
        config = a_reprocessed_observation(tmp_path)

        found = chandra.chandra_level1_event_list("5644", config)

        assert os.path.basename(found) == "acisf05644_000N004_evt1.fits.gz"

    def test_an_archive_route_download_has_none(self, tmp_path):
        """The archive route's filter does not fetch level 1, and that is not an error here."""
        config = a_downloaded_observation(tmp_path, "6298")

        assert chandra.chandra_level1_event_list("6298", config) is None


class TestReprocessingAnObservation:
    def test_it_runs_chandra_repro_the_way_the_plan_says(self, tmp_path, stub_chandra_repro):
        config = a_downloaded_observation(tmp_path, "5644", ACIS_5644_LEVEL1)
        config["products"] = "repro"

        chandra.chandra_repro_front_end("5644", config, env={})

        name, kwargs = stub_chandra_repro[0]
        assert name == "chandra_repro"
        assert kwargs["indir"] == chandra.chandra_archive_path("5644", config)
        assert kwargs["outdir"] == chandra.chandra_repro_path("5644", config)
        assert kwargs["set_ardlib"] == "no"

    def test_it_makes_the_output_directory_itself(self, tmp_path, stub_chandra_repro):
        """
        ``chandra_repro`` creates the last component of ``outdir`` and refuses to create
        any above it: pointed at ``<out>/5644/repro`` with no ``<out>/5644``, it stops with
        "Unable to create output directory". Measured on 2026-09-12.
        """
        config = a_downloaded_observation(tmp_path, "5644", ACIS_5644_LEVEL1)
        config["products"] = "repro"
        config["out_data_path"] = str(tmp_path / "somewhere" / "new")

        chandra.chandra_repro_front_end("5644", config, env={})

        assert os.path.isdir(chandra.chandra_repro_path("5644", config))

    def test_it_reads_the_reprocessed_observation_back(self, tmp_path, stub_chandra_repro):
        """
        The plan's shape survives: the task runs, and then step 4's reader reads what it
        wrote, giving the same :class:`Observation` the archive route gives.
        """
        config = a_downloaded_observation(tmp_path, "5644", ACIS_5644_LEVEL1)
        config["products"] = "repro"

        found = chandra.chandra_repro_front_end("5644", config, env={})

        assert found.obsid == "5644"
        assert found.detector == "aciss"
        assert found.data_mode == "GRADED"
        assert os.path.basename(found.event_list) == "acisf05644_repro_evt2.fits"
        assert os.path.basename(found.gti_file) == "acisf05644_repro_flt2.fits"
        assert os.path.basename(found.bad_pixel_file) == "acisf05644_repro_bpix1.fits"
        assert found.orbit_ephemeris.endswith("orbitf240581100N001_eph1.fits.gz")

    def test_nothing_downloaded_is_not_an_error(self, tmp_path, stub_chandra_repro):
        """Whether an empty directory means ``NO_SCIENCE_DATA`` is the caller's call."""
        config = {
            "input_data_path": str(tmp_path),
            "out_data_path": str(tmp_path),
            "products": "repro",
        }

        assert chandra.chandra_repro_front_end("5644", config, env={}) is None
        assert stub_chandra_repro == []

    def test_a_download_with_no_level_one_says_so_instead_of_reducing_level_two(
        self, tmp_path, stub_chandra_repro
    ):
        """
        This is the mismatch worth being loud about: the observation was downloaded with
        the archive route's filter and is being reduced with the reprocessing route. Quietly
        reducing the archive's level-2 file would answer a configuration error with the
        wrong data.
        """
        config = a_downloaded_observation(tmp_path, "6298")
        config["products"] = "repro"

        with pytest.raises(FileNotFoundError, match="no level-1 event list"):
            chandra.chandra_repro_front_end("6298", config, env={})

        assert stub_chandra_repro == []

    def test_a_clean_return_code_with_no_event_list_is_an_error(self, tmp_path, monkeypatch):
        """A zero return code proves nothing; ``ciao.run`` cannot check names it never chose."""
        config = a_downloaded_observation(tmp_path, "5644", ACIS_5644_LEVEL1)
        config["products"] = "repro"
        monkeypatch.setattr(ciao, "run", lambda name, **kwargs: SimpleNamespace(stdout=""))

        with pytest.raises(RuntimeError, match="wrote no"):
            chandra.chandra_repro_front_end("5644", config, env={})

    def test_it_records_what_was_remade_and_what_was_not(self, tmp_path, stub_chandra_repro):
        config = a_downloaded_observation(tmp_path, "5644", ACIS_5644_LEVEL1)
        config["products"] = "repro"
        directory = tmp_path / "records"

        with record_step(str(directory), "5644", "chandra_repro") as rec:
            chandra.chandra_repro_front_end("5644", config, rec=rec, env={})

        values = json.loads(next(directory.glob("*repro*.json")).read_text())["values"]
        assert values["repro_directory"] == chandra.chandra_repro_path("5644", config)
        assert "acisf05644_repro_evt2.fits" in values["reprocessed_products"]
        assert "acisf05644_000N004_msk1.fits" not in values["reprocessed_products"]
        # The header's CALDBVER is the archive's even after a reprocessing; ASCDSVER moves.
        assert values["caldb_version_is_from_the_archive"] is True
        assert values["ascds_version"] == "CIAO 4.18.0"


#: ``380``'s download under the reprocessing route's filter: level 1 as well, one per part.
ACIS_380_LEVEL1 = dict(
    ACIS_380_TIMES,
    **{
        "secondary/acisf00380_001N005_evt1.fits.gz": (*_380_PART_001[:2], {"OBI_NUM": 1}),
        "secondary/acisf00380_002N006_evt1.fits.gz": (*_380_PART_002[:2], {"OBI_NUM": 2}),
    },
)

_380_SPANS = {1: _380_PART_001[:2], 2: _380_PART_002[:2]}


def a_timed_file(path, tstart, tstop, **keywords):
    """A one-row table whose first extension carries the times and ``keywords``."""
    hdu = fits.BinTableHDU.from_columns([fits.Column("TIME", "1D", array=[tstart])])
    for key, value in dict(TSTART=tstart, TSTOP=tstop, **keywords).items():
        hdu.header[key] = value
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)


@pytest.fixture
def stub_splitobs_and_repro(monkeypatch):
    """
    A ``splitobs`` and a ``chandra_repro`` that leave what the real ones left on ``380``.

    Measured on 2026-09-13: ``splitobs`` makes ``<outroot>_001`` and ``<outroot>_002``, and
    ``chandra_repro`` on each writes ``acisf00380_repro_evt2.fits`` -- the same name in
    both -- spanning that part alone, beside its own flt2, bpix1, mask and aspect solution.
    ``skip`` names parts either task should quietly leave out.
    """
    calls = []
    skip = {"splitobs": set(), "chandra_repro": set()}

    def fake_run(name, *, produces, args=(), capture=False, **kwargs):
        calls.append((name, kwargs))
        if name == "splitobs":
            for number in _380_SPANS:
                if number not in skip["splitobs"]:
                    os.makedirs(f"{kwargs['outroot']}_{number:03d}", exist_ok=True)
        elif name == "chandra_repro":
            number = int(kwargs["indir"][-3:])
            if number in skip["chandra_repro"]:
                return SimpleNamespace(stdout="")
            tstart, tstop = _380_SPANS[number]
            outdir = pathlib.Path(kwargs["outdir"])
            outdir.mkdir(parents=True, exist_ok=True)
            an_event_file(
                outdir / "acisf00380_repro_evt2.fits",
                TSTART=tstart,
                TSTOP=tstop,
                OBI_NUM=number,
                INSTRUME="ACIS",
                SIM_Z=-233.58743446083,
                ASCDSVER="CIAO 4.18.0",
                **_380_CONFIGURATION,
            )
            for written in (
                "acisf00380_repro_flt2.fits",
                "acisf00380_repro_bpix1.fits",
                f"acisf00380_{number:03d}N005_msk1.fits",
                f"pcadf00380_{number:03d}N001_asol1.fits",
            ):
                a_timed_file(outdir / written, tstart, tstop, **_380_CONFIGURATION)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(ciao, "run", fake_run)
    return SimpleNamespace(calls=calls, skip=skip)


class TestReprocessingAnObservationInParts:
    """
    ``chandra_repro`` refuses an observation in parts: the CXC says to separate them with
    ``splitobs`` first, and then reprocess each as if it were an observation of its own.
    Matteo's ruling of 2026-09-13, measured on ``380`` the same day.
    """

    def _380(self, tmp_path):
        config = a_timed_observation(tmp_path, "380", ACIS_380_LEVEL1)
        config["products"] = "repro"
        return config

    def test_splitobs_runs_first_and_chandra_repro_once_per_part(
        self, tmp_path, stub_splitobs_and_repro
    ):
        config = self._380(tmp_path)

        chandra.chandra_repro_front_end("380", config, env={})

        calls = stub_splitobs_and_repro.calls
        assert [name for name, _ in calls] == ["splitobs", "chandra_repro", "chandra_repro"]
        outroot = os.path.join(config["out_data_path"], "380", "split", "380")
        assert calls[0][1]["indir"] == chandra.chandra_archive_path("380", config)
        assert calls[0][1]["outroot"] == outroot
        assert calls[1][1]["indir"] == outroot + "_001"
        assert calls[1][1]["outdir"] == chandra.chandra_repro_path("380", config) + "_obi001"
        assert calls[2][1]["indir"] == outroot + "_002"
        assert calls[2][1]["outdir"] == chandra.chandra_repro_path("380", config) + "_obi002"
        assert all(kwargs["set_ardlib"] == "no" for _, kwargs in calls[1:])

    def test_each_part_reads_its_own_reprocessed_products(self, tmp_path, stub_splitobs_and_repro):
        config = self._380(tmp_path)

        observation = chandra.chandra_repro_front_end("380", config, env={})

        first, second = observation.parts
        mine = chandra.chandra_repro_path("380", config) + "_obi002"
        assert second.event_list == os.path.join(mine, "acisf00380_repro_evt2.fits")
        assert second.gti_file == os.path.join(mine, "acisf00380_repro_flt2.fits")
        assert second.bad_pixel_file == os.path.join(mine, "acisf00380_repro_bpix1.fits")
        assert second.mask_file == os.path.join(mine, "acisf00380_002N005_msk1.fits")
        assert second.aspect_solutions == (os.path.join(mine, "pcadf00380_002N001_asol1.fits"),)
        assert os.path.dirname(first.event_list).endswith("repro_obi001")

    def test_orbit_files_still_come_from_the_download_paired_by_time(
        self, tmp_path, stub_splitobs_and_repro
    ):
        """``chandra_repro`` copies no orbit file, on one part or on several."""
        config = self._380(tmp_path)

        first, second = chandra.chandra_repro_front_end("380", config, env={}).parts

        assert os.path.basename(first.orbit_ephemeris) == "orbitf073742700N001_eph1.fits.gz"
        assert os.path.basename(second.orbit_ephemeris) == "orbitf077025900N001_eph1.fits.gz"

    def test_a_part_splitobs_left_out_is_an_error(self, tmp_path, stub_splitobs_and_repro):
        """Measured: with ``ASCDS_CALIB`` unset, ``splitobs`` makes nothing and says little."""
        config = self._380(tmp_path)
        stub_splitobs_and_repro.skip["splitobs"].add(2)

        with pytest.raises(RuntimeError, match="splitobs.*part 2"):
            chandra.chandra_repro_front_end("380", config, env={})

    def test_a_part_chandra_repro_did_not_reprocess_is_an_error(
        self, tmp_path, stub_splitobs_and_repro
    ):
        """Measured: ``chandra_repro`` pointed at a missing directory returns 0."""
        config = self._380(tmp_path)
        stub_splitobs_and_repro.skip["chandra_repro"].add(2)

        with pytest.raises(RuntimeError, match="wrote no.*repro_obi002"):
            chandra.chandra_repro_front_end("380", config, env={})

    def test_a_reprocessed_part_is_not_cut_again(self, tmp_path, stub_splitobs_and_repro):
        """Its event list already spans that part alone; a second copy would only cost disk."""
        config = self._380(tmp_path)
        observation = chandra.chandra_repro_front_end("380", config, env={})
        before = len(stub_splitobs_and_repro.calls)

        one = chandra.chandra_part_observation(observation, observation.parts[1], config)

        assert stub_splitobs_and_repro.calls[before:] == []
        assert one.event_list == observation.parts[1].event_list
        assert one.stem.endswith("_obi002")

    def test_the_reprocessed_header_is_the_one_read(self, tmp_path, stub_splitobs_and_repro):
        """The archive's merged list still says the CIAO that made it, not the one that ran."""
        config = self._380(tmp_path)
        directory = tmp_path / "records"

        with record_step(str(directory), "380", "chandra_repro") as rec:
            chandra.chandra_repro_front_end("380", config, rec=rec, env={})

        values = json.loads(next(directory.glob("*repro*.json")).read_text())["values"]
        assert values["ascds_version"] == "CIAO 4.18.0"
        assert values["repro_directories"] == [
            chandra.chandra_repro_path("380", config) + "_obi001",
            chandra.chandra_repro_path("380", config) + "_obi002",
        ]
        assert "repro_obi002/acisf00380_repro_evt2.fits" in values["reprocessed_products"]


class TestTheConfiguration:
    """
    The defaults have to survive a caller who names only the paths, which is exactly what
    ``core.download_and_process_observation`` does.
    """

    def test_a_partial_config_is_completed(self):
        config = chandra.chandra_config({"out_data_path": "/data"})

        assert config["products"] == "archive"
        assert config["psf_ecf"] == 0.9

    def test_what_the_caller_named_wins(self):
        assert chandra.chandra_config({"products": "repro"})["products"] == "repro"

    def test_none_means_every_default(self):
        assert chandra.chandra_config(None)["flare_sigma"] == 3.0

    def test_the_callers_dictionary_is_not_modified(self):
        config = {"products": "repro"}

        chandra.chandra_config(config)

        assert config == {"products": "repro"}

    def test_the_paths_come_out_absolute(self):
        config = chandra.chandra_config({"out_data_path": "relative"})

        assert os.path.isabs(config["out_data_path"])


class TestChoosingTheRoute:
    """
    Whether the archive's own products can be read, decided by looking at ``primary/``.

    Every Chandra observation directory has the same top level, so XMM's question -- is
    there a ``PPS/``? -- has no Chandra analogue one level up. The answer is one level
    down: a ``primary/`` with no ``*_evt2`` has nothing the archive route can reduce.
    Listed live for ``5644`` on 2026-09-12; the names below are that listing.
    """

    PRIMARY_5644 = [
        "acisf05644N004_cntr_img2.fits.gz",
        "acisf05644N004_cntr_img2.jpg",
        "acisf05644N004_evt2.fits.gz",
        "acisf05644N004_full_img2.fits.gz",
        "acisf05644N004_full_img2.jpg",
        "acisf05644_000N004_bpix1.fits.gz",
        "acisf05644_000N004_fov1.fits.gz",
        "orbitf240581100N001_eph1.fits.gz",
        "pcadf05644_000N001_asol1.fits.gz",
    ]

    def test_a_primary_directory_with_a_level_2_list_takes_the_archive_route(self):
        assert chandra.chandra_route_from_listing(self.PRIMARY_5644) == "archive"

    def test_one_without_takes_the_reprocessing_route(self):
        without = [name for name in self.PRIMARY_5644 if "_evt2" not in name]

        assert chandra.chandra_route_from_listing(without) == "repro"

    def test_an_empty_listing_says_nothing(self):
        """Not evidence of anything, and not a reason to fetch the whole of level 1."""
        assert chandra.chandra_route_from_listing([]) is None

    def test_an_uncompressed_event_list_counts(self):
        assert chandra.chandra_route_from_listing(["hrcf06298N006_evt2.fits"]) == "archive"

    def test_a_level_1_list_does_not_count(self):
        assert chandra.chandra_route_from_listing(["acisf05644_000N004_evt1.fits.gz"]) == "repro"

    def an_archive_holding(self, monkeypatch, entries):
        from heasarc_retrieve_pipeline import core

        asked = []

        def listing(url):
            asked.append(url)
            return entries

        monkeypatch.setattr(core, "list_archive_directory", listing)
        return asked

    def test_the_route_is_taken_from_the_archive(self, monkeypatch):
        self.an_archive_holding(monkeypatch, self.PRIMARY_5644)

        assert chandra.chandra_resolve_config({}, "https://x/4/5644/")["products"] == "archive"

    def test_it_lists_primary_and_not_the_top_level(self, monkeypatch):
        asked = self.an_archive_holding(monkeypatch, self.PRIMARY_5644)

        chandra.chandra_resolve_config({}, "https://x/4/5644")

        assert asked == ["https://x/4/5644/primary/"]

    def test_an_observation_with_no_level_2_is_moved_to_the_reprocessing_route(self, monkeypatch):
        self.an_archive_holding(monkeypatch, ["pcadf05644_000N001_asol1.fits.gz"])

        assert chandra.chandra_resolve_config({}, "https://x/4/5644/")["products"] == "repro"

    def test_a_user_who_asked_for_reprocessing_keeps_it_and_nothing_is_listed(self, monkeypatch):
        asked = self.an_archive_holding(monkeypatch, self.PRIMARY_5644)

        resolved = chandra.chandra_resolve_config({"products": "repro"}, "https://x/4/5644/")

        assert resolved["products"] == "repro"
        assert asked == []

    def test_an_archive_that_cannot_be_listed_leaves_the_route_alone(self, monkeypatch):
        """``None`` is "I could not look", not "there is nothing there"."""
        self.an_archive_holding(monkeypatch, None)

        assert chandra.chandra_resolve_config({}, "https://x/4/5644/")["products"] == "archive"

    def test_an_empty_primary_leaves_the_route_alone(self, monkeypatch):
        self.an_archive_holding(monkeypatch, [])

        assert chandra.chandra_resolve_config({}, "https://x/4/5644/")["products"] == "archive"

    def test_the_resolved_config_is_a_complete_one(self, monkeypatch):
        self.an_archive_holding(monkeypatch, self.PRIMARY_5644)

        resolved = chandra.chandra_resolve_config({"out_data_path": "/data"}, "https://x/4/5644/")

        assert resolved["psf_ecf"] == 0.9

    def test_the_callers_dictionary_is_not_modified(self, monkeypatch):
        self.an_archive_holding(monkeypatch, [])
        config = {"products": "archive"}

        chandra.chandra_resolve_config(config, "https://x/4/5644/")

        assert config == {"products": "archive"}

    def test_the_filter_follows_the_route_that_was_chosen(self, monkeypatch):
        """An observation with no level 2 ends up asking for level 1, not for nothing."""
        self.an_archive_holding(monkeypatch, ["pcadf05644_000N001_asol1.fits.gz"])

        resolved = chandra.chandra_resolve_config({}, "https://x/4/5644/")

        assert chandra.chandra_download_filter(resolved) == {
            "re_exclude": chandra.REPRO_DOWNLOAD_EXCLUDE_RE
        }


class TestTheMissionIsRegistered:
    def test_chandra_is_a_mission(self):
        from heasarc_retrieve_pipeline import core

        entry = core.MISSION_CONFIG["chandra"]

        assert entry["table"] == "chanmaster"
        assert entry["obsid_processing"] is chandra.process_chandra_obsid
        assert entry["default_config"] is chandra.DEFAULT_CONFIG

    def test_the_route_is_resolved_and_the_filter_chosen_through_the_registry(self):
        from heasarc_retrieve_pipeline import core

        entry = core.MISSION_CONFIG["chandra"]

        assert entry["download_filter"] is chandra.chandra_download_filter
        assert entry["resolve_config"] is chandra.chandra_resolve_config

    def test_its_catalogue_query_asks_for_the_columns_the_plan_names(self):
        from heasarc_retrieve_pipeline.core import obsid_query

        query = obsid_query("5644", "chandra")

        assert "public.chanmaster" in query
        for column in ("detector", "grating", "data_mode"):
            assert column in query


@pytest.fixture
def stub_every_step(monkeypatch):
    """
    Every step ``process_chandra_obsid`` calls, replaced by one that writes down its call.

    What is under test is the orchestration -- the order, and what each step is handed --
    which no unit test of a single step can reach. Each step is already tested on its own
    above, against files shaped like the real ones.
    """
    calls = []
    # Only what the flow itself reads, for its log line: every step that would read more
    # is stubbed.
    observation = SimpleNamespace(
        obsid="5644",
        detector="aciss",
        grating="NONE",
        mode="timed",
        time_resolution=SimpleNamespace(seconds=0.44104),
    )
    position = SimpleNamespace(x=4100.38, y=4131.82, chip_id=7)
    regions = SimpleNamespace(source="[sky=circle(1,1,1)]", background="[sky=annulus(1,1,2,3)]")

    def recording(name, returns):
        def step(*args, **kwargs):
            calls.append((name, args, kwargs))
            return returns

        return step

    for name, returns in (
        ("chandra_archive_front_end", observation),
        ("chandra_repro_front_end", observation),
        ("chandra_source_regions", (position, regions)),
        ("chandra_flare_lightcurve", "curve.fits"),
        ("chandra_flare_gti", np.array([[0.0, 1.0]])),
        ("chandra_clean_event_list", "cl.evt"),
        ("chandra_pileup", None),
        ("chandra_barycenter", "cl_bary.evt"),
        ("chandra_barycentered_source_events", "src_bary.evt"),
        ("chandra_compress_barycentered_events", "cl_bary.evt.gz"),
        ("chandra_calculate_spectra", None),
    ):
        monkeypatch.setattr(chandra, name, recording(name, returns))
    monkeypatch.setattr(ciao, "ciao_environment", lambda obsid, config: {"PFILES": obsid})
    return calls


class TestReducingAnObservation:
    RA, DEC = 148.96267, 69.67931

    def reduce(self, tmp_path, config=None, **kwargs):
        base = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}
        return chandra.process_chandra_obsid.fn(
            "5644", config=dict(base, **(config or {})), **kwargs
        )

    def steps(self, calls):
        return [name for name, _, _ in calls]

    def test_the_steps_run_in_the_order_the_data_force(self, tmp_path, stub_every_step):
        """
        Regions before screening, because the flare curve is measured with the source cut
        out; barycentring before the spectrum, because timing is what this is judged on
        and ``specextract`` is the slow, fragile part.
        """
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        assert self.steps(stub_every_step) == [
            "chandra_archive_front_end",
            "chandra_source_regions",
            "chandra_flare_lightcurve",
            "chandra_flare_gti",
            "chandra_clean_event_list",
            "chandra_pileup",
            "chandra_barycenter",
            "chandra_barycentered_source_events",
            "chandra_compress_barycentered_events",
            "chandra_calculate_spectra",
        ]

    def test_the_reprocessing_route_takes_the_other_front_end(self, tmp_path, stub_every_step):
        self.reduce(tmp_path, config={"products": "repro"}, ra=self.RA, dec=self.DEC)

        assert self.steps(stub_every_step)[0] == "chandra_repro_front_end"
        assert "chandra_archive_front_end" not in self.steps(stub_every_step)

    def test_the_barycentring_is_made_at_the_position_asked_for(self, tmp_path, stub_every_step):
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        ((_, args, kwargs),) = [c for c in stub_every_step if c[0] == "chandra_barycenter"]
        assert (kwargs["ra"], kwargs["dec"]) == (self.RA, self.DEC)
        assert args[2] == "cl.evt", "the cleaned list, not the raw one"

    def test_the_source_events_are_cut_from_the_barycentred_list(self, tmp_path, stub_every_step):
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        ((_, args, _),) = [
            c for c in stub_every_step if c[0] == "chandra_barycentered_source_events"
        ]
        assert args[2] == "cl_bary.evt"

    def test_it_is_the_barycentred_list_that_is_compressed(self, tmp_path, stub_every_step):
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        ((_, args, _),) = [
            c for c in stub_every_step if c[0] == "chandra_compress_barycentered_events"
        ]
        assert args[0] == "cl_bary.evt"

    def test_every_later_step_reads_the_cleaned_list(self, tmp_path, stub_every_step):
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        for name in ("chandra_pileup", "chandra_calculate_spectra"):
            ((_, args, _),) = [c for c in stub_every_step if c[0] == name]
            assert args[2] == "cl.evt", name

    def test_the_cleaned_list_is_screened_with_the_flare_gti(self, tmp_path, stub_every_step):
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        ((_, args, _),) = [c for c in stub_every_step if c[0] == "chandra_clean_event_list"]
        np.testing.assert_array_equal(args[2], [[0.0, 1.0]])

    def test_every_ciao_step_shares_one_private_environment(self, tmp_path, stub_every_step):
        """One ``PFILES`` per observation, and the same one for all of its tasks."""
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        environments = {id(kwargs["env"]) for name, _, kwargs in stub_every_step if "env" in kwargs}
        assert len(environments) == 1

    ONE_PART = [
        "chandra_part_observation",
        "chandra_source_regions",
        "chandra_flare_lightcurve",
        "chandra_flare_gti",
        "chandra_clean_event_list",
        "chandra_pileup",
        "chandra_barycenter",
        "chandra_barycentered_source_events",
        "chandra_compress_barycentered_events",
        "chandra_calculate_spectra",
    ]

    def _in_parts(self, calls, monkeypatch):
        """``1411``'s shape: parts 0 and 2, each handed back as an observation of its own."""
        parts = (SimpleNamespace(number=0), SimpleNamespace(number=2))
        whole = SimpleNamespace(
            obsid="1411",
            detector="hrci",
            grating="NONE",
            mode="imaging",
            time_resolution=SimpleNamespace(seconds=4.9e-3),
            parts=parts,
        )

        def front_end(*args, **kwargs):
            calls.append(("chandra_archive_front_end", args, kwargs))
            return whole

        def part_observation(observation, part, config, **kwargs):
            calls.append(("chandra_part_observation", (observation, part, config), kwargs))
            return SimpleNamespace(
                obsid="1411",
                detector="hrci",
                grating="NONE",
                mode="imaging",
                time_resolution=SimpleNamespace(seconds=5.0e-3),
                parts=(part,),
                part=part,
            )

        monkeypatch.setattr(chandra, "chandra_archive_front_end", front_end)
        monkeypatch.setattr(chandra, "chandra_part_observation", part_observation)
        return whole

    def _reduce_1411(self, tmp_path, config=None, **kwargs):
        base = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}
        return chandra.process_chandra_obsid.fn(
            "1411", config=dict(base, **(config or {})), **kwargs
        )

    def test_each_part_is_reduced_in_turn_as_its_own_observation(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        self._in_parts(stub_every_step, monkeypatch)

        self._reduce_1411(tmp_path, ra=self.RA, dec=self.DEC)

        assert self.steps(stub_every_step) == (
            ["chandra_archive_front_end"] + self.ONE_PART + self.ONE_PART
        )

    def test_every_step_is_handed_the_part_and_not_the_whole(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        self._in_parts(stub_every_step, monkeypatch)

        self._reduce_1411(tmp_path, ra=self.RA, dec=self.DEC)

        handed = [
            args[0].part.number
            for name, args, _ in stub_every_step
            if name
            in (
                "chandra_source_regions",
                "chandra_flare_gti",
                "chandra_barycenter",
                "chandra_calculate_spectra",
            )
        ]
        assert handed == [0, 0, 0, 0, 2, 2, 2, 2]

    def test_every_record_of_a_part_carries_its_label(self, tmp_path, stub_every_step, monkeypatch):
        """One heading per part on the report page, the way XMM has one per exposure."""
        self._in_parts(stub_every_step, monkeypatch)

        self._reduce_1411(tmp_path, ra=self.RA, dec=self.DEC)

        records = [json.loads(path.read_text()) for path in (tmp_path / "1411").rglob("*.json")]
        keyed = {
            (record["step"], record.get("key"))
            for record in records
            if record.get("step") not in (None, "chandra_front_end")
        }
        assert keyed == {
            (step, key)
            for step in (
                "source_region",
                "flare_filtering",
                "clean_event_list",
                "pileup_check",
                "barycenter",
                "calculate_spectra",
            )
            for key in ("obi000", "obi002")
        }

    def test_each_part_s_tools_log_to_files_of_its_own(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        self._in_parts(stub_every_step, monkeypatch)

        self._reduce_1411(tmp_path, ra=self.RA, dec=self.DEC)

        logs = [kwargs["log_to"] for _, _, kwargs in stub_every_step if "log_to" in kwargs]
        assert all("obi000" in log or "obi002" in log for log in logs)
        assert len(set(logs)) == len(logs)

    def test_a_part_that_fails_does_not_take_the_other_down(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        """XMM's rule for its exposures, measured on 0560590201: write the loss down, keep
        what worked."""
        self._in_parts(stub_every_step, monkeypatch)

        def broken_for_part_0(observation, *args, **kwargs):
            stub_every_step.append(("chandra_flare_lightcurve", (observation,) + args, kwargs))
            if observation.part.number == 0:
                raise RuntimeError("dmextract fell over")
            return "curve.fits"

        monkeypatch.setattr(chandra, "chandra_flare_lightcurve", broken_for_part_0)

        assert self._reduce_1411(tmp_path, ra=self.RA, dec=self.DEC) is None

        spectra = [
            args[0].part.number
            for name, args, _ in stub_every_step
            if name == "chandra_calculate_spectra"
        ]
        assert spectra == [2]
        record = next((tmp_path / "1411").rglob("*flare_filtering*obi000*.json")).read_text()
        assert "failed" in record

    def test_when_every_part_fails_the_observation_fails(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        self._in_parts(stub_every_step, monkeypatch)

        def broken(*args, **kwargs):
            raise RuntimeError("dmextract fell over")

        monkeypatch.setattr(chandra, "chandra_flare_lightcurve", broken)

        with pytest.raises(RuntimeError, match="no part of 1411 .*dmextract fell over"):
            self._reduce_1411(tmp_path, ra=self.RA, dec=self.DEC)

    def test_no_ephemeris_means_no_source_events_and_the_rest_still_runs(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        monkeypatch.setattr(chandra, "chandra_barycenter", lambda *a, **k: None)

        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        assert "chandra_barycentered_source_events" not in self.steps(stub_every_step)
        assert "chandra_compress_barycentered_events" not in self.steps(stub_every_step)
        assert "chandra_calculate_spectra" in self.steps(stub_every_step)

    def test_an_observation_with_nothing_to_reduce_is_not_a_failure(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        monkeypatch.setattr(chandra, "chandra_archive_front_end", lambda *a, **k: None)

        result = self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        assert result == chandra.NO_SCIENCE_DATA
        assert stub_every_step == [], "nothing should run on an empty observation"

    def test_without_a_position_it_reads_the_observation_and_stops_saying_why(
        self, tmp_path, stub_every_step
    ):
        result = self.reduce(tmp_path)

        assert result is None
        assert self.steps(stub_every_step) == ["chandra_archive_front_end"]
        directory = pathlib.Path(tmp_path) / "5644"
        record = next(directory.rglob("*source_region*.json")).read_text()
        assert "skipped" in record
        assert "no source position" in record

    def test_a_reduction_returns_none(self, tmp_path, stub_every_step):
        assert self.reduce(tmp_path, ra=self.RA, dec=self.DEC) is None

    def test_the_output_directories_exist_before_any_step_runs(self, tmp_path, stub_every_step):
        self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        assert (tmp_path / "5644" / "event_cl").is_dir()
        assert (tmp_path / "5644" / "products").is_dir()

    def test_a_failing_step_fails_the_observation_and_records_why(
        self, tmp_path, stub_every_step, monkeypatch
    ):
        """One observation, one outcome: there are no other cameras to carry on with."""

        def broken(*args, **kwargs):
            raise RuntimeError("dmextract fell over")

        monkeypatch.setattr(chandra, "chandra_flare_lightcurve", broken)

        with pytest.raises(RuntimeError, match="dmextract fell over"):
            self.reduce(tmp_path, ra=self.RA, dec=self.DEC)

        record = next((tmp_path / "5644").rglob("*flare_filtering*.json")).read_text()
        assert "failed" in record
