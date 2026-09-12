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

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import chandra
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
