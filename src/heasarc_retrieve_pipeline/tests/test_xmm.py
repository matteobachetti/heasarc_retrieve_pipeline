"""
Offline tests for the XMM-Newton reduction.

No SAS is needed for any of these, and that is deliberate: SAS cannot be installed with
pip or conda (see :mod:`heasarc_retrieve_pipeline.sas`), so nothing in continuous
integration can run a real task. Everything that can be decided from a file name, a path
or a configuration is therefore decided in pure Python and tested here.

The file names are real. They were listed from the public S3 mirror of the HEASARC archive
on 2026-09-07, and the two observations behind them are the ones
``docs/xmm_integration_plan.md`` picked as test targets.
"""

import os
import re

import pytest

from heasarc_retrieve_pipeline import xmm


# Her X-1, the end-to-end test target. Every PPS file of it that this step cares about,
# plus the ones that are there to be ignored. Four EPIC event lists in two modes:
# pn in FastTiming, MOS1 FastUncompressed in *both* imaging and timing under the one
# exposure S004, and MOS2 PrimeFullWindow in imaging.
HER_X_1_PPS = [
    "P0153950401M1S004MIEVLI0000.FTZ",
    "P0153950401M1S004TIEVLI0000.FTZ",
    "P0153950401M2S005MIEVLI0000.FTZ",
    "P0153950401PNS003TIEVLI0000.FTZ",
    "P0153950401M1S004FBKTSR0000.FTZ",
    "P0153950401M1S004FBKTSR0000.PDF",
    "P0153950401M2S005FBKTSR0000.FTZ",
    "P0153950401M2S005FBKTSR0000.PDF",
    "P0153950401R1S001FBKTSR0000.FTZ",
    "P0153950401OBX000CALIND0000.FTZ",
    "P0153950401OBX000ATTTSR0000.FTZ",
    "P0153950401OBX000ORBTSR0000.FTZ",
    "P0153950401EPX000OBSMLI0000.FTZ",
    "P0153950401EPX000OBSMLI0000.HTM",
    "P0153950401OMX000OBSMLI0000.ASC",
    "P0153950401OMX000OBSMLI0000.FTZ",
    "P0153950401EPX000REGION0000.ASC",
    "P0153950401EPX000SUMMAR0000.HTM",
    "P0153950401OMX000SUMMAR0000.HTM",
    "PP0153950401EEVLIS000_0.HTM",
    "PP0153950401REVLIS000_0.HTM",
]

# The other measured observation: four EPIC event lists, all imaging, and MOS1 observed
# twice -- scheduled exposure S001 and unscheduled U002.
MKN_421_PPS = [
    "P0123700101M1S001MIEVLI0000.FTZ",
    "P0123700101M1U002MIEVLI0000.FTZ",
    "P0123700101M2S002MIEVLI0000.FTZ",
    "P0123700101PNS003PIEVLI0000.FTZ",
    "P0123700101M1S001FBKTSR0000.FTZ",
    "P0123700101M1U002FBKTSR0000.FTZ",
    "P0123700101M2S002FBKTSR0000.FTZ",
    "P0123700101PNS003FBKTSR0000.FTZ",
    "P0123700101OBX000CALIND0000.FTZ",
]


# What the download filter must keep out of Her X-1, and a sample of what it must drop.
# Listed from the public S3 mirror on 2026-09-07: the whole observation is 461 files and
# 205.8 MB, of which the filter keeps these 19 and 39.8 MB. The rejects below are not a
# random sample -- every one of them is a near miss of some kind.
HER_X_1_KEPT = [
    # The four EPIC event lists. 33.9 MB of the 39.8, and the reason for all the rest.
    "PPS/P0153950401M1S004MIEVLI0000.FTZ",
    "PPS/P0153950401M1S004TIEVLI0000.FTZ",
    "PPS/P0153950401M2S005MIEVLI0000.FTZ",
    "PPS/P0153950401PNS003TIEVLI0000.FTZ",
    # Background flare time series, one per exposure that has one. The RGS curve comes
    # along: it is four kilobytes, and excluding it would mean naming the instruments in
    # the regex as well as in the parser, in two places that could then disagree.
    "PPS/P0153950401M1S004FBKTSR0000.FTZ",
    "PPS/P0153950401M2S005FBKTSR0000.FTZ",
    "PPS/P0153950401R1S001FBKTSR0000.FTZ",
    # Observation-level: the calibration index that becomes SAS_CCF, and the attitude and
    # orbit the reduction and the barycentring need.
    "PPS/P0153950401OBX000ATTTSR0000.FTZ",
    "PPS/P0153950401OBX000CALIND0000.FTZ",
    "PPS/P0153950401OBX000ORBTSR0000.FTZ",
    # EPIC, and only EPIC: the source list the position is cross-checked against, the
    # regions PPS itself used, and the summary page a human opens.
    "PPS/P0153950401EPX000OBSMLI0000.FTZ",
    "PPS/P0153950401EPX000REGION0000.ASC",
    "PPS/P0153950401EPX000SUMMAR0000.HTM",
    # ODF housekeeping, 5.3 MB, downloaded on both routes.
    "ODF/0420_0153950401_SCX00000ATS.FIT.gz",
    "ODF/0420_0153950401_SCX00000RAS.ASC.gz",
    "ODF/0420_0153950401_SCX00000ROS.ASC.gz",
    "ODF/0420_0153950401_SCX00000SUM.ASC",
    "ODF/0420_0153950401_SCX00000TCS.FIT.gz",
    "ODF/0420_0153950401_SCX00000TCX.FIT.gz",
]

HER_X_1_DROPPED = [
    # Companions of files that are kept. These are what the extension has to be pinned
    # for: a PDF of a light curve is not a light curve.
    "PPS/P0153950401M1S004FBKTSR0000.PDF",
    "PPS/P0153950401EPX000OBSMLI0000.HTM",
    # The Optical Monitor's own OBSMLI, under the same product code as EPIC's.
    "PPS/P0153950401OMX000OBSMLI0000.ASC",
    "PPS/P0153950401OMX000OBSMLI0000.FTZ",
    # PPS products this pipeline makes for itself.
    "PPS/P0153950401M1S004EXPMAP1000.FTZ",
    "PPS/P0153950401M2S005SRCTSR8001.FTZ",
    "PPS/P0153950401OBX000RADMON0000.FTZ",
    # Not a PPS name at all, and one of them contains EVLI.
    "PPS/P0153950401M1S004IMAGE_8000.FTZ",
    "PPS/PP0153950401EEVLIS000_0.HTM",
    # RGS, whose event list is EVENLI and not one of the three EPIC codes.
    "PPS/P0153950401R1S001EVENLI0000.FTZ",
    # Raw telemetry. 33.5 MB of it, and the PPS route has no use for any of it.
    "ODF/0420_0153950401_PNS00304TIE.FIT.gz",
    "ODF/0420_0153950401_M1S00400AUX.FIT.gz",
    "ODF/MANIFEST.266826",
    # Same SCX00000 prefix as the housekeeping, different file.
    "ODF/0420_0153950401_SCX00000P3S.FIT.gz",
    # The catalogue cutouts and the Optical Monitor mosaic, 5.3 MB of pictures.
    "4XMM/C0153950401EPX000SRCIMG8010001.png",
    "om_mosaic/0153950401_UVW2_E.fits.gz",
]

HER_X_1_ARCHIVE = HER_X_1_KEPT + HER_X_1_DROPPED

# 0973390101 has pps_flag = "Y" in xmmmaster and no PPS directory at HEASARC: 103 files,
# all of them under ODF/. It is the observation that proves the route has to be probed.
NO_PPS_ARCHIVE = [
    "ODF/4720_0973390101_M1U00200AUX.FIT.gz",
    "ODF/4720_0973390101_M1U00210IME.FIT.gz",
    "ODF/4720_0973390101_SCX00000ATS.FIT.gz",
    "ODF/4720_0973390101_SCX00000RAS.ASC.gz",
    "ODF/4720_0973390101_SCX00000ROS.ASC.gz",
    "ODF/4720_0973390101_SCX00000SUM.ASC",
    "ODF/4720_0973390101_SCX00000TCS.FIT.gz",
    "ODF/4720_0973390101_SCX00000TCX.FIT.gz",
]


def what_a_filter_keeps(arguments, entries, obsid="0153950401"):
    """
    Run a download filter over a recorded listing, the way a transport would.

    Both transports match against the whole remote name, not the basename: an HTTPS URL
    for one, a bucket key for the other. They are spelled differently and the filter has
    to work on either, so both are tried here and the answers must agree.
    """
    include = arguments.get("re_include", "")
    exclude = arguments.get("re_exclude", "")
    include = re.compile(include) if include else None
    exclude = re.compile(exclude) if exclude else None

    kept = {}
    for flavour, base in [
        ("https", f"https://heasarc.gsfc.nasa.gov/FTP/xmm/data/rev0/{obsid}/"),
        ("s3", f"xmm/data/rev0/{obsid}/"),
    ]:
        kept[flavour] = [
            entry
            for entry in entries
            if (include is None or include.search(base + entry))
            and not (exclude is not None and exclude.search(base + entry))
        ]
    assert kept["https"] == kept["s3"], "the filter reads an S3 key and a URL differently"
    return kept["s3"]


def a_downloaded_observation(tmp_path, obsid, names):
    """Lay the named PPS files out where a download would have put them."""
    config = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}
    pps = tmp_path / obsid / "PPS"
    pps.mkdir(parents=True)
    for name in names:
        (pps / name).write_text("not really a FITS file\n")
    return config


class TestReadingAPpsFileName:
    """
    A PPS name is ``P<OBSID><INST><EXPID><PRODUCT><NNNN>.<EXT>``, and every field of it
    means something the reduction needs. Nothing here opens a file.
    """

    def test_every_field_is_read_off_the_name(self):
        parsed = xmm.parse_pps_name("P0153950401PNS003TIEVLI0000.FTZ")

        assert parsed == xmm.PpsName(
            obsid="0153950401",
            instrument="PN",
            expid="S003",
            product="TIEVLI",
            index="0000",
            extension="FTZ",
        )

    def test_an_unscheduled_exposure_reads_like_a_scheduled_one(self):
        """``U002`` is an unscheduled exposure; only the letter differs."""
        parsed = xmm.parse_pps_name("P0123700101M1U002MIEVLI0000.FTZ")

        assert (parsed.instrument, parsed.expid) == ("M1", "U002")

    def test_an_observation_level_product_is_read_too(self):
        """``CALIND`` belongs to the observation, not to a camera: ``OB`` and ``X000``."""
        parsed = xmm.parse_pps_name("P0153950401OBX000CALIND0000.FTZ")

        assert (parsed.instrument, parsed.expid, parsed.product) == ("OB", "X000", "CALIND")

    @pytest.mark.parametrize(
        "name",
        [
            # The PPS summary pages have a different shape entirely, and one of them has
            # EVLI in the middle of it. Reading it as an event list would invent an
            # observation called "0153950401E".
            "PP0153950401EEVLIS000_0.HTM",
            "PP0153950401REVLIS000_0.HTM",
            "P0153950401PNS003TIEVLI0000",
            "0153950401PNS003TIEVLI0000.FTZ",
            "nu80002092008A06_cl.evt",
            "",
        ],
    )
    def test_anything_of_another_shape_is_not_a_pps_name(self, name):
        assert xmm.parse_pps_name(name) is None


class TestWhichCameraAndWhichMode:
    def test_the_camera_comes_from_the_instrument_code(self):
        assert xmm.EPIC_INSTRUMENTS == {"PN": "pn", "M1": "mos1", "M2": "mos2"}

    @pytest.mark.parametrize("code", ["R1", "R2", "OM", "EP", "OB", "RG"])
    def test_what_is_not_an_epic_camera_is_not_one(self, code):
        """RGS, the Optical Monitor, and the products that belong to no camera."""
        assert code not in xmm.EPIC_INSTRUMENTS

    def test_the_mode_comes_from_the_product_code(self):
        """``PIEVLI`` and ``MIEVLI`` are pn and MOS imaging; ``TIEVLI`` is either in
        timing."""
        assert xmm.EVENT_LIST_MODES == {
            "PIEVLI": xmm.IMAGING,
            "MIEVLI": xmm.IMAGING,
            "TIEVLI": xmm.TIMING,
        }


class TestFindingTheExposuresOfAnObservation:
    """
    The front end of the PPS route: what was observed, in what mode, and which file holds
    it. Pure parsing -- the files here are empty and it makes no difference.
    """

    def exposures(self, tmp_path, obsid=None, names=None):
        obsid = obsid or "0153950401"
        config = a_downloaded_observation(tmp_path, obsid, HER_X_1_PPS if names is None else names)
        return xmm.xmm_exposures_from_pps(obsid, config)

    def test_it_finds_every_epic_event_list(self, tmp_path):
        exposures = self.exposures(tmp_path)

        assert [(e.instrument, e.expid, e.mode) for e in exposures] == [
            ("pn", "S003", xmm.TIMING),
            ("mos1", "S004", xmm.IMAGING),
            ("mos1", "S004", xmm.TIMING),
            ("mos2", "S005", xmm.IMAGING),
        ]

    def test_one_exposure_can_hold_two_modes_at_once(self, tmp_path):
        """
        The reason the key is ``(instrument, expid, mode)`` and not ``(instrument,
        expid)``. MOS ``FastUncompressed`` reads the central CCD in timing and the outer
        six in imaging, and PPS writes both under the one exposure ``M1S004``. Keyed on
        the exposure alone, one of the two would be dropped without a word.
        """
        exposures = self.exposures(tmp_path)
        mos1 = [e for e in exposures if e.instrument == "mos1"]

        assert len(mos1) == 2
        assert {e.expid for e in mos1} == {"S004"}
        assert {e.mode for e in mos1} == {xmm.IMAGING, xmm.TIMING}

    def test_each_exposure_names_its_own_event_list(self, tmp_path):
        exposures = self.exposures(tmp_path)

        assert [os.path.basename(e.event_list) for e in exposures] == [
            "P0153950401PNS003TIEVLI0000.FTZ",
            "P0153950401M1S004MIEVLI0000.FTZ",
            "P0153950401M1S004TIEVLI0000.FTZ",
            "P0153950401M2S005MIEVLI0000.FTZ",
        ]

    def test_the_event_list_is_where_the_download_left_it(self, tmp_path):
        exposures = self.exposures(tmp_path)

        assert os.path.isfile(exposures[0].event_list)

    def test_the_flare_light_curve_is_paired_with_its_exposure(self, tmp_path):
        """One ``FBKTSR`` per exposure, and it serves both of that exposure's modes."""
        exposures = self.exposures(tmp_path)
        mos1 = [e for e in exposures if e.instrument == "mos1"]

        assert all(
            os.path.basename(e.flare_lightcurve) == "P0153950401M1S004FBKTSR0000.FTZ" for e in mos1
        )

    def test_an_exposure_with_no_light_curve_says_so(self, tmp_path):
        """
        Real, and not an oversight: PPS writes no background time series for the pn
        timing exposure of this observation. Step 6 has to find its own flares there.
        """
        exposures = self.exposures(tmp_path)
        (pn,) = [e for e in exposures if e.instrument == "pn"]

        assert pn.flare_lightcurve is None

    def test_the_pdf_beside_a_light_curve_is_not_mistaken_for_it(self, tmp_path):
        exposures = self.exposures(tmp_path)

        assert all(not str(e.flare_lightcurve).endswith(".PDF") for e in exposures)

    def test_the_rgs_light_curve_belongs_to_no_epic_exposure(self, tmp_path):
        """``R1S001FBKTSR`` is in the same directory and is not ours."""
        exposures = self.exposures(tmp_path)

        assert all("R1S001" not in str(e.flare_lightcurve) for e in exposures)

    def test_the_other_observation_reads_as_four_imaging_exposures(self, tmp_path):
        exposures = self.exposures(tmp_path, "0123700101", MKN_421_PPS)

        assert [(e.instrument, e.expid, e.mode) for e in exposures] == [
            ("pn", "S003", xmm.IMAGING),
            ("mos1", "S001", xmm.IMAGING),
            ("mos1", "U002", xmm.IMAGING),
            ("mos2", "S002", xmm.IMAGING),
        ]

    def test_two_exposures_of_one_camera_keep_their_own_light_curves(self, tmp_path):
        exposures = self.exposures(tmp_path, "0123700101", MKN_421_PPS)
        mos1 = {
            e.expid: os.path.basename(e.flare_lightcurve)
            for e in exposures
            if e.instrument == "mos1"
        }

        assert mos1 == {
            "S001": "P0123700101M1S001FBKTSR0000.FTZ",
            "U002": "P0123700101M1U002FBKTSR0000.FTZ",
        }

    def test_an_observation_with_no_epic_event_lists_yields_none(self, tmp_path):
        """
        Which is what the flow will report as ``NO_SCIENCE_DATA``: a real observation with
        nothing in it for this pipeline, counted separately and left on disk, the same
        treatment a NuSTAR slew gets. Note that this is "no event lists **of any mode**":
        a timing-only observation is science, not an empty one.
        """
        names = [n for n in HER_X_1_PPS if "EVLI" not in n]

        assert self.exposures(tmp_path, names=names) == []

    def test_an_observation_that_was_never_downloaded_yields_none(self, tmp_path):
        config = {"input_data_path": str(tmp_path), "out_data_path": str(tmp_path)}

        assert xmm.xmm_exposures_from_pps("0153950401", config) == []

    def test_the_submode_is_not_something_a_name_can_tell(self, tmp_path):
        """``FastTiming`` and ``FastUncompressed`` are header keywords, not name fields."""
        exposures = self.exposures(tmp_path)

        assert all(e.submode is None for e in exposures)


class TestWhereTheFilesGo:
    OBSID = "0153950401"
    CONFIG = {"input_data_path": "/data/in", "out_data_path": "/data/out"}

    def test_the_downloaded_products_are_read_from_the_input_path(self):
        assert xmm.xmm_pps_path(self.OBSID, self.CONFIG) == "/data/in/0153950401/PPS"
        assert xmm.xmm_odf_path(self.OBSID, self.CONFIG) == "/data/in/0153950401/ODF"

    def test_the_archive_spells_those_two_in_capitals(self):
        """HEASARC serves ``PPS/`` and ``ODF/``, and a case-insensitive filesystem will
        hide a mistake here until the pipeline runs on Linux."""
        assert xmm.xmm_pps_path(self.OBSID, self.CONFIG).endswith("/PPS")

    def test_the_reduction_writes_under_the_output_path(self):
        assert xmm.xmm_base_output_path(self.OBSID, self.CONFIG) == "/data/out/0153950401"

    def test_the_subdirectories_are_the_ones_the_report_already_knows(self):
        """``report.OBSERVATION_SUBDIRECTORIES`` recognises ``event_cl`` and ``products``,
        so ``hrp-report`` finds an XMM tree with no change of its own."""
        assert xmm.xmm_pipeline_output_path(self.OBSID, self.CONFIG).endswith("/event_cl")
        assert xmm.xmm_product_output_path(self.OBSID, self.CONFIG).endswith("/products")

    def test_a_path_builder_takes_the_obsid_first(self):
        """Matching NuSTAR, which is the majority and what ``check_name_length`` drives
        from. NICER takes them the other way round and is the odd one out."""
        for builder in (
            xmm.xmm_base_output_path,
            xmm.xmm_pps_path,
            xmm.xmm_odf_path,
            xmm.xmm_pipeline_output_path,
            xmm.xmm_product_output_path,
        ):
            assert builder(self.OBSID, self.CONFIG).startswith("/data/")


class TestFindingTheObservationLevelProducts:
    def test_the_calibration_index_is_the_one_the_soc_used(self, tmp_path):
        config = a_downloaded_observation(tmp_path, "0153950401", HER_X_1_PPS)

        calind = xmm.xmm_calind_file("0153950401", config)

        assert os.path.basename(calind) == "P0153950401OBX000CALIND0000.FTZ"

    def test_the_source_list_is_epic_and_not_the_optical_monitor(self, tmp_path):
        """
        The trap this guards is real: the Optical Monitor emits an ``OBSMLI`` too, so
        this observation carries ``EPX000OBSMLI`` *and* ``OMX000OBSMLI``. Reading the
        wrong one would cross-check the position against the wrong catalogue.
        """
        config = a_downloaded_observation(tmp_path, "0153950401", HER_X_1_PPS)

        source_list = xmm.xmm_source_list_file("0153950401", config)

        assert os.path.basename(source_list) == "P0153950401EPX000OBSMLI0000.FTZ"

    def test_the_html_companion_is_not_the_source_list(self, tmp_path):
        config = a_downloaded_observation(tmp_path, "0153950401", HER_X_1_PPS)

        assert xmm.xmm_source_list_file("0153950401", config).endswith(".FTZ")

    def test_a_product_that_is_not_there_is_reported_as_missing(self, tmp_path):
        config = a_downloaded_observation(tmp_path, "0153950401", MKN_421_PPS[:1])

        assert xmm.xmm_calind_file("0153950401", config) is None
        assert xmm.xmm_source_list_file("0153950401", config) is None


class TestWhatIsWorthDownloading:
    """
    An XMM observation is 200 MB to 1.2 GB, and a reduction of the EPIC cameras wants
    about a fortieth of it. The filter is what makes a long observation an ordinary
    download: *short* and *small* are different axes, and without it SAX J1808's 35 ks
    would be 393 MB instead of 78.

    Everything here runs against listings recorded from the real archive, because both
    traps in this regex were found by listing observations and neither would have been
    found by reasoning about the file naming scheme.
    """

    def test_the_pps_route_keeps_exactly_what_it_needs(self):
        arguments = xmm.xmm_download_filter({"products": "pps"})

        assert what_a_filter_keeps(arguments, HER_X_1_ARCHIVE) == HER_X_1_KEPT

    def test_every_epic_event_list_survives(self):
        kept = what_a_filter_keeps(xmm.xmm_download_filter({"products": "pps"}), HER_X_1_ARCHIVE)

        assert [name for name in kept if "EVLI" in name] == [
            "PPS/P0153950401M1S004MIEVLI0000.FTZ",
            "PPS/P0153950401M1S004TIEVLI0000.FTZ",
            "PPS/P0153950401M2S005MIEVLI0000.FTZ",
            "PPS/P0153950401PNS003TIEVLI0000.FTZ",
        ]

    def test_the_optical_monitor_source_list_is_left_behind(self):
        """The trap that would cross-check an X-ray position against an optical catalogue:
        OM emits an OBSMLI under the same product code EPIC does."""
        kept = what_a_filter_keeps(xmm.xmm_download_filter({"products": "pps"}), HER_X_1_ARCHIVE)

        assert "PPS/P0153950401OMX000OBSMLI0000.FTZ" not in kept
        assert "PPS/P0153950401EPX000OBSMLI0000.FTZ" in kept

    def test_the_pictures_beside_a_product_are_not_the_product(self):
        kept = what_a_filter_keeps(xmm.xmm_download_filter({"products": "pps"}), HER_X_1_ARCHIVE)

        assert not [name for name in kept if name.endswith((".PDF", ".PNG", ".png"))]

    def test_the_housekeeping_comes_on_the_pps_route_too(self):
        """Six files, 5.3 MB, and the barycentring needs them: the PPS products carry no
        orbit or attitude of their own."""
        kept = what_a_filter_keeps(xmm.xmm_download_filter({"products": "pps"}), HER_X_1_ARCHIVE)

        assert sorted(name.split("SCX00000")[-1] for name in kept if name.startswith("ODF/")) == [
            "ATS.FIT.gz",
            "RAS.ASC.gz",
            "ROS.ASC.gz",
            "SUM.ASC",
            "TCS.FIT.gz",
            "TCX.FIT.gz",
        ]

    def test_the_raw_telemetry_stays_at_the_archive(self):
        kept = what_a_filter_keeps(xmm.xmm_download_filter({"products": "pps"}), HER_X_1_ARCHIVE)

        assert "ODF/0420_0153950401_PNS00304TIE.FIT.gz" not in kept
        assert "ODF/0420_0153950401_SCX00000P3S.FIT.gz" not in kept

    def test_the_odf_route_takes_the_whole_odf(self):
        arguments = xmm.xmm_download_filter({"products": "odf"})

        assert what_a_filter_keeps(arguments, HER_X_1_ARCHIVE) == [
            name for name in HER_X_1_ARCHIVE if name.startswith("ODF/")
        ]

    def test_the_odf_route_wants_none_of_the_archive_reduction(self):
        kept = what_a_filter_keeps(xmm.xmm_download_filter({"products": "odf"}), HER_X_1_ARCHIVE)

        assert not [name for name in kept if name.startswith("PPS/")]

    def test_an_observation_with_no_pps_yields_only_housekeeping(self):
        """Which is the whole problem the route probe exists to solve: this succeeds, and
        leaves nothing to reduce."""
        kept = what_a_filter_keeps(
            xmm.xmm_download_filter({"products": "pps"}), NO_PPS_ARCHIVE, obsid="0973390101"
        )

        assert len(kept) == 6
        assert all("SCX00000" in name for name in kept)

    def test_another_observation_is_not_swept_in(self):
        """The filter is applied to a whole URL or bucket key, and the OBSID is part of
        both. It must not match a neighbour's files if a listing ever straddles two."""
        kept = what_a_filter_keeps(
            xmm.xmm_download_filter({"products": "pps"}),
            ["PPS/P0123700101PNS003PIEVLI0000.FTZ"] + HER_X_1_KEPT,
        )

        assert "PPS/P0123700101PNS003PIEVLI0000.FTZ" in kept

    def test_the_filter_names_only_what_recursive_download_takes(self):
        for products in ("pps", "odf"):
            assert set(xmm.xmm_download_filter({"products": products})) <= {
                "re_include",
                "re_exclude",
            }

    def test_an_unknown_route_is_refused_rather_than_guessed(self):
        """Silently downloading the whole 1.2 GB observation is not a good answer to a
        typo in a configuration file."""
        with pytest.raises(ValueError, match="pps"):
            xmm.xmm_download_filter({"products": "PPS "})


class TestChoosingTheRoute:
    """
    Which reduction an observation gets, decided by looking rather than by asking.

    ``xmmmaster`` carries a ``pps_flag`` saying whether the Pipeline Processing System
    reduced an observation, and 22779 of 25087 rows say ``Y``. It is a hint and not a
    guarantee: ``0973390101`` says ``Y`` and HEASARC mirrors no PPS directory for it at
    all -- 103 files, every one of them under ``ODF/``. Trusting the flag there means
    downloading the housekeeping, finding no event lists, and reporting a real
    observation as empty.

    The listing is one request and it is authoritative, so the flag is not read at all.
    """

    def test_an_observation_with_a_pps_directory_takes_the_pps_route(self):
        assert xmm.xmm_route_from_listing(["4XMM/", "ODF/", "PPS/", "om_mosaic/"]) == "pps"

    def test_an_observation_with_only_an_odf_takes_the_odf_route(self):
        assert xmm.xmm_route_from_listing(["ODF/"]) == "odf"

    def test_a_directory_holding_neither_says_so(self):
        """Rather than guessing. An observation directory with no PPS and no ODF is not
        something this module has ever seen, and inventing a route for it would turn an
        unknown into a download."""
        assert xmm.xmm_route_from_listing(["4XMM/", "om_mosaic/"]) is None

    def test_an_empty_directory_says_so_too(self):
        assert xmm.xmm_route_from_listing([]) is None

    def test_a_listing_without_slashes_reads_the_same(self):
        """Not every transport marks its directories, and the answer must not depend on
        which one asked."""
        assert xmm.xmm_route_from_listing(["ODF", "PPS"]) == "pps"

    def an_archive_holding(self, monkeypatch, entries):
        from heasarc_retrieve_pipeline import core

        monkeypatch.setattr(core, "list_archive_directory", lambda url: entries)

    def test_the_route_is_taken_from_the_archive(self, monkeypatch):
        self.an_archive_holding(monkeypatch, ["ODF/", "PPS/"])

        assert xmm.xmm_resolve_config({}, "https://x/0153950401/")["products"] == "pps"

    def test_an_observation_with_no_pps_is_moved_to_the_odf_route(self, monkeypatch):
        self.an_archive_holding(monkeypatch, ["ODF/"])

        assert xmm.xmm_resolve_config({}, "https://x/0973390101/")["products"] == "odf"

    def test_a_user_who_asked_for_the_odf_route_keeps_it(self, monkeypatch):
        """The demotion only ever runs one way. Reprocessing from the telemetry is a
        legitimate thing to ask for even when the archive's own reduction is right
        there -- an old SAS version, or a doubt about the products."""
        self.an_archive_holding(monkeypatch, ["ODF/", "PPS/"])

        resolved = xmm.xmm_resolve_config({"products": "odf"}, "https://x/0153950401/")

        assert resolved["products"] == "odf"

    def test_the_archive_is_not_even_asked_in_that_case(self, monkeypatch):
        from heasarc_retrieve_pipeline import core

        asked = []
        monkeypatch.setattr(core, "list_archive_directory", lambda url: asked.append(url))

        xmm.xmm_resolve_config({"products": "odf"}, "https://x/0153950401/")

        assert asked == []

    def test_an_archive_that_cannot_be_listed_leaves_the_route_alone(self, monkeypatch):
        """``None`` means "I could not look", not "there is nothing there". Downgrading
        on a timeout would fetch a quarter of a gigabyte of telemetry for an observation
        whose PPS products are sitting in the archive."""
        self.an_archive_holding(monkeypatch, None)

        assert xmm.xmm_resolve_config({}, "https://x/0153950401/")["products"] == "pps"

    def test_a_directory_holding_neither_leaves_the_route_alone(self, monkeypatch):
        self.an_archive_holding(monkeypatch, ["4XMM/"])

        assert xmm.xmm_resolve_config({}, "https://x/0153950401/")["products"] == "pps"

    def test_the_resolved_config_is_a_complete_one(self, monkeypatch):
        """It is what the reduction runs with, so the partial config
        ``core.download_and_process_observation`` builds has to come out whole."""
        self.an_archive_holding(monkeypatch, ["ODF/", "PPS/"])

        resolved = xmm.xmm_resolve_config({"out_data_path": "/data"}, "https://x/0153950401/")

        assert resolved["src_radius_arcsec"] == 30.0
        assert resolved["flare_rate_limit"] == {"pn": 0.4, "mos": 0.35}

    def test_the_caller_dictionary_is_not_modified(self, monkeypatch):
        self.an_archive_holding(monkeypatch, ["ODF/"])
        config = {"products": "pps"}

        xmm.xmm_resolve_config(config, "https://x/0973390101/")

        assert config == {"products": "pps"}

    def test_the_filter_follows_the_route_that_was_chosen(self, monkeypatch):
        """The two halves together: an observation with no PPS directory ends up asking
        for the whole ODF, not for PPS products that are not there."""
        self.an_archive_holding(monkeypatch, ["ODF/"])

        resolved = xmm.xmm_resolve_config({}, "https://x/0973390101/")
        kept = what_a_filter_keeps(
            xmm.xmm_download_filter(resolved), NO_PPS_ARCHIVE, obsid="0973390101"
        )

        assert kept == NO_PPS_ARCHIVE


class TestTheConfiguration:
    """
    The mission defaults have to survive a caller who only names the paths, which is
    exactly what ``core.download_and_process_observation`` does.
    """

    def test_the_defaults_are_there_when_nothing_is_given(self):
        config = xmm.xmm_config(None)

        assert config["products"] == "pps"

    def test_a_caller_who_names_only_the_paths_still_gets_them(self, tmp_path):
        """
        ``absolute_config`` replaces the whole configuration with the default only when it
        is given ``None``; a partial config passes through as it is, and every missing key
        then has to be defaulted again at the point of use. Merging once, here, is what
        keeps the defaults in one place -- and what stops ``config["products"]`` raising
        ``KeyError`` in the middle of a reduction.
        """
        config = xmm.xmm_config({"out_data_path": str(tmp_path)})

        assert config["products"] == "pps"
        assert config["out_data_path"] == str(tmp_path)

    def test_what_the_caller_says_wins(self):
        config = xmm.xmm_config({"products": "odf"})

        assert config["products"] == "odf"

    def test_the_paths_are_pinned_before_anything_can_chdir(self, tmp_path):
        """Relative paths mean "wherever this process is standing", and a worker process
        stands somewhere else. See ``utils.absolute_config``."""
        config = xmm.xmm_config({"out_data_path": "out"})

        assert os.path.isabs(config["out_data_path"])

    def test_the_defaults_are_not_modified(self):
        xmm.xmm_config({"products": "odf"})

        assert xmm.DEFAULT_CONFIG["products"] == "pps"

    def test_the_caller_dictionary_is_not_modified(self):
        given = {"products": "odf"}

        xmm.xmm_config(given)

        assert given == {"products": "odf"}

    def test_a_nested_default_is_not_shared_between_runs(self):
        """``flare_rate_limit`` is a dictionary of its own, so a shallow copy would hand
        every run the same one."""
        first = xmm.xmm_config(None)

        first["flare_rate_limit"]["pn"] = 99.0

        assert xmm.xmm_config(None)["flare_rate_limit"]["pn"] == 0.4
