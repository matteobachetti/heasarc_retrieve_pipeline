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
