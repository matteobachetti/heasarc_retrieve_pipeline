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

import copy
import glob
import gzip
import os
import re
import warnings
from types import SimpleNamespace

import numpy as np
import pytest

from heasarc_retrieve_pipeline import sas, xmm
from heasarc_retrieve_pipeline.diagnostics import record_step
from heasarc_retrieve_pipeline.utils import NO_SCIENCE_DATA


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


def a_flare_lightcurve(
    path,
    rates,
    tstart=133579244.991178,
    cadence=26.0,
    threshold=54.2379532,
    instrument="EMOS1",
    errors=None,
):
    """
    Write a file shaped like a PPS ``FBKTSR``, with the numbers of a real one.

    The layout is Her X-1's ``P0153950401M1S004FBKTSR0000.FTZ``: a ``RATE`` extension of
    ``TIME``, ``RATE``, ``ERROR``, ``FRACEXP`` and ``T_ELAPSED``, times at bin *centres*
    offset by half a bin from ``TSTART``, and the threshold PPS chose in ``FLCUTTHR``.
    """
    from astropy.io import fits

    rates = np.asarray(rates, dtype=float)
    times = tstart + cadence / 2 + cadence * np.arange(len(rates))
    columns = fits.ColDefs(
        [
            fits.Column(name="TIME", format="D", unit="s", array=times),
            fits.Column(name="RATE", format="E", unit="count/s", array=rates),
            fits.Column(
                name="ERROR",
                format="E",
                unit="count/s",
                array=np.ones_like(rates) if errors is None else np.asarray(errors, float),
            ),
            fits.Column(name="FRACEXP", format="E", array=np.ones_like(rates)),
            fits.Column(name="T_ELAPSED", format="D", array=times - tstart),
        ]
    )
    rate_hdu = fits.BinTableHDU.from_columns(columns, name="RATE")
    header = rate_hdu.header
    header["TSTART"] = tstart
    header["TSTOP"] = tstart + cadence * len(rates)
    header["TIMEDEL"] = cadence
    header["INSTRUME"] = instrument
    header["HDUCLAS1"] = "LIGHTCURVE"
    if threshold is not None:
        header["FLCUTTHR"] = (threshold, "Optimised flare cut threshold")

    fits.HDUList([fits.PrimaryHDU(), rate_hdu]).writeto(path, overwrite=True)
    return str(path)


def an_exposure(path, instrument="mos1", mode=xmm.IMAGING, event_list="events.FTZ"):
    return xmm.Exposure(
        instrument=instrument,
        expid="S004",
        mode=mode,
        event_list=event_list,
        flare_lightcurve=path,
    )


class TestReadingAFlareLightCurve:
    """
    What a ``FBKTSR`` holds, and what has to be read out of it rather than assumed.

    ``epiclccorr`` writes the background rate of one exposure, and the header carries the
    two things the thresholding needs beside the numbers: the bin width, and PPS's own
    view of where the cut belongs.
    """

    def test_the_curve_comes_back(self, tmp_path):
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0, 2.0, 3.0])

        curve = xmm.read_flare_lightcurve(path)

        assert curve.rate.tolist() == [1.0, 2.0, 3.0]

    def test_the_times_are_bin_centres(self, tmp_path):
        """Measured on the real file: the first TIME sits half a bin after TSTART, and
        the spacing is exactly TIMEDEL. It matters, because the thresholding takes each
        sample to cover ``[t - cadence/2, t + cadence/2]``."""
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0, 2.0], tstart=100.0, cadence=26.0)

        curve = xmm.read_flare_lightcurve(path)

        assert curve.time.tolist() == [113.0, 139.0]
        assert curve.tstart == 100.0
        assert curve.tstop == 152.0

    def test_the_cadence_is_read_from_the_header(self, tmp_path):
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0, 2.0], cadence=10.0)

        assert xmm.read_flare_lightcurve(path).cadence == 10.0

    def test_a_curve_without_timedel_falls_back_to_the_spacing(self, tmp_path):
        from astropy.io import fits

        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0, 2.0, 3.0], cadence=26.0)
        with fits.open(path, mode="update") as hdul:
            del hdul["RATE"].header["TIMEDEL"]

        assert xmm.read_flare_lightcurve(path).cadence == 26.0

    def test_the_threshold_pps_chose_is_read(self, tmp_path):
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0], threshold=54.2379532)

        assert xmm.read_flare_lightcurve(path).pps_threshold == 54.2379532

    def test_a_curve_without_one_says_so(self, tmp_path):
        """Every EPIC FBKTSR carries FLCUTTHR -- 41 of 41 across twelve observations --
        and the RGS ones do not. RGS is not reduced here, so this is only ever reached by
        something unexpected, and it must not be an exception."""
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0], threshold=None)

        assert xmm.read_flare_lightcurve(path).pps_threshold is None

    def test_a_signalling_nan_is_read_without_a_warning(self, tmp_path):
        """
        PPS writes dead bins as a *signalling* NaN, bit pattern ``0x7f800001``, and not as
        the quiet ``0x7fc00000`` that astropy or numpy would produce. Widening one to
        double raises the processor's invalid-operation flag, which numpy reports as a
        warning -- once per dead bin, and Mkn 421's pn curve has 322 of them.
        """
        from astropy.io import fits

        signalling = np.array([0x3F800000, 0x7F800001], dtype=">u4").view(">f4")
        column = fits.Column(name="RATE", format="E", array=signalling)
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0, 1.0])
        with fits.open(path, mode="update") as hdul:
            hdul["RATE"].data["RATE"] = column.array

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            curve = xmm.read_flare_lightcurve(path)

        assert np.isnan(curve.rate[1])
        assert curve.rate[0] == 1.0


class TestChoosingTheFlareThreshold:
    """
    Which number the light curve is cut at.

    The SAS cookbook's 0.4 and 0.35 counts/s are *not* it, and this is the thing about
    step 6 that had to be measured rather than reasoned about. Those numbers are for a
    light curve you build yourself with ``evselect`` above 10 keV over the whole field.
    ``FBKTSR`` is made by ``epiclccorr`` and is on quite another scale: the median rate is
    36.0 counts/s on Her X-1's MOS1, 1.0 on Mkn 421's, and 20.5 on the *unscheduled*
    exposure of that same camera in that same observation. A factor of twenty between two
    exposures of one observation is the proof that no single number can do this job.

    PPS has already done it, per exposure, in ``FLCUTTHR``.
    """

    def a_curve(self, threshold):
        return xmm.FlareLightCurve(
            time=np.array([0.0, 26.0]),
            rate=np.array([1.0, 2.0]),
            rate_error=None,
            cadence=26.0,
            tstart=-13.0,
            tstop=39.0,
            pps_threshold=threshold,
        )

    def test_the_default_is_what_pps_chose(self):
        threshold, source = xmm.xmm_flare_threshold(self.a_curve(54.24), "mos1", xmm.xmm_config({}))

        assert threshold == 54.24
        assert source == "pps"

    def test_a_configured_limit_wins(self):
        config = xmm.xmm_config({"flare_rate_limit": {"mos1": 3.0}})

        threshold, source = xmm.xmm_flare_threshold(self.a_curve(54.24), "mos1", config)

        assert threshold == 3.0
        assert source == "config"

    def test_a_limit_may_name_the_camera_family(self):
        """``mos`` is how the SAS documentation talks about the pair, and writing the same
        number twice is how the two come to disagree."""
        config = xmm.xmm_config({"flare_rate_limit": {"mos": 3.0}})

        assert xmm.xmm_flare_threshold(self.a_curve(54.24), "mos1", config)[0] == 3.0
        assert xmm.xmm_flare_threshold(self.a_curve(54.24), "mos2", config)[0] == 3.0

    def test_the_camera_itself_beats_its_family(self):
        config = xmm.xmm_config({"flare_rate_limit": {"mos": 3.0, "mos2": 9.0}})

        assert xmm.xmm_flare_threshold(self.a_curve(54.24), "mos2", config)[0] == 9.0

    def test_a_limit_for_another_camera_does_not_apply(self):
        config = xmm.xmm_config({"flare_rate_limit": {"pn": 3.0}})

        threshold, source = xmm.xmm_flare_threshold(self.a_curve(54.24), "mos1", config)

        assert (threshold, source) == (54.24, "pps")

    def test_with_neither_there_is_no_threshold(self):
        threshold, source = xmm.xmm_flare_threshold(self.a_curve(None), "mos1", xmm.xmm_config({}))

        assert threshold is None
        assert source is None

    def test_the_cookbook_numbers_are_kept_for_the_route_they_belong_to(self):
        """They are right for the light curve step 10 builds with ``evselect``, and wrong
        for this one. Losing them would mean rediscovering them."""
        assert xmm.xmm_config({})["odf_flare_rate_limit"] == {"pn": 0.4, "mos": 0.35}


class TestTheFlareGoodTimeIntervals:
    """
    Which stretches of an exposure survive the background cut.

    Pure Python throughout -- ``evselect`` never sees a light curve -- so this is the part
    of the XMM reduction with the most offline test value after the name parsing. The
    interval arithmetic itself is ``utils``', already tested there; what is tested here is
    that the right numbers are handed to it and the right thing recorded afterwards.
    """

    def test_a_quiet_exposure_keeps_all_of_itself(self, tmp_path):
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0, 1.0, 1.0], tstart=0.0, threshold=5.0)

        gti = xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({}))

        assert gti.tolist() == [[0.0, 78.0]]

    def test_a_flare_in_the_middle_is_cut_out(self, tmp_path):
        path = a_flare_lightcurve(
            tmp_path / "lc.fits", [1.0, 9.0, 1.0], tstart=0.0, cadence=10.0, threshold=5.0
        )

        gti = xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({}))

        assert gti.tolist() == [[0.0, 10.0], [20.0, 30.0]]

    def test_a_bin_is_cut_over_its_own_width(self, tmp_path):
        """Not from one sample time to the next: a sample stands for the bin around it,
        so a single bad bin removes half a cadence on either side of its centre."""
        path = a_flare_lightcurve(
            tmp_path / "lc.fits", [1.0, 9.0, 1.0, 1.0], tstart=0.0, cadence=10.0, threshold=5.0
        )

        removed = xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({}))

        assert removed.tolist() == [[0.0, 10.0], [20.0, 40.0]]

    def test_an_exposure_that_is_all_flare_keeps_nothing(self, tmp_path):
        path = a_flare_lightcurve(tmp_path / "lc.fits", [9.0, 9.0], tstart=0.0, threshold=5.0)

        assert xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({})).tolist() == []

    def test_a_gap_in_the_coverage_is_not_a_flare(self, tmp_path):
        """Real curves carry NaN in dead bins -- 322 of 6181 in Mkn 421's pn. Missing
        information is not evidence of a bright background."""
        path = a_flare_lightcurve(
            tmp_path / "lc.fits",
            [1.0, np.nan, 1.0],
            tstart=0.0,
            cadence=10.0,
            threshold=5.0,
        )

        assert xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({})).tolist() == [[0.0, 30.0]]

    def test_an_exposure_with_no_light_curve_is_not_screened(self, tmp_path):
        """PPS wrote no FBKTSR for the pn timing exposure of Her X-1. That is real data,
        not a broken download, and it must not raise."""
        exposure = xmm.Exposure(instrument="pn", expid="S003", mode=xmm.TIMING, event_list="e.FTZ")

        assert xmm.xmm_flare_gti(exposure, xmm.xmm_config({})) is None

    def test_an_exposure_with_no_threshold_is_not_screened(self, tmp_path):
        path = a_flare_lightcurve(tmp_path / "lc.fits", [1.0, 9.0], threshold=None)

        assert xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({})) is None

    def test_the_configured_limit_is_the_one_applied(self, tmp_path):
        path = a_flare_lightcurve(
            tmp_path / "lc.fits", [1.0, 9.0, 1.0], tstart=0.0, cadence=10.0, threshold=100.0
        )
        config = xmm.xmm_config({"flare_rate_limit": {"mos": 5.0}})

        assert xmm.xmm_flare_gti(an_exposure(path), config).tolist() == [[0.0, 10.0], [20.0, 30.0]]


class TestWhatTheFlareScreeningRecords:
    """
    The page has to show what was thrown away, because a cut that removed too much and a
    cut that removed nothing both leave an output file that looks perfectly good.
    """

    def a_record(self, tmp_path, rates, **kwargs):
        from heasarc_retrieve_pipeline.diagnostics import record_step

        path = a_flare_lightcurve(tmp_path / "lc.fits", rates, tstart=0.0, cadence=10.0, **kwargs)
        with record_step(str(tmp_path / "diag"), "0153950401", "flare_filtering") as rec:
            gti = xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({}), rec=rec)
        return gti, rec

    def test_the_threshold_and_where_it_came_from_are_recorded(self, tmp_path):
        _, rec = self.a_record(tmp_path, [1.0, 9.0, 1.0], threshold=5.0)

        assert rec.values["threshold"] == 5.0
        assert rec.values["threshold_source"] == "pps"

    def test_the_exposure_before_and_after_are_recorded(self, tmp_path):
        _, rec = self.a_record(tmp_path, [1.0, 9.0, 1.0], threshold=5.0)

        assert rec.values["exposure_before"] == 30.0
        assert rec.values["exposure_after"] == 20.0
        assert rec.values["removed_fraction"] == pytest.approx(1 / 3)

    def test_the_curve_is_recorded_so_the_cut_can_be_drawn(self, tmp_path):
        _, rec = self.a_record(tmp_path, [1.0, 9.0, 1.0], threshold=5.0)

        assert rec.arrays["lc_time"].tolist() == [5.0, 15.0, 25.0]
        assert rec.arrays["lc_rate"].tolist() == [1.0, 9.0, 1.0]

    def test_what_was_removed_is_recorded_as_well_as_what_was_kept(self, tmp_path):
        _, rec = self.a_record(tmp_path, [1.0, 9.0, 1.0], threshold=5.0)

        assert rec.arrays["gti_after"].tolist() == [[0.0, 10.0], [20.0, 30.0]]
        assert rec.arrays["removed"].tolist() == [[10.0, 20.0]]

    def test_an_exposure_with_no_light_curve_is_a_skip_and_not_a_failure(self, tmp_path):
        from heasarc_retrieve_pipeline.diagnostics import record_step

        exposure = xmm.Exposure(instrument="pn", expid="S003", mode=xmm.TIMING, event_list="e.FTZ")
        with record_step(str(tmp_path / "diag"), "0153950401", "flare_filtering") as rec:
            xmm.xmm_flare_gti(exposure, xmm.xmm_config({}), rec=rec)

        assert rec.status == "skipped"
        assert "light curve" in rec.reason

    def test_a_heavy_cut_is_warned_about(self, tmp_path, caplog):
        """Mkn 421's pn loses 39% of itself to PPS's own threshold. That is very likely
        right -- it is a famously flare-wrecked observation -- but nobody should have to
        open the page to find out that it happened."""
        path = a_flare_lightcurve(
            tmp_path / "lc.fits", [1.0, 9.0, 9.0, 1.0], tstart=0.0, cadence=10.0, threshold=5.0
        )

        with caplog.at_level("WARNING"):
            xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({}))

        assert "50" in caplog.text

    def test_a_light_cut_is_not_warned_about(self, tmp_path, caplog):
        path = a_flare_lightcurve(
            tmp_path / "lc.fits", [1.0] * 19 + [9.0], tstart=0.0, cadence=10.0, threshold=5.0
        )

        with caplog.at_level("WARNING"):
            xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({}))

        assert caplog.text == ""

    def test_the_fraction_that_warns_is_configurable(self, tmp_path, caplog):
        path = a_flare_lightcurve(
            tmp_path / "lc.fits", [1.0] * 19 + [9.0], tstart=0.0, cadence=10.0, threshold=5.0
        )

        with caplog.at_level("WARNING"):
            xmm.xmm_flare_gti(an_exposure(path), xmm.xmm_config({"flare_warn_fraction": 0.01}))

        assert "removed" in caplog.text


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
        assert resolved["odf_flare_rate_limit"] == {"pn": 0.4, "mos": 0.35}

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
        """``odf_flare_rate_limit`` is a dictionary of its own, so a shallow copy would
        hand every run the same one."""
        first = xmm.xmm_config(None)

        first["odf_flare_rate_limit"]["pn"] = 99.0

        assert xmm.xmm_config(None)["odf_flare_rate_limit"]["pn"] == 0.4


#: The output ``ecoordconv`` prints, copied from the examples page of the task's own
#: documentation (SAS 22.1.0). The documentation states that these strings may be searched
#: for in a script and that every effort is made to keep them constant between versions,
#: which is what makes parsing them defensible.
ECOORDCONV_OUTPUT = """ecoordconv:-  Region Centre:
 Theta: Phi: 18.4712 2.59867
 X: Y: 27010 26888
 DETX: DETY: -353.754 160.874
 IM_X: IM_Y: 101.90963 101.81583
 RA: DEC: 275.505 64.3385
 RAWX: RAWY: 43 186
 CCD(s):  4 centred on CCD: 4
"""


def a_source_list(path, rows):
    """
    A stand-in for a PPS ``OBSMLI``, with the three columns the cross-check reads.

    The real product has 249 columns and no count rate anywhere; ``EP_TOT_FLUX`` is the
    EPIC-combined flux in erg cm^-2 s^-1, and is the brightness the check reports.
    """
    from astropy.io import fits

    ra = [row[0] for row in rows]
    dec = [row[1] for row in rows]
    flux = [row[2] for row in rows]
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="RA", format="D", unit="deg", array=np.array(ra, dtype=float)),
            fits.Column(name="DEC", format="D", unit="deg", array=np.array(dec, dtype=float)),
            fits.Column(name="EP_TOT_FLUX", format="E", array=np.array(flux, dtype=float)),
        ],
        name=xmm.SOURCE_LIST_EXTENSION,
    )
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


class TestTheScreeningExpression:
    """
    The standard EPIC event screening, which stays a string handed to ``evselect``.

    The expressions use ``#XMMEA_EP`` and ``#XMMEA_EM``, macros SAS expands from the
    calibration rather than filters we could reimplement in numpy, so the whole point is
    that this function builds a string and does no arithmetic of its own.
    """

    def test_pn_gets_the_pn_macro_and_the_single_pixel_patterns(self):
        expression = xmm.xmm_screening_expression("pn", xmm.IMAGING)

        assert "#XMMEA_EP" in expression
        assert "PATTERN<=4" in expression

    def test_pn_alone_rejects_the_flagged_events(self):
        # FLAG==0 is deliberately not applied to MOS: on MOS it also throws away events
        # near the chip edges that the standard threads keep.
        assert "FLAG==0" in xmm.xmm_screening_expression("pn", xmm.IMAGING)
        assert "FLAG==0" not in xmm.xmm_screening_expression("mos1", xmm.IMAGING)

    def test_both_mos_cameras_get_the_mos_macro_and_the_wider_patterns(self):
        for instrument in ("mos1", "mos2"):
            expression = xmm.xmm_screening_expression(instrument, xmm.IMAGING)

            assert "#XMMEA_EM" in expression
            assert "PATTERN<=12" in expression

    def test_every_camera_keeps_the_same_energy_band(self):
        for instrument in ("pn", "mos1", "mos2"):
            for mode in (xmm.IMAGING, xmm.TIMING):
                assert "(PI in [200:12000])" in xmm.xmm_screening_expression(instrument, mode)

    def test_pn_is_screened_the_same_way_in_both_modes(self):
        # There is no timing-specific screening macro in SAS 22.1.0 -- #XMMEA_EP,
        # #XMMEA_EM and #XMMEA_SM are the only EPIC ones -- and xmmextractor, SAS's own
        # automatic reduction, documents applying PATTERN<=4, FLAG==0 and #XMMEA_EP to pn
        # whatever mode it is in.
        assert xmm.xmm_screening_expression("pn", xmm.TIMING) == xmm.xmm_screening_expression(
            "pn", xmm.IMAGING
        )

    def test_mos_in_timing_keeps_only_the_single_pixel_events(self):
        # The one real difference between the modes, and it is xmmextractor's:
        # PATTERN<=12 in imaging, PATTERN==0 in timing.
        expression = xmm.xmm_screening_expression("mos1", xmm.TIMING)

        assert "PATTERN==0" in expression
        assert "PATTERN<=12" not in expression
        assert "#XMMEA_EM" in expression

    def test_a_mode_nobody_has_heard_of_is_an_error(self):
        with pytest.raises(KeyError):
            xmm.xmm_screening_expression("pn", "burst")

    def test_a_good_time_interval_file_is_added_as_a_filter(self):
        expression = xmm.xmm_screening_expression("pn", xmm.IMAGING, gti_file="flare.gti")

        assert expression.endswith("&& gti(flare.gti,TIME)")

    def test_a_timing_exposure_is_filtered_on_time_the_same_way(self):
        expression = xmm.xmm_screening_expression("pn", xmm.TIMING, gti_file="flare.gti")

        assert expression.endswith("&& gti(flare.gti,TIME)")

    def test_without_a_file_nothing_about_time_is_said(self):
        assert "gti(" not in xmm.xmm_screening_expression("pn", xmm.IMAGING)


class TestWritingAGoodTimeIntervalFile:
    """
    Handing the flare intervals to SAS.

    ``evselect`` cannot be given an array; its ``gti()`` selector reads a file, and
    ``selectlib`` requires that file to be an OGIP-standard GTI table. Writing it here
    rather than rebuilding it with ``tabgtigen`` keeps the intervals that get recorded and
    the intervals that get applied the same intervals.
    """

    def test_the_intervals_come_back_out(self, tmp_path):
        from astropy.io import fits

        path = xmm.write_gti_file(tmp_path / "flare.gti", np.array([[10.0, 20.0], [30.0, 44.0]]))

        with fits.open(path) as hdul:
            table = hdul[xmm.GTI_EXTENSION]
            assert table.data["START"].tolist() == [10.0, 30.0]
            assert table.data["STOP"].tolist() == [20.0, 44.0]

    def test_the_extension_says_it_is_a_standard_gti(self, tmp_path):
        from astropy.io import fits

        path = xmm.write_gti_file(tmp_path / "flare.gti", np.array([[10.0, 20.0]]))

        with fits.open(path) as hdul:
            header = hdul[xmm.GTI_EXTENSION].header
            assert header["HDUCLASS"] == "OGIP"
            assert header["HDUCLAS1"] == "GTI"

    def test_the_times_are_seconds(self, tmp_path):
        from astropy.io import fits

        path = xmm.write_gti_file(tmp_path / "flare.gti", np.array([[10.0, 20.0]]))

        with fits.open(path) as hdul:
            columns = hdul[xmm.GTI_EXTENSION].columns
            assert columns["START"].unit == "s"
            assert columns["STOP"].unit == "s"

    def test_an_exposure_that_was_all_flare_still_writes_a_file(self, tmp_path):
        # An empty GTI is a real answer -- keep nothing -- and evselect can apply it. A
        # missing file, by contrast, would fail the task with a confusing message.
        from astropy.io import fits

        path = xmm.write_gti_file(tmp_path / "flare.gti", np.zeros((0, 2)))

        with fits.open(path) as hdul:
            assert len(hdul[xmm.GTI_EXTENSION].data) == 0

    def test_the_pipeline_can_read_its_own_file_back(self, tmp_path):
        from astropy.io import fits

        from heasarc_retrieve_pipeline.utils import read_gti

        gti = np.array([[10.0, 20.0], [30.0, 44.0]])
        path = xmm.write_gti_file(tmp_path / "flare.gti", gti)

        with fits.open(path) as hdul:
            assert np.allclose(read_gti(hdul), gti)


class TestParsingTheCoordinateConversion:
    """
    Reading the sky position out of what ``ecoordconv`` prints.

    The task has no output file at all -- it answers on standard output -- so the parser
    is the interface, and it is a pure function tested against the documented format.
    """

    def test_the_sky_position_is_found(self):
        assert xmm.parse_ecoordconv_sky_position(ECOORDCONV_OUTPUT) == (27010.0, 26888.0)

    def test_the_detector_and_image_lines_are_not_mistaken_for_it(self):
        x, y = xmm.parse_ecoordconv_sky_position(ECOORDCONV_OUTPUT)

        assert (x, y) != (-353.754, 160.874)
        assert (x, y) != (101.90963, 101.81583)

    def test_a_position_off_the_boresight_can_be_negative(self):
        text = "ecoordconv:-  Region Centre:\n X: Y: -1239.05 1711.11\n"

        assert xmm.parse_ecoordconv_sky_position(text) == (-1239.05, 1711.11)

    def test_output_without_a_sky_position_is_an_error(self):
        # ecoordconv exits zero when it cannot convert, so silence here would be a
        # position of (0, 0) and a region extracted from the corner of the detector.
        with pytest.raises(ValueError, match="no sky position"):
            xmm.parse_ecoordconv_sky_position("ecoordconv:-  Region Centre:\n RA: DEC: 1 2\n")


class TestTheExtractionRegions:
    """
    The region strings, in sky coordinates, as ``evselect`` and ``especget`` want them.

    XMM's sky pixel is 0.05 arcsec, from the ``ecoordconv`` documentation. It is a named
    constant because a bare 20 in an expression is unreadable and a bare 0.05 is worse.
    """

    def test_arcseconds_become_sky_pixels(self):
        assert xmm.arcsec_to_sky_pixels(30.0) == 600.0

    def test_the_sky_pixel_is_the_documented_size(self):
        assert xmm.SKY_PIXEL_ARCSEC == 0.05

    def test_a_source_region_is_a_circle_in_sky_pixels(self):
        region = xmm.circle_region(26000.0, 25000.0, 30.0)

        assert region == "((X,Y) IN circle(26000.0000,25000.0000,600.0000))"

    def test_a_background_region_is_an_annulus_around_the_same_point(self):
        region = xmm.annulus_region(26000.0, 25000.0, 45.0, 90.0)

        assert region == "((X,Y) IN annulus(26000.0000,25000.0000,900.0000,1800.0000))"

    def test_the_configuration_radii_are_what_gets_used(self):
        config = xmm.xmm_config(dict(src_radius_arcsec=20.0))

        source, background = xmm.xmm_extraction_regions(26000.0, 25000.0, config)

        assert "circle(26000.0000,25000.0000,400.0000)" in source
        # 1.5 and 3.0 times the source radius, from the defaults.
        assert "annulus(26000.0000,25000.0000,600.0000,1200.0000)" in background


class TestTheTimingStrips:
    """
    Timing mode's extraction regions, which are detector columns and not sky positions.

    A timing read-out collapses one dimension to read the source faster, so there is no
    sky image to put a circle on. The region is a strip of ``RAWX`` columns instead, and
    the numbers come from the configuration exactly as the annulus factors do.
    """

    def test_a_strip_is_a_range_of_columns(self):
        assert xmm.rawx_region(31, 45) == "(RAWX in [31:45])"

    def test_pn_gets_the_cookbook_strips(self):
        source, background = xmm.xmm_timing_regions("pn", xmm.xmm_config({}))

        assert source == "(RAWX in [31:45])"
        assert background == "(RAWX in [3:5])"

    def test_the_configured_strips_are_what_gets_used(self):
        config = xmm.xmm_config(dict(timing_src_rawx=dict(pn=(30, 46))))

        source, _ = xmm.xmm_timing_regions("pn", config)

        assert source == "(RAWX in [30:46])"

    def test_mos_has_no_strip_and_says_so_loudly(self, caplog):
        # Decided with Matteo, 2026-09-07: there is no published MOS timing strip worth
        # trusting, so a MOS timing exposure is skipped rather than extracted at an
        # invented position. SAS's own driver centres the strip on the source; doing the
        # same here is a later step, not a guess made now.
        with caplog.at_level("WARNING"):
            assert xmm.xmm_timing_regions("mos1", xmm.xmm_config({})) is None

        assert "mos1" in caplog.text
        assert "timing_src_rawx" in caplog.text

    def test_a_mos_strip_put_in_the_configuration_is_honoured(self):
        config = xmm.xmm_config(
            dict(timing_src_rawx=dict(mos=(300, 320)), timing_bkg_rawx=dict(mos=(100, 200)))
        )

        assert xmm.xmm_timing_regions("mos2", config) == (
            "(RAWX in [300:320])",
            "(RAWX in [100:200])",
        )

    def test_a_source_strip_without_a_background_one_is_not_half_an_answer(self, caplog):
        config = xmm.xmm_config(dict(timing_src_rawx=dict(mos=(300, 320))))

        with caplog.at_level("WARNING"):
            assert xmm.xmm_timing_regions("mos1", config) is None


class TestTheRegionsOfOneExposure:
    """
    One question -- where do the events come from -- answered for either mode.

    Everything downstream of the cleaning wants a source and a background selection, and
    nothing downstream should have to care which mode produced them.
    """

    def test_an_imaging_exposure_gets_the_sky_circle_and_annulus(self):
        exposure = an_exposure(None, instrument="pn", mode=xmm.IMAGING)
        config = xmm.xmm_config({})

        source, background = xmm.xmm_exposure_regions(exposure, config, sky=(26000.0, 25000.0))

        assert source == xmm.circle_region(26000.0, 25000.0, config["src_radius_arcsec"])
        assert "annulus" in background

    def test_a_timing_exposure_gets_the_strips_and_ignores_the_sky(self):
        exposure = an_exposure(None, instrument="pn", mode=xmm.TIMING)

        regions = xmm.xmm_exposure_regions(exposure, xmm.xmm_config({}), sky=(26000.0, 25000.0))

        assert regions == ("(RAWX in [31:45])", "(RAWX in [3:5])")

    def test_an_imaging_exposure_without_a_sky_position_is_a_programming_error(self):
        exposure = an_exposure(None, instrument="pn", mode=xmm.IMAGING)

        with pytest.raises(ValueError, match="sky position"):
            xmm.xmm_exposure_regions(exposure, xmm.xmm_config({}))

    def test_a_mos_timing_exposure_has_no_regions(self, caplog):
        exposure = an_exposure(None, instrument="mos1", mode=xmm.TIMING)

        with caplog.at_level("WARNING"):
            assert xmm.xmm_exposure_regions(exposure, xmm.xmm_config({})) is None


class TestFindingTheSourceInThePpsSourceList:
    """
    The ``OBSMLI`` cross-check.

    It exists to make a mistyped position visible, and it is only ever a cross-check: the
    detection never moves the region, because the source list misses real targets.
    """

    def test_the_nearest_detection_is_the_one_reported(self, tmp_path):
        path = a_source_list(tmp_path / "obsmli.fits", [(10.0, 20.0, 1e-12), (10.01, 20.0, 5e-12)])

        found = xmm.nearest_detection(path, 10.0001, 20.0)

        assert found.offset_arcsec < 1.0

    def test_the_flux_comes_back_and_not_a_count_rate(self, tmp_path):
        path = a_source_list(tmp_path / "obsmli.fits", [(10.0, 20.0, 3.5e-12)])

        found = xmm.nearest_detection(path, 10.0, 20.0)

        assert found.flux == pytest.approx(3.5e-12)

    def test_the_offset_is_in_arcseconds(self, tmp_path):
        # One arcsecond of declination, which is one arcsecond of separation.
        path = a_source_list(tmp_path / "obsmli.fits", [(10.0, 20.0 + 1.0 / 3600.0, 1e-12)])

        found = xmm.nearest_detection(path, 10.0, 20.0)

        assert found.offset_arcsec == pytest.approx(1.0, abs=1e-3)

    def test_the_runner_up_is_reported_too(self, tmp_path):
        # An unambiguous match is one where the second-nearest detection is far away, so
        # the number that says whether to believe the match is the runner-up's offset.
        path = a_source_list(tmp_path / "obsmli.fits", [(10.0, 20.0, 1e-12), (10.1, 20.0, 1e-12)])

        found = xmm.nearest_detection(path, 10.0, 20.0)

        assert found.next_offset_arcsec == pytest.approx(
            0.1 * 3600.0 * np.cos(np.radians(20.0)), rel=1e-3
        )

    def test_a_single_detection_has_no_runner_up(self, tmp_path):
        path = a_source_list(tmp_path / "obsmli.fits", [(10.0, 20.0, 1e-12)])

        assert xmm.nearest_detection(path, 10.0, 20.0).next_offset_arcsec is None

    def test_an_empty_source_list_finds_nothing(self, tmp_path):
        path = a_source_list(tmp_path / "obsmli.fits", [])

        assert xmm.nearest_detection(path, 10.0, 20.0) is None

    def test_a_missing_source_list_finds_nothing(self, tmp_path):
        assert xmm.nearest_detection(str(tmp_path / "absent.fits"), 10.0, 20.0) is None


class TestWhatThePositionCheckRecords:
    """
    What the page shows about the position, and what the check refuses to do about it.
    """

    def test_the_offset_is_recorded(self, tmp_path):
        obsid = "0153950401"
        pps = tmp_path / obsid / "PPS"
        pps.mkdir(parents=True)
        a_source_list(pps / f"P{obsid}EPX000OBSMLI0000.FTZ", [(10.0, 20.0, 1e-12)])
        config = xmm.xmm_config(dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path)))

        with record_step(str(tmp_path / "diag"), obsid, "source_position") as rec:
            xmm.xmm_check_source_position(obsid, config, 10.0, 20.0, rec=rec)

        assert rec.values["offset_arcsec"] == pytest.approx(0.0, abs=1e-6)

    def test_a_wild_offset_warns_and_does_not_raise(self, tmp_path, caplog):
        # The Crab: the nearest OBSMLI detection is 328.79 arcsec from the pulsar, because
        # the nebula is extended and maximum-likelihood point-source detection does not
        # find it. A pipeline that aborted here would refuse the Crab.
        obsid = "0611180201"
        pps = tmp_path / obsid / "PPS"
        pps.mkdir(parents=True)
        a_source_list(pps / f"P{obsid}EPX000OBSMLI0000.FTZ", [(10.1, 20.0, 1e-12)])
        config = xmm.xmm_config(dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path)))

        with caplog.at_level("WARNING"):
            xmm.xmm_check_source_position(obsid, config, 10.0, 20.0)

        assert "arcsec" in caplog.text

    def test_no_source_list_is_a_skip_and_not_a_failure(self, tmp_path):
        obsid = "0153950401"
        (tmp_path / obsid / "PPS").mkdir(parents=True)
        config = xmm.xmm_config(dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path)))

        with record_step(str(tmp_path / "diag"), obsid, "source_position") as rec:
            xmm.xmm_check_source_position(obsid, config, 10.0, 20.0, rec=rec)

        assert rec.status == "skipped"


def an_event_file(path, hdu="EVENTS", **keywords):
    """A file with the shape ``epatplot`` leaves behind: events, and its numbers on them."""
    from astropy.io import fits

    events = fits.BinTableHDU.from_columns(
        [fits.Column(name="TIME", format="D", array=np.array([1.0, 2.0]))], name="EVENTS"
    )
    primary = fits.PrimaryHDU()
    destination = primary if hdu == "PRIMARY" else events
    for key, value in keywords.items():
        destination.header[key] = value
    fits.HDUList([primary, events]).writeto(str(path), overwrite=True)
    return str(path)


PILED_UP = dict(SNGL_OTM=0.82, ESGL_OTM=0.02, DBLE_OTM=1.31, EDBL_OTM=0.03)
NOT_PILED_UP = dict(SNGL_OTM=0.99, ESGL_OTM=0.02, DBLE_OTM=1.02, EDBL_OTM=0.03)


class StubSas:
    """
    A SAS that writes what each task claims to produce, and remembers how it was called.

    In the mould of ``test_segments.py``'s ``StubNuproducts``. The files have to be
    written because :func:`heasarc_retrieve_pipeline.sas.run` checks a task's outputs, and
    ``epatplot`` has to write its keywords onto its input because that -- not its standard
    output -- is where its answer is documented to appear.
    """

    #: Where the stubbed ``ecoordconv`` says the source is. The centre of the box
    #: ``a_windowed_event_file`` fills, so a flow test measures a window around the
    #: position the flow was told about rather than off the edge of the events.
    SKY = (26000.0, 26000.0)

    def __init__(self, keywords=None, sky=None, barycen_timesys="TDB"):
        self.calls = []
        self.environments = []
        self.keywords = keywords
        self.sky = sky or self.SKY
        #: What the stubbed ``barycen`` leaves in ``TIMESYS``. ``None`` stands for a task
        #: that returns success having converted nothing, which is the case the caller's
        #: header check exists for.
        self.barycen_timesys = barycen_timesys

    def __call__(self, name, *, produces, log_to=None, capture=False, env=None, cwd=None, **params):
        self.calls.append((name, dict(params, cwd=cwd)))
        self.environments.append(dict(env or {}))
        for output in produces if isinstance(produces, (list, tuple)) else [produces]:
            os.makedirs(os.path.dirname(str(output)), exist_ok=True)
            if not os.path.exists(str(output)):
                if str(output).endswith(".evt"):
                    an_event_file(str(output))
                else:
                    open(str(output), "w").write("stub\n")
        if name == "epatplot":
            assert params["modifyinset"] == "yes", "the ratios would not be written"
        if name == "epatplot" and self.keywords is not None:
            an_event_file(params["set"], **self.keywords)
        if name == "barycen":
            # The real task edits its input in place and its answer is the rewritten time
            # system, so the stub has to leave that behind or the check cannot be tested.
            path = params["table"].split(":")[0]
            if self.barycen_timesys is None:
                an_event_file(path)
            else:
                an_event_file(path, TIMESYS=self.barycen_timesys, TIMEREF="SOLARSYSTEM")
        if name == "odfingest":
            # The real task writes a summary whose name it chooses from the ODF it read,
            # which is why the caller globs for the suffix instead of assuming a name.
            open(os.path.join(params["outdir"], "3906_0153950401_SCX00000SUM.SAS"), "w").write(
                "summary\n"
            )
        if name == "ecoordconv":
            # The real task writes no file: its answer *is* its standard output, in the
            # two-line shape its documentation pins. Both lines carry the same numbers,
            # which is the trap the parser's start-of-line anchor exists for.
            x, y = self.sky
            return SimpleNamespace(stdout=f" X: Y: {x} {y}\n IM_X: IM_Y: {x} {y}\n", returncode=0)
        return None

    def task(self, name):
        return [params for called, params in self.calls if called == name]


@pytest.fixture
def stub_sas(monkeypatch):
    def install(keywords=None, sky=None, barycen_timesys="TDB"):
        from heasarc_retrieve_pipeline import sas

        stub = StubSas(keywords, sky=sky, barycen_timesys=barycen_timesys)
        monkeypatch.setattr(sas, "run", stub)
        return stub

    return install


class TestWhereTheSpectraGo:
    """
    The five files a fitted spectrum is made of, named by us rather than by the task.

    ``especget``'s ``filestem`` convention (``<stem>_src.ds``, ``_bgd.ds``, ``_src.arf``,
    ``_src.rmf``) is a naming scheme that could change between SAS versions. Setting
    ``withfilestem=no`` and naming all four outright makes the version irrelevant, which
    is better than pinning a convention and hoping.
    """

    CONFIG = dict(out_data_path="/data")

    def paths(self, mode=None):
        exposure = an_exposure(None, instrument="pn", mode=mode or xmm.TIMING)
        return xmm.xmm_spectrum_paths("0153950401", exposure, xmm.xmm_config(self.CONFIG))

    def test_the_five_products_are_named_for_the_exposure(self):
        paths = self.paths()

        assert os.path.basename(paths.source) == "pnS004_timing_src.pi"
        assert os.path.basename(paths.background) == "pnS004_timing_bkg.pi"
        assert os.path.basename(paths.arf) == "pnS004_timing.arf"
        assert os.path.basename(paths.rmf) == "pnS004_timing.rmf"
        assert os.path.basename(paths.grouped) == "pnS004_timing_grp.pi"

    def test_they_go_in_the_products_directory_and_not_beside_the_events(self):
        products = xmm.xmm_product_output_path("0153950401", xmm.xmm_config(self.CONFIG))

        assert all(
            os.path.dirname(path) == products for path in (self.paths().source, self.paths().rmf)
        )

    def test_the_two_modes_of_one_exposure_do_not_collide(self):
        assert self.paths(xmm.IMAGING).source != self.paths(xmm.TIMING).source


class TestExtractingTheSpectra:
    """
    ``especget`` and ``specgroup``, and what has to be true about how they are called.

    ``especget`` is a metatask: it runs ``evselect`` for both spectra, ``arfgen`` for the
    effective area and the ``BACKSCAL`` areas, and ``rmfgen`` for the response, which is
    the slow part. ``specgroup`` then bins the source spectrum for fitting.
    """

    def a_run(self, tmp_path, stub_sas, exposure=None, **extra):
        stub = stub_sas()
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path), **extra))
        exposure = exposure or an_exposure(None, instrument="pn", mode=xmm.TIMING)
        events = str(tmp_path / "cleaned.evt")
        an_event_file(events)
        with record_step(str(tmp_path / "diag"), "0153950401", "calculate_spectra") as rec:
            paths = xmm.xmm_calculate_spectra(
                "0153950401",
                exposure,
                config,
                events,
                ra=254.4574995,
                dec=35.3423889,
                sky=(26000.0, 25000.0),
                rec=rec,
            )
        return paths, rec, stub

    def test_the_source_and_background_expressions_are_the_regions(self, tmp_path, stub_sas):
        _, _, stub = self.a_run(tmp_path, stub_sas)

        (call,) = stub.task("especget")
        assert call["srcexp"] == "(RAWX in [31:45])"
        assert call["backexp"] == "(RAWX in [3:5])"

    def test_the_four_outputs_are_named_and_the_filestem_is_refused(self, tmp_path, stub_sas):
        paths, _, stub = self.a_run(tmp_path, stub_sas)

        (call,) = stub.task("especget")
        assert call["withfilestem"] == "no"
        assert call["srcspecset"] == os.path.basename(paths.source)
        assert call["bckspecset"] == os.path.basename(paths.background)
        assert call["srcarfset"] == os.path.basename(paths.arf)
        assert call["srcrmfset"] == os.path.basename(paths.rmf)

    def test_the_tasks_run_in_the_products_directory_and_are_given_bare_names(
        self, tmp_path, stub_sas
    ):
        # especget writes the names it is given into BACKFILE, RESPFILE and ANCRFILE, and
        # a FITS header card holds 80 characters -- which an absolute path here exceeds on
        # its own. Measured on the real Her X-1 run: 140 characters before this, and the
        # same trap that truncated the file names in an addspec merge.
        paths, _, stub = self.a_run(tmp_path, stub_sas)
        products = os.path.dirname(paths.source)

        for task in ("especget", "specgroup"):
            (call,) = stub.task(task)
            assert call["cwd"] == products
            assert not any(
                isinstance(value, str) and value.startswith(products + "/")
                for key, value in call.items()
                if key != "table"
            )

    def test_the_position_asked_for_is_handed_to_arfgen(self, tmp_path, stub_sas):
        # A timing spectrum's region is a strip of columns, so the centre of the region
        # says nothing about where the source is. arfgen needs the real position for the
        # vignetting and encircled-energy corrections, and this is where it gets it.
        _, _, stub = self.a_run(tmp_path, stub_sas)

        (call,) = stub.task("especget")
        assert call["withsourcepos"] == "yes"
        assert call["sourcecoords"] == "eqpos"
        assert call["sourcex"] == 254.4574995
        assert call["sourcey"] == 35.3423889

    def test_the_grouping_is_told_which_response_it_is_grouping_against(self, tmp_path, stub_sas):
        # oversample is a resolution criterion, so specgroup cannot honour it without the
        # RMF; and addfilenames is what puts BACKFILE, RESPFILE and ANCRFILE in the
        # grouped spectrum, which is what lets XSPEC open one file and find the rest.
        paths, _, stub = self.a_run(tmp_path, stub_sas)

        (call,) = stub.task("specgroup")
        assert call["spectrumset"] == os.path.basename(paths.source)
        assert call["groupedset"] == os.path.basename(paths.grouped)
        assert call["rmfset"] == os.path.basename(paths.rmf)
        assert call["arfset"] == os.path.basename(paths.arf)
        assert call["backgndset"] == os.path.basename(paths.background)
        assert call["addfilenames"] == "yes"

    def test_the_grouping_numbers_come_from_the_configuration(self, tmp_path, stub_sas):
        _, _, stub = self.a_run(tmp_path, stub_sas, spectrum_min_counts=50, spectrum_oversample=5)

        (call,) = stub.task("specgroup")
        assert call["mincounts"] == 50
        assert call["oversample"] == 5

    def test_an_imaging_exposure_is_extracted_from_its_sky_regions(self, tmp_path, stub_sas):
        exposure = an_exposure(None, instrument="mos2", mode=xmm.IMAGING)

        _, _, stub = self.a_run(tmp_path, stub_sas, exposure=exposure)

        (call,) = stub.task("especget")
        assert "circle(26000.0000,25000.0000" in call["srcexp"]
        assert "annulus(26000.0000,25000.0000" in call["backexp"]

    def test_a_camera_with_no_timing_strip_is_a_skip_and_not_a_failure(self, tmp_path, stub_sas):
        exposure = an_exposure(None, instrument="mos1", mode=xmm.TIMING)

        paths, rec, stub = self.a_run(tmp_path, stub_sas, exposure=exposure)

        assert paths is None
        assert rec.status == "skipped"
        assert stub.calls == []

    def test_what_was_extracted_is_recorded(self, tmp_path, stub_sas):
        paths, rec, _ = self.a_run(tmp_path, stub_sas)

        assert rec.values["source_spectrum"] == os.path.basename(paths.source)
        assert rec.values["grouped_spectrum"] == os.path.basename(paths.grouped)
        assert rec.values["source_region"] == "(RAWX in [31:45])"

    def test_the_curves_are_recorded_so_the_page_can_draw_them(self, tmp_path, stub_sas):
        stub_sas()
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path)))
        exposure = an_exposure(None, instrument="pn", mode=xmm.TIMING)
        paths = xmm.xmm_spectrum_paths("0153950401", exposure, config)
        os.makedirs(os.path.dirname(paths.source), exist_ok=True)
        a_spectrum(paths.source, [10, 20])
        a_spectrum(paths.background, [1, 2])
        a_response(paths.rmf, [1.0, 2.0, 3.0])
        events = str(tmp_path / "cleaned.evt")
        an_event_file(events)

        with record_step(str(tmp_path / "diag"), "0153950401", "calculate_spectra") as rec:
            xmm.xmm_calculate_spectra(
                "0153950401", exposure, config, events, ra=254.4, dec=35.3, rec=rec
            )

        # `spec_<stem>_...` is report.spectrum_figure's convention. Recorded flat, the
        # arrays were written, read back and silently not drawn: the page came out with
        # the whole Spectra section missing and nothing anywhere said why.
        stem = xmm._exposure_stem(exposure)
        assert rec.arrays[f"spec_{stem}_src_energy"].tolist() == [1.5, 2.5]
        assert rec.arrays[f"spec_{stem}_src_rate"].tolist() == [0.01, 0.02]
        assert rec.arrays[f"spec_{stem}_bkg_rate"].tolist() == [0.001, 0.002]

    def test_the_recorded_band_is_the_one_the_page_will_draw(self, tmp_path, stub_sas):
        from heasarc_retrieve_pipeline import report

        stub_sas()
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path)))
        exposure = an_exposure(None, instrument="pn", mode=xmm.TIMING)
        paths = xmm.xmm_spectrum_paths("0153950401", exposure, config)
        os.makedirs(os.path.dirname(paths.source), exist_ok=True)
        a_spectrum(paths.source, [10, 20])
        a_spectrum(paths.background, [1, 2])
        a_response(paths.rmf, [1.0, 2.0, 3.0])
        events = str(tmp_path / "cleaned.evt")
        an_event_file(events)

        with record_step(str(tmp_path / "diag"), "0153950401", "calculate_spectra") as rec:
            xmm.xmm_calculate_spectra(
                "0153950401", exposure, config, events, ra=254.4, dec=35.3, rec=rec
            )

        assert rec.values["energy_band"] == list(xmm.SPECTRUM_PLOT_BAND_KEV)
        figure = report.spectrum_figure(dict(values=rec.values), rec.arrays)
        assert figure is not None and len(figure.data) == 2

    def test_an_unreadable_spectrum_records_no_curve_and_does_not_raise(self, tmp_path, stub_sas):
        # The stub writes files that are not spectra, so this is the "especget produced
        # something this cannot read" case, and it must not lose the extraction.
        _, rec, _ = self.a_run(tmp_path, stub_sas)

        assert rec.arrays == {}
        assert rec.status != "failed"


def a_spectrum(path, counts, exposure=1000.0, first_channel=0):
    """A PHA in the shape ``especget`` writes one: channels, counts, and a live time."""
    from astropy.io import fits

    counts = np.asarray(counts)
    channel = np.arange(first_channel, first_channel + counts.size)
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CHANNEL", format="J", array=channel),
            fits.Column(name="COUNTS", format="J", array=counts),
        ],
        name="SPECTRUM",
    )
    hdu.header["EXPOSURE"] = exposure
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(str(path), overwrite=True)
    return str(path)


def a_response(path, edges, first_channel=0):
    """A response whose ``EBOUNDS`` says what each channel is worth in keV."""
    from astropy.io import fits

    edges = np.asarray(edges, dtype=float)
    channel = np.arange(first_channel, first_channel + edges.size - 1)
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CHANNEL", format="J", array=channel),
            fits.Column(name="E_MIN", format="E", unit="keV", array=edges[:-1]),
            fits.Column(name="E_MAX", format="E", unit="keV", array=edges[1:]),
        ],
        name="EBOUNDS",
    )
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(str(path), overwrite=True)
    return str(path)


class TestReadingASpectrumForThePage:
    """
    Turning a spectrum into something that can be drawn.

    The energy scale comes from the response, not from a formula. NuSTAR's
    ``read_spectrum`` converts channel to energy with ``E = 0.04 * PI + 1.6``, which is
    that mission's own linear relation; XMM has no such number to hardcode, and it does
    not need one -- the ``EBOUNDS`` extension of the RMF ``especget`` just made says
    exactly what each channel is worth.
    """

    def test_the_energies_are_the_channel_midpoints_of_the_response(self, tmp_path):
        spectrum = a_spectrum(tmp_path / "src.pi", [10, 20, 30])
        rmf = a_response(tmp_path / "src.rmf", [1.0, 2.0, 3.0, 4.0])

        read = xmm.read_xmm_spectrum(spectrum, rmf)

        assert read["energy"].tolist() == [1.5, 2.5, 3.5]

    def test_counts_become_a_rate_per_kev(self, tmp_path):
        # 10 counts in a 2 keV bin over 1000 s is 0.005 counts/s/keV. Dividing by the bin
        # width is what stops the drawn shape depending on how the channels were binned.
        spectrum = a_spectrum(tmp_path / "src.pi", [10], exposure=1000.0)
        rmf = a_response(tmp_path / "src.rmf", [1.0, 3.0])

        read = xmm.read_xmm_spectrum(spectrum, rmf)

        assert read["rate"].tolist() == [0.005]
        assert read["rate_err"][0] == pytest.approx(np.sqrt(10) / 1000.0 / 2.0)

    def test_channels_the_response_does_not_describe_are_dropped(self, tmp_path):
        spectrum = a_spectrum(tmp_path / "src.pi", [1, 2, 3, 4])
        rmf = a_response(tmp_path / "src.rmf", [1.0, 2.0, 3.0])

        read = xmm.read_xmm_spectrum(spectrum, rmf)

        assert read["energy"].size == 2

    def test_a_spectrum_that_starts_at_another_channel_still_lines_up(self, tmp_path):
        # A grouped or truncated spectrum need not start at channel 0, so the two files
        # are matched on channel number rather than on row order.
        spectrum = a_spectrum(tmp_path / "src.pi", [7], first_channel=2)
        rmf = a_response(tmp_path / "src.rmf", [1.0, 2.0, 3.0, 4.0])

        read = xmm.read_xmm_spectrum(spectrum, rmf)

        assert read["energy"].tolist() == [3.5]

    def test_a_response_without_ebounds_is_no_answer_rather_than_a_wrong_one(self, tmp_path):
        from astropy.io import fits

        spectrum = a_spectrum(tmp_path / "src.pi", [1, 2])
        empty = str(tmp_path / "empty.rmf")
        fits.HDUList([fits.PrimaryHDU()]).writeto(empty, overwrite=True)

        assert xmm.read_xmm_spectrum(spectrum, empty) is None

    def test_a_missing_file_is_not_an_error(self, tmp_path):
        assert xmm.read_xmm_spectrum(str(tmp_path / "gone.pi"), str(tmp_path / "gone.rmf")) is None


class TestReadingThePileupNumbers:
    """
    Where ``epatplot``'s answer is, which is not its standard output.

    The task documents that it appends the observed-to-model singles and doubles pattern
    fractions to the *input* event set, as ``SNGL_OTM`` and ``DBLE_OTM`` with one-sigma
    errors ``ESGL_OTM`` and ``EDBL_OTM``. Reading keywords beats scraping a screen.
    """

    def test_the_numbers_come_off_the_event_extension(self, tmp_path):
        ratios = xmm.read_pileup_ratios(an_event_file(tmp_path / "src.evt", **PILED_UP))

        assert ratios.singles == 0.82
        assert ratios.singles_error == 0.02
        assert ratios.doubles == 1.31
        assert ratios.doubles_error == 0.03

    def test_they_are_found_wherever_in_the_file_they_are_written(self, tmp_path):
        path = an_event_file(tmp_path / "src.evt", hdu="PRIMARY", **PILED_UP)

        assert xmm.read_pileup_ratios(path).singles == 0.82

    def test_a_file_without_them_yields_nothing_rather_than_a_zero(self, tmp_path):
        assert xmm.read_pileup_ratios(an_event_file(tmp_path / "src.evt")) is None

    def test_pileup_is_a_low_singles_ratio_and_a_high_doubles_one(self, tmp_path):
        piled = xmm.read_pileup_ratios(an_event_file(tmp_path / "a.evt", **PILED_UP))
        clean = xmm.read_pileup_ratios(an_event_file(tmp_path / "b.evt", **NOT_PILED_UP))

        assert piled.is_piled_up(sigma=3.0)
        assert not clean.is_piled_up(sigma=3.0)

    def test_a_ratio_within_its_errors_of_one_is_not_pileup(self, tmp_path):
        # 0.95 +/- 0.02 is three sigma from 1.0, and 0.95 +/- 0.05 is one. The same
        # number means different things with different statistics, so the error bars
        # decide and not the ratio alone.
        noisy = xmm.read_pileup_ratios(
            an_event_file(
                tmp_path / "c.evt", SNGL_OTM=0.95, ESGL_OTM=0.05, DBLE_OTM=1.0, EDBL_OTM=0.05
            )
        )

        assert not noisy.is_piled_up(sigma=3.0)


class TestThePileupCheck:
    """
    Running ``epatplot`` on the events a spectrum would be built from.

    Pile-up is a property of the source region, not of the field, so the region is cut out
    first. The check never changes anything: it reports two numbers and, when they say the
    spectrum is piled up, says so loudly enough to be seen on the page.
    """

    def a_check(self, tmp_path, stub_sas, keywords=NOT_PILED_UP, exposure=None, **extra):
        stub = stub_sas(keywords)
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path), **extra))
        exposure = exposure or an_exposure(None, instrument="pn", mode=xmm.TIMING)
        events = str(tmp_path / "cleaned.evt")
        an_event_file(events)
        with record_step(str(tmp_path / "diag"), "0153950401", "pileup_check") as rec:
            ratios = xmm.xmm_pileup_check(
                "0153950401", exposure, config, events, sky=(26000.0, 25000.0), rec=rec
            )
        return ratios, rec, stub

    def test_the_source_region_is_cut_out_before_the_patterns_are_counted(self, tmp_path, stub_sas):
        _, _, stub = self.a_check(tmp_path, stub_sas)

        (selection,) = stub.task("evselect")
        assert selection["expression"] == "(RAWX in [31:45])"
        assert selection["table"] == str(tmp_path / "cleaned.evt")

    def test_epatplot_reads_the_source_events_and_not_the_whole_field(self, tmp_path, stub_sas):
        _, _, stub = self.a_check(tmp_path, stub_sas)

        (plot,) = stub.task("epatplot")
        assert plot["set"].endswith("pnS004_timing_src.evt")

    def test_an_imaging_exposure_is_checked_at_its_sky_position(self, tmp_path, stub_sas):
        exposure = an_exposure(None, instrument="mos2", mode=xmm.IMAGING)

        _, _, stub = self.a_check(tmp_path, stub_sas, exposure=exposure)

        (selection,) = stub.task("evselect")
        assert "circle(26000.0000,25000.0000" in selection["expression"]

    def test_the_two_numbers_are_recorded_for_the_page(self, tmp_path, stub_sas):
        ratios, rec, _ = self.a_check(tmp_path, stub_sas, keywords=PILED_UP)

        assert ratios.singles == 0.82
        assert rec.values["singles"] == 0.82
        assert rec.values["doubles"] == 1.31
        assert rec.values["piled_up"] is True

    def test_a_clean_spectrum_is_recorded_as_one(self, tmp_path, stub_sas):
        _, rec, _ = self.a_check(tmp_path, stub_sas)

        assert rec.values["piled_up"] is False

    def test_pileup_warns_and_does_not_raise(self, tmp_path, stub_sas, caplog):
        with caplog.at_level("WARNING"):
            self.a_check(tmp_path, stub_sas, keywords=PILED_UP)

        assert "piled up" in caplog.text.lower()

    def test_a_camera_with_no_timing_strip_is_a_skip_and_not_a_failure(self, tmp_path, stub_sas):
        exposure = an_exposure(None, instrument="mos1", mode=xmm.TIMING)

        ratios, rec, stub = self.a_check(tmp_path, stub_sas, exposure=exposure)

        assert ratios is None
        assert rec.status == "skipped"
        assert stub.calls == []

    def test_a_task_that_wrote_no_numbers_is_a_skip_too(self, tmp_path, stub_sas):
        ratios, rec, _ = self.a_check(tmp_path, stub_sas, keywords=None)

        assert ratios is None
        assert rec.status == "skipped"

    def test_the_plot_lands_beside_the_events_it_describes(self, tmp_path, stub_sas):
        self.a_check(tmp_path, stub_sas)

        plot = xmm.xmm_pileup_plot_path(
            "0153950401",
            an_exposure(None, instrument="pn", mode=xmm.TIMING),
            xmm.xmm_config(dict(out_data_path=str(tmp_path))),
        )
        assert os.path.exists(plot)
        # ``.pdf`` and not the ``.ps`` epatplot's own device parameter suggests: SAS
        # 22.1.0 draws the plot from Python and writes PDF whatever it is asked for.
        assert os.path.basename(plot) == "pnS004_timing_pat.pdf"


class TestWhereTheCleanedFilesGo:
    """
    One exposure can hold two modes, so the names have to carry the mode.
    """

    def test_the_cleaned_event_list_is_named_for_camera_exposure_and_mode(self, tmp_path):
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path)))
        exposure = an_exposure(None, instrument="mos1", mode=xmm.IMAGING)

        path = xmm.xmm_cleaned_event_list_path("0153950401", exposure, config)

        assert os.path.basename(path) == "mos1S004_imaging_cl.evt"

    def test_the_two_modes_of_one_exposure_do_not_collide(self, tmp_path):
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path)))
        imaging = an_exposure(None, instrument="mos1", mode=xmm.IMAGING)
        timing = an_exposure(None, instrument="mos1", mode=xmm.TIMING)

        assert xmm.xmm_cleaned_event_list_path(
            "0153950401", imaging, config
        ) != xmm.xmm_cleaned_event_list_path("0153950401", timing, config)

    def test_the_flare_file_sits_beside_the_events_it_filtered(self, tmp_path):
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path)))
        exposure = an_exposure(None, instrument="pn", mode=xmm.IMAGING)

        path = xmm.xmm_flare_gti_path("0153950401", exposure, config)

        assert os.path.basename(path) == "pnS004_imaging_flare.gti"
        assert os.path.dirname(path) == xmm.xmm_pipeline_output_path("0153950401", config)


class TestTheObservationDate:
    """
    ``DATE-OBS``, read off an event list and shaped for ``cifbuild``.

    The date is the whole input to the calibration index: it selects the constituents
    valid for the observation's epoch, at their current issue. It comes from a file we
    have already downloaded, so building the index needs no ODF and no extra request.
    """

    def test_the_date_is_read_from_the_events_header(self, tmp_path):
        path = an_event_file(tmp_path / "events.ds", **{"DATE-OBS": "2002-03-27T21:11:14"})

        assert xmm.xmm_observation_date(path) == "2002-03-27"

    def test_the_time_of_day_is_dropped(self):
        assert xmm.cifbuild_date("2021-04-06T23:02:03") == "2021-04-06"

    def test_a_date_with_no_time_of_day_survives(self):
        assert xmm.cifbuild_date("2021-04-06") == "2021-04-06"

    def test_a_file_without_the_keyword_gives_nothing(self, tmp_path):
        path = an_event_file(tmp_path / "events.ds", TELESCOP="XMM")

        assert xmm.xmm_observation_date(path) is None


class TestBuildingTheCalibrationIndex:
    """
    ``cifbuild``, run as the normal path rather than as a fallback.

    Matteo's ruling of 2026-09-07: the mirror is ESA's *Valid CCF Set*, which holds what
    is needed to process any ODF at the current date, and a new analysis wants that rather
    than the superseded issues an archival ``CALIND`` names. So ``SAS_CCF`` points at an
    index built from the observation date, and ``CALIND`` is kept only as the record of
    what ESA used.
    """

    CONFIG = dict(out_data_path=None)

    def config(self, tmp_path):
        return xmm.xmm_config(dict(self.CONFIG, out_data_path=str(tmp_path)))

    def build(self, tmp_path, stub_sas, date="2002-03-27T21:11:14"):
        events = an_event_file(tmp_path / "events.ds", **{"DATE-OBS": date})
        stub = stub_sas()
        built = xmm.xmm_build_calibration_index("0153950401", self.config(tmp_path), events)
        return stub, built

    def test_the_index_goes_beside_the_cleaned_events(self, tmp_path, stub_sas):
        _, built = self.build(tmp_path, stub_sas)

        assert built == xmm.xmm_calibration_index_path("0153950401", self.config(tmp_path))
        assert os.path.dirname(built) == xmm.xmm_pipeline_output_path(
            "0153950401", self.config(tmp_path)
        )

    def test_the_observation_date_is_what_selects_the_constituents(self, tmp_path, stub_sas):
        stub, _ = self.build(tmp_path, stub_sas)
        (params,) = stub.task("cifbuild")

        assert params["withobservationdate"] == "yes"
        assert params["observationdate"] == "2002-03-27"

    def test_the_paths_it_records_are_absolute(self, tmp_path, stub_sas):
        stub, _ = self.build(tmp_path, stub_sas)
        (params,) = stub.task("cifbuild")

        assert params["fullpath"] == "yes"

    def test_it_runs_where_the_index_goes_and_is_given_a_bare_name(self, tmp_path, stub_sas):
        stub, built = self.build(tmp_path, stub_sas)
        (params,) = stub.task("cifbuild")

        assert params["calindexset"] == os.path.basename(built)
        assert params["cwd"] == os.path.dirname(built)

    def test_an_observation_with_no_date_is_not_guessed_at(self, tmp_path, stub_sas):
        events = an_event_file(tmp_path / "events.ds", TELESCOP="XMM")
        stub = stub_sas()

        with pytest.raises(ValueError, match="DATE-OBS"):
            xmm.xmm_build_calibration_index("0153950401", self.config(tmp_path), events)

        assert stub.task("cifbuild") == []


def a_windowed_event_file(path, ccds, **keywords):
    """
    An event list with sky coordinates and a CCD number, the shape a window check reads.

    ``ccds`` maps a CCD number to ``(x0, x1, y0, y1)``, the sky box its events fill. The
    box is what a window looks like from the outside: a windowed CCD fills a small one, an
    unwindowed CCD fills the whole chip.
    """
    from astropy.io import fits

    ccd, x, y = [], [], []
    for number, (x0, x1, y0, y1) in ccds.items():
        for corner_x in np.linspace(x0, x1, 12):
            for corner_y in np.linspace(y0, y1, 12):
                ccd.append(number)
                x.append(corner_x)
                y.append(corner_y)
    events = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CCDNR", format="I", array=np.array(ccd, dtype=np.int16)),
            fits.Column(name="X", format="E", array=np.array(x, dtype=np.float32)),
            fits.Column(name="Y", format="E", array=np.array(y, dtype=np.float32)),
        ],
        name="EVENTS",
    )
    for key, value in keywords.items():
        events.header[key] = value
    fits.HDUList([fits.PrimaryHDU(), events]).writeto(str(path), overwrite=True)
    return str(path)


class TestReadingTheSubmode:
    """
    ``SUBMODE`` off the event list header, and onto the exposure.

    The front end stays pure -- file names and a listing, no FITS -- so this is a separate
    pass rather than something ``xmm_exposures_from_pps`` does. It is what tells Full
    Frame from Small Window, which nothing in the file name does.
    """

    def test_the_submode_is_read_from_the_events_header(self, tmp_path):
        path = an_event_file(tmp_path / "events.ds", SUBMODE="PrimePartialW3")

        assert xmm.read_submode(path) == "PrimePartialW3"

    def test_a_file_without_the_keyword_gives_nothing(self, tmp_path):
        path = an_event_file(tmp_path / "events.ds", TELESCOP="XMM")

        assert xmm.read_submode(path) is None

    def test_the_exposures_come_back_carrying_it(self, tmp_path):
        first = an_exposure(
            None, event_list=an_event_file(tmp_path / "a.ds", SUBMODE="PrimeLargeWindow")
        )
        second = an_exposure(
            None, event_list=an_event_file(tmp_path / "b.ds", SUBMODE="PrimePartialW3")
        )

        got = xmm.xmm_with_submodes([first, second])

        assert [exposure.submode for exposure in got] == [
            "PrimeLargeWindow",
            "PrimePartialW3",
        ]

    def test_nothing_else_about_the_exposure_changes(self, tmp_path):
        exposure = an_exposure(
            None, event_list=an_event_file(tmp_path / "a.ds", SUBMODE="PrimeFullWindow")
        )

        (got,) = xmm.xmm_with_submodes([exposure])

        assert (got.instrument, got.expid, got.mode) == (
            exposure.instrument,
            exposure.expid,
            exposure.mode,
        )
        assert got.event_list == exposure.event_list

    def test_an_unreadable_exposure_is_left_alone_rather_than_failing_the_run(self, tmp_path):
        exposure = an_exposure(None, event_list=str(tmp_path / "never-written.ds"))

        (got,) = xmm.xmm_with_submodes([exposure])

        assert got.submode is None


class TestWhetherTheBackgroundFitsTheWindow:
    """
    Whether the annulus the configuration asks for lands on exposed detector.

    Measured from the events rather than looked up in a table of submodes. The reason is
    that the number wanted is not really the window size: it is how far there is exposure
    from *this* source in *this* observation, which a chip gap or a windowed CCD or the
    edge of the field all bound. Measuring it needs no handbook constant and no list of
    submodes to keep current.

    It only ever warns. A background region clipped by the window is not silently wrong --
    SAS's ``backscale`` measures the exposed area and ``BACKSCAL`` follows it, which is why
    Her X-1's ratio came out 5.17 against the 5.00 its strips imply. What a clipped region
    costs is counts, and that is a judgement for whoever reads the page.
    """

    #: A windowed central CCD 300 sky pixels each way from the source, and an untouched
    #: outer CCD far off to one side. 300 sky pixels is 15 arcsec at 0.05 per pixel.
    WINDOWED = {1: (25700, 26300, 25700, 26300), 2: (30000, 40000, 30000, 40000)}

    def reach(self, tmp_path, ccds=None, x=26000, y=26000):
        path = a_windowed_event_file(tmp_path / "events.ds", ccds or self.WINDOWED)
        return xmm.xmm_window_reach_arcsec(path, x, y)

    def test_the_reach_is_measured_on_the_ccd_the_source_lands_on(self, tmp_path):
        assert self.reach(tmp_path) == pytest.approx(15.0)

    def test_an_outer_ccd_does_not_widen_a_windowed_one(self, tmp_path):
        wider = {**self.WINDOWED, 2: (0, 60000, 0, 60000)}

        assert self.reach(tmp_path, wider) == pytest.approx(15.0)

    def test_a_source_off_centre_reaches_less_on_its_near_side(self, tmp_path):
        assert self.reach(tmp_path, x=25800) == pytest.approx(5.0)

    def test_a_file_with_no_sky_columns_gives_nothing(self, tmp_path):
        path = an_event_file(tmp_path / "plain.ds")

        assert xmm.xmm_window_reach_arcsec(path, 26000, 26000) is None


class TestWhatTheWindowCheckRecords:
    """The warning itself: what it compares, and that it never stops the reduction."""

    CONFIG = dict(out_data_path="/data")

    def check(self, tmp_path, ccds, radius=30.0, mode=xmm.IMAGING, submode="PrimePartialW3"):
        path = a_windowed_event_file(tmp_path / "events.ds", ccds, SUBMODE=submode)
        exposure = an_exposure(None, mode=mode, event_list="never-opened.FTZ")
        exposure = copy.replace(exposure, submode=submode)
        config = xmm.xmm_config(dict(self.CONFIG, src_radius_arcsec=radius))
        # The cleaned events, not the exposure's own raw list -- and `never-opened.FTZ`
        # does not exist, so a regression that went back to the raw list would fail here
        # rather than quietly measure the wrong file.
        return xmm.xmm_check_extraction_window(exposure, config, path, 26000, 26000)

    ROOMY = {1: (20000, 32000, 20000, 32000)}
    TIGHT = {1: (25700, 26300, 25700, 26300)}

    def test_a_default_annulus_inside_a_roomy_window_is_fine(self, tmp_path):
        fit = self.check(tmp_path, self.ROOMY)

        assert fit.fits is True
        assert fit.needed_arcsec == pytest.approx(90.0)

    def test_a_widened_radius_that_outgrows_the_window_is_flagged(self, tmp_path):
        fit = self.check(tmp_path, self.TIGHT, radius=30.0)

        assert fit.fits is False
        assert fit.reach_arcsec == pytest.approx(15.0)
        assert fit.needed_arcsec == pytest.approx(90.0)

    def test_the_submode_is_carried_so_the_page_can_name_it(self, tmp_path):
        assert self.check(tmp_path, self.TIGHT).submode == "PrimePartialW3"

    def test_it_warns_and_does_not_raise(self, tmp_path, caplog):
        with caplog.at_level("WARNING"):
            self.check(tmp_path, self.TIGHT)

        assert "clipped" in caplog.text

    def test_an_annulus_that_fits_says_nothing(self, tmp_path, caplog):
        with caplog.at_level("WARNING"):
            self.check(tmp_path, self.ROOMY)

        assert caplog.text == ""

    def test_a_timing_exposure_has_no_window_to_check(self, tmp_path):
        assert self.check(tmp_path, self.TIGHT, mode=xmm.TIMING) is None


def a_reducible_observation(tmp_path, obsid="0153950401", event_lists=None, submode=None):
    """
    A downloaded PPS tree whose event lists and light curves are real enough to open.

    ``a_downloaded_observation`` writes text files, which is all a name parser needs.
    The flow opens them -- for ``DATE-OBS``, for ``SUBMODE``, for the flare curve -- so
    this writes FITS.
    """
    if event_lists is None:
        event_lists = ["PNS003TIEVLI", "M1S004MIEVLI", "M2S005MIEVLI"]
    pps = tmp_path / obsid / "PPS"
    pps.mkdir(parents=True)
    for stem in event_lists:
        a_windowed_event_file(
            pps / f"P{obsid}{stem}0000.FTZ",
            {1: (25000, 27000, 25000, 27000)},
            **{"DATE-OBS": "2002-03-27T21:11:14", "SUBMODE": submode or "PrimeFullWindow"},
        )
        a_flare_lightcurve(pps / f"P{obsid}{stem[:6]}FBKTSR0000.FTZ", [1.0, 1.0, 90.0, 1.0])
    a_source_list(pps / f"P{obsid}EPX000OBSMLI0000.FTZ", [(254.4576, 35.3427, 9.9e-11)])
    return dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path))


class TestReducingAnObservation:
    """
    ``process_xmm_obsid``: the order the steps run in, and what each one is handed.

    Every SAS task is stubbed, so what is under test is the orchestration -- which is
    exactly the part no unit test of an individual step can reach.
    """

    RA, DEC = 254.4575, 35.3423

    def reduce(self, tmp_path, stub_sas, config=None, **kwargs):
        base = a_reducible_observation(tmp_path, **kwargs)
        stub = stub_sas()
        result = xmm.process_xmm_obsid.fn(
            "0153950401", config=dict(base, **(config or {})), ra=self.RA, dec=self.DEC
        )
        return stub, result

    def test_every_exposure_is_barycentred(self, tmp_path, stub_sas):
        an_odf(tmp_path)
        stub, _ = self.reduce(tmp_path, stub_sas)

        corrected = [params["table"] for params in stub.task("barycen")]
        assert len(corrected) == 3, "one barycentred copy per exposure"
        assert all(t.endswith("_cl_bary.evt:EVENTS") for t in corrected), corrected

    def test_the_odf_is_ingested_once_for_the_whole_observation(self, tmp_path, stub_sas):
        an_odf(tmp_path)
        stub, _ = self.reduce(tmp_path, stub_sas)

        assert len(stub.task("odfingest")) == 1, "every exposure shares one ODF"

    def test_an_observation_with_no_odf_still_reduces(self, tmp_path, stub_sas):
        # No ODF downloaded: barycentring is the one thing that cannot be done, and it is
        # not worth failing an otherwise complete reduction over.
        stub, result = self.reduce(tmp_path, stub_sas)

        assert result is None
        assert stub.task("barycen") == []
        assert len(stub.task("especget")) == 3, "the spectra are unaffected"

    def test_an_observation_with_no_epic_data_is_not_a_failure(self, tmp_path, stub_sas):
        stub, result = self.reduce(tmp_path, stub_sas, event_lists=[])

        assert result == NO_SCIENCE_DATA
        assert stub.calls == [], "nothing should have been run on an empty observation"

    def test_the_calibration_index_is_built_before_any_other_task(self, tmp_path, stub_sas):
        stub, _ = self.reduce(tmp_path, stub_sas)

        assert stub.calls[0][0] == "cifbuild"
        assert len(stub.task("cifbuild")) == 1, "the index is per observation, not per exposure"

    def test_every_task_after_it_is_pointed_at_the_index_we_built(self, tmp_path, stub_sas):
        stub, _ = self.reduce(tmp_path, stub_sas)
        config = xmm.xmm_config(dict(out_data_path=str(tmp_path), input_data_path=str(tmp_path)))
        index = xmm.xmm_calibration_index_path("0153950401", config)

        assert {env.get("SAS_CCF") for env in stub.environments[1:]} == {index}

    def test_every_exposure_is_screened(self, tmp_path, stub_sas):
        stub, _ = self.reduce(tmp_path, stub_sas)

        assert len(stub.task("evselect")) >= 3

    def test_the_position_is_converted_once_per_imaging_exposure_only(self, tmp_path, stub_sas):
        stub, _ = self.reduce(tmp_path, stub_sas)

        # PNS003 is timing; M1S004 and M2S005 are imaging. A timing read-out has no sky
        # image, so converting into one would be meaningless rather than merely wasteful.
        assert len(stub.task("ecoordconv")) == 2

    def test_the_odf_route_says_it_is_not_built_yet_rather_than_half_running(
        self, tmp_path, stub_sas
    ):
        with pytest.raises(NotImplementedError, match="ODF"):
            self.reduce(tmp_path, stub_sas, config=dict(products="odf"))

    def test_the_diagnostics_the_report_reads_are_written(self, tmp_path, stub_sas):
        self.reduce(tmp_path, stub_sas)
        written = {path.name for path in (tmp_path / "0153950401" / "diagnostics").glob("*.json")}

        assert any(name.startswith("flare_filtering") for name in written)
        assert any(name.startswith("source_position") for name in written)


class TestTheWindowEdgeIsRobustToStrayEvents:
    """
    A few events with bad sky coordinates must not widen the measured window.

    Measured on ``0870940101``: taking the extremes made MOS1's ``PrimePartialW3`` chip
    8.8 by 11.4 arcmin when it reads out about 5.5, and made that observation's pn reach
    102.5 arcsec against a robust 87.6 -- the difference between clearing the 90 arcsec
    annulus and not. The error runs the dangerous way, because an inflated reach suppresses
    a warning rather than raising a spurious one.
    """

    def test_a_stray_event_does_not_widen_the_window(self, tmp_path):
        honest = a_windowed_event_file(tmp_path / "honest.ds", {1: (25700, 26300, 25700, 26300)})
        assert xmm.xmm_window_reach_arcsec(honest, 26000, 26000) == pytest.approx(15.0)

        from astropy.io import fits

        with fits.open(honest) as hdul:
            events = hdul["EVENTS"].data
            strays = fits.BinTableHDU.from_columns(
                fits.ColDefs(
                    [
                        fits.Column(
                            name="CCDNR",
                            format="I",
                            array=np.concatenate([events["CCDNR"], [1, 1]]),
                        ),
                        fits.Column(
                            name="X", format="E", array=np.concatenate([events["X"], [0, 60000]])
                        ),
                        fits.Column(
                            name="Y", format="E", array=np.concatenate([events["Y"], [0, 60000]])
                        ),
                    ]
                ),
                name="EVENTS",
            )
            fits.HDUList([fits.PrimaryHDU(), strays]).writeto(tmp_path / "strays.ds")

        widened = xmm.xmm_window_reach_arcsec(str(tmp_path / "strays.ds"), 26000, 26000)

        assert widened == pytest.approx(15.0, abs=1.0), (
            "two bad events out of 146 moved the window edge"
        )


class TestTheSpectraReachThePage:
    """
    What ``xmm_calculate_spectra`` records has to be what ``report.spectrum_figure`` reads.

    Two separate things went wrong here on the first real run and neither failed anything:
    the arrays were recorded flat, under names the figure does not look for, and the
    figure's energy band is NuSTAR's 3-79 keV, which keeps only the hard tail of an XMM
    spectrum. The page came out with the section missing altogether.
    """

    def test_the_arrays_are_named_the_way_the_figure_looks_them_up(self):
        from heasarc_retrieve_pipeline import report

        exposure = an_exposure(None, instrument="mos2", mode=xmm.IMAGING)
        stem = xmm._exposure_stem(exposure)
        recorded = {f"spec_{stem}_src_energy", f"spec_{stem}_bkg_energy"}

        found = {
            key[len("spec_") : -len("_src_energy")]
            for key in recorded
            if key.startswith("spec_") and key.endswith("_src_energy")
        }

        assert found == {stem}
        assert report.NUSTAR_SPECTRUM_BAND_KEV == (3.0, 79.0)

    def test_the_band_is_the_screening_band_and_not_nustars(self):
        from heasarc_retrieve_pipeline import report

        low, high = xmm.SPECTRUM_PLOT_BAND_KEV

        assert (low, high) == (0.2, 12.0)
        assert xmm.SPECTRUM_PLOT_BAND_KEV != report.NUSTAR_SPECTRUM_BAND_KEV
        # The band the events were screened to, so nothing drawn is outside the data.
        assert "PI in [200:12000]" in xmm.SCREENING_EXPRESSIONS[("pn", xmm.IMAGING)]

    def test_a_record_naming_a_band_is_drawn_over_that_band(self):
        from heasarc_retrieve_pipeline import report

        energy = np.array([0.5, 1.0, 5.0, 50.0])
        arrays = {
            "spec_pnS003_imaging_src_energy": energy,
            "spec_pnS003_imaging_src_rate": np.array([1.0, 2.0, 3.0, 4.0]),
        }
        record = dict(values=dict(energy_band=list(xmm.SPECTRUM_PLOT_BAND_KEV)))

        figure = report.spectrum_figure(record, arrays)

        (trace,) = figure.data
        assert list(trace.x) == [0.5, 1.0, 5.0], "the 50 keV point is outside XMM's band"

    def test_a_record_naming_no_band_still_gets_nustars(self):
        from heasarc_retrieve_pipeline import report

        energy = np.array([0.5, 1.0, 5.0, 50.0])
        arrays = {
            "spec_A_src_energy": energy,
            "spec_A_src_rate": np.array([1.0, 2.0, 3.0, 4.0]),
        }

        figure = report.spectrum_figure(dict(values={}), arrays)

        (trace,) = figure.data
        assert list(trace.x) == [5.0, 50.0], "NuSTAR's records must keep drawing as before"


class TestTheFlareCurveIsDrawn:
    """
    XMM records one background curve, not NuSTAR's two bands before and after.

    ``report.flare_figure`` drew NuSTAR's three empty panels for it on the first real run:
    it returned a figure rather than ``None``, so nothing looked wrong, and the page showed
    axis titles reading "3-10 keV (solar stray light)" over no data at all.
    """

    ARRAYS = dict(
        lc_time=np.linspace(0.0, 1000.0, 50),
        lc_rate=np.full(50, 2.0),
        lc_rate_err=np.full(50, 0.2),
        removed=np.array([[400.0, 500.0]]),
    )
    VALUES = dict(threshold=3.4, exposure_before=1000.0, exposure_after=900.0)

    def test_one_curve_gives_one_panel_with_data_in_it(self):
        from heasarc_retrieve_pipeline import report

        figure = report.flare_figure(dict(values=self.VALUES), dict(self.ARRAYS))

        assert figure is not None
        assert len(figure.data) == 1, "NuSTAR's three panels are not XMM's picture"
        assert list(figure.data[0].y) == [2.0] * 50

    def test_the_threshold_and_the_removed_interval_are_both_shown(self):
        from heasarc_retrieve_pipeline import report

        figure = report.flare_figure(dict(values=self.VALUES), dict(self.ARRAYS))
        shapes = figure.layout.shapes

        assert any(getattr(shape, "y0", None) == 3.4 for shape in shapes), "no threshold line"
        assert any(getattr(shape, "x0", None) == 400.0 for shape in shapes), "nothing shaded"

    def test_an_empty_curve_draws_nothing_rather_than_empty_axes(self):
        from heasarc_retrieve_pipeline import report

        empty = dict(lc_time=np.array([]), lc_rate=np.array([]))

        assert report.flare_figure(dict(values={}), empty) is None


def an_odf(tmp_path, obsid="0153950401", compressed=True):
    """
    The housekeeping the archive ships beside the PPS products, in miniature.

    ``compressed`` writes the ``.FIT.gz``/``.ASC.gz`` HEASARC actually serves; ``False``
    writes them already plain, the other case the staging has to handle.
    """
    odf = tmp_path / obsid / "ODF"
    odf.mkdir(parents=True, exist_ok=True)
    for name in (f"3906_{obsid}_SCX00000ATS.FIT", f"3906_{obsid}_SCX00000ROS.ASC"):
        body = f"contents of {name}\n".encode()
        if compressed:
            with gzip.open(odf / (name + ".gz"), "wb") as out:
                out.write(body)
        else:
            (odf / name).write_bytes(body)
    return odf


def staged_names(directory):
    return sorted(os.path.basename(p) for p in glob.glob(os.path.join(directory, "*")))


class TestStagingTheOdf:
    """``xmm_stage_odf``: the archive's names in, the names ``odfingest`` reads out."""

    PLAIN = ["3906_0153950401_SCX00000ATS.FIT", "3906_0153950401_SCX00000ROS.ASC"]

    def config(self, tmp_path):
        return dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path))

    def test_the_compressed_constituents_are_decompressed(self, tmp_path):
        an_odf(tmp_path)

        staged = xmm.xmm_stage_odf("0153950401", self.config(tmp_path))

        # Not .FTZ and not .gz: odfingest does not find either while scanning an ODF
        # directory, and ingests as though the housekeeping were absent.
        assert staged_names(staged) == self.PLAIN

    def test_the_contents_survive_the_decompression(self, tmp_path):
        an_odf(tmp_path)

        staged = xmm.xmm_stage_odf("0153950401", self.config(tmp_path))

        body = open(os.path.join(staged, self.PLAIN[0])).read()
        assert body == f"contents of {self.PLAIN[0]}\n"

    def test_files_that_arrive_uncompressed_are_copied_unchanged(self, tmp_path):
        an_odf(tmp_path, compressed=False)

        staged = xmm.xmm_stage_odf("0153950401", self.config(tmp_path))

        assert staged_names(staged) == self.PLAIN

    def test_an_observation_with_no_odf_stages_nothing(self, tmp_path):
        assert xmm.xmm_stage_odf("0153950401", self.config(tmp_path)) is None

    def test_the_staging_directory_is_under_the_output_tree(self):
        config = dict(input_data_path="/in", out_data_path="/out")

        assert xmm.xmm_staged_odf_path("0153950401", config) == "/out/0153950401/event_cl/odf"


class TestIngestingTheOdf:
    """``xmm_odf_summary``: running ``odfingest`` and finding what it wrote."""

    def config(self, tmp_path):
        return dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path))

    def test_odfingest_is_told_where_the_staged_odf_is(self, tmp_path, stub_sas):
        an_odf(tmp_path)
        stub = stub_sas()

        xmm.xmm_odf_summary("0153950401", self.config(tmp_path))

        staged = xmm.xmm_staged_odf_path("0153950401", self.config(tmp_path))
        (params,) = stub.task("odfingest")
        assert params["odfdir"] == staged
        assert params["outdir"] == staged

    def test_sas_odf_points_at_the_directory_while_ingesting(self, tmp_path, stub_sas):
        an_odf(tmp_path)
        stub = stub_sas()

        xmm.xmm_odf_summary("0153950401", self.config(tmp_path), env={"SAS_CCF": "/c/ccf.cif"})

        env = stub.environments[0]
        assert env["SAS_ODF"] == xmm.xmm_staged_odf_path("0153950401", self.config(tmp_path))
        assert env["SAS_CCF"] == "/c/ccf.cif", "the rest of the environment must survive"

    def test_the_summary_file_is_found_by_suffix(self, tmp_path, stub_sas):
        an_odf(tmp_path)
        stub_sas()

        summary = xmm.xmm_odf_summary("0153950401", self.config(tmp_path))

        assert os.path.basename(summary) == "3906_0153950401_SCX00000SUM.SAS"

    def test_no_odf_means_no_summary_and_no_task(self, tmp_path, stub_sas):
        stub = stub_sas()

        assert xmm.xmm_odf_summary("0153950401", self.config(tmp_path)) is None
        assert stub.calls == []


class TestBarycentringAnExposure:
    """
    ``xmm_barycenter``: the copy, the environment, and what is recorded.

    SAS ``barycen`` edits its input, so *which* file it is handed matters as much as what
    it is told.
    """

    def setup_files(self, tmp_path):
        events = tmp_path / "pnS003_imaging_cl.evt"
        an_event_file(str(events))
        return str(events), str(tmp_path / "sum.SAS")

    def recorder(self, tmp_path):
        return record_step(str(tmp_path / "diag"), "0153950401", "barycenter")

    def test_the_original_event_list_is_not_the_one_corrected(self, tmp_path, stub_sas):
        events, summary = self.setup_files(tmp_path)
        stub = stub_sas()

        output = xmm.xmm_barycenter("0153950401", {}, events, summary)

        (params,) = stub.task("barycen")
        assert params["table"] == f"{output}:EVENTS"
        assert output != events, "barycen edits in place, so it must be given a copy"
        assert output.endswith("pnS003_imaging_cl_bary.evt")

    def test_the_copy_is_in_place_before_the_task_runs(self, tmp_path, stub_sas, monkeypatch):
        events, summary = self.setup_files(tmp_path)
        stub = stub_sas()
        seen = {}

        def run(name, **kwargs):
            seen[name] = os.path.exists(str(kwargs["produces"].path))
            return stub(name, **kwargs)

        monkeypatch.setattr(sas, "run", run)
        xmm.xmm_barycenter("0153950401", {}, events, summary)

        assert seen["barycen"], "barycen corrects a file that must already be there"

    def test_the_original_is_left_alone(self, tmp_path, stub_sas):
        events, summary = self.setup_files(tmp_path)
        before = open(events, "rb").read()
        stub_sas()

        xmm.xmm_barycenter("0153950401", {}, events, summary)

        assert open(events, "rb").read() == before

    def test_sas_odf_points_at_the_summary_file(self, tmp_path, stub_sas):
        events, summary = self.setup_files(tmp_path)
        stub = stub_sas()

        xmm.xmm_barycenter("0153950401", {}, events, summary, env={"SAS_CCF": "/c.cif"})

        assert stub.environments[0]["SAS_ODF"] == summary
        assert stub.environments[0]["SAS_CCF"] == "/c.cif"

    def test_without_a_summary_nothing_runs_and_the_reason_is_recorded(self, tmp_path, stub_sas):
        events, _ = self.setup_files(tmp_path)
        stub = stub_sas()

        with self.recorder(tmp_path) as rec:
            assert xmm.xmm_barycenter("0153950401", {}, events, None, rec=rec) is None

        assert stub.calls == []
        assert rec.values["barycentered"] is False
        assert "no ODF summary" in rec.values["reason"]

    def test_a_task_that_converted_nothing_is_not_taken_at_its_word(self, tmp_path, stub_sas):
        # barycen edits in place, so produces=IN_PLACE can only confirm that the copy we
        # made ourselves is still there. The time system is the real evidence.
        events, summary = self.setup_files(tmp_path)
        stub_sas(barycen_timesys=None)

        with pytest.raises(ValueError, match="rather than TDB"):
            xmm.xmm_barycenter("0153950401", {}, events, summary)

    def test_the_time_system_is_recorded(self, tmp_path, stub_sas):
        events, summary = self.setup_files(tmp_path)
        stub_sas()

        with self.recorder(tmp_path) as rec:
            xmm.xmm_barycenter("0153950401", {}, events, summary, rec=rec)

        assert rec.values["timesys"] == "TDB"
        assert rec.values["timeref"] == "SOLARSYSTEM"

    def test_a_successful_correction_is_recorded(self, tmp_path, stub_sas):
        events, summary = self.setup_files(tmp_path)
        stub_sas()

        with self.recorder(tmp_path) as rec:
            xmm.xmm_barycenter("0153950401", {}, events, summary, rec=rec)

        assert rec.values["barycentered"] is True
        assert rec.values["barycentered_file"] == "pnS003_imaging_cl_bary.evt"


def an_odf_event_list(
    path, instrument="EPN", expid="S003", datamode="IMAGING", submode="PrimeFullWindow"
):
    """
    An ``epproc``/``emproc`` output, carrying the header keywords SAS writes on one.

    The name is deliberately not one the code may parse: the point of reading headers is
    that the name is SAS's business and has changed between releases.
    """
    path = str(path)
    an_event_file(
        path,
        INSTRUME=instrument,
        EXPIDSTR=expid,
        DATAMODE=datamode,
        SUBMODE=submode,
    )
    return path


class TestWhatTheOdfPipelineProduced:
    """
    ``xmm_exposures_from_odf``: identity from header keywords, not from file names.
    """

    def events_dir(self, tmp_path):
        config = dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path))
        directory = xmm.xmm_odf_events_path("0153950401", config)
        os.makedirs(directory, exist_ok=True)
        return config, directory

    def test_the_camera_exposure_and_mode_come_from_the_header(self, tmp_path):
        config, directory = self.events_dir(tmp_path)
        an_odf_event_list(
            os.path.join(directory, "whatever_SAS_calls_it_ImagingEvts.ds"),
            instrument="EMOS2",
            expid="S018",
            datamode="IMAGING",
            submode="PrimePartialW3",
        )

        (exposure,) = xmm.xmm_exposures_from_odf("0153950401", config)

        assert exposure.instrument == "mos2"
        assert exposure.expid == "S018"
        assert exposure.mode == xmm.IMAGING
        assert exposure.submode == "PrimePartialW3"

    def test_the_file_name_is_never_parsed(self, tmp_path):
        # A name that says MOS1 timing while the header says pn imaging. The header wins,
        # which is the whole reason this route does not parse names.
        config, directory = self.events_dir(tmp_path)
        an_odf_event_list(
            os.path.join(directory, "0405_0153950401_EMOS1_S004_TimingEvts.ds"),
            instrument="EPN",
            expid="U002",
            datamode="IMAGING",
        )

        (exposure,) = xmm.xmm_exposures_from_odf("0153950401", config)

        assert (exposure.instrument, exposure.expid, exposure.mode) == ("pn", "U002", xmm.IMAGING)

    def test_the_cameras_come_back_in_the_order_the_pps_route_uses(self, tmp_path):
        config, directory = self.events_dir(tmp_path)
        for name, instrument in (("c.ds", "EMOS2"), ("a.ds", "EPN"), ("b.ds", "EMOS1")):
            an_odf_event_list(
                os.path.join(directory, name.replace(".ds", "Evts.ds")), instrument=instrument
            )

        exposures = xmm.xmm_exposures_from_odf("0153950401", config)

        assert [e.instrument for e in exposures] == ["pn", "mos1", "mos2"]

    def test_imaging_precedes_timing_within_one_exposure(self, tmp_path):
        config, directory = self.events_dir(tmp_path)
        an_odf_event_list(os.path.join(directory, "tEvts.ds"), expid="S004", datamode="TIMING")
        an_odf_event_list(os.path.join(directory, "iEvts.ds"), expid="S004", datamode="IMAGING")

        modes = [e.mode for e in xmm.xmm_exposures_from_odf("0153950401", config)]

        assert modes == [xmm.IMAGING, xmm.TIMING]

    def test_the_rgs_and_the_optical_monitor_are_not_epic(self, tmp_path):
        config, directory = self.events_dir(tmp_path)
        an_odf_event_list(os.path.join(directory, "rgsEvts.ds"), instrument="RGS1")
        an_odf_event_list(os.path.join(directory, "omEvts.ds"), instrument="OM")

        assert xmm.xmm_exposures_from_odf("0153950401", config) == []

    def test_an_observation_the_pipeline_produced_nothing_for_is_empty(self, tmp_path):
        config, _ = self.events_dir(tmp_path)

        assert xmm.xmm_exposures_from_odf("0153950401", config) == []

    def test_there_is_no_flare_curve_because_the_odf_has_none(self, tmp_path):
        config, directory = self.events_dir(tmp_path)
        an_odf_event_list(os.path.join(directory, "aEvts.ds"))

        (exposure,) = xmm.xmm_exposures_from_odf("0153950401", config)

        assert exposure.flare_lightcurve is None

    def test_an_unreadable_file_is_skipped_with_a_warning(self, tmp_path, caplog):
        config, directory = self.events_dir(tmp_path)
        open(os.path.join(directory, "brokenEvts.ds"), "w").write("not FITS\n")
        an_odf_event_list(os.path.join(directory, "goodEvts.ds"))

        with caplog.at_level("WARNING"):
            exposures = xmm.xmm_exposures_from_odf("0153950401", config)

        assert len(exposures) == 1
        assert "brokenEvts.ds" in caplog.text


class TestRunningTheOdfPipeline:
    """
    ``xmm_run_odf_pipeline``: both tasks run, and neither is trusted for having returned.
    """

    def config(self, tmp_path):
        return dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path))

    def produce(self, directory, names=("pnEvts.ds",)):
        def write(name, **kwargs):
            for output in names:
                an_odf_event_list(os.path.join(directory, output))

        return write

    def test_both_tasks_are_run(self, tmp_path, stub_sas, monkeypatch):
        config = self.config(tmp_path)
        directory = xmm.xmm_odf_events_path("0153950401", config)
        stub = stub_sas()
        os.makedirs(directory, exist_ok=True)
        write = self.produce(directory)

        def run(name, **kwargs):
            result = stub(name, **kwargs)
            write(name)
            return result

        monkeypatch.setattr(sas, "run", run)
        xmm.xmm_run_odf_pipeline("0153950401", config)

        assert [name for name, _ in stub.calls] == ["epproc", "emproc"]

    def test_the_tasks_run_where_their_output_goes(self, tmp_path, stub_sas, monkeypatch):
        config = self.config(tmp_path)
        directory = xmm.xmm_odf_events_path("0153950401", config)
        stub = stub_sas()
        os.makedirs(directory, exist_ok=True)

        def run(name, **kwargs):
            result = stub(name, **kwargs)
            an_odf_event_list(os.path.join(directory, "aEvts.ds"))
            return result

        monkeypatch.setattr(sas, "run", run)
        xmm.xmm_run_odf_pipeline("0153950401", config)

        assert all(params["cwd"] == directory for params in stub.task("epproc"))

    def test_one_camera_failing_does_not_stop_the_other(
        self, tmp_path, stub_sas, monkeypatch, caplog
    ):
        config = self.config(tmp_path)
        directory = xmm.xmm_odf_events_path("0153950401", config)
        stub = stub_sas()
        os.makedirs(directory, exist_ok=True)

        def run(name, **kwargs):
            if name == "epproc":
                raise RuntimeError("epproc returned 1")
            result = stub(name, **kwargs)
            an_odf_event_list(os.path.join(directory, "mosEvts.ds"), instrument="EMOS1")
            return result

        monkeypatch.setattr(sas, "run", run)
        with caplog.at_level("WARNING"):
            xmm.xmm_run_odf_pipeline("0153950401", config)

        assert "epproc failed" in caplog.text
        assert len(xmm.xmm_exposures_from_odf("0153950401", config)) == 1

    def test_two_tasks_that_returned_zero_and_wrote_nothing_is_a_failure(self, tmp_path, stub_sas):
        # Both tasks "succeed" and leave no event list. A return code is not evidence.
        config = self.config(tmp_path)
        stub_sas()

        with pytest.raises(RuntimeError, match="produced an event list"):
            xmm.xmm_run_odf_pipeline("0153950401", config)
