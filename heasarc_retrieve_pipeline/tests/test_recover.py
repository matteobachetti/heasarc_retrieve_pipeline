"""
Offline tests for measuring an observation that nobody recorded.

Every tree reduced before this package recorded anything looks like this: the event
files, the regions and the joined files are all there, and the diagnostics directory is
empty or absent. What matters here is that recovery reads those products and writes the
datasets, that it never writes a product of its own, and that it leaves alone anything
that was already measured -- a page build on a finished observation must stay free.
"""

import os

import numpy as np
import pytest

from astropy.io import fits

from heasarc_retrieve_pipeline import recover, report
from heasarc_retrieve_pipeline.diagnostics import (
    diagnostics_path,
    read_arrays,
    read_records,
    record_step,
)

pytest.importorskip("skimage")
pytest.importorskip("statsmodels")

OBSID = "90101201002"


def cleaned_event_file(path, n_source=4000, n_background=2000, seed=1234):
    """A cleaned NuSTAR event file with one bright source, as a reduction leaves it."""
    rng = np.random.default_rng(seed)
    x = np.concatenate([rng.normal(500, 4, n_source), rng.uniform(300, 700, n_background)])
    y = np.concatenate([rng.normal(500, 4, n_source), rng.uniform(300, 700, n_background)])
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="X", format="E", array=x),
            fits.Column(name="Y", format="E", array=y),
            fits.Column(name="PI", format="J", array=rng.integers(40, 1900, x.size)),
            fits.Column(name="TIME", format="D", array=np.linspace(0, 1000, x.size)),
        ],
        name="EVENTS",
    )
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


def a_reduction_nobody_recorded(tmp_path, obsid=OBSID, modules=("A", "B")):
    """An observation directory with cleaned event files and no diagnostics at all."""
    pipedir = os.path.join(str(tmp_path), obsid, "event_pipe")
    os.makedirs(pipedir, exist_ok=True)
    for fpm in modules:
        cleaned_event_file(os.path.join(pipedir, f"nu{obsid}{fpm}01_cl.evt"))
    return pipedir


def flare_pair(tmp_path, obsid=OBSID, root="_src1", removed=(400.0, 600.0)):
    """The two files the flare filtering leaves: the input, and its filtered copy.

    The filtered one is the input with an interval cut out of its GTI, which is what
    ``apply_gti`` produces and what recovery has to read back.
    """
    base = os.path.join(str(tmp_path), obsid)
    os.makedirs(base, exist_ok=True)
    event_file = os.path.join(base, f"nu{obsid}{root}.evt")
    make_event_file_with_gti(event_file, gti=[(0.0, 1000.0)])
    filtered = event_file.replace(".evt", "_noflares.evt")
    make_event_file_with_gti(
        filtered, gti=[(0.0, removed[0]), (removed[1], 1000.0)]
    )
    return event_file, filtered


def make_event_file_with_gti(path, gti, tstart=0.0, tstop=1000.0, nevents=800, seed=42):
    """A NuSTAR-shaped event file with the given good time intervals."""
    rng = np.random.default_rng(seed)
    times = np.sort(rng.uniform(tstart, tstop, nevents))
    events = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="TIME", format="D", array=times),
            # PI 35 is 3 keV and PI 1935 is 79 keV, via E = 0.04 * PI + 1.6, so this
            # spans both of the bands the diagnostic compares.
            fits.Column(name="PI", format="J", array=rng.integers(35, 1935, nevents)),
        ],
        name="EVENTS",
    )
    events.header["TIMEZERO"] = 0.0
    events.header["TSTART"] = tstart
    events.header["TSTOP"] = tstop
    events.header["MJDREFI"] = 55197
    events.header["MJDREFF"] = 0.00076601852
    events.header["ONTIME"] = sum(stop - start for start, stop in gti)
    events.header["LIVETIME"] = 0.9 * events.header["ONTIME"]
    events.header["EXPOSURE"] = events.header["LIVETIME"]

    gti_hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=np.array([g[0] for g in gti])),
            fits.Column(name="STOP", format="D", array=np.array([g[1] for g in gti])),
        ],
        name="GTI",
    )
    fits.HDUList([fits.PrimaryHDU(), events, gti_hdu]).writeto(path, overwrite=True)
    return str(path)


def pha_file(path, exposure=1000.0, counts=None, nchan=100, seed=3):
    """A PHA spectrum shaped like the one ``nuproducts`` writes."""
    rng = np.random.default_rng(seed)
    if counts is None:
        counts = rng.poisson(50, nchan)
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CHANNEL", format="J", array=np.arange(nchan) + 35),
            fits.Column(name="COUNTS", format="J", array=np.asarray(counts)),
        ],
        name="SPECTRUM",
    )
    hdu.header["EXPOSURE"] = exposure
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


def a_products_directory(tmp_path, obsid=OBSID, stems=("nu90101201002A01",)):
    """A products directory as a finished reduction leaves it, with no records."""
    products = os.path.join(str(tmp_path), obsid, "products")
    os.makedirs(products, exist_ok=True)
    for stem in stems:
        pha_file(os.path.join(products, stem + "_sr.pha"))
        pha_file(os.path.join(products, stem + "_bk.pha"), counts=np.full(100, 5))
    return products



def a_goes_light_curve(path, tstart=0.0, tstop=1000.0, nbins=20):
    """A solar X-ray light curve in the observation's mission elapsed time."""
    time = np.linspace(tstart, tstop, nbins)
    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="TIME", format="D", array=time),
            fits.Column(name="XRSA", format="D", array=np.full(nbins, 5e-8)),
            fits.Column(name="XRSB", format="D", array=np.full(nbins, 2e-6)),
        ],
        name="GOES",
    )
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return path

class TestRecoveringTheSeparation:
    """The focal plane of a reduction that finished before anything recorded it."""

    def test_a_dataset_is_written_for_every_event_file(self, tmp_path):
        a_reduction_nobody_recorded(tmp_path)

        recovered = recover.recover_observation(OBSID, str(tmp_path))

        assert len(recovered) == 2
        records = read_records(diagnostics_path(OBSID, dict(out_data_path=str(tmp_path))))
        assert {r["key"] for r in records} == {
            f"nu{OBSID}A01_cl",
            f"nu{OBSID}B01_cl",
        }
        assert all(r["arrays"] for r in records)

    def test_the_dataset_is_the_one_the_figure_needs(self, tmp_path):
        a_reduction_nobody_recorded(tmp_path, modules=("A",))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_observation(OBSID, str(tmp_path))

        (record,) = read_records(directory)
        fig = report.separation_figure(record, read_arrays(directory, record))
        assert fig is not None
        assert fig.data[0].z.shape == (99, 99)

    def test_it_says_the_work_was_not_done_by_this_run(self, tmp_path):
        a_reduction_nobody_recorded(tmp_path, modules=("A",))

        recover.recover_observation(OBSID, str(tmp_path))

        (record,) = read_records(diagnostics_path(OBSID, dict(out_data_path=str(tmp_path))))
        assert record["arrays_from_earlier_run"] is True
        assert record["status"] == "skipped"
        assert "earlier run" in record["reason"]

    def test_nothing_is_written_next_to_the_event_files(self, tmp_path):
        """Recovery reads a reduction. It must never add to one."""
        pipedir = a_reduction_nobody_recorded(tmp_path)
        before = sorted(os.listdir(pipedir))

        recover.recover_observation(OBSID, str(tmp_path))

        assert sorted(os.listdir(pipedir)) == before

    def test_the_split_directory_is_read_too(self, tmp_path):
        a_reduction_nobody_recorded(tmp_path, modules=("A",))
        splitdir = os.path.join(str(tmp_path), OBSID, "split")
        os.makedirs(splitdir, exist_ok=True)
        cleaned_event_file(os.path.join(splitdir, f"nu{OBSID}A06_chu2_cl.evt"))

        recover.recover_observation(OBSID, str(tmp_path))

        records = read_records(diagnostics_path(OBSID, dict(out_data_path=str(tmp_path))))
        assert f"nu{OBSID}A06_chu2_cl" in {r["key"] for r in records}

    def test_a_file_too_faint_to_measure_says_so(self, tmp_path):
        pipedir = os.path.join(str(tmp_path), OBSID, "event_pipe")
        os.makedirs(pipedir, exist_ok=True)
        cleaned_event_file(
            os.path.join(pipedir, f"nu{OBSID}A01_cl.evt"), n_source=3, n_background=3
        )

        recover.recover_observation(OBSID, str(tmp_path))

        (record,) = read_records(diagnostics_path(OBSID, dict(out_data_path=str(tmp_path))))
        assert record["status"] == "skipped"
        assert "20 events" in record["reason"]


class TestWhatRecoveryLeavesAlone:
    """A finished observation must not pay for this."""

    def test_a_step_that_already_has_a_dataset_is_not_measured_again(self, tmp_path):
        a_reduction_nobody_recorded(tmp_path, modules=("A",))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))
        with record_step(directory, OBSID, "separate_sources", key=f"nu{OBSID}A01_cl") as rec:
            rec.value(n_peaks=1)
            rec.array(image=np.zeros((4, 4), dtype=np.float32))

        recovered = recover.recover_observation(OBSID, str(tmp_path))

        assert recovered == []
        (record,) = read_records(directory)
        assert read_arrays(directory, record)["image"].shape == (4, 4)
        assert record["arrays_from_earlier_run"] is False

    def test_a_skipped_record_that_inherited_a_dataset_is_left_alone(self, tmp_path):
        """The rerun case: there is already a payload, so there is nothing to recover."""
        a_reduction_nobody_recorded(tmp_path, modules=("A",))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))
        key = f"nu{OBSID}A01_cl"
        with record_step(directory, OBSID, "separate_sources", key=key) as rec:
            rec.array(image=np.zeros((4, 4), dtype=np.float32))
        with record_step(directory, OBSID, "separate_sources", key=key) as rec:
            rec.skip("SEPARATE_DONE.TXT already exists")

        assert recover.recover_observation(OBSID, str(tmp_path)) == []

    def test_an_observation_that_was_never_reduced_recovers_nothing(self, tmp_path):
        os.makedirs(os.path.join(str(tmp_path), OBSID), exist_ok=True)

        assert recover.recover_observation(OBSID, str(tmp_path)) == []

    def test_an_observation_directory_that_does_not_exist_is_not_an_error(self, tmp_path):
        assert recover.recover_observation(OBSID, str(tmp_path)) == []

    def test_a_recovery_that_raises_is_logged_and_passed_over(self, tmp_path):
        """A page with a figure missing beats no page at all."""
        a_reduction_nobody_recorded(tmp_path, modules=("A",))

        original = recover.recover_separations

        def explode(obsid, outdir, measured=None):
            raise RuntimeError("the event file is a directory")

        recover.recover_separations = explode
        try:
            assert recover.recover_observation(OBSID, str(tmp_path)) == []
            path = report.write_observation_page(OBSID, str(tmp_path))
        finally:
            recover.recover_separations = original

        assert os.path.exists(path)


class TestThePageOfAnUnrecordedReduction:
    """End to end: a tree with products and no records still gets a page with figures."""

    def test_the_page_has_the_focal_plane_on_it(self, tmp_path):
        a_reduction_nobody_recorded(tmp_path, modules=("A",))

        path = report.write_observation_page(OBSID, str(tmp_path))

        with open(path) as fobj:
            page = fobj.read()
        assert "plotly-graph-div" in page
        assert "an earlier run left" in page

    def test_recovery_can_be_turned_off(self, tmp_path):
        a_reduction_nobody_recorded(tmp_path, modules=("A",))

        report.write_observation_page(OBSID, str(tmp_path), recover=False)

        assert read_records(diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))) == []


class TestRecoveringTheFlareFiltering:
    """
    The light curve of the solar-flare cut, from a reduction that recorded nothing.

    Both halves of the comparison are on disk -- the filtering writes a copy and never
    touches its input -- so this one is exact rather than approximate.
    """

    def test_the_diagnostic_is_recovered_from_the_pair(self, tmp_path):
        flare_pair(tmp_path)

        recovered = recover.recover_flare_filtering(OBSID, str(tmp_path))

        assert len(recovered) == 1
        (record,) = [
            r
            for r in read_records(diagnostics_path(OBSID, dict(out_data_path=str(tmp_path))))
            if r["step"] == "flare_filtering"
        ]
        assert record["arrays"]
        assert record["arrays_from_earlier_run"] is True

    def test_both_bands_are_there_before_and_after(self, tmp_path):
        flare_pair(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_observation(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        arrays = read_arrays(directory, record)
        for band in ("3_10", "10_79"):
            for when in ("before", "after"):
                assert f"lc_{band}_{when}_time" in arrays
                assert f"lc_{band}_{when}_rate" in arrays

    def test_the_removed_interval_is_the_one_the_cut_took_out(self, tmp_path):
        flare_pair(tmp_path, removed=(400.0, 600.0))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_observation(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        removed = read_arrays(directory, record)["removed"]
        np.testing.assert_allclose(removed, [[400.0, 600.0]])

    def test_the_figure_can_be_drawn_from_it(self, tmp_path):
        flare_pair(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_observation(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        assert report.flare_figure(record, read_arrays(directory, record)) is not None

    def test_the_live_time_lost_is_recorded(self, tmp_path):
        flare_pair(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_observation(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        assert record["values"]["nevents_after"] < record["values"]["nevents_before"]

    def test_nothing_is_written_next_to_the_products(self, tmp_path):
        flare_pair(tmp_path)
        base = os.path.join(str(tmp_path), OBSID)
        before = sorted(os.listdir(base))

        recover.recover_observation(OBSID, str(tmp_path))

        assert sorted(os.listdir(base)) == sorted(before + ["diagnostics"])

    def test_a_filtered_file_whose_input_is_gone_is_passed_over(self, tmp_path):
        event_file, _ = flare_pair(tmp_path)
        os.unlink(event_file)

        assert recover.recover_observation(OBSID, str(tmp_path)) == []

    def test_an_observation_that_was_never_filtered_recovers_nothing(self, tmp_path):
        base = os.path.join(str(tmp_path), OBSID)
        os.makedirs(base, exist_ok=True)
        make_event_file_with_gti(
            os.path.join(base, f"nu{OBSID}_src1.evt"), gti=[(0.0, 1000.0)]
        )

        assert recover.recover_flare_filtering(OBSID, str(tmp_path)) == []

    def test_a_step_that_already_has_a_dataset_is_left_alone(self, tmp_path):
        flare_pair(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))
        with record_step(directory, OBSID, "flare_filtering", key=f"nu{OBSID}_src1") as rec:
            rec.array(removed=np.array([[1.0, 2.0]]))

        assert recover.recover_flare_filtering(OBSID, str(tmp_path)) == []


    def test_the_observations_light_curve_is_used(self, tmp_path):
        flare_pair(tmp_path)
        base = os.path.join(str(tmp_path), OBSID)
        a_goes_light_curve(os.path.join(base, f"nu{OBSID}_goes.fits"))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_flare_filtering(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        assert record["values"]["goes_light_curve"] == f"nu{OBSID}_goes.fits"
        assert "goes_xrsb" in read_arrays(directory, record)

    def test_the_older_per_file_light_curve_is_found_too(self, tmp_path):
        """Before it was fetched once per observation it was fetched once per file."""
        event_file, _ = flare_pair(tmp_path)
        a_goes_light_curve(event_file.replace(".evt", "_goes.fits"))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_flare_filtering(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        assert record["values"]["goes_light_curve"] == f"nu{OBSID}_src1_goes.fits"
        assert "goes_xrsb" in read_arrays(directory, record)

    def test_the_observations_light_curve_wins(self, tmp_path):
        """Both names can be present; the one this package writes now is the newer."""
        event_file, _ = flare_pair(tmp_path)
        base = os.path.join(str(tmp_path), OBSID)
        a_goes_light_curve(os.path.join(base, f"nu{OBSID}_goes.fits"))
        a_goes_light_curve(event_file.replace(".evt", "_goes.fits"))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_flare_filtering(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        assert record["values"]["goes_light_curve"] == f"nu{OBSID}_goes.fits"

    def test_no_light_curve_anywhere_still_recovers_the_rest(self, tmp_path):
        flare_pair(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_flare_filtering(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "flare_filtering"]
        assert record["values"]["goes_light_curve"] is None
        assert "lc_3_10_before_rate" in read_arrays(directory, record)


class TestRecoveringTheSpectra:
    """The observation's last product, which nothing drew until now."""

    def test_the_spectra_are_read_back(self, tmp_path):
        a_products_directory(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recovered = recover.recover_spectra(OBSID, str(tmp_path))

        assert recovered == ["nu90101201002A01"]
        (record,) = [r for r in read_records(directory) if r["step"] == "calculate_spectra"]
        arrays = read_arrays(directory, record)
        assert "spec_nu90101201002A01_src_energy" in arrays
        assert "spec_nu90101201002A01_bkg_rate" in arrays

    def test_the_channels_become_energies_in_the_nustar_band(self, tmp_path):
        a_products_directory(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_spectra(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "calculate_spectra"]
        energy = read_arrays(directory, record)["spec_nu90101201002A01_src_energy"]
        # Channel 35 is 3 keV, via E = 0.04 * PI + 1.6.
        assert energy[0] == pytest.approx(3.0, abs=0.01)

    def test_the_figure_can_be_drawn_from_it(self, tmp_path):
        a_products_directory(tmp_path, stems=("nuA01", "nuB01"))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_spectra(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "calculate_spectra"]
        fig = report.spectrum_figure(record, read_arrays(directory, record))
        assert fig is not None
        assert len(fig.data) == 4, "a source and a background for each of two stems"

    def test_an_observation_with_no_products_recovers_nothing(self, tmp_path):
        assert recover.recover_spectra(OBSID, str(tmp_path)) == []

    def test_a_run_that_recorded_its_spectra_is_left_alone(self, tmp_path):
        a_products_directory(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))
        with record_step(directory, OBSID, "calculate_spectra") as rec:
            rec.array(spec_already_src_energy=np.array([3.0, 4.0]))

        assert recover.recover_spectra(OBSID, str(tmp_path)) == []

    def test_a_background_that_was_never_written_is_not_an_error(self, tmp_path):
        products = a_products_directory(tmp_path)
        os.unlink(os.path.join(products, "nu90101201002A01_bk.pha"))
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))

        recover.recover_spectra(OBSID, str(tmp_path))

        (record,) = [r for r in read_records(directory) if r["step"] == "calculate_spectra"]
        arrays = read_arrays(directory, record)
        assert "spec_nu90101201002A01_src_rate" in arrays
        assert "spec_nu90101201002A01_bkg_rate" not in arrays


class TestRecoveringTheJoining:
    """
    The intervals of every file the joining merged.

    Nothing is re-derived here: good time intervals are read out of a small extension,
    and the merged files are still named exactly as the joining named them.
    """

    def a_joined_observation(self, tmp_path, obsid=OBSID, label="_src1"):
        """A base directory with per-module and combined files, and their inputs."""
        base = os.path.join(str(tmp_path), obsid)
        pipedir = os.path.join(base, "event_pipe")
        os.makedirs(pipedir, exist_ok=True)
        for fpm in "A", "B":
            make_event_file_with_gti(
                os.path.join(pipedir, f"nu{obsid}{fpm}01_cl{label}.evt"),
                gti=[(0.0, 400.0), (600.0, 1000.0)],
            )
            make_event_file_with_gti(
                os.path.join(base, f"nu{obsid}{fpm}{label}.evt"),
                gti=[(0.0, 400.0), (600.0, 1000.0)],
            )
        combined = os.path.join(base, f"nu{obsid}{label}.evt")
        make_event_file_with_gti(combined, gti=[(0.0, 400.0), (600.0, 1000.0)])
        return combined

    def record_of(self, tmp_path, key="src1"):
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))
        (record,) = [
            r
            for r in read_records(directory)
            if r["step"] == "join_source_data" and r["key"] == key
        ]
        return record, read_arrays(directory, record)

    def test_the_joining_is_recovered(self, tmp_path):
        combined = self.a_joined_observation(tmp_path)

        recovered = recover.recover_joining(OBSID, str(tmp_path))

        assert recovered == [combined]
        record, _ = self.record_of(tmp_path)
        assert record["arrays_from_earlier_run"] is True

    def test_every_input_and_both_merges_are_there(self, tmp_path):
        self.a_joined_observation(tmp_path)

        recover.recover_joining(OBSID, str(tmp_path))

        _, arrays = self.record_of(tmp_path)
        assert "gti_A_in_0" in arrays
        assert "gti_B_in_0" in arrays
        assert "gti_A_out" in arrays
        assert "gti_B_out" in arrays
        assert "gti_combined" in arrays

    def test_the_intervals_are_the_ones_on_disk(self, tmp_path):
        self.a_joined_observation(tmp_path)

        recover.recover_joining(OBSID, str(tmp_path))

        _, arrays = self.record_of(tmp_path)
        np.testing.assert_allclose(
            arrays["gti_combined"], [[0.0, 400.0], [600.0, 1000.0]]
        )

    def test_the_figure_can_be_drawn_from_it(self, tmp_path):
        self.a_joined_observation(tmp_path)

        recover.recover_joining(OBSID, str(tmp_path))

        record, arrays = self.record_of(tmp_path)
        assert report.gti_figure(record, arrays) is not None

    def test_the_background_product_is_recovered_too(self, tmp_path):
        self.a_joined_observation(tmp_path, label="_back")

        recover.recover_joining(OBSID, str(tmp_path))

        record, _ = self.record_of(tmp_path, key="back")
        assert record["values"]["combined"] == f"nu{OBSID}_back.evt"

    def test_the_unsplit_mode_06_file_is_not_counted_twice(self, tmp_path):
        """nusplitsc has already replaced it with its CHU-resolved parts."""
        self.a_joined_observation(tmp_path)
        splitdir = os.path.join(str(tmp_path), OBSID, "split")
        os.makedirs(splitdir, exist_ok=True)
        for name in (f"nu{OBSID}A06_cl_src1.evt", f"nu{OBSID}A06_chu1_cl_src1.evt"):
            make_event_file_with_gti(os.path.join(splitdir, name), gti=[(0.0, 100.0)])

        recover.recover_joining(OBSID, str(tmp_path))

        record, _ = self.record_of(tmp_path)
        assert f"nu{OBSID}A06_chu1_cl_src1.evt" in record["values"]["inputs_A"]
        assert f"nu{OBSID}A06_cl_src1.evt" not in record["values"]["inputs_A"]

    def test_an_observation_that_was_never_joined_recovers_nothing(self, tmp_path):
        os.makedirs(os.path.join(str(tmp_path), OBSID), exist_ok=True)

        assert recover.recover_joining(OBSID, str(tmp_path)) == []

    def test_a_run_that_recorded_its_joining_is_left_alone(self, tmp_path):
        self.a_joined_observation(tmp_path)
        directory = diagnostics_path(OBSID, dict(out_data_path=str(tmp_path)))
        with record_step(directory, OBSID, "join_source_data", key="src1") as rec:
            rec.array(gti_combined=np.array([[1.0, 2.0]]))

        assert recover.recover_joining(OBSID, str(tmp_path)) == []
