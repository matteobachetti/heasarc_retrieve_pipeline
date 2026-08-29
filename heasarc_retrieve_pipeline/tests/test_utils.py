"""Offline tests for the pure helpers in :mod:`heasarc_retrieve_pipeline.utils`.

These need neither the network nor a HEASOFT installation. Every function tested here is
pure: it takes arrays and headers in, and gives arrays and headers back.
"""

import os

import numpy as np
import pytest

from astropy.io import fits

from heasarc_retrieve_pipeline.utils import (
    absolute_config,
    apply_gti,
    binned_lightcurve,
    good_intervals,
    intersect_intervals,
    intervals_above_threshold,
    intervals_removed,
    mask_from_gti,
    merge_intervals,
    read_gti,
)


class TestGoodIntervals:
    """The complement of a set of bad intervals inside ``[tstart, tstop]``.

    Every case here is one the running-``previous_gti_start`` loop in ``get_goes_gtis``
    got wrong. See issue 12 in ``docs/known_issues.rst``.
    """

    def test_no_bad_intervals_keeps_the_whole_observation(self):
        assert np.allclose(good_intervals([], 0, 100), [[0, 100]])

    def test_one_bad_interval_in_the_middle(self):
        assert np.allclose(good_intervals([(40, 60)], 0, 100), [[0, 40], [60, 100]])

    def test_bad_interval_straddling_tstart(self):
        """The bug: this used to emit ``START=0, STOP=-10``, a negative-length interval."""
        assert np.allclose(good_intervals([(-10, 30)], 0, 100), [[30, 100]])

    def test_bad_interval_straddling_tstop(self):
        assert np.allclose(good_intervals([(80, 130)], 0, 100), [[0, 80]])

    def test_bad_intervals_entirely_outside_are_ignored(self):
        assert np.allclose(
            good_intervals([(-50, -10), (200, 300)], 0, 100), [[0, 100]]
        )

    def test_bad_interval_covering_everything_leaves_nothing(self):
        assert len(good_intervals([(-10, 200)], 0, 100)) == 0

    def test_bad_interval_exactly_covering_everything_leaves_nothing(self):
        assert len(good_intervals([(0, 100)], 0, 100)) == 0

    def test_overlapping_bad_intervals_are_merged(self):
        assert np.allclose(
            good_intervals([(20, 50), (40, 70)], 0, 100), [[0, 20], [70, 100]]
        )

    def test_touching_bad_intervals_are_merged(self):
        assert np.allclose(
            good_intervals([(20, 50), (50, 70)], 0, 100), [[0, 20], [70, 100]]
        )

    def test_bad_intervals_out_of_order_are_sorted_first(self):
        assert np.allclose(
            good_intervals([(70, 80), (20, 30)], 0, 100),
            [[0, 20], [30, 70], [80, 100]],
        )

    def test_bad_interval_nested_in_another(self):
        assert np.allclose(
            good_intervals([(20, 80), (30, 40)], 0, 100), [[0, 20], [80, 100]]
        )

    def test_zero_length_bad_interval_changes_nothing(self):
        assert np.allclose(good_intervals([(50, 50)], 0, 100), [[0, 100]])

    def test_result_is_always_sorted_disjoint_and_inside_the_bounds(self):
        """The invariants, on a deliberately nasty input."""
        bad = [(90, 95), (-20, 5), (30, 35), (34, 40), (99, 200), (60, 60)]
        gti = good_intervals(bad, 0, 100)

        assert np.all(gti[:, 1] > gti[:, 0]), "every interval has positive length"
        assert np.all(np.diff(gti.flatten()) >= 0), "sorted and disjoint"
        assert gti.min() >= 0 and gti.max() <= 100, "inside [tstart, tstop]"

    def test_accepts_an_array_of_pairs(self):
        bad = np.array([[40.0, 60.0]])
        assert np.allclose(good_intervals(bad, 0, 100), [[0, 40], [60, 100]])


def make_event_file(
    times,
    gti,
    ontime=None,
    livetime=None,
    exposure=None,
    timezero=0.0,
    gti_extname="GTI",
    extra_extension_first=False,
):
    """A minimal event file in memory, shaped like the mission files the pipeline reads."""
    gti = np.asarray(gti, dtype=float)
    ontime = float(np.sum(gti[:, 1] - gti[:, 0])) if ontime is None else ontime
    livetime = ontime if livetime is None else livetime
    exposure = livetime if exposure is None else exposure

    events = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="TIME", format="D", array=np.asarray(times, dtype=float)),
            fits.Column(name="PI", format="J", array=np.arange(len(times))),
        ],
        name="EVENTS",
    )
    events.header["TIMEZERO"] = timezero
    events.header["ONTIME"] = ontime
    events.header["LIVETIME"] = livetime
    events.header["EXPOSURE"] = exposure

    gti_hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=gti[:, 0]),
            fits.Column(name="STOP", format="D", array=gti[:, 1]),
        ],
        name=gti_extname,
    )
    gti_hdu.header["TIMEZERO"] = timezero

    hdus = [fits.PrimaryHDU(), events]
    if extra_extension_first:
        hdus.append(fits.BinTableHDU.from_columns(
            [fits.Column(name="X", format="D", array=np.zeros(3))], name="SOMETHING"
        ))
    hdus.append(gti_hdu)
    return fits.HDUList(hdus)


class TestApplyGti:
    """Filtering an event file on a new GTI, header included.

    The bug this guards is issue 5: the old code replaced the GTI extension and left both
    the event table and the exposure keywords untouched.
    """

    def test_events_outside_the_new_gti_are_dropped(self):
        hdul = make_event_file(times=[5.0, 15.0, 25.0, 35.0, 45.0], gti=[[0, 50]])
        apply_gti(hdul, [[10, 30]])

        assert np.allclose(hdul["EVENTS"].data["TIME"], [15.0, 25.0])

    def test_events_on_the_gti_boundary_are_kept(self):
        hdul = make_event_file(times=[10.0, 30.0], gti=[[0, 50]])
        apply_gti(hdul, [[10, 30]])

        assert len(hdul["EVENTS"].data) == 2

    def test_other_columns_are_filtered_along_with_time(self):
        hdul = make_event_file(times=[5.0, 15.0, 25.0, 35.0], gti=[[0, 50]])
        apply_gti(hdul, [[10, 30]])

        assert np.allclose(hdul["EVENTS"].data["PI"], [1, 2])

    def test_the_new_gti_is_installed(self):
        hdul = make_event_file(times=[15.0], gti=[[0, 50]])
        apply_gti(hdul, [[10, 20], [30, 40]])

        assert np.allclose(hdul["GTI"].data["START"], [10, 30])
        assert np.allclose(hdul["GTI"].data["STOP"], [20, 40])

    def test_ontime_is_the_exact_gti_total(self):
        hdul = make_event_file(times=[15.0], gti=[[0, 50]])
        apply_gti(hdul, [[10, 20], [30, 45]])

        assert hdul["EVENTS"].header["ONTIME"] == pytest.approx(25.0)

    def test_livetime_and_exposure_scale_with_ontime(self):
        hdul = make_event_file(
            times=[15.0], gti=[[0, 50]], ontime=50.0, livetime=40.0, exposure=40.0
        )
        apply_gti(hdul, [[10, 20], [30, 45]])  # 25 s of 50 s survives

        assert hdul["EVENTS"].header["LIVETIME"] == pytest.approx(20.0)
        assert hdul["EVENTS"].header["EXPOSURE"] == pytest.approx(20.0)

    def test_the_gti_extension_is_found_by_name_not_by_index(self):
        hdul = make_event_file(
            times=[5.0, 15.0], gti=[[0, 50]], extra_extension_first=True
        )
        assert hdul.index_of("GTI") == 3

        apply_gti(hdul, [[10, 30]])

        assert np.allclose(hdul["GTI"].data["START"], [10])
        assert np.allclose(hdul["EVENTS"].data["TIME"], [15.0])

    def test_a_stdgti_extension_is_recognised(self):
        hdul = make_event_file(times=[5.0, 15.0], gti=[[0, 50]], gti_extname="STDGTI")
        apply_gti(hdul, [[10, 30]])

        assert np.allclose(hdul["STDGTI"].data["START"], [10])

    def test_timezero_is_honoured(self):
        """Event times are ``TIME + TIMEZERO``; the GTI is on that same scale."""
        hdul = make_event_file(
            times=[5.0, 15.0, 25.0], gti=[[0, 50]], timezero=1000.0
        )
        apply_gti(hdul, [[1010, 1030]])

        assert np.allclose(hdul["EVENTS"].data["TIME"], [15.0, 25.0])

    def test_an_empty_gti_leaves_no_events(self):
        hdul = make_event_file(times=[5.0, 15.0], gti=[[0, 50]])
        apply_gti(hdul, np.zeros((0, 2)))

        assert len(hdul["EVENTS"].data) == 0
        assert hdul["EVENTS"].header["ONTIME"] == pytest.approx(0.0)
        assert hdul["EVENTS"].header["EXPOSURE"] == pytest.approx(0.0)

    def test_it_reports_what_it_removed(self):
        hdul = make_event_file(
            times=[5.0, 15.0, 25.0, 45.0], gti=[[0, 50]], livetime=40.0
        )
        stats = apply_gti(hdul, [[10, 30]])

        assert stats["nevents_before"] == 4
        assert stats["nevents_after"] == 2
        assert stats["ontime_before"] == pytest.approx(50.0)
        assert stats["ontime_after"] == pytest.approx(20.0)
        assert stats["livetime_before"] == pytest.approx(40.0)
        assert stats["livetime_after"] == pytest.approx(16.0)

    def test_a_gti_as_a_fits_record_array_works_too(self):
        """``get_goes_gtis`` hands over the table it read from a GTI file."""
        source = make_event_file(times=[], gti=[[10, 30]])
        hdul = make_event_file(times=[5.0, 15.0, 35.0], gti=[[0, 50]])
        apply_gti(hdul, source["GTI"].data)

        assert np.allclose(hdul["EVENTS"].data["TIME"], [15.0])


class TestBinnedLightcurve:
    """A GTI-aware binned light curve, on input simple enough to check by hand."""

    def test_counts_and_rate_in_full_bins(self):
        times = np.array([1.0, 2.0, 3.0, 11.0])
        lc = binned_lightcurve(times, [[0, 20]], dt=10.0)

        assert np.allclose(lc["time"], [5.0, 15.0])
        assert np.allclose(lc["counts"], [3, 1])
        assert np.allclose(lc["exposure"], [10.0, 10.0])
        assert np.allclose(lc["rate"], [0.3, 0.1])

    def test_poisson_errors(self):
        lc = binned_lightcurve(np.zeros(4) + 1.0, [[0, 10]], dt=10.0)

        assert np.allclose(lc["rate"], [0.4])
        assert np.allclose(lc["rate_err"], [np.sqrt(4) / 10.0])

    def test_bins_outside_the_gti_are_dropped(self):
        times = np.array([5.0, 45.0])
        lc = binned_lightcurve(times, [[0, 10], [40, 50]], dt=10.0)

        assert np.allclose(lc["time"], [5.0, 45.0])

    def test_a_bin_straddling_a_gti_edge_gets_only_its_good_exposure(self):
        """The bin 0--10 is covered from 0 to 8, so its exposure is 8 s, not 10."""
        times = np.array([1.0, 2.0, 3.0, 4.0])
        lc = binned_lightcurve(times, [[0, 8]], dt=10.0, min_fraction=0.5)

        assert np.allclose(lc["exposure"], [8.0])
        assert np.allclose(lc["rate"], [0.5])

    def test_a_barely_covered_bin_is_dropped(self):
        times = np.array([1.0, 15.0, 16.0])
        lc = binned_lightcurve(times, [[0, 2], [10, 20]], dt=10.0, min_fraction=0.5)

        assert np.allclose(lc["time"], [15.0])

    def test_exposure_sums_over_several_gtis_inside_one_bin(self):
        lc = binned_lightcurve(np.array([1.0]), [[0, 3], [5, 9]], dt=10.0, min_fraction=0.5)

        assert np.allclose(lc["exposure"], [7.0])

    def test_an_empty_gti_gives_an_empty_light_curve(self):
        lc = binned_lightcurve(np.array([1.0]), np.zeros((0, 2)), dt=10.0)

        assert len(lc["time"]) == 0

    def test_no_events_still_gives_the_bins_with_zero_rate(self):
        lc = binned_lightcurve(np.array([]), [[0, 20]], dt=10.0)

        assert np.allclose(lc["rate"], [0.0, 0.0])
        assert np.allclose(lc["exposure"], [10.0, 10.0])


class TestMaskFromGti:
    def test_edges_count_as_inside(self):
        mask = mask_from_gti([0.0, 5.0, 10.0, 15.0], [[0, 10]])
        assert mask.tolist() == [True, True, True, False]

    def test_several_intervals(self):
        mask = mask_from_gti([5.0, 15.0, 25.0], [[0, 10], [20, 30]])
        assert mask.tolist() == [True, False, True]

    def test_an_empty_gti_selects_nothing(self):
        assert mask_from_gti([5.0, 15.0], np.zeros((0, 2))).tolist() == [False, False]

    def test_no_times(self):
        assert mask_from_gti([], [[0, 10]]).tolist() == []


class TestIntervalsRemoved:
    def test_a_hole_punched_in_the_middle(self):
        assert np.allclose(
            intervals_removed([[0, 100]], [[0, 40], [60, 100]]), [[40, 60]]
        )

    def test_a_whole_interval_dropped(self):
        assert np.allclose(intervals_removed([[0, 10], [20, 30]], [[0, 10]]), [[20, 30]])

    def test_nothing_removed(self):
        assert len(intervals_removed([[0, 100]], [[0, 100]])) == 0

    def test_pre_existing_gaps_are_not_reported_as_removed(self):
        """The gap between the two intervals was never good time; it is not our doing."""
        removed = intervals_removed([[0, 10], [20, 30]], [[0, 10], [20, 25]])

        assert np.allclose(removed, [[25, 30]])

    def test_everything_removed(self):
        assert np.allclose(
            intervals_removed([[0, 10], [20, 30]], np.zeros((0, 2))),
            [[0, 10], [20, 30]],
        )


class TestReadGti:
    def test_it_finds_the_gti_extension_by_name(self):
        hdul = make_event_file(times=[], gti=[[0, 10], [20, 30]], extra_extension_first=True)
        assert np.allclose(read_gti(hdul), [[0, 10], [20, 30]])

    def test_timezero_is_added(self):
        hdul = make_event_file(times=[], gti=[[0, 10]], timezero=1000.0)
        assert np.allclose(read_gti(hdul), [[1000, 1010]])


class TestApplyGtiWithAStaleHeader:
    """The scaling must come from the GTI, not from a keyword that disagrees with it.

    HEASOFT's ``ftmerge`` copies ONTIME from the first input rather than recomputing it,
    so the merged NuSTAR products carry an ONTIME far smaller than their own GTI total.
    Scaling on the keyword made the filtered file claim more live time than the
    unfiltered one.
    """

    def test_the_ratio_comes_from_the_gti_not_from_ontime(self):
        # As on the real nu80002092008_src1.evt: ONTIME says 36058 s over a 58889 s GTI.
        hdul = make_event_file(
            times=[15.0], gti=[[0, 58889]], ontime=36058.0, livetime=33646.0
        )
        apply_gti(hdul, [[0, 56851]])

        expected = 33646.0 * 56851 / 58889
        assert hdul["EVENTS"].header["LIVETIME"] == pytest.approx(expected)
        assert hdul["EVENTS"].header["LIVETIME"] < 33646.0, "filtering cannot add live time"
        assert hdul["EVENTS"].header["ONTIME"] == pytest.approx(56851.0)

    def test_ontime_before_is_reported_from_the_gti_too(self):
        hdul = make_event_file(times=[], gti=[[0, 100]], ontime=50.0)
        stats = apply_gti(hdul, [[0, 100]])

        assert stats["ontime_before"] == pytest.approx(100.0)


class TestMergeIntervals:
    def test_overlapping_intervals_are_merged(self):
        assert np.allclose(merge_intervals([[0, 10], [5, 20]]), [[0, 20]])

    def test_touching_intervals_are_merged(self):
        assert np.allclose(merge_intervals([[0, 10], [10, 20]]), [[0, 20]])

    def test_disjoint_intervals_are_kept_apart(self):
        assert np.allclose(merge_intervals([[0, 10], [20, 30]]), [[0, 10], [20, 30]])

    def test_out_of_order_input_is_sorted(self):
        assert np.allclose(merge_intervals([[20, 30], [0, 10]]), [[0, 10], [20, 30]])

    def test_zero_and_negative_length_intervals_are_dropped(self):
        assert np.allclose(merge_intervals([[5, 5], [10, 8], [20, 30]]), [[20, 30]])

    def test_nested_intervals(self):
        assert np.allclose(merge_intervals([[0, 100], [20, 30]]), [[0, 100]])

    def test_nothing_in_nothing_out(self):
        assert len(merge_intervals([])) == 0

    def test_a_tolerance_bridges_a_gap_too_small_to_be_real(self):
        assert np.allclose(merge_intervals([[0, 10], [10.5, 20]], tolerance=1.0), [[0, 20]])

    def test_the_tolerance_does_not_bridge_a_real_gap(self):
        assert np.allclose(
            merge_intervals([[0, 10], [11, 20]], tolerance=0.5), [[0, 10], [11, 20]]
        )

    def test_without_a_tolerance_the_smallest_gap_still_separates(self):
        merged = merge_intervals([[0, 10], [10 + 1e-9, 20]])
        assert len(merged) == 2


class TestIntervalsAboveThreshold:
    """Turning a sampled light curve into the intervals where it is too bright.

    This is how the GOES X-ray flux becomes a set of times to exclude, alongside the HEK
    catalogue of flare start and end times.
    """

    def test_each_hot_sample_covers_its_own_cadence_bin(self):
        times = np.array([0.0, 60.0, 120.0])
        values = np.array([1e-7, 1e-5, 1e-7])

        assert np.allclose(
            intervals_above_threshold(times, values, 5e-6), [[30.0, 90.0]]
        )

    def test_jittery_sample_times_do_not_split_one_bright_stretch(self):
        """Real GOES timestamps are not exactly one cadence apart.

        Measured on the GOES-15 1-minute series covering 80002092008, the spacing varies
        by about 600 ns around 60 s. Taken literally, ``t + cadence/2`` of one sample falls
        a few tens of nanoseconds short of ``t - cadence/2`` of the next, and a single
        bright stretch comes back as several intervals separated by slivers of "good" time
        far shorter than any instrument can use.
        """
        spacing = 60.0 + 6e-7 * np.array([1, -1] * 10)
        times = 129813203.8 + np.cumsum(spacing)
        values = np.full(20, 1e-5)

        intervals = intervals_above_threshold(times, values, 5e-6)

        assert len(intervals) == 1

    def test_a_real_gap_between_bright_stretches_is_kept(self):
        """The jitter tolerance must not swallow a genuine quiet minute."""
        times = np.arange(6) * 60.0
        values = np.array([1e-5, 1e-5, 1e-7, 1e-7, 1e-5, 1e-5])

        assert len(intervals_above_threshold(times, values, 5e-6)) == 2

    def test_consecutive_hot_samples_merge_into_one_interval(self):
        times = np.array([0.0, 60.0, 120.0, 180.0])
        values = np.array([1e-7, 1e-5, 1e-5, 1e-7])

        assert np.allclose(
            intervals_above_threshold(times, values, 5e-6), [[30.0, 150.0]]
        )

    def test_separate_flares_stay_separate(self):
        times = np.arange(0.0, 600.0, 60.0)
        values = np.full(times.size, 1e-7)
        values[[1, 7]] = 1e-5

        assert np.allclose(
            intervals_above_threshold(times, values, 5e-6), [[30, 90], [390, 450]]
        )

    def test_a_sample_exactly_at_the_threshold_counts(self):
        times = np.array([0.0, 60.0])
        values = np.array([5e-6, 1e-9])

        assert len(intervals_above_threshold(times, values, 5e-6)) == 1

    def test_nan_samples_are_never_excluded(self):
        """A gap in the GOES coverage is missing information, not a flare."""
        times = np.array([0.0, 60.0, 120.0])
        values = np.array([1e-7, np.nan, 1e-7])

        assert len(intervals_above_threshold(times, values, 5e-6)) == 0

    def test_nothing_above_the_threshold(self):
        times = np.arange(0.0, 300.0, 60.0)
        assert len(intervals_above_threshold(times, np.full(5, 1e-9), 5e-6)) == 0

    def test_everything_above_the_threshold_gives_one_interval(self):
        times = np.arange(0.0, 300.0, 60.0)
        result = intervals_above_threshold(times, np.full(5, 1e-4), 5e-6)

        assert np.allclose(result, [[-30.0, 270.0]])

    def test_the_cadence_can_be_given_explicitly(self):
        times = np.array([0.0, 60.0, 120.0])
        values = np.array([1e-7, 1e-5, 1e-7])

        assert np.allclose(
            intervals_above_threshold(times, values, 5e-6, cadence=10.0),
            [[55.0, 65.0]],
        )

    def test_no_samples_at_all(self):
        assert len(intervals_above_threshold([], [], 5e-6)) == 0


class TestIntersectIntervals:
    def test_a_simple_overlap(self):
        assert np.allclose(intersect_intervals([[0, 100]], [[50, 150]]), [[50, 100]])

    def test_no_overlap_gives_nothing(self):
        assert len(intersect_intervals([[0, 50]], [[100, 150]])) == 0

    def test_several_intervals_on_both_sides(self):
        result = intersect_intervals([[0, 100], [200, 300]], [[50, 250]])

        assert np.allclose(result, [[50, 100], [200, 250]])

    def test_touching_at_a_single_point_is_not_an_overlap(self):
        assert len(intersect_intervals([[0, 50]], [[50, 100]])) == 0

    def test_an_empty_side_gives_nothing(self):
        assert len(intersect_intervals([[0, 100]], np.zeros((0, 2)))) == 0

    def test_it_accepts_a_fits_gti_table(self):
        hdul = make_event_file(times=[], gti=[[0, 100]])
        assert np.allclose(intersect_intervals(hdul["GTI"].data, [[50, 150]]), [[50, 100]])


class TestAbsoluteConfig:
    """Configuration paths are pinned once, not resolved against a moving target.

    Every path the pipeline builds hangs off ``input_data_path`` and ``out_data_path``.
    While those were relative (``"./"``), the meaning of every one of them depended on the
    process working directory *at the moment it was used*, which is what made concurrent
    observations impossible. See issue 26 in ``docs/known_issues.rst``.
    """

    DEFAULT = dict(out_data_path="./", input_data_path="./", max_radius=80)

    def test_the_default_is_resolved_against_the_current_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        config = absolute_config(None, self.DEFAULT)

        assert config["out_data_path"] == str(tmp_path)
        assert config["input_data_path"] == str(tmp_path)

    def test_a_relative_path_given_by_the_caller_is_resolved_too(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        config = absolute_config(dict(out_data_path="out", input_data_path="raw"), self.DEFAULT)

        assert config["out_data_path"] == os.path.join(tmp_path, "out")
        assert config["input_data_path"] == os.path.join(tmp_path, "raw")

    def test_an_absolute_path_is_left_alone(self, tmp_path):
        given = dict(out_data_path="/data/out", input_data_path="/data/raw")

        config = absolute_config(given, self.DEFAULT)

        assert config["out_data_path"] == "/data/out"
        assert config["input_data_path"] == "/data/raw"

    def test_the_other_settings_survive(self):
        assert absolute_config(None, self.DEFAULT)["max_radius"] == 80

    def test_the_caller_dict_is_not_modified(self):
        given = dict(out_data_path="out", input_data_path="raw")

        absolute_config(given, self.DEFAULT)

        assert given == dict(out_data_path="out", input_data_path="raw")

    def test_the_defaults_are_not_modified(self):
        default = dict(self.DEFAULT)

        absolute_config(None, default)

        assert default == self.DEFAULT

    def test_a_later_chdir_cannot_move_the_paths(self, tmp_path, monkeypatch):
        """The whole point: resolve once, at flow entry, not at every path build."""
        monkeypatch.chdir(tmp_path)
        config = absolute_config(None, self.DEFAULT)

        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)

        assert config["out_data_path"] == str(tmp_path)
