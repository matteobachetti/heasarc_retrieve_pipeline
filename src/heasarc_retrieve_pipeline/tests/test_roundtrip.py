"""Offline tests for the split/merge round-trip check.

The comparisons themselves are pure arithmetic over FITS tables, so all of this runs
without HEASOFT. The one part that cannot -- ``addspec`` -- is exercised against the same
recorded double the merge tests use.
"""

import os

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import roundtrip
from heasarc_retrieve_pipeline.roundtrip import (
    compare_events,
    compare_spectra,
    segment_families,
    stage_observation,
)

OBSID = "80002092008"


def write_spectrum(path, counts, exposure, gti):
    """A source spectrum shaped like the ones ``nuproducts`` writes."""
    counts = np.asarray(counts, dtype=np.int32)
    gti = np.asarray(gti, dtype=float)
    spectrum = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CHANNEL", format="J", array=np.arange(counts.size)),
            fits.Column(name="COUNTS", format="J", array=counts),
        ],
        name="SPECTRUM",
    )
    spectrum.header["EXPOSURE"] = exposure
    spectrum.header["DETCHANS"] = counts.size
    gti_hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=gti[:, 0]),
            fits.Column(name="STOP", format="D", array=gti[:, 1]),
        ],
        name="GTI",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fits.HDUList([fits.PrimaryHDU(), spectrum, gti_hdu]).writeto(path, overwrite=True)
    return str(path)


def write_events(path, times, gti):
    """An event file shaped like the ones the split writes."""
    gti = np.asarray(gti, dtype=float)
    events = fits.BinTableHDU.from_columns(
        [fits.Column(name="TIME", format="D", array=np.asarray(times, dtype=float))],
        name="EVENTS",
    )
    gti_hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=gti[:, 0]),
            fits.Column(name="STOP", format="D", array=gti[:, 1]),
        ],
        name="GTI",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fits.HDUList([fits.PrimaryHDU(), events, gti_hdu]).writeto(path, overwrite=True)
    return str(path)


class TestCompareSpectra:
    """A split that partitions its parent, and the ways one can fail to."""

    def three(self, tmp_path, first, second, parent=None):
        parent = [3, 5, 7] if parent is None else parent
        return (
            write_spectrum(tmp_path / "p.pha", parent, 100.0, [[0, 100]]),
            [
                write_spectrum(tmp_path / "s1.pha", first, 40.0, [[0, 40]]),
                write_spectrum(tmp_path / "s2.pha", second, 60.0, [[40, 100]]),
            ],
        )

    def test_a_clean_partition_passes(self, tmp_path):
        parent, segments = self.three(tmp_path, [1, 2, 3], [2, 3, 4])
        result = compare_spectra(parent, segments)

        assert result["counts_match"]
        assert result["exposure_match"]
        assert result["gti_match"]
        assert result["channels_wrong"] == 0

    def test_a_lost_count_is_caught_in_the_channel_it_was_lost_from(self, tmp_path):
        parent, segments = self.three(tmp_path, [1, 2, 3], [2, 3, 3])
        result = compare_spectra(parent, segments)

        assert not result["counts_match"]
        assert result["channels_wrong"] == 1
        assert result["segment_counts"] == result["parent_counts"] - 1

    def test_counts_moved_between_channels_are_caught(self, tmp_path):
        """The totals still agree, which is exactly why the check is per channel."""
        parent, segments = self.three(tmp_path, [2, 1, 3], [2, 3, 4])
        result = compare_spectra(parent, segments)

        assert result["segment_counts"] == result["parent_counts"]
        assert not result["counts_match"]
        assert result["channels_wrong"] == 2

    def test_exposures_that_do_not_add_up_are_caught(self, tmp_path):
        parent = write_spectrum(tmp_path / "p.pha", [3], 100.0, [[0, 100]])
        segments = [
            write_spectrum(tmp_path / "s1.pha", [1], 40.0, [[0, 40]]),
            write_spectrum(tmp_path / "s2.pha", [2], 55.0, [[45, 100]]),
        ]
        result = compare_spectra(parent, segments)

        assert result["counts_match"]
        assert not result["exposure_match"]
        assert result["segment_exposure"] == 95.0

    def test_a_rounding_difference_in_the_exposure_is_not_a_failure(self, tmp_path):
        parent = write_spectrum(tmp_path / "p.pha", [3], 100.0, [[0, 100]])
        segments = [
            write_spectrum(tmp_path / "s1.pha", [1], 40.0, [[0, 40]]),
            write_spectrum(tmp_path / "s2.pha", [2], 60.0 - 1e-9, [[40, 100]]),
        ]

        assert compare_spectra(parent, segments)["exposure_match"]

    def test_one_segment_is_a_legitimate_split(self, tmp_path):
        """A cut outside the observation leaves everything in one piece."""
        parent = write_spectrum(tmp_path / "p.pha", [3, 5], 100.0, [[0, 100]])
        segment = write_spectrum(tmp_path / "s1.pha", [3, 5], 100.0, [[0, 100]])

        assert compare_spectra(parent, [segment])["counts_match"]


class TestCompareEvents:
    def test_the_segments_events_are_the_parents(self, tmp_path):
        parent = write_events(tmp_path / "p.evt", [1.0, 5.0, 9.0], [[0, 10]])
        segments = [
            write_events(tmp_path / "s1.evt", [1.0], [[0, 4]]),
            write_events(tmp_path / "s2.evt", [5.0, 9.0], [[4, 10]]),
        ]
        result = compare_events(parent, segments)

        assert result["times_match"]
        assert result["gti_match"]

    def test_a_dropped_event_is_caught(self, tmp_path):
        parent = write_events(tmp_path / "p.evt", [1.0, 5.0, 9.0], [[0, 10]])
        segments = [
            write_events(tmp_path / "s1.evt", [1.0], [[0, 4]]),
            write_events(tmp_path / "s2.evt", [9.0], [[4, 10]]),
        ]
        result = compare_events(parent, segments)

        assert not result["times_match"]
        assert result["segment_events"] == 2

    def test_a_duplicated_event_is_caught(self, tmp_path):
        """Overlapping bounds would count an event twice, and the totals would hide it."""
        parent = write_events(tmp_path / "p.evt", [1.0, 5.0], [[0, 10]])
        segments = [
            write_events(tmp_path / "s1.evt", [1.0, 5.0], [[0, 6]]),
            write_events(tmp_path / "s2.evt", [5.0], [[4, 10]]),
        ]

        assert not compare_events(parent, segments)["times_match"]

    def test_good_time_that_does_not_add_up_is_caught(self, tmp_path):
        parent = write_events(tmp_path / "p.evt", [1.0], [[0, 10]])
        segments = [
            write_events(tmp_path / "s1.evt", [1.0], [[0, 4]]),
            write_events(tmp_path / "s2.evt", [], [[5, 10]]),
        ]

        assert not compare_events(parent, segments)["gti_match"]


class TestSegmentFamilies:
    def products(self, tmp_path, names):
        products = tmp_path / OBSID / "products"
        for name in names:
            write_spectrum(products / name, [1], 10.0, [[0, 10]])
        return {"out_data_path": str(tmp_path)}

    def test_segments_are_grouped_under_their_parent(self, tmp_path):
        config = self.products(
            tmp_path,
            [
                f"nu{OBSID}A01_sr.pha",
                f"nu{OBSID}A01_sr_seg1.pha",
                f"nu{OBSID}A01_sr_seg2.pha",
            ],
        )
        families = segment_families(OBSID, config)

        assert list(families) == [f"nu{OBSID}A01"]
        assert [os.path.basename(p) for p in families[f"nu{OBSID}A01"][1]] == [
            f"nu{OBSID}A01_sr_seg1.pha",
            f"nu{OBSID}A01_sr_seg2.pha",
        ]

    def test_the_modules_and_modes_are_separate_families(self, tmp_path):
        config = self.products(
            tmp_path,
            [
                f"nu{OBSID}A01_sr.pha",
                f"nu{OBSID}A01_sr_seg1.pha",
                f"nu{OBSID}B01_sr.pha",
                f"nu{OBSID}B01_sr_seg1.pha",
                f"nu{OBSID}A06_chu12_N_sr.pha",
                f"nu{OBSID}A06_chu12_N_sr_seg1.pha",
            ],
        )

        assert sorted(segment_families(OBSID, config)) == [
            f"nu{OBSID}A01",
            f"nu{OBSID}A06_chu12_N",
            f"nu{OBSID}B01",
        ]

    def test_segments_are_ordered_by_number_not_by_name(self, tmp_path):
        """seg10 sorts before seg2 as a string, and that is the wrong order."""
        config = self.products(
            tmp_path,
            [f"nu{OBSID}A01_sr.pha"] + [f"nu{OBSID}A01_sr_seg{n}.pha" for n in (1, 2, 10)],
        )
        _parent, segments = segment_families(OBSID, config)[f"nu{OBSID}A01"]

        assert [os.path.basename(p) for p in segments] == [
            f"nu{OBSID}A01_sr_seg1.pha",
            f"nu{OBSID}A01_sr_seg2.pha",
            f"nu{OBSID}A01_sr_seg10.pha",
        ]

    def test_a_segment_with_no_parent_is_left_out(self, tmp_path):
        config = self.products(tmp_path, [f"nu{OBSID}A01_sr_seg1.pha"])

        assert segment_families(OBSID, config) == {}

    def test_a_whole_observation_spectrum_is_not_a_segment(self, tmp_path):
        config = self.products(tmp_path, [f"nu{OBSID}A01_sr.pha"])

        assert segment_families(OBSID, config) == {}


class TestStageObservation:
    def tree(self, tmp_path):
        base = tmp_path / "out" / OBSID
        for subdir in ("event_pipe", "products", "split"):
            os.makedirs(base / subdir)
            with open(base / subdir / "a_file", "w") as fobj:
                fobj.write(subdir)
        return {"out_data_path": str(tmp_path / "out")}

    def test_the_copy_is_a_copy(self, tmp_path):
        config = self.tree(tmp_path)
        staged = stage_observation(OBSID, config, str(tmp_path / "work"))

        assert staged["out_data_path"] == str(tmp_path / "work")
        assert os.path.exists(tmp_path / "work" / OBSID / "event_pipe" / "a_file")

    def test_the_original_is_not_touched_when_the_copy_is_written_to(self, tmp_path):
        config = self.tree(tmp_path)
        staged = stage_observation(OBSID, config, str(tmp_path / "work"))
        with open(os.path.join(staged["out_data_path"], OBSID, "products", "a_file"), "w") as fobj:
            fobj.write("written by the check")

        with open(tmp_path / "out" / OBSID / "products" / "a_file") as fobj:
            assert fobj.read() == "products"

    def test_the_mode_06_directory_is_left_behind_by_default(self, tmp_path):
        """Not a flag on the split: there is simply nothing in split/ to find."""
        config = self.tree(tmp_path)
        stage_observation(OBSID, config, str(tmp_path / "work"))

        assert not os.path.exists(tmp_path / "work" / OBSID / "split")

    def test_mode_06_can_be_asked_for(self, tmp_path):
        config = self.tree(tmp_path)
        stage_observation(OBSID, config, str(tmp_path / "work"), with_mode06=True)

        assert os.path.exists(tmp_path / "work" / OBSID / "split" / "a_file")

    def test_an_earlier_copy_is_replaced_rather_than_merged_into(self, tmp_path):
        config = self.tree(tmp_path)
        stale = tmp_path / "work" / OBSID / "products" / "left_by_an_earlier_run"
        os.makedirs(stale.parent)
        open(stale, "w").close()

        stage_observation(OBSID, config, str(tmp_path / "work"))

        assert not os.path.exists(stale)

    def test_an_unreduced_observation_says_so(self, tmp_path):
        config = {"out_data_path": str(tmp_path / "out")}
        with pytest.raises(FileNotFoundError, match="not reduced"):
            stage_observation(OBSID, config, str(tmp_path / "work"))


class TestTheCommandLine:
    def test_it_refuses_an_observation_that_is_not_there(self, tmp_path, capsys):
        with pytest.raises(FileNotFoundError):
            roundtrip.main([str(tmp_path), OBSID, "56000"])

    def test_the_workdir_defaults_beside_the_output_tree(self, tmp_path, monkeypatch):
        seen = {}

        def fake_check(obsid, config, mjds, workdir, **kwargs):
            seen.update(obsid=obsid, workdir=workdir, mjds=mjds, kwargs=kwargs)
            return {"config": config, "spectra": {}, "events": {}, "merged": None}

        monkeypatch.setattr(roundtrip, "check_roundtrip", fake_check)
        roundtrip.main([str(tmp_path / "out"), OBSID, "56000.5"])

        assert seen["workdir"] == str(tmp_path / "roundtrip")
        assert seen["mjds"] == [56000.5]

    def test_the_flags_reach_the_check(self, tmp_path, monkeypatch):
        seen = {}

        def fake_check(obsid, config, mjds, workdir, **kwargs):
            seen.update(kwargs)
            return {"config": config, "spectra": {}, "events": {}, "merged": None}

        monkeypatch.setattr(roundtrip, "check_roundtrip", fake_check)
        roundtrip.main(
            [
                str(tmp_path),
                OBSID,
                "56000",
                "--utc",
                "--with-mode06",
                "--no-addspec",
            ]
        )

        assert seen == {"scale": "utc", "with_mode06": True, "addspec": False}

    def test_a_clean_run_exits_zero_and_a_failure_does_not(self, tmp_path, monkeypatch):
        parent = write_spectrum(tmp_path / "p.pha", [3], 100.0, [[0, 100]])
        good = write_spectrum(tmp_path / "s1.pha", [3], 100.0, [[0, 100]])
        bad = write_spectrum(tmp_path / "s2.pha", [2], 100.0, [[0, 100]])

        for segment, expected in ((good, 0), (bad, 1)):

            def fake_check(obsid, config, mjds, workdir, _segment=segment, **kwargs):
                return {
                    "config": config,
                    "spectra": {"stem": compare_spectra(parent, [_segment])},
                    "events": {},
                    "merged": None,
                }

            monkeypatch.setattr(roundtrip, "check_roundtrip", fake_check)
            assert roundtrip.main([str(tmp_path), OBSID, "56000"]) == expected


class TestPairingSegmentEventFilesWithTheirParents:
    """``check_roundtrip`` finds the event halves by name, and the names vary.

    ``insert_tag`` keeps whatever extension the parent had, so a segment of a compressed
    parent is ``..._seg1.evt.gz``. Splitting the name on the last dot would leave ``.gz``
    attached to the stem and the parent would never be found.
    """

    def tree(self, tmp_path, monkeypatch, parent_name, gzipped=False):
        base = tmp_path / "work" / OBSID
        os.makedirs(base / "products")
        os.makedirs(tmp_path / "out" / OBSID)

        suffix = ".evt.gz" if gzipped else ".evt"
        write_events(base / (parent_name + suffix), [1.0, 5.0, 9.0], [[0, 10]])
        write_events(base / (parent_name + "_seg1" + suffix), [1.0], [[0, 4]])
        write_events(base / (parent_name + "_seg2" + suffix), [5.0, 9.0], [[4, 10]])

        monkeypatch.setattr(
            roundtrip,
            "stage_observation",
            lambda *a, **k: {"out_data_path": str(tmp_path / "work")},
        )
        monkeypatch.setattr(roundtrip, "split_obsid", lambda *a, **k: {"bounds": []})
        return {"out_data_path": str(tmp_path / "out")}

    def test_an_uncompressed_pair_is_found(self, tmp_path, monkeypatch):
        config = self.tree(tmp_path, monkeypatch, f"nu{OBSID}A_src1_bary")
        result = roundtrip.check_roundtrip(
            OBSID, config, [56000.0], str(tmp_path / "work"), addspec=False
        )

        assert list(result["events"]) == [f"nu{OBSID}A_src1_bary.evt"]
        assert result["events"][f"nu{OBSID}A_src1_bary.evt"]["times_match"]

    def test_a_compressed_pair_is_found_too(self, tmp_path, monkeypatch):
        config = self.tree(tmp_path, monkeypatch, f"nu{OBSID}A_src1_bary", gzipped=True)
        result = roundtrip.check_roundtrip(
            OBSID, config, [56000.0], str(tmp_path / "work"), addspec=False
        )

        assert list(result["events"]) == [f"nu{OBSID}A_src1_bary.evt.gz"]
        assert result["events"][f"nu{OBSID}A_src1_bary.evt.gz"]["times_match"]

    def test_a_segment_with_no_parent_is_not_compared(self, tmp_path, monkeypatch):
        config = self.tree(tmp_path, monkeypatch, f"nu{OBSID}A_src1_bary")
        os.unlink(tmp_path / "work" / OBSID / f"nu{OBSID}A_src1_bary.evt")
        result = roundtrip.check_roundtrip(
            OBSID, config, [56000.0], str(tmp_path / "work"), addspec=False
        )

        assert result["events"] == {}

    def test_both_segments_land_under_the_one_parent(self, tmp_path, monkeypatch):
        """Not one family per segment: the comparison is of the whole set against one file."""
        config = self.tree(tmp_path, monkeypatch, f"nu{OBSID}A_src1_bary")
        result = roundtrip.check_roundtrip(
            OBSID, config, [56000.0], str(tmp_path / "work"), addspec=False
        )
        comparison = result["events"][f"nu{OBSID}A_src1_bary.evt"]

        assert len(result["events"]) == 1
        assert comparison["segment_events"] == comparison["parent_events"] == 3


class TestASpectrumGivenAsARate:
    """``addspec`` may hand back a ``RATE`` column where ``nuproducts`` wrote ``COUNTS``.

    The round trip has to compare the two anyway, so the rate is turned back into counts
    with the file's own exposure. This is the reason the comparison carries a tolerance.
    """

    def rate_spectrum(self, path, rates, exposure):
        spectrum = fits.BinTableHDU.from_columns(
            [
                fits.Column(name="CHANNEL", format="J", array=np.arange(len(rates))),
                fits.Column(name="RATE", format="E", array=np.asarray(rates, dtype=float)),
            ],
            name="SPECTRUM",
        )
        spectrum.header["EXPOSURE"] = exposure
        fits.HDUList([fits.PrimaryHDU(), spectrum]).writeto(path, overwrite=True)
        return str(path)

    def test_a_rate_is_compared_as_counts(self, tmp_path):
        parent = write_spectrum(tmp_path / "p.pha", [10, 20], 100.0, [[0, 100]])
        merged = self.rate_spectrum(tmp_path / "m.pha", [0.1, 0.2], 100.0)

        assert compare_spectra(parent, [merged])["counts_match"]

    def test_a_rate_that_does_not_match_is_still_caught(self, tmp_path):
        parent = write_spectrum(tmp_path / "p.pha", [10, 20], 100.0, [[0, 100]])
        merged = self.rate_spectrum(tmp_path / "m.pha", [0.1, 0.3], 100.0)

        result = compare_spectra(parent, [merged])
        assert not result["counts_match"]
        assert result["channels_wrong"] == 1

    def test_a_spectrum_with_neither_column_says_which_it_has(self, tmp_path):
        path = str(tmp_path / "odd.pha")
        spectrum = fits.BinTableHDU.from_columns(
            [fits.Column(name="CHANNEL", format="J", array=np.arange(2))],
            name="SPECTRUM",
        )
        spectrum.header["EXPOSURE"] = 1.0
        fits.HDUList([fits.PrimaryHDU(), spectrum]).writeto(path, overwrite=True)

        with pytest.raises(KeyError, match="CHANNEL"):
            compare_spectra(path, [path])


class TestASplitThatLeavesOnePiece:
    """A cut outside the observation is a legitimate split, not a failed one.

    ``segment_bounds`` never renumbers, so a cut before the start leaves ``seg1`` empty and
    everything in ``seg2``; the empty one is skipped and one segment reaches the co-adding
    step. ``addspec`` on a single file is a slow, lossy copy and ``merge_spectra`` refuses
    it -- which must not be reported as the round trip failing.
    """

    def test_the_co_adding_step_says_there_was_nothing_to_do(self, tmp_path, monkeypatch):
        products = tmp_path / OBSID / "products"
        stem = f"nu{OBSID}A01"
        parent = write_spectrum(products / f"{stem}_sr.pha", [3], 100.0, [[0, 100]])
        segment = write_spectrum(products / f"{stem}_sr_seg2.pha", [3], 100.0, [[0, 100]])
        config = {"out_data_path": str(tmp_path)}

        def refuse(*args, **kwargs):
            raise AssertionError("addspec must not be reached with one input")

        monkeypatch.setattr(roundtrip, "merge_spectra", refuse)
        merged = roundtrip.addspec_roundtrip(OBSID, config, {stem: (parent, [segment])})

        assert merged == {stem: "single segment"}

    def test_it_is_not_reported_as_a_failure(self, tmp_path, capsys):
        status = roundtrip._report(
            {"spectra": {}, "events": {}, "merged": {"stem": "single segment"}}
        )

        assert status == 0
        assert "nothing to co-add" in capsys.readouterr().out
