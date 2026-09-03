"""
Offline tests for the per-step diagnostics records.

These are what the report is built from, so the properties that matter are the awkward
ones: a step that raises must still leave a record *and* still raise, a step that never
finished must be visible as such, and everything numpy must survive contact with
:mod:`json`.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest

from heasarc_retrieve_pipeline.diagnostics import (
    MANIFEST,
    canonical_metadata,
    catalogue_row,
    diagnostics_path,
    read_arrays,
    read_manifest,
    read_records,
    record_step,
    write_manifest,
)

OBSID = "90202038002"


class TestWhereItGoes:
    def test_the_directory_sits_in_the_observation_directory(self):
        assert diagnostics_path(OBSID, {"out_data_path": "out"}) == os.path.join(
            "out", OBSID, "diagnostics"
        )

    def test_the_key_becomes_part_of_the_file_name(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "separate_sources", key="nu123A01_cl"):
            pass

        assert os.path.exists(tmp_path / "separate_sources__nu123A01_cl.json")

    def test_a_step_without_a_key_is_named_after_itself(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "calculate_spectra"):
            pass

        assert os.path.exists(tmp_path / "calculate_spectra.json")

    def test_an_awkward_key_cannot_escape_the_directory(self, tmp_path):
        """A key is often a file name, and a file name is sometimes a path."""
        with record_step(str(tmp_path), OBSID, "step", key="../../etc/passwd"):
            pass

        assert [p.name for p in tmp_path.iterdir()] == ["step__.._.._etc_passwd.json"]

    def test_a_step_may_not_be_called_manifest(self, tmp_path):
        """It would overwrite the manifest, and then be skipped when read back."""
        with pytest.raises(ValueError, match="reserved"):
            record_step(str(tmp_path), OBSID, "manifest")


class TestHowAStepEnds:
    def test_a_step_that_finishes_is_done(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.value(files=2)

        record = read_records(str(tmp_path))[0]
        assert record["status"] == "done"
        assert record["values"] == {"files": 2}
        assert record["duration_s"] >= 0
        assert record["started_iso"].startswith("20")

    def test_a_step_that_skips_says_why(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "separate") as rec:
            rec.skip("fewer than 20 events passed the energy and position filter")

        record = read_records(str(tmp_path))[0]
        assert record["status"] == "skipped"
        assert "fewer than 20 events" in record["reason"]

    def test_skipping_does_not_raise(self, tmp_path):
        """The caller goes on to return normally, exactly as it did before."""
        reached_the_end = False
        with record_step(str(tmp_path), OBSID, "separate") as rec:
            rec.skip("nothing to do")
            reached_the_end = True

        assert reached_the_end

    def test_a_step_that_raises_is_failed(self, tmp_path):
        with pytest.raises(ValueError):
            with record_step(str(tmp_path), OBSID, "join"):
                raise ValueError("no source in FPMA")

        record = read_records(str(tmp_path))[0]
        assert record["status"] == "failed"
        assert record["error"] == "ValueError: no source in FPMA"
        assert "raise ValueError" in record["traceback"]

    def test_a_step_that_raises_still_raises(self, tmp_path):
        """``process_observations`` counts a failed observation by catching it."""
        with pytest.raises(KeyError):
            with record_step(str(tmp_path), OBSID, "join"):
                raise KeyError("obsid")

    def test_a_step_that_never_finished_is_still_running(self, tmp_path):
        """A killed run should name the step it died in."""
        recorder = record_step(str(tmp_path), OBSID, "level2")
        recorder.__enter__()

        assert read_records(str(tmp_path))[0]["status"] == "running"

    def test_values_recorded_before_a_failure_are_kept(self, tmp_path):
        with pytest.raises(RuntimeError):
            with record_step(str(tmp_path), OBSID, "regions") as rec:
                rec.value(rlimit=42.0)
                raise RuntimeError("nustar_gen said no")

        assert read_records(str(tmp_path))[0]["values"] == {"rlimit": 42.0}


class TestNumpySurvivesJson:
    """Everything in this package is numpy, and three quarters of it is not JSON."""

    def test_numpy_scalars_are_written(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "separate") as rec:
            rec.value(
                median=np.float32(4.5),
                count=np.int64(7),
                accepted=np.bool_(True),
                mad=np.float64(1.25),
            )

        values = read_records(str(tmp_path))[0]["values"]
        assert values == {"median": 4.5, "count": 7, "accepted": True, "mad": 1.25}

    def test_a_masked_catalogue_cell_becomes_null(self, tmp_path):
        """``public_date`` is routinely masked, and masked raises outright."""
        with record_step(str(tmp_path), OBSID, "observation") as rec:
            rec.value(public_date=np.ma.masked)

        assert read_records(str(tmp_path))[0]["values"] == {"public_date": None}

    def test_a_non_finite_float_becomes_null(self, tmp_path):
        """``NaN`` and ``Infinity`` are what :mod:`json` writes, and neither is JSON."""
        with record_step(str(tmp_path), OBSID, "flares") as rec:
            rec.value(chi2=float("nan"), rate=float("inf"))

        text = open(tmp_path / "flares.json").read()
        assert "NaN" not in text and "Infinity" not in text
        assert read_records(str(tmp_path))[0]["values"] == {"chi2": None, "rate": None}

    def test_an_array_of_values_is_flattened_to_a_list(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "separate") as rec:
            rec.value(peak_fluxes=np.array([3, 1, 2]))

        assert read_records(str(tmp_path))[0]["values"] == {"peak_fluxes": [3, 1, 2]}

    def test_the_record_is_strict_json(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "separate") as rec:
            rec.value(median=np.float32(4.5), public_date=np.ma.masked)

        with open(tmp_path / "separate.json") as fobj:
            json.load(fobj, parse_constant=_no_constants)


def _no_constants(name):
    raise AssertionError(f"{name} is not valid JSON")


class TestArrays:
    def test_arrays_round_trip_with_their_dtypes(self, tmp_path):
        image = np.arange(16, dtype=np.uint16).reshape(4, 4)
        times = np.linspace(0, 1, 5, dtype=np.float32)

        with record_step(str(tmp_path), OBSID, "separate", key="A01") as rec:
            rec.array(image=image, times=times)

        record = read_records(str(tmp_path))[0]
        arrays = read_arrays(str(tmp_path), record)
        assert arrays["image"].dtype == np.uint16
        np.testing.assert_array_equal(arrays["image"], image)
        np.testing.assert_allclose(arrays["times"], times)

    def test_the_payload_sits_beside_the_record(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "separate", key="A01") as rec:
            rec.array(image=np.zeros((2, 2)))

        record = read_records(str(tmp_path))[0]
        assert record["arrays"] == "separate__A01.npz"
        assert os.path.exists(tmp_path / "separate__A01.npz")

    def test_a_record_without_arrays_has_none(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.value(files=1)

        record = read_records(str(tmp_path))[0]
        assert record["arrays"] is None
        assert read_arrays(str(tmp_path), record) is None


class TestRecordingNothing:
    """``diagnostics_dir=None`` is what keeps every call site optional."""

    def test_nothing_is_written(self, tmp_path):
        with record_step(None, OBSID, "separate") as rec:
            rec.value(median=1.0)
            rec.array(image=np.zeros((2, 2)))

        assert list(tmp_path.iterdir()) == []

    def test_an_exception_still_propagates(self, tmp_path):
        with pytest.raises(ValueError):
            with record_step(None, OBSID, "separate"):
                raise ValueError("boom")

    def test_skipping_still_works(self):
        with record_step(None, OBSID, "separate") as rec:
            rec.skip("nothing to do")
        assert rec.reason == "nothing to do"


class TestConcurrency:
    """Several tasks of one observation record at the same time, in threads."""

    def test_every_writer_with_its_own_key_lands(self, tmp_path):
        keys = [f"nu{OBSID}A06_chu{n}_N_cl" for n in range(8)]

        def record(key):
            with record_step(str(tmp_path), OBSID, "separate_sources", key=key) as rec:
                rec.value(key=key)
                rec.array(image=np.zeros((10, 10)))

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(record, keys))

        records = read_records(str(tmp_path))
        assert sorted(r["key"] for r in records) == sorted(keys)
        assert all(r["status"] == "done" for r in records)

    def test_no_temporary_file_is_left_behind(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "separate", key="A01") as rec:
            rec.array(image=np.zeros((4, 4)))

        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "separate__A01.json",
            "separate__A01.npz",
        ]


class TestReadingBack:
    def test_an_unreadable_record_does_not_hide_the_others(self, tmp_path):
        """The report has to render whatever survived a crashed run."""
        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.value(files=2)
        (tmp_path / "truncated.json").write_text('{"step": "flares", ')

        records = read_records(str(tmp_path))

        assert [r["step"] for r in records] == ["join"]

    def test_records_come_back_oldest_first(self, tmp_path):
        for step in "level2", "regions", "separate":
            with record_step(str(tmp_path), OBSID, step):
                pass

        assert [r["step"] for r in read_records(str(tmp_path))] == [
            "level2",
            "regions",
            "separate",
        ]

    def test_a_directory_that_does_not_exist_reads_as_empty(self, tmp_path):
        assert read_records(str(tmp_path / "nope")) == []

    def test_the_manifest_is_not_a_step(self, tmp_path):
        write_manifest(str(tmp_path), OBSID, {"obsid": OBSID})
        with record_step(str(tmp_path), OBSID, "join"):
            pass

        assert [r["step"] for r in read_records(str(tmp_path))] == ["join"]


class TestTheManifest:
    def test_it_round_trips(self, tmp_path):
        write_manifest(str(tmp_path), OBSID, {"name": "M82", "exposure": 1000.0}, url="https://x/y")

        manifest = read_manifest(str(tmp_path))
        assert manifest["obsid"] == OBSID
        assert manifest["catalogue"] == {"name": "M82", "exposure": 1000.0}
        assert manifest["metadata"]["source_name"] == "M82"
        assert manifest["url"] == "https://x/y"

    def test_it_is_named_so_the_reader_can_tell_it_apart(self, tmp_path):
        write_manifest(str(tmp_path), OBSID, {"obsid": OBSID})

        assert os.path.exists(tmp_path / MANIFEST)

    def test_nothing_recorded_reads_back_as_nothing(self, tmp_path):
        assert read_manifest(str(tmp_path)) is None

    def test_no_directory_means_no_manifest(self, tmp_path):
        assert write_manifest(None, OBSID, {"obsid": OBSID}) is None


class TestCatalogueRow:
    def test_the_astroquery_row_id_is_left_out(self):
        assert catalogue_row({"obsid": "1", "__row": 7}) == {"obsid": "1"}

    def test_numpy_cells_come_back_as_plain_values(self):
        row = {"exposure_a": np.float32(1000.0), "public_date": np.ma.masked}

        assert catalogue_row(row) == {"exposure_a": 1000.0, "public_date": None}

    def test_an_astropy_row_is_read_by_its_column_names(self):
        table = pytest.importorskip("astropy.table").Table(
            {"obsid": ["90202038002"], "exposure_a": [1000.0], "__row": [7]}
        )

        assert catalogue_row(table[0]) == {"obsid": "90202038002", "exposure_a": 1000.0}


class TestCanonicalMetadata:
    """Three missions and two query paths name the same quantity three ways."""

    @pytest.mark.parametrize("column", ["source_name", "name", "target_name"])
    def test_whichever_column_carries_the_target(self, column):
        assert canonical_metadata({column: "M82"})["source_name"] == "M82"

    @pytest.mark.parametrize("column", ["exposure", "exposure_a"])
    def test_whichever_column_carries_the_exposure(self, column):
        assert canonical_metadata({column: 1000.0})["exposure"] == 1000.0

    def test_the_first_candidate_present_wins(self):
        metadata = canonical_metadata({"source_name": "M82", "name": "wrong"})

        assert metadata["source_name"] == "M82"

    def test_a_column_that_is_absent_is_absent(self):
        assert "cycle" not in canonical_metadata({"name": "M82"})

    def test_a_column_that_is_present_but_masked_is_kept_as_null(self):
        """Present-but-unknown and never-asked are different things."""
        metadata = canonical_metadata(catalogue_row({"public_date": np.ma.masked}))

        assert metadata["public_date"] is None

    def test_an_rxte_row(self):
        metadata = canonical_metadata(
            {"target_name": "Sco X-1", "exposure": 3000.0, "cycle": 5, "prnb": 12}
        )

        assert metadata == {
            "source_name": "Sco X-1",
            "exposure": 3000.0,
            "cycle": 5,
            "prnb": 12,
        }


class TestARerunKeepsWhatTheLastRunMeasured:
    """
    The dataset a figure is drawn from belongs to the observation, not to one run.

    A step that skips because its output was already on disk did not measure anything.
    It must not erase the measurement made by the run that produced that output --
    which is what a rerun used to do, leaving the ``.npz`` orphaned on disk and the
    figure missing from the page.
    """

    def measure(self, tmp_path, **arrays):
        """A run that does the work."""
        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.value(inputs=4)
            rec.array(**(arrays or {"gti": np.array([[0.0, 10.0]])}))

    def test_a_skipped_rerun_still_points_at_the_payload(self, tmp_path):
        self.measure(tmp_path)

        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.skip("JOIN_DONE_SRC1.TXT already exists")

        record = read_records(str(tmp_path))[0]
        assert record["status"] == "skipped"
        assert record["arrays"] == "join.npz"
        np.testing.assert_array_equal(read_arrays(str(tmp_path), record)["gti"], [[0.0, 10.0]])

    def test_the_figure_can_tell_it_was_not_measured_now(self, tmp_path):
        self.measure(tmp_path)

        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.skip("JOIN_DONE_SRC1.TXT already exists")

        assert read_records(str(tmp_path))[0]["arrays_from_earlier_run"] is True

    def test_a_run_that_measured_is_not_marked_as_earlier(self, tmp_path):
        self.measure(tmp_path)

        assert read_records(str(tmp_path))[0]["arrays_from_earlier_run"] is False

    def test_a_rerun_that_measures_again_replaces_the_payload(self, tmp_path):
        self.measure(tmp_path)

        self.measure(tmp_path, gti=np.array([[5.0, 20.0]]))

        record = read_records(str(tmp_path))[0]
        assert record["arrays_from_earlier_run"] is False
        np.testing.assert_array_equal(read_arrays(str(tmp_path), record)["gti"], [[5.0, 20.0]])

    def test_values_from_the_earlier_run_are_kept(self, tmp_path):
        self.measure(tmp_path)

        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.skip("JOIN_DONE_SRC1.TXT already exists")

        assert read_records(str(tmp_path))[0]["values"]["inputs"] == 4

    def test_this_run_wins_where_both_recorded_the_same_value(self, tmp_path):
        self.measure(tmp_path)

        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.value(inputs=9)
            rec.skip("JOIN_DONE_SRC1.TXT already exists")

        assert read_records(str(tmp_path))[0]["values"]["inputs"] == 9

    def test_a_payload_that_is_gone_is_not_promised(self, tmp_path):
        self.measure(tmp_path)
        os.unlink(tmp_path / "join.npz")

        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.skip("JOIN_DONE_SRC1.TXT already exists")

        record = read_records(str(tmp_path))[0]
        assert record["arrays"] is None
        assert record["arrays_from_earlier_run"] is False

    def test_a_rerun_killed_mid_step_does_not_lose_the_payload(self, tmp_path):
        """The record written on entry is the one an interrupted run leaves behind."""
        self.measure(tmp_path)

        record_step(str(tmp_path), OBSID, "join").__enter__()

        record = read_records(str(tmp_path))[0]
        assert record["status"] == "running"
        assert record["arrays"] == "join.npz"

    def test_an_unreadable_earlier_record_does_not_stop_the_new_one(self, tmp_path):
        (tmp_path / "join.json").write_text("{ this is not json")

        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.value(inputs=1)

        assert read_records(str(tmp_path))[0]["values"]["inputs"] == 1

    def test_a_first_run_that_measures_nothing_has_no_payload(self, tmp_path):
        with record_step(str(tmp_path), OBSID, "join") as rec:
            rec.skip("JOIN_DONE_SRC1.TXT already exists")

        record = read_records(str(tmp_path))[0]
        assert record["arrays"] is None
        assert record["arrays_from_earlier_run"] is False
