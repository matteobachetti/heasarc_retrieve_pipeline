"""
The per-observation report of everything the reduction had to skip.

A mode-06 CHU subset that cannot be reduced is skipped and the observation still counts as
reduced. That is only defensible if the skips can be audited afterwards without reading a
40 MB cluster log, which is what ``skipped_inputs.txt`` is for.
"""

import glob
import os
import threading

from heasarc_retrieve_pipeline.utils import (
    read_skipped_inputs,
    record_skipped_input,
    skipped_inputs_file,
)


OBSID = "90202038002"
FILE = f"nu{OBSID}A06_chu1_N_cl.evt"
REASON = "no source found in the 3-80 keV image"


class TestOneSkip:
    def test_a_skip_round_trips(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, REASON)

        assert read_skipped_inputs(OBSID, config) == [(FILE, REASON)]

    def test_the_report_sits_in_the_observation_directory(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, REASON)

        assert skipped_inputs_file(OBSID, config) == str(
            tmp_path / OBSID / "skipped_inputs.txt"
        )
        assert os.path.exists(skipped_inputs_file(OBSID, config))

    def test_the_file_says_what_it_is(self, tmp_path):
        """It has to explain itself to whoever finds it in an output tree next year."""
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, REASON)

        first = open(skipped_inputs_file(OBSID, config)).readline()
        assert first.startswith("#")
        assert OBSID in first

    def test_it_is_greppable(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, REASON)

        text = open(skipped_inputs_file(OBSID, config)).read()
        assert FILE in text
        assert REASON in text

    def test_nothing_recorded_reads_back_as_nothing(self, tmp_path):
        assert read_skipped_inputs(OBSID, {"out_data_path": str(tmp_path)}) == []


class TestItIsIdempotent:
    """A resumed run walks the same files again, and must not grow the report each time."""

    def test_the_same_pair_twice_leaves_one_line(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, REASON)
        record_skipped_input(OBSID, config, FILE, REASON)

        assert read_skipped_inputs(OBSID, config) == [(FILE, REASON)]

    def test_the_same_file_for_a_different_reason_is_a_second_line(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, REASON)
        record_skipped_input(OBSID, config, FILE, "too far from the mode-01 position")

        assert len(read_skipped_inputs(OBSID, config)) == 2

    def test_several_files_are_all_kept(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        for chu in "1", "2", "3":
            record_skipped_input(OBSID, config, f"nu{OBSID}A06_chu{chu}_N_cl.evt", REASON)

        assert [name for name, _ in read_skipped_inputs(OBSID, config)] == [
            f"nu{OBSID}A06_chu{chu}_N_cl.evt" for chu in ("1", "2", "3")
        ]


class TestWhatIsRecorded:
    def test_the_item_is_a_base_name_not_a_path(self, tmp_path):
        """Workers see the output tree through a /tmp symlink whose name changes every
        run, so an absolute path recorded today means nothing tomorrow."""
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, f"/tmp/hrpq8x2/out/{OBSID}/split/{FILE}", REASON)

        assert read_skipped_inputs(OBSID, config) == [(FILE, REASON)]

    def test_a_reason_spread_over_lines_stays_on_one(self, tmp_path):
        """One line per skip is what makes the file greppable."""
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, "no source\nin the image")

        assert read_skipped_inputs(OBSID, config) == [(FILE, "no source in the image")]


class TestTheRewriteIsClean:
    def test_no_temporary_file_is_left_behind(self, tmp_path):
        config = {"out_data_path": str(tmp_path)}

        record_skipped_input(OBSID, config, FILE, REASON)
        record_skipped_input(OBSID, config, "nu90202038002B06_chu2_N_cl.evt", REASON)

        directory = os.path.dirname(skipped_inputs_file(OBSID, config))
        assert glob.glob(os.path.join(directory, "*.tmp")) == []
        assert os.listdir(directory) == ["skipped_inputs.txt"]

    def test_concurrent_skips_all_survive(self, tmp_path):
        """Every task of one observation writes to the same report, and each rewrite
        replaces the whole file."""
        config = {"out_data_path": str(tmp_path)}
        names = [f"nu{OBSID}A06_chu{n}_N_cl.evt" for n in range(20)]

        threads = [
            threading.Thread(target=record_skipped_input, args=(OBSID, config, name, REASON))
            for name in names
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sorted(name for name, _ in read_skipped_inputs(OBSID, config)) == sorted(names)
