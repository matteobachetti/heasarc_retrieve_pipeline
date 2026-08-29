"""
Offline tests for what makes several observations safe to reduce at the same time.

The hazard these guard is measured, not theoretical. Running the same HEASOFT tool
eight ways at once from one process fails about one call in ten, because ``heasoftpy``
rewrites a parameter file in ``~/pfiles`` that every call shares::

    8 threads,   shared ~/pfiles   19 failures in 200 calls
    8 processes, shared ~/pfiles    9 failures in 200 calls
    8 processes, private PFILES     0 failures in 200 calls

``PFILES`` is an environment variable, and the environment belongs to the process, so
the isolation has to be per process. See issue 26 in ``docs/known_issues.rst``.
"""

import os
from concurrent.futures import ProcessPoolExecutor

import pytest
from astropy.table import Table

from heasarc_retrieve_pipeline.core import (
    download_link_column,
    obsid_query,
    observation_work_items,
    prepare_worker,
)


def worker_state(root):
    """What a worker process looks like once :func:`prepare_worker` has run.

    Calls it twice on purpose: an observation calls it every time, and it must settle the
    process the first time and then leave it alone.
    """
    first = prepare_worker(root)
    second = prepare_worker(root)
    return os.getpid(), os.environ.get("PFILES"), os.path.realpath(os.getcwd()), first == second


class TestPrepareWorker:
    """Each worker process gets its own parameter files and its own directory."""

    def run_in_workers(self, root, n_workers=2, n_calls=6):
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            return list(pool.map(worker_state, [str(root)] * n_calls))

    def test_no_two_workers_share_a_directory(self, tmp_path):
        states = self.run_in_workers(tmp_path)

        by_pid = {pid: cwd for pid, _, cwd, _ in states}
        assert len(set(by_pid.values())) == len(by_pid)

    def test_a_worker_stands_in_its_own_directory(self, tmp_path):
        states = self.run_in_workers(tmp_path, n_workers=1)

        pid, _, cwd, _ = states[0]
        assert cwd == os.path.realpath(os.path.join(tmp_path, f"worker_{pid}"))

    def test_the_setup_happens_once_per_process(self, tmp_path):
        """Every observation calls it; only the first call in a process does anything."""
        states = self.run_in_workers(tmp_path, n_workers=1)

        assert all(same for _, _, _, same in states)

    def test_the_parameter_files_are_private_and_the_system_ones_stay_readable(
        self, tmp_path, monkeypatch
    ):
        headas = tmp_path / "headas"
        (headas / "syspfiles").mkdir(parents=True)
        monkeypatch.setenv("HEADAS", str(headas))

        states = self.run_in_workers(tmp_path / "work", n_workers=2)

        for pid, pfiles, _, _ in states:
            private, system = pfiles.split(";")
            assert private == os.path.join(tmp_path, "work", f"worker_{pid}", "pfiles")
            assert os.path.isdir(private)
            assert system == str(headas / "syspfiles")

    def test_without_heasoft_nothing_is_claimed_about_pfiles(self, tmp_path, monkeypatch):
        monkeypatch.delenv("HEADAS", raising=False)
        monkeypatch.delenv("PFILES", raising=False)

        states = self.run_in_workers(tmp_path, n_workers=1)

        assert states[0][1] is None


class TestDownloadLinkColumn:
    """Which mirror the downloads come from."""

    def test_s3_is_the_default(self):
        assert download_link_column(environ={}) == "aws"

    def test_forcing_s3_wins(self):
        assert download_link_column(force_s3=True, environ={}) == "aws"

    def test_forcing_heasarc_gives_the_https_archive(self):
        assert download_link_column(force_heasarc=True, environ={}) == "access_url"

    def test_on_sciserver_the_local_copy_is_used(self):
        assert download_link_column(environ={"SCISERVER_USER_ID": "42"}) == "sciserver"

    def test_an_explicit_choice_beats_sciserver(self):
        environ = {"SCISERVER_USER_ID": "42"}

        assert download_link_column(force_s3=True, environ=environ) == "aws"
        assert download_link_column(force_heasarc=True, environ=environ) == "access_url"


def catalogue(*rows):
    """A catalogue table of ``(obsid, __row, ra, dec)``."""
    return Table(rows=list(rows), names=("obsid", "__row", "ra", "dec"))


def datalink(*rows):
    """A datalink table of ``(ID, aws, access_url)``."""
    return Table(rows=list(rows), names=("ID", "aws", "access_url"))


class TestObservationWorkItems:
    """Turning catalogue rows plus datalink rows into one unit of work each."""

    CATALOGUE = catalogue(("90901333002", "1", 148.9, 69.6), ("80002092008", "2", 10.0, -20.0))
    LINKS = datalink(
        ("http://x/?1", "s3://bucket/90901333002", "https://heasarc/90901333002"),
        ("http://x/?2", "s3://bucket/80002092008", "https://heasarc/80002092008"),
    )

    def test_one_item_per_row_with_the_chosen_mirror(self):
        items = observation_work_items(self.CATALOGUE, self.LINKS, "aws")

        assert [item["obsid"] for item in items] == ["90901333002", "80002092008"]
        assert [item["url"] for item in items] == [
            "s3://bucket/90901333002",
            "s3://bucket/80002092008",
        ]

    def test_the_pointing_is_used_when_no_source_position_is_given(self):
        items = observation_work_items(self.CATALOGUE, self.LINKS, "aws")

        assert (items[0]["ra"], items[0]["dec"]) == (148.9, 69.6)
        assert (items[1]["ra"], items[1]["dec"]) == (10.0, -20.0)

    def test_a_source_position_overrides_every_pointing(self):
        from astropy.coordinates import SkyCoord

        position = SkyCoord(83.63, 22.01, unit="deg")

        items = observation_work_items(self.CATALOGUE, self.LINKS, "aws", position)

        for item in items:
            assert item["ra"] == pytest.approx(83.63)
            assert item["dec"] == pytest.approx(22.01)

    def test_an_observation_with_no_downloadable_products_is_skipped(self):
        """Proprietary data come back from the datalink service with an empty URL."""
        links = datalink(
            ("http://x/?1", "", ""),
            ("http://x/?2", "s3://bucket/80002092008", "https://heasarc/80002092008"),
        )

        items = observation_work_items(self.CATALOGUE, links, "aws")

        assert [item["obsid"] for item in items] == ["80002092008"]

    def test_an_observation_missing_from_the_datalink_answer_is_skipped(self):
        links = datalink(("http://x/?2", "s3://b/80002092008", "https://h/80002092008"))

        items = observation_work_items(self.CATALOGUE, links, "aws")

        assert [item["obsid"] for item in items] == ["80002092008"]

    def test_rows_are_matched_by_identity_not_by_order(self):
        links = datalink(
            ("http://x/?2", "s3://bucket/80002092008", "https://h/80002092008"),
            ("http://x/?1", "s3://bucket/90901333002", "https://h/90901333002"),
        )

        items = observation_work_items(self.CATALOGUE, links, "aws")

        assert items[0]["url"] == "s3://bucket/90901333002"
        assert items[1]["url"] == "s3://bucket/80002092008"


class TestObsidQuery:
    """The catalogue query behind "reduce these observations"."""

    def test_one_obsid_is_looked_up_by_itself(self):
        query = obsid_query("90901333002", "nustar")

        assert "cat.obsid IN ('90901333002')" in query

    def test_many_obsids_are_one_query(self):
        query = obsid_query(["90901333002", "80002092008"], "nustar")

        assert "cat.obsid IN ('90901333002', '80002092008')" in query
        assert query.count("SELECT") == 1

    def test_the_mission_decides_the_table_and_the_columns(self):
        assert "public.numaster" in obsid_query("1", "nustar")
        assert "public.xtemaster" in obsid_query("1", "rxte")
        assert "cycle, prnb" in obsid_query("1", "rxte")

    def test_an_obsid_that_is_not_an_identifier_is_refused(self):
        """An OBSID goes into the query text, so it may only look like an OBSID."""
        with pytest.raises(ValueError, match="not a valid OBSID"):
            obsid_query("90901333002'; DROP TABLE cat --", "nustar")

    def test_an_empty_list_is_refused(self):
        with pytest.raises(ValueError, match="No OBSID"):
            obsid_query([], "nustar")
