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

from heasarc_retrieve_pipeline import core
from heasarc_retrieve_pipeline.core import (
    download_link_column,
    obsid_query,
    observation_work_items,
    prepare_worker,
)


def worker_state(roots):
    """What a worker process looks like once :func:`prepare_worker` has run.

    Calls it twice on purpose: an observation calls it every time, and it must settle the
    process the first time and then leave it alone.
    """
    first = prepare_worker(*roots)
    second = prepare_worker(*roots)
    return os.getpid(), os.environ.get("PFILES"), os.path.realpath(os.getcwd()), first == second


class TestPrepareWorker:
    """Each worker process gets its own parameter files and its own directory.

    The two roots are separate because they cost different things: the parameter files
    are kilobytes rewritten around every HEASOFT call, and want the fastest local disk;
    the working directory was measured peaking at 182.5 MB for one observation, and wants
    room.
    """

    def run_in_workers(self, root, n_workers=2, n_calls=6, pfiles_root=None):
        if pfiles_root is None:
            pfiles_root = root
        roots = (str(pfiles_root), str(root))
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            return list(pool.map(worker_state, [roots] * n_calls))

    def test_the_parameter_files_and_the_working_directory_can_be_far_apart(
        self, tmp_path, monkeypatch
    ):
        """The flow puts one on local disk and the other on the roomy filesystem."""
        headas = tmp_path / "headas"
        (headas / "syspfiles").mkdir(parents=True)
        monkeypatch.setenv("HEADAS", str(headas))

        states = self.run_in_workers(
            tmp_path / "roomy", n_workers=1, pfiles_root=tmp_path / "fast"
        )

        pid, pfiles, cwd, _ = states[0]
        assert pfiles.split(";")[0] == os.path.join(
            tmp_path, "fast", f"worker_{pid}", "pfiles"
        )
        assert cwd == os.path.realpath(os.path.join(tmp_path, "roomy", f"worker_{pid}"))

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


class TestOneFailureDoesNotStopTheRest:
    """A bad observation must cost only that observation.

    The old loop got this right by accident, with ``return_state=True`` and no report. The
    flow is expected to do it on purpose: every OBSID is attempted, the failures come back
    named, and the run does not stop at the first one.
    """

    def items(self, n=3):
        return [
            dict(obsid=f"obs{i}", url=f"https://example.invalid/obs{i}/", ra=1.0, dec=2.0)
            for i in range(n)
        ]

    def run(self, monkeypatch, tmp_path, failing, no_science=()):
        """Run the flow with the download and the reduction stubbed out."""
        processed = []

        def stub_download(url, outdir, test_str=".", test=False):
            return []

        def stub_processing(obsid, config=None, ra=None, dec=None, flags=None):
            if obsid in failing:
                raise ValueError(f"{obsid} is no good")
            if obsid in no_science:
                return core.NO_SCIENCE_DATA
            processed.append(obsid)
            return obsid

        monkeypatch.setattr(core, "recursive_download", stub_download)
        monkeypatch.setattr(
            core, "prepare_worker", lambda pfiles_root, work_root: str(tmp_path)
        )
        monkeypatch.setitem(
            core.MISSION_CONFIG["nustar"], "obsid_processing", stub_processing
        )

        failed = core.process_observations(
            self.items(),
            outdir=str(tmp_path),
            mission="nustar",
            pfiles_root=str(tmp_path / ".pfiles"),
            work_root=str(tmp_path / ".workers"),
        )
        return failed, processed

    def test_the_good_ones_all_run(self, tmp_path, monkeypatch):
        _, processed = self.run(monkeypatch, tmp_path, failing={"obs1"})

        assert sorted(processed) == ["obs0", "obs2"]

    def test_the_bad_one_is_named(self, tmp_path, monkeypatch):
        failed, _ = self.run(monkeypatch, tmp_path, failing={"obs1"})

        assert failed == ["obs1"]

    def test_the_first_failure_does_not_stop_the_rest(self, tmp_path, monkeypatch):
        """The failure is the first item submitted, so a plain loop would end there."""
        failed, processed = self.run(monkeypatch, tmp_path, failing={"obs0"})

        assert failed == ["obs0"]
        assert sorted(processed) == ["obs1", "obs2"]

    def test_all_failing_is_reported_and_not_raised(self, tmp_path, monkeypatch):
        failed, processed = self.run(monkeypatch, tmp_path, failing={"obs0", "obs1", "obs2"})

        assert failed == ["obs0", "obs1", "obs2"]
        assert processed == []

    def test_nothing_failing_returns_an_empty_list(self, tmp_path, monkeypatch):
        failed, processed = self.run(monkeypatch, tmp_path, failing=set())

        assert failed == []
        assert sorted(processed) == ["obs0", "obs1", "obs2"]


class TestTheFlowUsesAShortWorkspace:
    """The reduction runs under a short name, and the parameter files are off the output.

    A HEASOFT build that truncates file names at 128 characters -- measured on the user's
    cluster, 2376 truncations, every one at exactly 128 -- makes an output root longer
    than 67 characters unusable, because the pipeline adds 61 characters of its own after
    it. The flow hands the workers a symbolic link instead of the real directory, and the
    workers' parameter files go to local disk rather than the shared filesystem. Their
    working directories do not: those were measured at 182.5 MB apiece. See
    :func:`heasarc_retrieve_pipeline.utils.short_workspace` and issue 39 in
    ``docs/known_issues.rst``.
    """

    def run(self, monkeypatch, outdir, **kwargs):
        """Run the flow with everything but the workspace stubbed out."""
        seen = {}

        class Recorder:
            def with_options(self, **kwargs):
                return self

            def __call__(
                self,
                items,
                outdir,
                mission,
                pfiles_root,
                work_root,
                flags=None,
                test=False,
            ):
                seen["outdir"] = outdir
                seen["pfiles_root"] = pfiles_root
                seen["work_root"] = work_root
                # A worker writes its results through the name it was given.
                open(os.path.join(outdir, "a_result.txt"), "w").close()
                os.makedirs(os.path.join(pfiles_root, "worker_1"), exist_ok=True)
                os.makedirs(os.path.join(work_root, "worker_1"), exist_ok=True)
                return []

        monkeypatch.setattr(core, "locate_data", lambda table, catalog_name=None: table)
        monkeypatch.setattr(
            core,
            "observation_work_items",
            lambda table, links, column, position: [
                dict(obsid="obs0", url="https://example.invalid/", ra=1.0, dec=2.0)
            ],
        )
        monkeypatch.setattr(core, "process_observations", Recorder())

        core.retrieve_and_process_data(
            Table({"__row": [0]}), outdir=str(outdir), **kwargs
        )
        return seen

    def long_outdir(self, tmp_path):
        outdir = tmp_path / ("d" * 120)
        outdir.mkdir()
        return outdir

    def test_the_workers_get_a_shorter_name_than_the_real_directory(
        self, tmp_path, monkeypatch
    ):
        outdir = self.long_outdir(tmp_path)

        seen = self.run(monkeypatch, outdir)

        assert len(seen["outdir"]) < len(str(outdir))

    def test_what_a_worker_writes_lands_in_the_real_directory(self, tmp_path, monkeypatch):
        outdir = self.long_outdir(tmp_path)

        self.run(monkeypatch, outdir)

        assert (outdir / "a_result.txt").is_file()

    def test_the_parameter_files_are_not_inside_the_output_directory(
        self, tmp_path, monkeypatch
    ):
        """They used to be ``<outdir>/.workers/*/pfiles``, on the shared filesystem."""
        outdir = self.long_outdir(tmp_path)

        seen = self.run(monkeypatch, outdir)

        assert not os.path.realpath(seen["pfiles_root"]).startswith(
            os.path.realpath(str(outdir))
        )

    def test_the_working_directories_stay_where_there_is_room(self, tmp_path, monkeypatch):
        """182.5 MB per worker will not fit on a shared /tmp with 7.9 GB free."""
        outdir = self.long_outdir(tmp_path)

        seen = self.run(monkeypatch, outdir)

        assert os.path.realpath(seen["work_root"]).startswith(
            os.path.realpath(str(outdir))
        )

    def test_scratch_dir_moves_the_working_directories(self, tmp_path, monkeypatch):
        outdir = self.long_outdir(tmp_path)
        fast = tmp_path / "fast"
        fast.mkdir()

        seen = self.run(monkeypatch, outdir, scratch_dir=str(fast))

        assert os.path.realpath(seen["work_root"]).startswith(
            os.path.realpath(str(fast))
        )

    def test_the_workspace_is_cleaned_up_but_the_output_is_not(self, tmp_path, monkeypatch):
        outdir = self.long_outdir(tmp_path)

        seen = self.run(monkeypatch, outdir)

        assert not os.path.lexists(seen["outdir"])
        assert not os.path.exists(seen["pfiles_root"])
        assert not os.path.exists(seen["work_root"])
        assert (outdir / "a_result.txt").is_file()


class TestTheFlowRefusesNamesHeasoftCannotHandle:
    """A path too long to work stops the run in milliseconds, not after 90 GB.

    On the user's cluster the whole 56-observation run downloaded, ran the Level-2
    pipeline, and only then failed at ``nusplitsc`` -- 1050 times, all from file names
    truncated to 128 characters. Nothing in that chain said "too long". See issue 39 in
    ``docs/known_issues.rst``.
    """

    def run(self, monkeypatch, outdir, tmpdir=None):
        """Run the flow far enough to reach the check, with the reduction stubbed out."""
        called = []

        class Recorder:
            def with_options(self, **kwargs):
                return self

            def __call__(self, *args, **kwargs):
                called.append(kwargs)
                return []

        monkeypatch.setattr(core, "locate_data", lambda table, catalog_name=None: table)
        monkeypatch.setattr(
            core,
            "observation_work_items",
            lambda table, links, column, position: [
                dict(obsid="80002092008", url="https://example.invalid/", ra=1.0, dec=2.0)
            ],
        )
        monkeypatch.setattr(core, "process_observations", Recorder())
        if tmpdir is not None:
            monkeypatch.setattr(core, "short_workspace", _no_workspace(tmpdir))

        core.retrieve_and_process_data(Table({"__row": [0]}), outdir=str(outdir))
        return called

    def test_a_workable_output_directory_runs(self, tmp_path, monkeypatch):
        outdir = tmp_path / "out"
        outdir.mkdir()

        assert self.run(monkeypatch, outdir) != []

    def test_an_impossible_one_is_refused_before_anything_is_downloaded(
        self, tmp_path, monkeypatch
    ):
        """With the short name unavailable, a 120-character root cannot be made to fit."""
        outdir = tmp_path / ("d" * 120)
        outdir.mkdir()
        no_shorter = tmp_path / ("t" * 150)
        no_shorter.mkdir()

        with pytest.raises(ValueError, match="HEASOFT limit"):
            self.run(monkeypatch, outdir, tmpdir=str(no_shorter))

    def test_the_message_names_the_offending_path(self, tmp_path, monkeypatch):
        outdir = tmp_path / ("d" * 120)
        outdir.mkdir()
        no_shorter = tmp_path / ("t" * 150)
        no_shorter.mkdir()

        with pytest.raises(ValueError, match="A06_chu123_N_cl_"):
            self.run(monkeypatch, outdir, tmpdir=str(no_shorter))

    def test_the_short_name_is_what_saves_a_long_output_directory(
        self, tmp_path, monkeypatch
    ):
        """The same 120-character root is fine once the workspace has renamed it."""
        outdir = tmp_path / ("d" * 120)
        outdir.mkdir()

        assert self.run(monkeypatch, outdir) != []


def _no_workspace(tmpdir):
    """``short_workspace`` forced to put its directory somewhere that is no shorter."""
    from heasarc_retrieve_pipeline.utils import short_workspace

    def wrapped(outdir, tmpdir=tmpdir, scratch_dir=None):
        return short_workspace(outdir, tmpdir=tmpdir, scratch_dir=scratch_dir)

    return wrapped


class TestObservationsWithNoScienceData:
    """A slew is not a failure.

    A NuSTAR slew has an OBSID, a numaster row and downloadable files, and nothing in the
    headers marks it as one -- only the observing modes Level 2 produces give it away. Four
    of the 56 M82 observations reprocessed in 2026 were slews. Counting them as failures
    hides the observations that really did break.
    """

    def run(self, monkeypatch, tmp_path, failing=(), no_science=()):
        harness = TestOneFailureDoesNotStopTheRest()
        return harness.run(monkeypatch, tmp_path, set(failing), no_science=set(no_science))

    def test_a_slew_is_not_counted_as_a_failure(self, tmp_path, monkeypatch):
        failed, _ = self.run(monkeypatch, tmp_path, no_science={"obs1"})

        assert failed == []

    def test_the_others_still_run(self, tmp_path, monkeypatch):
        _, processed = self.run(monkeypatch, tmp_path, no_science={"obs1"})

        assert sorted(processed) == ["obs0", "obs2"]

    def test_failures_are_still_counted_alongside_slews(self, tmp_path, monkeypatch):
        failed, processed = self.run(
            monkeypatch, tmp_path, failing={"obs0"}, no_science={"obs1"}
        )

        assert failed == ["obs0"]
        assert processed == ["obs2"]


class TestExposureCondition:
    """Zero exposure means different things in different master catalogues.

    numaster means it: a NuSTAR observation with exposure_a of zero has no data, and
    downloading it wastes time and disk. nicermastr does not always mean it: NICER's own
    pipeline sometimes filters an observation away and records zero exposure for data that
    are perfectly usable.
    """

    def test_nustar_drops_zero_exposure(self):
        assert core.exposure_condition("nustar") == "cat.exposure_a > 0"

    def test_nicer_keeps_zero_exposure(self):
        assert core.exposure_condition("nicer") == "cat.exposure >= 0"

    def test_rxte_keeps_zero_exposure(self):
        assert core.exposure_condition("rxte") == "cat.exposure >= 0"

    def test_every_mission_drops_planned_but_unexecuted_observations(self):
        """A null or negative exposure is a plan, not an observation, for all of them."""
        for mission in core.MISSION_CONFIG:
            assert core.exposure_condition(mission).endswith(("> 0", ">= 0"))

    def test_naming_an_obsid_keeps_it_whatever_its_exposure(self):
        """An explicit OBSID must come back, even for NuSTAR, even at zero exposure."""
        assert "cat.exposure_a >= 0" in obsid_query("30502022001", "nustar")
