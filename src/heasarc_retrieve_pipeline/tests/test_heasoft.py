"""
Offline tests for the HEASOFT invocation lock.

What they guard is measured: two HEASOFT calls at once in one process delete each other's
parameter file about one time in ten. See :mod:`heasarc_retrieve_pipeline.heasoft`.
"""

import ast
import inspect
import os
import pathlib
import threading
import time
from types import SimpleNamespace

import pytest

from heasarc_retrieve_pipeline import heasoft


class RecordingTool:
    """A fake HEASOFT tool that notices when another call overlaps it."""

    def __init__(self, duration=0.01):
        self.duration = duration
        self.running = 0
        self.most_at_once = 0
        self.calls = 0
        self.guard = threading.Lock()

    def __call__(self, *args, **kwargs):
        with self.guard:
            self.running += 1
            self.most_at_once = max(self.most_at_once, self.running)
            self.calls += 1
        time.sleep(self.duration)
        with self.guard:
            self.running -= 1
        return "done"


def call_from_threads(target, n_threads=8):
    threads = [threading.Thread(target=target) for _ in range(n_threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


class TestOneToolAtATime:
    def test_threads_never_overlap_a_tool(self, monkeypatch):
        tool = RecordingTool()
        monkeypatch.setattr(heasoft, "hsp", SimpleNamespace(ftmerge=tool), raising=False)
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        call_from_threads(lambda: heasoft.run("ftmerge", produces=[], infile="a", outfile="b"))

        assert tool.calls == 8
        assert tool.most_at_once == 1

    def test_two_different_tools_do_not_overlap_either(self, monkeypatch):
        """The parameter directory is shared by every tool, not one per tool."""
        tool = RecordingTool()
        monkeypatch.setattr(
            heasoft, "hsp", SimpleNamespace(ftmerge=tool, ftsort=tool), raising=False
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        names = ["ftmerge", "ftsort"] * 4
        threads = [
            threading.Thread(target=heasoft.run, args=(name,), kwargs={"produces": []})
            for name in names
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert tool.most_at_once == 1

    def test_the_task_interface_is_locked_too(self, monkeypatch):
        """``HSPTask`` reads the parameter file when it is built, not only when called."""
        tool = RecordingTool()
        monkeypatch.setattr(
            heasoft, "hsp", SimpleNamespace(HSPTask=lambda name: tool), raising=False
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        call_from_threads(lambda: heasoft.run_task("nupipeline", produces=[], indir="x"))

        assert tool.most_at_once == 1

    def test_the_result_comes_back(self, monkeypatch):
        monkeypatch.setattr(heasoft, "hsp", SimpleNamespace(ftlist=lambda **kw: kw), raising=False)
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft.run("ftlist", produces=[], infile="x") == {
            "infile": "x",
            "allow_failure": True,
        }

    def test_without_heasoftpy_the_error_says_so(self, monkeypatch):
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", False)

        with pytest.raises(ImportError, match="heasoftpy"):
            heasoft.run("ftlist", produces=[], infile="x")


class TestFailuresAreNoticed:
    """``heasoft`` asks for ``allow_failure=True``: a tool that exits non-zero comes back
    as an ordinary result, and a caller that does not look carries on with a file that was
    never written. Measured on a real run: ``fappend`` returned quietly, and the merged
    event file went downstream with no GTI extension until something much later tripped
    over it.
    """

    def result(self, returncode, stdout="", stderr=""):
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    def test_a_non_zero_return_code_raises(self, monkeypatch):
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(fappend=lambda **kw: self.result(1, "no such extension")),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(RuntimeError) as excinfo:
            heasoft.run("fappend", produces=[], infile="a[1]", outfile="b")

        assert "fappend" in str(excinfo.value)
        assert "no such extension" in str(excinfo.value)

    def test_a_zero_return_code_is_returned(self, monkeypatch):
        good = self.result(0)
        monkeypatch.setattr(
            heasoft, "hsp", SimpleNamespace(ftlist=lambda **kw: good), raising=False
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft.run("ftlist", produces=[], infile="a") is good

    def test_the_task_interface_checks_too(self, monkeypatch):
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(HSPTask=lambda name: lambda **kw: self.result(2, "boom")),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(RuntimeError, match="nupipeline"):
            heasoft.run_task("nupipeline", produces=[], indir="x")

    def test_a_result_without_a_return_code_is_left_alone(self, monkeypatch):
        """Not every tool wrapper returns an ``HSPResult``; do not invent a failure."""
        monkeypatch.setattr(
            heasoft, "hsp", SimpleNamespace(ftlist=lambda **kw: None), raising=False
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft.run("ftlist", produces=[], infile="a") is None


class TestAskingForTheBehaviourRatherThanInheritingIt:
    """``allow_failure`` is asked for on every call, not set once on ``heasoftpy.Config``.

    ``Config`` is process-wide: setting it there would change what happens for every other
    user of ``heasoftpy`` in the same interpreter, who would stop getting the exceptions
    they expect. A keyword changes this module's calls and nothing else -- and ``heasoftpy``
    gives an explicit keyword precedence over ``Config``, which is what makes that work.
    """

    def test_run_asks_for_it(self, monkeypatch):
        seen = {}

        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(ftlist=lambda **kw: seen.update(kw)),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        heasoft.run("ftlist", produces=[], infile="a")

        assert seen["allow_failure"] is True

    def test_the_task_interface_asks_for_it_too(self, monkeypatch):
        seen = {}

        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(HSPTask=lambda name: lambda **kw: seen.update(kw)),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        heasoft.run_task("nupipeline", produces=[], indir="x")

        assert seen["allow_failure"] is True

    def test_a_caller_who_asks_for_something_else_gets_it(self, monkeypatch):
        """``setdefault``, not an override: ``"warn"`` is a third thing ``heasoftpy`` takes."""
        seen = {}

        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(ftlist=lambda **kw: seen.update(kw)),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        heasoft.run("ftlist", produces=[], infile="a", allow_failure="warn")

        assert seen["allow_failure"] == "warn"


class TestAHeasoftpyThatRaisesInstead:
    """The other half of ``allow_failure``, and why asking for it is not enough.

    ``heasoftpy`` 1.5 warns that the default is becoming ``False``; on the HEASARC conda
    channel it already has, and there a failed tool raises ``HSPTaskException`` -- which is
    an ``Exception``, not a ``RuntimeError``, and never names the tool. That broke a CI job
    on the very case these tests exist for. The keyword settles it today, but ``heasoftpy``
    discards keywords it does not recognise, so a version that renames or drops it would go
    back to raising without a word. The translation below is what actually holds the
    contract.
    """

    class FakeHSPTaskException(Exception):
        """Stands in for ``heasoftpy.HSPTaskException``, which has no HEASOFT to need."""

    def raising(self, message):
        def tool(**kwargs):
            raise self.FakeHSPTaskException(message)

        return tool

    def test_run_reports_it_as_a_runtime_error_naming_the_tool(self, monkeypatch):
        monkeypatch.setattr(heasoft, "HSP_FAILURE", self.FakeHSPTaskException)
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(ftmerge=self.raising("Return Code: 105\nalready exists")),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(RuntimeError) as excinfo:
            heasoft.run("ftmerge", produces=[], infile="a,b", outfile="m")

        assert "ftmerge" in str(excinfo.value)
        assert "105" in str(excinfo.value)
        assert "already exists" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, self.FakeHSPTaskException)

    def test_the_task_interface_is_translated_too(self, monkeypatch):
        monkeypatch.setattr(heasoft, "HSP_FAILURE", self.FakeHSPTaskException)
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(HSPTask=lambda name: self.raising("boom")),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(RuntimeError, match="nupipeline"):
            heasoft.run_task("nupipeline", produces=[], indir="x")

    def test_an_unrelated_exception_is_not_dressed_up_as_a_tool_failure(self, monkeypatch):
        """Only ``heasoftpy``'s own failure gets translated; a bug stays a bug."""
        monkeypatch.setattr(heasoft, "HSP_FAILURE", self.FakeHSPTaskException)

        def broken(**kwargs):
            raise KeyError("PFILES")

        monkeypatch.setattr(heasoft, "hsp", SimpleNamespace(ftlist=broken), raising=False)
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(KeyError):
            heasoft.run("ftlist", produces=[], infile="a")

    def test_nothing_to_catch_without_heasoftpy(self, monkeypatch):
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", False)

        assert heasoft._hsp_failure() == ()

    def test_a_heasoftpy_that_renamed_the_exception_says_so(self, monkeypatch, caplog):
        """Nothing left to catch is a thing to be told about, not to discover in a run."""
        monkeypatch.setattr(heasoft, "hsp", SimpleNamespace(), raising=False)
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft._hsp_failure() == ()
        assert "HSPTaskException" in caplog.text

    def test_the_exception_is_found_when_it_is_there(self, monkeypatch):
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(HSPTaskException=self.FakeHSPTaskException),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft._hsp_failure() is self.FakeHSPTaskException


class TestAToolMustProduceWhatItPromised:
    """A zero return code is not evidence that a file was written.

    Measured on a real run: ``ftmgtime`` was handed an empty list of input GTIs, returned
    0, wrote nothing at all, and the failure only surfaced one step later as ``ftsort
    failed with return code 33`` -- pointing at the wrong tool entirely.
    """

    def written(self, path, content="something"):
        path.write_text(content)
        return str(path)

    def test_an_existing_non_empty_file_passes(self, tmp_path):
        heasoft._check_outputs("ftmerge", self.written(tmp_path / "a.fits"))

    def test_a_list_of_files_all_have_to_be_there(self, tmp_path):
        first = self.written(tmp_path / "a.fits")
        heasoft._check_outputs("ftmerge", [first, self.written(tmp_path / "b.fits")])

        with pytest.raises(RuntimeError, match="missing.fits"):
            heasoft._check_outputs("ftmerge", [first, str(tmp_path / "missing.fits")])

    def test_a_missing_file_raises_naming_the_tool_and_the_path(self, tmp_path):
        with pytest.raises(RuntimeError) as excinfo:
            heasoft._check_outputs("ftmgtime", str(tmp_path / "nowhere.gti"))

        assert "ftmgtime" in str(excinfo.value)
        assert "nowhere.gti" in str(excinfo.value)

    def test_an_empty_file_raises(self, tmp_path):
        empty = self.written(tmp_path / "empty.gti", content="")

        with pytest.raises(RuntimeError, match="empty.gti"):
            heasoft._check_outputs("ftmgtime", empty)

    def test_a_directory_with_something_in_it_passes(self, tmp_path):
        splitdir = tmp_path / "split"
        splitdir.mkdir()
        self.written(splitdir / "chu1.evt")

        heasoft._check_outputs("nusplitsc", str(splitdir))

    def test_an_empty_directory_raises(self, tmp_path):
        splitdir = tmp_path / "split"
        splitdir.mkdir()

        with pytest.raises(RuntimeError, match="split"):
            heasoft._check_outputs("nusplitsc", str(splitdir))

    def test_in_place_checks_the_file_is_still_there(self, tmp_path):
        edited = self.written(tmp_path / "merged.gti")

        heasoft._check_outputs("fthedit", heasoft.IN_PLACE(edited))

        with pytest.raises(RuntimeError, match="gone.gti"):
            heasoft._check_outputs("fthedit", heasoft.IN_PLACE(str(tmp_path / "gone.gti")))

    def test_the_clobber_marker_is_not_part_of_the_name(self, tmp_path):
        """HEASOFT reads a leading ``!`` as "overwrite this"; the file on disk has no ``!``."""
        self.written(tmp_path / "sorted.evt")

        heasoft._check_outputs("ftsort", "!" + str(tmp_path / "sorted.evt"))

    def test_a_tool_that_returns_zero_and_writes_nothing_still_raises(self, monkeypatch, tmp_path):
        """The whole point: the check runs after the return code has already said fine."""
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(ftmgtime=lambda **kw: SimpleNamespace(returncode=0)),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(RuntimeError, match="ftmgtime"):
            heasoft.run("ftmgtime", produces=str(tmp_path / "never.gti"), ingtis="")

    def test_run_task_checks_its_outputs_too(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(HSPTask=lambda name: lambda **kw: SimpleNamespace(returncode=0)),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(RuntimeError, match="nupipeline"):
            heasoft.run_task("nupipeline", produces=str(tmp_path / "no_such_dir"), indir="x")


def test_produces_is_a_required_argument():
    """Keep it mandatory. A caller who has to write the output down cannot forget that a
    zero return code proves nothing."""
    for function in (heasoft.run, heasoft.run_task):
        parameters = inspect.signature(function).parameters

        assert "produces" in parameters, f"{function.__name__} lost its produces argument"
        assert parameters["produces"].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters["produces"].default is inspect.Parameter.empty


MODULES = sorted(
    p
    for p in pathlib.Path(heasoft.__file__).parent.glob("*.py")
    if p.name not in ("__init__.py", "_version.py", "heasoft.py")
)


def heasoftpy_calls(source):
    """Calls of the form ``hsp.<something>(...)``, which bypass the lock."""
    tree = ast.parse(source)
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id == "hsp":
                offenders.append(func.attr)
    return offenders


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_no_module_calls_a_heasoft_tool_behind_the_lock(path):
    offenders = heasoftpy_calls(path.read_text())

    assert offenders == [], f"{path.name} calls heasoftpy directly: {offenders}"


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_only_one_module_imports_heasoftpy(path):
    assert "import heasoftpy" not in path.read_text(), f"{path.name} imports heasoftpy"


class TestPrivatePfilesAreHeldOnTo:
    """PFILES must still be this process's own when a tool actually runs.

    Each worker process gets a private parameter directory precisely so that four of them
    cannot delete one another's ``<tool>.par``. In the 2026 reprocessing of 56 M82
    observations that isolation did not hold: 1016 ``fthedit`` calls ran, and a handful
    failed with the parameter file resolving to the shared ``$HOME/pfiles`` instead --
    under all four worker PIDs, not one bad process. Seven observations were lost that way.

    heasoftpy reads ``os.environ["PFILES"]`` afresh on every call (``core.find_pfile``), so
    checking it immediately before the call is enough to undo whatever put the shared
    directory back, and says so in the log the first time.
    """

    def worker(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADAS", str(tmp_path / "headas"))
        monkeypatch.setattr(heasoft, "_EXPECTED_PFILES", None)
        monkeypatch.setattr(heasoft, "_PFILES_REPAIRED", False)
        return heasoft.use_private_pfiles(str(tmp_path / "mine"))

    def seen_by_the_tool(self, monkeypatch):
        """Run a tool that records the PFILES in force at the moment it is called."""
        seen = []

        def tool(*args, **kwargs):
            seen.append(os.environ.get("PFILES"))
            return "done"

        monkeypatch.setattr(heasoft, "hsp", SimpleNamespace(ftlist=tool), raising=False)
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)
        return seen

    def test_it_sets_pfiles_to_the_private_directory_first(self, monkeypatch, tmp_path):
        expected = self.worker(monkeypatch, tmp_path)

        assert os.environ["PFILES"] == expected
        assert str(tmp_path / "mine") == expected.split(";")[0]
        assert expected.endswith(os.path.join(str(tmp_path / "headas"), "syspfiles"))

    def test_a_clobbered_pfiles_is_repaired_before_the_tool_runs(self, monkeypatch, tmp_path):
        expected = self.worker(monkeypatch, tmp_path)
        seen = self.seen_by_the_tool(monkeypatch)
        monkeypatch.setenv("PFILES", "/home/someone/pfiles;/opt/heasoft/syspfiles")

        heasoft.run("ftlist", produces=[], infile="a")

        assert seen == [expected]

    def test_an_intact_pfiles_is_left_alone(self, monkeypatch, tmp_path):
        expected = self.worker(monkeypatch, tmp_path)
        seen = self.seen_by_the_tool(monkeypatch)

        heasoft.run("ftlist", produces=[], infile="a")

        assert seen == [expected]

    def test_nothing_is_touched_when_no_private_directory_was_claimed(self, monkeypatch):
        """A plain script that never called prepare_worker keeps its own environment."""
        monkeypatch.setattr(heasoft, "_EXPECTED_PFILES", None)
        seen = self.seen_by_the_tool(monkeypatch)
        monkeypatch.setenv("PFILES", "/whatever;/else")

        heasoft.run("ftlist", produces=[], infile="a")

        assert seen == ["/whatever;/else"]

    def test_run_task_is_guarded_too(self, monkeypatch, tmp_path):
        """HSPTask reads the parameter file when it is built, so it needs the guard first."""
        expected = self.worker(monkeypatch, tmp_path)
        seen = []

        class FakeTask:
            def __init__(self, name):
                seen.append(os.environ.get("PFILES"))

            def __call__(self, **params):
                return SimpleNamespace(returncode=0)

        monkeypatch.setattr(heasoft, "hsp", SimpleNamespace(HSPTask=FakeTask), raising=False)
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)
        monkeypatch.setenv("PFILES", "/home/someone/pfiles;/opt/heasoft/syspfiles")

        heasoft.run_task("nupipeline", produces=[], indir="a")

        assert seen == [expected]


class TestToolOutputCanGoToAFile:
    """
    A tool's own chatter belongs beside the observation, not in the batch log.

    ``heasoftpy`` writes every line a tool prints to ``sys.stdout`` and never flushes it.
    Under a scheduler that is a file rather than a terminal, so it is block buffered and
    arrives in bursts long after the pipeline messages it belongs between. In a real
    40 MB run, 98% of the lines were tool output and the closing summary was buried in
    it. Level 20 is ``heasoftpy``'s "capture, and write to a file rather than the
    screen"; the output still comes back on the result, so a failed tool still names
    itself with its own words.
    """

    def a_tool(self, monkeypatch, seen):
        monkeypatch.setattr(heasoft, "_LOG_STARTED", set())
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(nuproducts=lambda **kw: seen.update(kw)),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

    def test_a_log_asks_for_the_level_that_does_not_print(self, monkeypatch, tmp_path):
        seen = {}
        self.a_tool(monkeypatch, seen)
        path = tmp_path / "90202038002" / "logs" / "nuproducts.log"

        heasoft.run("nuproducts", produces=[], log_to=str(path), infile="a")

        assert seen["verbose"] == 20
        assert seen["logfile"] == str(path)

    def test_a_call_with_no_log_is_left_exactly_as_it_was(self, monkeypatch):
        seen = {}
        self.a_tool(monkeypatch, seen)

        heasoft.run("nuproducts", produces=[], infile="a")

        assert "verbose" not in seen
        assert "logfile" not in seen

    def test_the_directory_is_made(self, monkeypatch, tmp_path):
        seen = {}
        self.a_tool(monkeypatch, seen)
        path = tmp_path / "90202038002" / "logs" / "nuproducts.log"

        heasoft.run("nuproducts", produces=[], log_to=str(path), infile="a")

        assert path.parent.is_dir()

    def test_a_relative_path_is_resolved_before_the_worker_moves(self, monkeypatch, tmp_path):
        """A worker runs in a private working directory, so ``heasoftpy``'s ``open`` is
        relative to somewhere the caller did not mean."""
        seen = {}
        self.a_tool(monkeypatch, seen)
        monkeypatch.chdir(tmp_path)

        heasoft.run("nuproducts", produces=[], log_to="logs/nuproducts.log", infile="a")

        assert os.path.isabs(seen["logfile"])
        assert seen["logfile"] == str(tmp_path / "logs" / "nuproducts.log")

    def test_the_first_call_of_a_run_starts_the_file_again(self, monkeypatch, tmp_path):
        """``heasoftpy`` opens the log in append mode, which is wrong across runs."""
        seen = {}
        self.a_tool(monkeypatch, seen)
        path = tmp_path / "logs" / "nuproducts.log"
        path.parent.mkdir()
        path.write_text("what an earlier run wrote\n")

        heasoft.run("nuproducts", produces=[], log_to=str(path), infile="a")

        assert path.read_text() == ""

    def test_a_later_call_of_the_same_run_is_left_to_append(self, monkeypatch, tmp_path):
        """``nuproducts`` runs once per stem: the second must not erase the first."""
        seen = {}
        self.a_tool(monkeypatch, seen)
        path = tmp_path / "logs" / "nuproducts.log"

        heasoft.run("nuproducts", produces=[], log_to=str(path), infile="a")
        path.write_text("what the first call wrote\n")
        heasoft.run("nuproducts", produces=[], log_to=str(path), infile="b")

        assert path.read_text() == "what the first call wrote\n"

    def test_the_batch_log_is_told_where_the_output_went(self, monkeypatch, tmp_path, caplog):
        """Once per file, so the run log stays navigable without carrying the output."""
        seen = {}
        self.a_tool(monkeypatch, seen)
        path = tmp_path / "logs" / "nuproducts.log"

        with caplog.at_level("INFO", logger="heasarc_retrieve_pipeline"):
            heasoft.run("nuproducts", produces=[], log_to=str(path), infile="a")
            heasoft.run("nuproducts", produces=[], log_to=str(path), infile="b")

        assert caplog.text.count(str(path)) == 1
        assert "nuproducts" in caplog.text

    def test_the_task_interface_takes_one_too(self, monkeypatch, tmp_path):
        seen = {}
        monkeypatch.setattr(heasoft, "_LOG_STARTED", set())
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(HSPTask=lambda name: lambda **kw: seen.update(kw)),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)
        path = tmp_path / "logs" / "nupipeline.log"

        heasoft.run_task("nupipeline", produces=[], log_to=str(path), indir="x")

        assert seen["verbose"] == 20
        assert seen["logfile"] == str(path)
