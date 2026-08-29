"""
Offline tests for the HEASOFT invocation lock.

What they guard is measured: two HEASOFT calls at once in one process delete each other's
parameter file about one time in ten. See :mod:`heasarc_retrieve_pipeline.heasoft`.
"""

import ast
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

        call_from_threads(lambda: heasoft.run("ftmerge", infile="a", outfile="b"))

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
        threads = [threading.Thread(target=heasoft.run, args=(name,)) for name in names]
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

        call_from_threads(lambda: heasoft.run_task("nupipeline", indir="x"))

        assert tool.most_at_once == 1

    def test_the_result_comes_back(self, monkeypatch):
        monkeypatch.setattr(
            heasoft, "hsp", SimpleNamespace(ftlist=lambda **kw: kw), raising=False
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft.run("ftlist", infile="x") == {"infile": "x"}

    def test_without_heasoftpy_the_error_says_so(self, monkeypatch):
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", False)

        with pytest.raises(ImportError, match="heasoftpy"):
            heasoft.run("ftlist", infile="x")


class TestFailuresAreNoticed:
    """``heasoftpy`` defaults to ``allow_failure=True``: a tool that exits non-zero comes
    back as an ordinary result, and the caller carries on with a file that was never
    written. Measured on a real run: ``fappend`` returned quietly, and the merged event
    file went downstream with no GTI extension until something much later tripped over it.
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
            heasoft.run("fappend", infile="a[1]", outfile="b")

        assert "fappend" in str(excinfo.value)
        assert "no such extension" in str(excinfo.value)

    def test_a_zero_return_code_is_returned(self, monkeypatch):
        good = self.result(0)
        monkeypatch.setattr(
            heasoft, "hsp", SimpleNamespace(ftlist=lambda **kw: good), raising=False
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft.run("ftlist", infile="a") is good

    def test_the_task_interface_checks_too(self, monkeypatch):
        monkeypatch.setattr(
            heasoft,
            "hsp",
            SimpleNamespace(HSPTask=lambda name: lambda **kw: self.result(2, "boom")),
            raising=False,
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        with pytest.raises(RuntimeError, match="nupipeline"):
            heasoft.run_task("nupipeline", indir="x")

    def test_a_result_without_a_return_code_is_left_alone(self, monkeypatch):
        """Not every tool wrapper returns an ``HSPResult``; do not invent a failure."""
        monkeypatch.setattr(
            heasoft, "hsp", SimpleNamespace(ftlist=lambda **kw: None), raising=False
        )
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        assert heasoft.run("ftlist", infile="a") is None


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
