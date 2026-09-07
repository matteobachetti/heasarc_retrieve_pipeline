"""
Offline tests for the SAS invocation layer.

No SAS is needed: ``subprocess.run`` is monkeypatched, and everything these check --
the argument vector, the return code, the promised outputs, the environment -- is on our
side of that call.
"""

import ast
import inspect
import os
import pathlib
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from heasarc_retrieve_pipeline import sas


def a_sas_that_records_its_calls(monkeypatch, returncode=0, writes=None, stdout=None):
    """
    Replace ``subprocess.run`` with a double, and return the list of calls it saw.

    ``writes`` is a list of paths the pretend task creates, so that the output check has
    something real to look at. ``stdout`` is what the pretend task printed, for the tasks
    whose answer is what they print.
    """
    calls = []

    def run(argv, **kwargs):
        calls.append(SimpleNamespace(argv=argv, kwargs=kwargs))
        for path in writes or []:
            with open(path, "w") as fobj:
                fobj.write("something\n")
        return subprocess.CompletedProcess(argv, returncode, stdout=stdout)

    monkeypatch.setattr(sas.subprocess, "run", run)
    monkeypatch.setattr(sas, "HAS_SAS", True)
    return calls


class TestWhereTheTaskRuns:
    """
    The working directory a task is run from, which is not decoration.

    A SAS task writes the names it was given into the headers of the files it makes:
    ``especget`` puts ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE`` in a spectrum so that a
    fitting program can follow them. Given absolute paths it writes absolute paths, and a
    FITS header card holds 80 characters -- the same trap that truncated the file names in
    an ``addspec`` merge. Running the task where its outputs belong lets the caller pass
    plain names.
    """

    def test_the_task_runs_where_it_was_told_to(self, monkeypatch, tmp_path):
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run("evselect", produces=str(tmp_path / "out.fits"), cwd=str(tmp_path))

        assert calls[0].kwargs["cwd"] == str(tmp_path)

    def test_by_default_it_runs_where_this_process_is(self, monkeypatch, tmp_path):
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run("evselect", produces=str(tmp_path / "out.fits"))

        assert calls[0].kwargs["cwd"] is None

    def test_the_outputs_are_still_checked_by_their_full_path(self, monkeypatch, tmp_path):
        # The task is handed a bare name; the caller still says where that lands, so a
        # task that wrote nothing is caught wherever it ran.
        a_sas_that_records_its_calls(monkeypatch)

        with pytest.raises(RuntimeError, match="did not create"):
            sas.run(
                "especget",
                produces=str(tmp_path / "src.pi"),
                cwd=str(tmp_path),
                srcspecset="src.pi",
            )


class TestTheArgumentVector:
    """
    SAS takes ``keyword=value`` arguments, and they must reach it untouched.

    This is the whole reason the package does not use pySAS's ``MyTask.run()``: that
    builds a command line and hands it to a shell, which re-quotes every value. An
    ``evselect`` expression is full of characters a shell has opinions about.
    """

    def test_the_task_name_comes_first_and_the_parameters_follow(self, monkeypatch, tmp_path):
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run(
            "evselect",
            produces=str(tmp_path / "out.fits"),
            table="events.fits",
            withfilteredset="yes",
        )

        assert calls[0].argv == [
            "evselect",
            "table=events.fits",
            "withfilteredset=yes",
        ]

    def test_an_expression_reaches_the_task_exactly_as_written(self, monkeypatch, tmp_path):
        """Spaces, ``&&``, ``#`` and parentheses: a shell would mangle every one of them."""
        expression = "#XMMEA_EP && (PATTERN<=4) && (PI in [200:12000]) && FLAG==0"
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run("evselect", produces=str(tmp_path / "out.fits"), expression=expression)

        assert calls[0].argv[1] == f"expression={expression}"

    def test_no_shell_is_involved(self, monkeypatch, tmp_path):
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run("evselect", produces=str(tmp_path / "out.fits"), table="events.fits")

        assert calls[0].kwargs.get("shell", False) is False

    def test_a_boolean_becomes_the_word_sas_expects(self, monkeypatch, tmp_path):
        """SAS says ``yes``/``no``; Python's ``True`` would arrive as ``True``."""
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run(
            "evselect",
            produces=str(tmp_path / "out.fits"),
            withfilteredset=True,
            keepfilteroutput=False,
        )

        assert calls[0].argv[1:] == ["withfilteredset=yes", "keepfilteroutput=no"]

    def test_a_number_becomes_its_plain_text(self, monkeypatch, tmp_path):
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run("evselect", produces=str(tmp_path / "out.fits"), timebinsize=10.0, ccdnr=4)

        assert calls[0].argv[1:] == ["timebinsize=10.0", "ccdnr=4"]


class TestWhetherSasIsThere:
    """
    What ``has_sas`` may and may not ask.

    The probe used to require that ``import pysas`` succeed. It does not any more, and
    the reason is a real machine: SAS 22.1.0, every task on ``PATH``, and ESA's own pysas
    unable to import because ``beautifultable`` -- a third-party table formatter this
    package never touches -- was not installed in the environment. The pipeline called
    that "no SAS" and refused to reduce anything. Since ``sas.run`` reaches the tasks
    through :func:`subprocess.run` and never through pysas, whether pysas imports says
    nothing about whether a task can be run.
    """

    def a_machine(self, monkeypatch, sas_dir=None, on_path=None):
        monkeypatch.delenv("SAS_DIR", raising=False)
        if sas_dir is not None:
            monkeypatch.setenv("SAS_DIR", sas_dir)
        monkeypatch.setattr(sas.shutil, "which", lambda name: on_path)

    def test_an_initialised_sas_is_found(self, monkeypatch):
        self.a_machine(monkeypatch, sas_dir="/opt/sas", on_path="/opt/sas/bin/evselect")

        assert sas.has_sas() is True

    def test_without_sas_dir_there_is_no_sas(self, monkeypatch):
        self.a_machine(monkeypatch, sas_dir=None, on_path="/opt/sas/bin/evselect")

        assert sas.has_sas() is False

    def test_an_empty_sas_dir_is_no_sas_either(self, monkeypatch):
        self.a_machine(monkeypatch, sas_dir="", on_path="/opt/sas/bin/evselect")

        assert sas.has_sas() is False

    def test_a_stale_sas_dir_with_no_task_on_path_is_not_sas(self, monkeypatch):
        """The failure mode ``SAS_DIR`` alone cannot catch: a variable left over from
        another shell, in a process ``setsas.sh`` never reached."""
        self.a_machine(monkeypatch, sas_dir="/opt/sas", on_path=None)

        assert sas.has_sas() is False

    def test_it_is_evselect_that_is_looked_for(self, monkeypatch):
        asked = []
        monkeypatch.setenv("SAS_DIR", "/opt/sas")
        monkeypatch.setattr(sas.shutil, "which", lambda name: asked.append(name) or "/x")

        sas.has_sas()

        assert asked == ["evselect"]


class TestAFailedTaskIsNoticed:
    def test_a_non_zero_return_code_raises_naming_the_task(self, monkeypatch, tmp_path):
        a_sas_that_records_its_calls(monkeypatch, returncode=1, writes=[tmp_path / "out.fits"])

        with pytest.raises(RuntimeError, match="evselect"):
            sas.run("evselect", produces=str(tmp_path / "out.fits"), table="events.fits")

    def test_a_task_that_is_not_installed_says_so(self, monkeypatch, tmp_path):
        def run(argv, **kwargs):
            raise FileNotFoundError(2, "No such file or directory", argv[0])

        monkeypatch.setattr(sas.subprocess, "run", run)
        monkeypatch.setattr(sas, "HAS_SAS", True)

        with pytest.raises(RuntimeError, match="ecoordconv"):
            sas.run("ecoordconv", produces=[], imagefile="x")

    def test_nothing_runs_without_sas(self, monkeypatch):
        monkeypatch.setattr(sas, "HAS_SAS", False)

        with pytest.raises(ImportError, match="SAS"):
            sas.run("evselect", produces=[], table="events.fits")


class TestAZeroReturnCodeProvesNothing:
    """
    The same lesson ``heasoft.run`` learnt from ``ftmgtime``: a task can succeed and write
    nothing at all, and the failure then surfaces one step later under the wrong name.
    """

    def test_a_missing_output_raises(self, monkeypatch, tmp_path):
        a_sas_that_records_its_calls(monkeypatch)

        with pytest.raises(RuntimeError, match="did not create"):
            sas.run("evselect", produces=str(tmp_path / "never.fits"), table="events.fits")

    def test_an_empty_output_raises(self, monkeypatch, tmp_path):
        empty = tmp_path / "empty.fits"
        empty.touch()
        a_sas_that_records_its_calls(monkeypatch)

        with pytest.raises(RuntimeError, match="is empty"):
            sas.run("evselect", produces=str(empty), table="events.fits")

    def test_every_promised_output_is_checked(self, monkeypatch, tmp_path):
        a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "one.fits"])

        with pytest.raises(RuntimeError, match="two.fits"):
            sas.run(
                "especget",
                produces=[str(tmp_path / "one.fits"), str(tmp_path / "two.fits")],
                table="events.fits",
            )

    def test_an_empty_directory_raises(self, monkeypatch, tmp_path):
        (tmp_path / "products").mkdir()
        a_sas_that_records_its_calls(monkeypatch)

        with pytest.raises(RuntimeError, match="empty"):
            sas.run("epproc", produces=str(tmp_path / "products"))

    def test_a_file_edited_in_place_only_has_to_be_there(self, monkeypatch, tmp_path):
        """``barycen`` rewrites its input; the file existed before the call too."""
        events = tmp_path / "events.fits"
        events.write_text("times\n")
        a_sas_that_records_its_calls(monkeypatch)

        sas.run("barycen", produces=sas.IN_PLACE(str(events)), table=str(events))

    def test_an_empty_promise_checks_nothing(self, monkeypatch):
        """What a test double wants, and what a task with no file output gets."""
        a_sas_that_records_its_calls(monkeypatch)

        sas.run("ecoordconv", produces=[], imagefile="x")


class TestTheEnvironmentIsPerObservation:
    """
    ``SAS_CCF`` and ``SAS_ODF`` name one observation's calibration index and raw data. A
    worker process reduces several observations one after another, so setting them in
    ``os.environ`` would leave the second observation reading the first one's calibration.
    Every call carries its own copy instead.
    """

    def test_it_never_touches_the_real_environment(self, monkeypatch, tmp_path):
        monkeypatch.delenv("SAS_CCF", raising=False)

        sas.sas_environment(ccf=str(tmp_path / "ccf.cif"))

        assert "SAS_CCF" not in os.environ

    def test_it_starts_from_the_environment_it_was_given(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SAS_DIR", "/opt/sas")

        env = sas.sas_environment(ccf=str(tmp_path / "ccf.cif"))

        assert env["SAS_DIR"] == "/opt/sas"
        assert env["SAS_CCF"] == str(tmp_path / "ccf.cif")

    def test_what_it_is_not_given_it_does_not_invent(self, monkeypatch):
        monkeypatch.delenv("SAS_ODF", raising=False)
        monkeypatch.delenv("SAS_CCFPATH", raising=False)

        env = sas.sas_environment(ccf="/somewhere/ccf.cif")

        assert "SAS_ODF" not in env
        assert "SAS_CCFPATH" not in env

    def test_an_inherited_value_is_not_deleted_by_omission(self, monkeypatch):
        """``SAS_CCFPATH`` is normally set once, by the user, for the whole machine."""
        monkeypatch.setenv("SAS_CCFPATH", "/data/ccf")

        env = sas.sas_environment(ccf="/somewhere/ccf.cif")

        assert env["SAS_CCFPATH"] == "/data/ccf"

    def test_it_reaches_the_task(self, monkeypatch, tmp_path):
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])
        env = sas.sas_environment(ccf=str(tmp_path / "ccf.cif"))

        sas.run("evselect", produces=str(tmp_path / "out.fits"), env=env, table="e.fits")

        assert calls[0].kwargs["env"]["SAS_CCF"] == str(tmp_path / "ccf.cif")

    def test_a_call_with_no_environment_inherits_this_process(self, monkeypatch, tmp_path):
        calls = a_sas_that_records_its_calls(monkeypatch, writes=[tmp_path / "out.fits"])

        sas.run("evselect", produces=str(tmp_path / "out.fits"), table="e.fits")

        assert calls[0].kwargs.get("env") is None


class TestOneTaskAtATime:
    """
    SAS has no ``PFILES`` problem, but it does write scratch files into the working
    directory and its tasks are minutes-long subprocesses. Serialising them within a
    process costs nothing real and keeps one observation's tasks in a readable order, so
    the lock is kept for the same reason ``heasoft`` has one.
    """

    def test_threads_never_overlap_a_task(self, monkeypatch, tmp_path):
        overlap = SimpleNamespace(running=0, most_at_once=0, guard=threading.Lock())

        def run(argv, **kwargs):
            with overlap.guard:
                overlap.running += 1
                overlap.most_at_once = max(overlap.most_at_once, overlap.running)
            time.sleep(0.01)
            with overlap.guard:
                overlap.running -= 1
            return subprocess.CompletedProcess(argv, 0)

        monkeypatch.setattr(sas.subprocess, "run", run)
        monkeypatch.setattr(sas, "HAS_SAS", True)

        threads = [
            threading.Thread(target=lambda: sas.run("evselect", produces=[], table="e"))
            for _ in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert overlap.most_at_once == 1

    def test_the_lock_is_re_entrant(self, monkeypatch):
        """A task invoked from inside another lock-holding call must not deadlock."""
        a_sas_that_records_its_calls(monkeypatch)

        with sas.SAS_LOCK:
            sas.run("ecoordconv", produces=[], imagefile="x")


class TestTheTaskOutputGoesToAFile:
    def test_the_first_call_of_a_run_starts_the_log(self, monkeypatch, tmp_path):
        log = tmp_path / "logs" / "evselect.log"
        log.parent.mkdir()
        log.write_text("output of an earlier run\n")
        monkeypatch.setattr(sas, "_LOG_STARTED", set())
        calls = a_sas_that_records_its_calls(monkeypatch)

        sas.run("evselect", produces=[], log_to=str(log), table="e.fits")

        assert log.read_text() == ""
        assert calls[0].kwargs["stderr"] is subprocess.STDOUT

    def test_the_next_call_of_the_same_run_appends(self, monkeypatch, tmp_path):
        """``evselect`` runs once per exposure; a file each would only scatter them."""
        log = tmp_path / "logs" / "evselect.log"
        monkeypatch.setattr(sas, "_LOG_STARTED", set())
        a_sas_that_records_its_calls(monkeypatch)

        sas.run("evselect", produces=[], log_to=str(log), table="one.fits")
        with open(log, "a") as fobj:
            fobj.write("first\n")
        sas.run("evselect", produces=[], log_to=str(log), table="two.fits")

        assert log.read_text() == "first\n"


class TestReadingWhatATaskPrinted:
    """
    Some SAS tasks answer on standard output and write no file at all.

    ``ecoordconv`` is the one this package needs: it converts a celestial position to sky
    coordinates and prints the answer. There is no output file to check and none to read,
    so the run has to hand the text back.
    """

    def test_the_output_comes_back_as_text(self, monkeypatch):
        a_sas_that_records_its_calls(monkeypatch, stdout=" X: Y: 27010 26888\n")

        result = sas.run("ecoordconv", produces=[], capture=True, imageset="e.fits")

        assert result.stdout == " X: Y: 27010 26888\n"

    def test_the_pipe_is_asked_for_in_text_mode(self, monkeypatch):
        calls = a_sas_that_records_its_calls(monkeypatch, stdout="")

        sas.run("ecoordconv", produces=[], capture=True, imageset="e.fits")

        assert calls[0].kwargs["stdout"] is subprocess.PIPE
        assert calls[0].kwargs["text"] is True

    def test_a_captured_task_still_leaves_a_paper_trail(self, monkeypatch, tmp_path):
        """Reading a task's answer should not cost the log every other task writes."""
        log = tmp_path / "logs" / "ecoordconv.log"
        monkeypatch.setattr(sas, "_LOG_STARTED", set())
        a_sas_that_records_its_calls(monkeypatch, stdout=" X: Y: 27010 26888\n")

        sas.run("ecoordconv", produces=[], capture=True, log_to=str(log), imageset="e.fits")

        assert log.read_text() == " X: Y: 27010 26888\n"

    def test_without_capture_nothing_is_piped(self, monkeypatch):
        calls = a_sas_that_records_its_calls(monkeypatch)

        sas.run("evselect", produces=[], table="e.fits")

        assert calls[0].kwargs["stdout"] is None
        assert calls[0].kwargs["text"] is False


def test_produces_is_a_required_argument():
    """A caller who has to write the output down cannot forget that success proves
    nothing. The same guard ``heasoft.run`` has."""
    parameters = inspect.signature(sas.run).parameters

    assert "produces" in parameters, "sas.run lost its produces argument"
    assert parameters["produces"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["produces"].default is inspect.Parameter.empty


MODULES = sorted(
    p
    for p in pathlib.Path(sas.__file__).parent.glob("*.py")
    if p.name not in ("__init__.py", "_version.py")
)


def imported_modules(source):
    """
    Top-level names every ``import`` in the source brings in.

    Read from the syntax tree rather than by searching the text, so that prose about an
    import -- of which there is a good deal in ``sas.py`` -- is not mistaken for one.
    """
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.add(node.module.split(".")[0])
    return names


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_no_module_imports_pysas(path):
    """
    ESA's pysas is not a dependency of this package, not even an optional one.

    It was imported once, as the probe for "is there a SAS installation here". It is a
    poor probe -- see :class:`TestWhetherSasIsThere` -- and every other reason to reach
    for it is answered in the module docstring of :mod:`heasarc_retrieve_pipeline.sas`.
    An import that is used for nothing can still fail, and this one did.
    """
    assert "pysas" not in imported_modules(path.read_text()), f"{path.name} imports pysas"


def sas_task_calls(source):
    """Calls of the form ``subprocess.run(...)``, which bypass the lock and the check."""
    tree = ast.parse(source)
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id == "subprocess" and func.attr == "run":
                offenders.append(func.attr)
    return offenders


def test_the_xmm_module_runs_no_task_of_its_own():
    """
    Every SAS task goes through :func:`sas.run`, so that every one of them is locked,
    logged, and checked for the file it promised.

    ``nicer.py`` calls ``subprocess.run`` directly and is the exception this guard does
    not cover: ``nicerl2`` is a HEASOFT tool, and it takes the HEASOFT lock by hand.
    """
    xmm = pathlib.Path(sas.__file__).parent / "xmm.py"
    if not xmm.exists():
        pytest.skip("xmm.py is not written yet")

    assert sas_task_calls(xmm.read_text()) == []
