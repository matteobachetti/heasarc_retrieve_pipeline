"""
Offline tests for the CIAO task runner.

No CIAO is needed: ``subprocess.run`` is monkeypatched, so what is tested is the part
that is ours -- the argument vector, the return code, the output check and the
environment. Unlike SAS, CIAO *can* be installed from conda, so a real-tool job would be
possible; Matteo ruled on 2026-09-12 that continuous integration stays offline and
stubbed anyway, and real-tool tests live behind a ``ciao`` marker.
"""

import inspect
import os
import subprocess
from types import SimpleNamespace

import pytest

from heasarc_retrieve_pipeline import ciao


@pytest.fixture
def stub_ciao(monkeypatch):
    """A CIAO that is present, and a ``subprocess.run`` that records what it was given."""
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(SimpleNamespace(argv=argv, **kwargs))
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(ciao, "HAS_CIAO", True)
    monkeypatch.setattr(ciao.subprocess, "run", fake_run)
    return calls


class TestTheArgumentVector:
    def test_the_task_comes_first_and_parameters_follow_as_key_equals_value(
        self, stub_ciao, tmp_path
    ):
        out = tmp_path / "clean.fits"
        out.write_text("x")

        ciao.run("dmcopy", produces=str(out), infile="evt2.fits", outfile=str(out))

        assert stub_ciao[0].argv == ["dmcopy", "infile=evt2.fits", f"outfile={out}"]

    def test_a_filter_expression_reaches_the_task_exactly_as_written(self, stub_ciao, tmp_path):
        """
        The reason tasks are run with an argument vector and never through a shell. A
        Data Model filter is made of the characters a shell has opinions about::

            evt2.fits[EVENTS][sky=circle(4096,4096,20)][energy=500:7000]

        One list element, one argument, and nothing in between reinterprets it.
        """
        out = tmp_path / "f.fits"
        out.write_text("x")
        expression = "evt2.fits[EVENTS][sky=circle(4096,4096,20)][energy=500:7000]"

        ciao.run("dmcopy", produces=str(out), infile=expression, outfile=str(out))

        assert stub_ciao[0].argv[1] == f"infile={expression}"

    @pytest.mark.parametrize(
        "expression",
        [
            "evt.fits[#row=1:100]",  # a hash
            "evt.fits[EVENTS][grade=0,2,3,4,6]",  # brackets and commas
            "img.fits[sky = circle(100, 100, 5)]",  # spaces
            "evt.fits[ccd_id=7][bin sky=1]",  # a space inside a bracket
        ],
    )
    def test_every_awkward_character_survives(self, stub_ciao, tmp_path, expression):
        out = tmp_path / "f.fits"
        out.write_text("x")

        ciao.run("dmcopy", produces=str(out), infile=expression, outfile=str(out))

        assert stub_ciao[0].argv[1] == f"infile={expression}"

    @pytest.mark.parametrize("value, expected", [(True, "yes"), (False, "no")])
    def test_booleans_are_written_the_way_ciao_spells_them(self, value, expected):
        """Python's ``True`` would arrive as the string ``True``, which a task misreads."""
        assert ciao._argument("clobber", value) == f"clobber={expected}"

    def test_numbers_are_their_own_plain_text(self):
        assert ciao._argument("binsize", 0.5) == "binsize=0.5"
        assert ciao._argument("ecf", 90) == "ecf=90"

    def test_nothing_is_run_through_a_shell(self, stub_ciao, tmp_path):
        out = tmp_path / "f.fits"
        out.write_text("x")

        ciao.run("dmlist", produces=[], infile="x.fits", opt="header")

        assert stub_ciao[0].__dict__.get("shell") in (None, False)


class TestWhatTheCallMustLeaveBehind:
    def test_a_missing_output_raises_and_names_the_task(self, stub_ciao, tmp_path):
        """
        A zero return code is not evidence that a file was written. Checking here names
        the task that actually failed, instead of letting the next step complain about a
        file it never made.
        """
        with pytest.raises(RuntimeError, match="dmcopy"):
            ciao.run("dmcopy", produces=str(tmp_path / "never.fits"), infile="x")

    def test_an_empty_output_raises_too(self, stub_ciao, tmp_path):
        empty = tmp_path / "empty.fits"
        empty.write_text("")

        with pytest.raises(RuntimeError, match="empty.fits"):
            ciao.run("dmcopy", produces=str(empty), infile="x")

    def test_an_empty_output_directory_raises(self, stub_ciao, tmp_path):
        directory = tmp_path / "repro"
        directory.mkdir()

        with pytest.raises(RuntimeError, match="repro"):
            ciao.run("chandra_repro", produces=str(directory), indir="x")

    def test_a_directory_with_something_in_it_passes(self, stub_ciao, tmp_path):
        directory = tmp_path / "repro"
        directory.mkdir()
        (directory / "evt2.fits").write_text("x")

        ciao.run("chandra_repro", produces=str(directory), indir="x")

    def test_a_task_that_writes_no_file_checks_nothing(self, stub_ciao):
        """``dmcoords`` answers by parameter file, not by writing an output."""
        ciao.run("dmcoords", produces=[], infile="x", ra=148.9, dec=69.6)

    def test_a_file_the_task_only_edited_is_still_checked(self, stub_ciao, tmp_path):
        edited = tmp_path / "events.fits"
        edited.write_text("x")

        ciao.run("axbary", produces=ciao.IN_PLACE(str(edited)), infile=str(edited))

        with pytest.raises(RuntimeError, match="axbary"):
            ciao.run("axbary", produces=ciao.IN_PLACE(str(tmp_path / "gone.fits")), infile="x")

    def test_a_non_zero_return_code_raises_and_names_the_task(self, monkeypatch, tmp_path):
        monkeypatch.setattr(ciao, "HAS_CIAO", True)
        monkeypatch.setattr(
            ciao.subprocess,
            "run",
            lambda argv, **kw: subprocess.CompletedProcess(argv, 3),
        )

        with pytest.raises(RuntimeError, match="specextract failed with return code 3"):
            ciao.run("specextract", produces=[], infile="x")

    def test_a_task_missing_from_the_path_says_so(self, monkeypatch):
        monkeypatch.setattr(ciao, "HAS_CIAO", True)

        def missing(argv, **kw):
            raise FileNotFoundError(argv[0])

        monkeypatch.setattr(ciao.subprocess, "run", missing)

        with pytest.raises(RuntimeError, match="not on PATH"):
            ciao.run("dmcopy", produces=[], infile="x")

    def test_without_ciao_it_refuses_before_running_anything(self, monkeypatch):
        monkeypatch.setattr(ciao, "HAS_CIAO", False)

        with pytest.raises(ImportError, match="CIAO"):
            ciao.run("dmcopy", produces=[], infile="x")


class TestThePerObservationParameterFiles:
    """
    The one new failure mode Chandra brings, closed from the first CIAO commit.

    CIAO tools are parameter-file driven. ``chandra_repro`` and ``acis_set_ardlib`` write
    an observation's bad-pixel path into ``ardlib.par``, and ``specextract``, ``mkarf``
    and friends read it back. That is process-global state keyed to *one* observation,
    and the pipeline reduces several in sequence inside one pool worker. Two observations
    sharing a ``PFILES`` clobber each other, and the result is a spectrum built with the
    **wrong observation's bad pixels** -- wrong numbers, no error, no warning.
    """

    def test_two_observations_get_two_parameter_directories(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ASCDS_INSTALL", str(tmp_path / "ciao"))
        config = {"out_data_path": str(tmp_path)}

        first = ciao.ciao_environment("6298", config)["PFILES"]
        second = ciao.ciao_environment("17661", config)["PFILES"]

        assert first != second
        assert "6298" in first and "17661" in second

    def test_it_never_touches_this_process_environment(self, tmp_path, monkeypatch):
        """
        The whole point of returning an environment instead of setting one. A worker
        reduces several observations one after another, and a second reduction reading
        the first one's parameters is exactly the bug this prevents.
        """
        monkeypatch.setenv("ASCDS_INSTALL", str(tmp_path / "ciao"))
        monkeypatch.delenv("PFILES", raising=False)
        config = {"out_data_path": str(tmp_path)}

        ciao.ciao_environment("6298", config)

        assert "PFILES" not in os.environ

    def test_the_directory_is_made_and_the_system_copy_is_the_fallback(self, tmp_path, monkeypatch):
        """
        First entry is where a tool writes, after the ``;`` is the read-only system copy
        it falls back on -- the same shape ``heasoft.use_private_pfiles`` settled on.
        """
        install = tmp_path / "ciao"
        (install / "param").mkdir(parents=True)
        monkeypatch.setenv("ASCDS_INSTALL", str(install))
        config = {"out_data_path": str(tmp_path)}

        private, _, system = ciao.ciao_environment("6298", config)["PFILES"].partition(";")

        assert os.path.isdir(private)
        assert system.startswith(str(install / "param"))

    def test_the_contrib_parameters_are_on_the_fallback_when_they_exist(
        self, tmp_path, monkeypatch
    ):
        """``chandra_repro`` is a contrib script and its parameters live apart."""
        install = tmp_path / "ciao"
        (install / "param").mkdir(parents=True)
        (install / "contrib" / "param").mkdir(parents=True)
        monkeypatch.setenv("ASCDS_INSTALL", str(install))

        system = ciao.ciao_environment("6298", {"out_data_path": str(tmp_path)})["PFILES"]

        assert str(install / "contrib" / "param") in system

    def test_the_calibration_database_is_passed_through_when_configured(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("ASCDS_INSTALL", str(tmp_path / "ciao"))
        config = {"out_data_path": str(tmp_path), "caldb": str(tmp_path / "caldb")}

        assert ciao.ciao_environment("6298", config)["CALDB"] == str(tmp_path / "caldb")

    def test_the_installation_s_own_calibration_database_wins_over_the_machine_s(
        self, tmp_path, monkeypatch
    ):
        """
        The hazard this closes is a real one on Matteo's machine, where the shell exports
        ``CALDB=~/azure_software/caldb`` for HEASOFT. A conda CIAO keeps its own
        calibration database at ``$ASCDS_INSTALL/CALDB``, and a CIAO task pointed at
        HEASOFT's finds no Chandra data there at all.
        """
        install = tmp_path / "ciao"
        (install / "CALDB").mkdir(parents=True)
        monkeypatch.setenv("ASCDS_INSTALL", str(install))
        monkeypatch.setenv("CALDB", "/machine/wide/heasoft/caldb")

        environment = ciao.ciao_environment("6298", {"out_data_path": str(tmp_path)})

        assert environment["CALDB"] == str(install / "CALDB")

    def test_a_configured_one_still_wins_over_the_installation_s(self, tmp_path, monkeypatch):
        install = tmp_path / "ciao"
        (install / "CALDB").mkdir(parents=True)
        monkeypatch.setenv("ASCDS_INSTALL", str(install))
        config = {"out_data_path": str(tmp_path), "caldb": "/somewhere/else"}

        assert ciao.ciao_environment("6298", config)["CALDB"] == "/somewhere/else"

    def test_without_one_beside_the_installation_the_machine_s_is_left_alone(
        self, tmp_path, monkeypatch
    ):
        """A source installation keeps its calibration elsewhere, and then the machine
        knows better than we do."""
        monkeypatch.setenv("ASCDS_INSTALL", str(tmp_path / "ciao"))
        monkeypatch.setenv("CALDB", "/machine/wide/caldb")

        environment = ciao.ciao_environment("6298", {"out_data_path": str(tmp_path)})

        assert environment["CALDB"] == "/machine/wide/caldb"

    def test_the_working_path_is_the_observation_s_own(self, tmp_path, monkeypatch):
        """``ASCDS_WORK_PATH`` is where tools put scratch files, and two observations
        sharing one is the same hazard in a second place."""
        monkeypatch.setenv("ASCDS_INSTALL", str(tmp_path / "ciao"))

        environment = ciao.ciao_environment("6298", {"out_data_path": str(tmp_path)})

        assert "6298" in environment["ASCDS_WORK_PATH"]
        assert os.path.isdir(environment["ASCDS_WORK_PATH"])

    def test_without_an_installation_it_says_so(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ASCDS_INSTALL", raising=False)

        with pytest.raises(KeyError, match="ASCDS_INSTALL"):
            ciao.ciao_environment("6298", {"out_data_path": str(tmp_path)})


class TestTheProbe:
    def test_it_wants_both_the_variable_and_a_task_on_the_path(self, monkeypatch):
        """
        The shape ``has_sas`` settled on after ``import pysas`` proved the wrong probe.
        The variable says an installation exists; a task on ``PATH`` is the only proof
        the initialisation reached *this* process.
        """
        monkeypatch.setenv("ASCDS_INSTALL", "/opt/ciao")
        monkeypatch.setattr(ciao.shutil, "which", lambda name: None)

        assert ciao.has_ciao() is False

        monkeypatch.setattr(ciao.shutil, "which", lambda name: "/opt/ciao/bin/dmlist")

        assert ciao.has_ciao() is True

    def test_without_the_variable_it_is_false(self, monkeypatch):
        monkeypatch.delenv("ASCDS_INSTALL", raising=False)
        monkeypatch.setattr(ciao.shutil, "which", lambda name: "/opt/ciao/bin/dmlist")

        assert ciao.has_ciao() is False

    def test_it_probes_dmlist_which_every_installation_has(self, monkeypatch):
        asked = []
        monkeypatch.setenv("ASCDS_INSTALL", "/opt/ciao")
        monkeypatch.setattr(ciao.shutil, "which", lambda name: asked.append(name) or "x")

        ciao.has_ciao()

        assert asked == ["dmlist"]


class TestReadingWhatATaskSaid:
    """
    Not in the step-5 plan, which gave ``run`` only ``produces``, ``log_to`` and ``env``.
    ``capture`` and ``cwd`` are carried over from ``sas.run`` so that the two runners are
    the same shape, and they are here because Chandra needs both for the same reasons XMM
    did: ``dmlist`` answers on standard output and nowhere else, and ``specextract`` writes
    the file names it was handed into ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE``, where a
    header card holds 80 characters.
    """

    def _a_talkative_task(self, monkeypatch, said):
        monkeypatch.setattr(ciao, "HAS_CIAO", True)
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(SimpleNamespace(argv=argv, kwargs=kwargs))
            return subprocess.CompletedProcess(argv, 0, stdout=said, stderr=None)

        monkeypatch.setattr(ciao.subprocess, "run", fake_run)
        return calls

    def test_capture_returns_the_output_as_text(self, monkeypatch):
        self._a_talkative_task(monkeypatch, "physical(4096.5, 4096.5)\n")

        result = ciao.run("dmcoords", produces=[], capture=True, infile="x", ra=1.0, dec=2.0)

        assert result.stdout == "physical(4096.5, 4096.5)\n"

    def test_without_capture_nothing_is_piped(self, monkeypatch):
        calls = self._a_talkative_task(monkeypatch, None)

        ciao.run("dmcopy", produces=[], infile="x", outfile="y")

        assert calls[0].kwargs["stdout"] is None
        assert calls[0].kwargs["text"] is False

    def test_captured_output_still_reaches_the_log(self, monkeypatch, tmp_path):
        """A task whose result is read keeps the same paper trail as one whose result is a
        file."""
        self._a_talkative_task(monkeypatch, "4096.5 4096.5\n")
        log = tmp_path / "dmcoords.log"

        ciao.run("dmcoords", produces=[], log_to=str(log), capture=True, infile="x")

        assert log.read_text() == "4096.5 4096.5\n"

    def test_the_working_directory_is_passed_through(self, monkeypatch, tmp_path):
        calls = self._a_talkative_task(monkeypatch, None)

        ciao.run("specextract", produces=[], cwd=str(tmp_path), infile="x")

        assert calls[0].kwargs["cwd"] == str(tmp_path)

    def test_outputs_are_still_checked_by_full_path(self, monkeypatch, tmp_path):
        """``cwd`` changes where the task looks, not where we do."""
        self._a_talkative_task(monkeypatch, None)
        made = tmp_path / "src.pi"
        made.write_text("x")

        ciao.run("specextract", produces=str(made), cwd=str(tmp_path), outroot="src")


def test_produces_is_a_required_argument():
    """
    Keep it mandatory. A caller who has to write the output down cannot forget that a
    zero return code proves nothing. The twin of the same guard on ``heasoft.run`` and
    ``sas.run``.
    """
    parameters = inspect.signature(ciao.run).parameters

    assert "produces" in parameters, "ciao.run lost its produces argument"
    assert parameters["produces"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["produces"].default is inspect.Parameter.empty


class TestPositionalArguments:
    """
    ``pget`` is why these exist, and it is not an exotic case: it is how CIAO hands back
    what a task worked out. ``dmcoords`` answers by writing into its own parameter file
    rather than onto standard output, and ``pget dmcoords x y`` is the tool that reads it
    out again -- with the task name and the parameter names as bare words, not as
    ``key=value``. Every other CIAO task in this pipeline is called with keywords alone.
    """

    def test_they_follow_the_task_name_and_come_before_the_keywords(self, stub_ciao):
        ciao.run("pget", args=("dmcoords", "x", "y"), produces=[], mode="h")

        assert stub_ciao[0].argv == ["pget", "dmcoords", "x", "y", "mode=h"]

    def test_they_are_written_as_plain_text(self, stub_ciao):
        ciao.run("pget", args=("dmcoords", 7), produces=[])

        assert stub_ciao[0].argv == ["pget", "dmcoords", "7"]

    def test_a_task_with_none_is_unchanged(self, stub_ciao):
        ciao.run("dmcopy", produces=[], infile="a", outfile="b")

        assert stub_ciao[0].argv == ["dmcopy", "infile=a", "outfile=b"]
