"""
The one place where CIAO tasks are invoked.

CIAO is the Chandra Interactive Analysis of Observations, the Chandra X-ray Center's
reduction software. It plays the part HEASOFT plays for the other missions in this package
and SAS plays for XMM-Newton, and this module plays the part
:mod:`heasarc_retrieve_pipeline.heasoft` and :mod:`heasarc_retrieve_pipeline.sas` play for
those: one entry point, a lock around it, output checked before it returns.

Why this duplicates ``sas.py`` instead of sharing with it
---------------------------------------------------------

It is the third copy of the same eighty lines, and that is deliberate. The three runners
look alike and are not the same call: ``heasoft.run`` goes through ``heasoftpy``, which
imports the tool as a Python object and rewrites a parameter file around every call, while
``sas.run`` and this one build an argument vector and hand it to :func:`subprocess.run`.
Even the two that share a mechanism disagree about everything that matters -- what has to
be locked, what an output name may contain, what a missing tool means, and what the
environment has to carry. Factoring the shared lines into a common base would tie three
modules together at exactly the place where they are most likely to drift apart, so each
helper here instead carries a one-line pointer to its twin, and a change to one is easy to
find in the others.

Why CIAO is not a dependency
----------------------------

Unlike SAS, CIAO *can* be installed from conda. It is still not a dependency of this
package: it is a four-gigabyte installation with a calibration database beside it, it is
reached through ``dmlist`` and friends on ``PATH`` rather than by import, and the
continuous-integration job has no room for it. So CIAO is an *environment* requirement,
exactly like ``HEADAS`` and ``SAS_DIR``, and everything in this module is written so that
the parts that are ours -- the argument vector, the return code, the output check, the
environment -- are testable without it.

The one new hazard: ``ardlib.par``
----------------------------------

CIAO tools are parameter-file driven, and one parameter file is shared state keyed to a
single observation. ``chandra_repro`` and ``acis_set_ardlib`` write *this* observation's
bad-pixel file into ``ardlib.par``; ``specextract``, ``mkarf`` and ``mkacisrmf`` read it
back later without being told where it came from. A worker process in this pipeline
reduces several observations one after another, so two observations sharing one ``PFILES``
would give the second one the first one's bad pixels -- a wrong effective area, silently,
with no warning and a zero return code. :func:`ciao_environment` gives every observation
its own parameter directory, which closes that door from the first commit rather than
after somebody has published the numbers.
"""

import os
import shutil
import subprocess
import threading

from .utils import get_logger


def has_ciao():
    """
    Whether a real CIAO is available to call.

    Two questions, the shape :func:`heasarc_retrieve_pipeline.sas.has_sas` settled on.
    ``ASCDS_INSTALL`` is what ``ciao.sh`` sets and what the tools read; a task on ``PATH``
    is the only proof that the initialisation actually reached *this* process, and a stale
    ``ASCDS_INSTALL`` left over from another shell is a real failure mode.

    ``dmlist`` is the task asked about because every installation has it: it is part of the
    Data Model core, not of one of the optional instrument packages.

    Returns
    -------
    bool
    """
    return bool(os.environ.get("ASCDS_INSTALL")) and shutil.which("dmlist") is not None


#: Whether CIAO can be called. Resolved once, at import, as ``heasoft.HAS_HEASOFT`` is.
HAS_CIAO = has_ciao()

#: Held while any CIAO task runs in this process. Re-entrant, so a task invoked from inside
#: another lock-holding call cannot deadlock. Twin of ``heasoft.HEASOFT_LOCK``, and here for
#: the strong version of its reason: CIAO tasks read and rewrite parameter files, so two of
#: them running at once in one process can genuinely corrupt each other's state, not merely
#: interleave their logs.
CIAO_LOCK = threading.RLock()

#: Log files this process has already started. The first call of a run truncates and the
#: rest append, so that the several ``dmcopy`` calls of one observation read in the order
#: they ran without a rerun inheriting the last run's output. Twin of ``sas._LOG_STARTED``.
_LOG_STARTED = set()


class IN_PLACE:
    """
    Marker for an output a task edits rather than creates.

    ``axbary`` rewrites the event list it is given, so "the file exists and is not empty"
    is still the right check -- but the file existed before the call too, and saying so at
    the call site keeps the intent readable. Twin of ``heasoft.IN_PLACE``.

    Parameters
    ----------
    path : str
        The file the task edits.

    Examples
    --------
    >>> IN_PLACE("/tmp/events.fits").path
    '/tmp/events.fits'
    """

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return f"IN_PLACE({self.path!r})"


def _argument(key, value):
    """
    One ``keyword=value`` argument, in the spelling CIAO expects.

    Only booleans need translating: CIAO writes them ``yes`` and ``no``, and Python's
    ``True`` would arrive as the string ``True``, which a task reads as an error or, worse,
    as false. Everything else is its own plain text, and in particular a Data Model filter
    is passed through untouched -- see :func:`run`.

    Twin of ``sas._argument``; ``heasoft.run`` needs no equivalent, because ``heasoftpy``
    converts the values itself.

    Examples
    --------
    >>> _argument("clobber", True)
    'clobber=yes'
    >>> _argument("binsize", 0.5)
    'binsize=0.5'
    >>> _argument("infile", "evt2.fits[EVENTS][energy=500:7000]")
    'infile=evt2.fits[EVENTS][energy=500:7000]'
    """
    if isinstance(value, bool):
        value = "yes" if value else "no"
    return f"{key}={value}"


def _outputs_to_check(produces):
    """
    Normalise ``produces`` to a list of paths.

    ``heasoft._outputs_to_check`` also strips a leading ``!``, which is how a HEASOFT tool
    is told to overwrite. CIAO has no such convention -- it takes ``clobber=yes`` as an
    ordinary parameter, exactly as SAS does -- so a name here is only ever a name.

    Examples
    --------
    >>> _outputs_to_check("a.fits")
    ['a.fits']
    >>> _outputs_to_check(["a.fits", IN_PLACE("b.fits")])
    ['a.fits', 'b.fits']
    >>> _outputs_to_check([])
    []
    """
    items = produces if isinstance(produces, (list, tuple)) else [produces]
    return [str(item.path if isinstance(item, IN_PLACE) else item) for item in items]


def _check_outputs(name, produces):
    """
    Raise unless every file the task promised is there and has something in it.

    A zero return code is not evidence that a file was written, and the lesson was learnt
    the expensive way on the HEASOFT side: ``ftmgtime`` handed an empty list of GTIs
    returned 0, wrote nothing, and the failure surfaced one step later as a message about a
    different tool. Checking here names the task that actually failed. Twin of
    ``sas._check_outputs``.

    Parameters
    ----------
    name : str
        Task name, for the message.
    produces : str or IN_PLACE or list
        What the call was supposed to leave behind: a file that must exist and be
        non-empty, a directory that must exist and hold at least one entry -- which is what
        ``chandra_repro`` produces -- or an :class:`IN_PLACE` file the task only edited. An
        empty list checks nothing, which is what a test double wants and what a task with
        no file output gets: ``dmcoords`` answers by parameter file and ``dmlist`` on
        standard output.

    Raises
    ------
    RuntimeError
        Naming the task and the path that is missing or empty.
    """
    for path in _outputs_to_check(produces):
        if not os.path.exists(path):
            raise RuntimeError(f"{name} returned success but did not create {path}")
        if os.path.isdir(path):
            if not os.listdir(path):
                raise RuntimeError(f"{name} returned success but left {path} empty")
        elif os.path.getsize(path) == 0:
            raise RuntimeError(f"{name} returned success but {path} is empty")


def _log_stream(name, path):
    """
    Open the file one task's output goes to, truncating it once per run.

    Twin of ``sas._log_stream``, itself doing by hand what ``heasoftpy`` does through its
    ``logfile`` parameter. CIAO tasks are chatty and are called repeatedly -- ``dmcopy``
    runs once per region and per band -- so one file per task per observation is what makes
    a failed reduction legible, and a file each would only scatter it.

    Parameters
    ----------
    name : str
        Task name, for the line that says where its output went.
    path : str
        Where to write, normally
        :func:`~heasarc_retrieve_pipeline.utils.tool_log_file`.

    Returns
    -------
    file object
        Open for appending. The caller closes it.
    """
    # Resolved before the task runs: a worker reduces its observation from a private
    # working directory, and a relative path would follow that instead.
    path = os.path.abspath(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if path not in _LOG_STARTED:
        _LOG_STARTED.add(path)
        open(path, "w").close()
        get_logger().info(f"Output of {name} for this observation goes to {path}")
    return open(path, "a")


def ciao_environment(obsid, config):
    """
    A copy of this process's environment with one observation's private CIAO state.

    The variables are returned rather than set, and that is the point of the function --
    the same reasoning as :func:`heasarc_retrieve_pipeline.sas.sas_environment`, for a
    sharper reason. What is private here is ``PFILES``, the parameter-file search path, and
    the file that makes it matter is ``ardlib.par``: ``chandra_repro`` writes this
    observation's bad-pixel list into it and ``specextract`` reads it back. Two observations
    reduced in the same worker with one ``PFILES`` would give the second the first one's bad
    pixels, with no warning and a zero return code. See the module docstring.

    ``ASCDS_WORK_PATH`` is separated for the same reason one step further down: it is where
    tasks put their scratch files, and two observations sharing one is the same collision
    in a second place.

    Parameters
    ----------
    obsid : str or int
        The observation this environment belongs to. Only used to name directories, so the
        unpadded form is fine.
    config : dict
        The pipeline configuration. ``out_data_path`` says where the private directories
        are made; ``caldb``, if set, becomes ``CALDB``, and if it is not, a calibration
        database beside the installation does.

    Returns
    -------
    dict
        A new environment mapping. ``os.environ`` is not touched.

    Raises
    ------
    KeyError
        If ``ASCDS_INSTALL`` is not set, so there is no system parameter directory to fall
        back on.
    """
    install = os.environ["ASCDS_INSTALL"]
    out_data_path = config.get("out_data_path", "./")

    private = os.path.abspath(os.path.join(out_data_path, ".ciao", str(obsid), "param"))
    work = os.path.abspath(os.path.join(out_data_path, ".ciao", str(obsid), "work"))
    os.makedirs(private, exist_ok=True)
    os.makedirs(work, exist_ok=True)

    # A CIAO tool writes into the first directory and falls back to the read-only system
    # copies after the semicolon -- the shape ``heasoft.use_private_pfiles`` settled on.
    # ``contrib`` is where the parameters of the contributed scripts live, and
    # ``chandra_repro`` is one of those, so it is on the fallback whenever it exists.
    system = [os.path.join(install, "param")]
    contrib = os.path.join(install, "contrib", "param")
    if os.path.isdir(contrib):
        system.append(contrib)

    environment = dict(os.environ)
    environment["PFILES"] = f"{private};{':'.join(system)}"
    environment["ASCDS_WORK_PATH"] = work

    # A conda CIAO keeps its calibration database at $ASCDS_INSTALL/CALDB, and where that
    # exists it *is* this installation's calibration -- so it wins over whatever the
    # machine set. The hazard is not hypothetical: this machine exports CALDB for HEASOFT,
    # and a CIAO task sent there finds no Chandra data at all. A source installation keeps
    # its calibration somewhere else and has no such directory, and then the machine knows
    # better than we do and is left alone. Either way an explicit `caldb` in the config
    # overrides both. Not in the step-5 plan.
    beside_the_installation = os.path.join(install, "CALDB")
    if config.get("caldb") is not None:
        environment["CALDB"] = str(config["caldb"])
    elif os.path.isdir(beside_the_installation):
        environment["CALDB"] = beside_the_installation
        # These two travel with CALDB -- they are what CIAO's activation script sets
        # beside it -- and leaving HEASOFT's behind would point the index back at
        # HEASOFT's tree while CALDB itself pointed here.
        tools = os.path.join(beside_the_installation, "software", "tools")
        environment["CALDBCONFIG"] = os.path.join(tools, "caldb.config")
        environment["CALDBALIAS"] = os.path.join(tools, "alias_config.fits")
    return environment


def run(name, *, produces, args=(), log_to=None, capture=False, env=None, cwd=None, **params):
    """
    Run one CIAO task, one at a time in this process.

    The task is reached with an argument vector and never through a shell, because a Data
    Model filter is made of exactly the characters a shell has opinions about::

        evt2.fits[EVENTS][sky=circle(4096,4096,20)][energy=500:7000]

    A list has no such problem: one element per argument, and nothing in between
    reinterprets it.

    Parameters
    ----------
    name : str
        Task name, as CIAO installs it -- ``"dmcopy"``, ``"specextract"``, ``"axbary"``.
    produces : str or IN_PLACE or list, keyword-only, required
        What the call must leave behind -- see :func:`_check_outputs`. Mandatory on purpose:
        a caller who has to write the output down cannot forget that a zero return code
        proves nothing. Pass ``[]`` for a task that writes no file.
    args : sequence, optional
        Bare words to put after the task name and before the keywords. Almost no CIAO task
        wants any, and the one that does is ``pget``: ``dmcoords`` answers by writing into
        its own parameter file rather than onto standard output, and ``pget dmcoords x y``
        is how the answer is read back -- with the task and the parameter names as
        positional arguments. Not in the step-5 plan, which had keywords alone.
    log_to : str, optional
        Send the task's output to this file instead of the screen -- see
        :func:`_log_stream`. Standard error is merged into it, because CIAO writes its
        warnings there and they belong beside the lines they refer to.
    capture : bool, optional
        Return the task's output as text on the result's ``stdout``, instead of letting it
        go to the screen. ``dmlist`` is why this exists: it writes no output file and
        answers on standard output, so reading it is the only way to have the answer. With
        ``log_to`` as well the output is still written there, so a task whose result is read
        keeps the same paper trail as one whose result is a file.
    env : dict, optional
        Environment for the task, normally from :func:`ciao_environment`. ``None``, the
        default, inherits this process's own -- which for anything touching ``ardlib.par``
        is the wrong thing to do.
    cwd : str, optional
        Directory to run the task in. It matters because ``specextract`` writes the names it
        was *given* into ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE`` so that a fitting
        program can follow them, and a FITS header card holds 80 characters. Handed an
        absolute path a hundred characters long, it writes one. Running the task in the
        directory its outputs belong to lets the caller pass plain file names instead, which
        are shorter, survive the tree being moved, and are what a fitting program looks for
        beside the spectrum. ``produces`` is still checked by full path.
    **params
        Task parameters, passed as ``keyword=value`` -- see :func:`_argument`.

    Returns
    -------
    subprocess.CompletedProcess

    Raises
    ------
    ImportError
        If no CIAO installation was found.
    RuntimeError
        If the task is not on ``PATH``, exits with a non-zero return code, or does not
        produce what it said it would.
    """
    if not HAS_CIAO:
        raise ImportError(
            "No CIAO installation found. CIAO is an environment requirement, not a "
            "dependency of this package; initialise one with `. $ASCDS_INSTALL/bin/ciao.sh`"
            " -- see the module docstring of heasarc_retrieve_pipeline.ciao."
        )

    argv = (
        [name]
        + [str(arg) for arg in args]
        + [_argument(key, value) for key, value in params.items()]
    )
    get_logger().info(f"Running {' '.join(argv)}")

    stream = _log_stream(name, log_to) if log_to is not None and not capture else None
    destination = subprocess.PIPE if capture else stream
    try:
        with CIAO_LOCK:
            try:
                result = subprocess.run(
                    argv,
                    env=env,
                    cwd=cwd,
                    stdout=destination,
                    stderr=subprocess.STDOUT if destination is not None else None,
                    text=capture,
                    check=False,
                )
            except FileNotFoundError as error:
                raise RuntimeError(
                    f"{name} is not on PATH. Has CIAO been initialised in this process?"
                ) from error
    finally:
        if stream is not None:
            stream.close()

    if capture and log_to is not None:
        with _log_stream(name, log_to) as stream:
            stream.write(result.stdout)

    if result.returncode != 0:
        where = f" See {os.path.abspath(log_to)}." if log_to is not None else ""
        raise RuntimeError(f"{name} failed with return code {result.returncode}.{where}")

    _check_outputs(name, produces)
    return result
