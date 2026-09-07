"""
The one place where SAS tasks are invoked.

SAS is ESA's Science Analysis System, the reduction software for XMM-Newton. It plays the
part HEASOFT plays for the other missions in this package, and this module plays the part
:mod:`heasarc_retrieve_pipeline.heasoft` plays for HEASOFT: one entry point, a lock around
it, output checked before it returns.

Why this duplicates ``heasoft.py`` instead of sharing with it
-------------------------------------------------------------

The two look alike and are not the same call. ``heasoft.run`` goes through ``heasoftpy``,
which imports the tool as a Python object, reads and rewrites a parameter file around every
call, and returns an ``HSPResult``. ``sas.run`` builds an argument vector and hands it to
:func:`subprocess.run`. Everything interesting -- what has to be locked, what can fail, what
a return code means -- differs. Factoring the eighty shared lines into a common base would
tie the two together at exactly the place where they are most likely to drift apart, and
each of the copied helpers here carries a pointer to its twin so that a change to one is
easy to find in the other.

Why SAS is not a dependency
---------------------------

It cannot be. The ``pysas`` on PyPI is an unrelated speech-analysis package; ESA's pySAS
ships inside a SAS installation, at ``$SAS_DIR/lib/python/pysas``, and reaches Python
through ``setsas.sh``. There is no conda package either. So SAS is an *environment*
requirement, exactly like ``HEADAS``, and there is no continuous-integration job anywhere
that can run a real SAS task. Everything in this module is written so that the parts that
are ours -- the argument vector, the return code, the output check, the environment -- are
testable without it.

Why not pySAS's own task runner
-------------------------------

``pysas.sastask.MyTask.run()`` cannot report failure and cannot be trusted with an
expression. It runs the task with ``shell=True``, logs ``critical`` when the task exits
non-zero, returns ``None``, and never exposes ``process.returncode`` -- so a caller has no
way to tell a finished reduction from a broken one. Going through a shell also re-quotes
every value, and an ``evselect`` expression is made of exactly the characters a shell has
opinions about::

    #XMMEA_EP && (PATTERN<=4) && (PI in [200:12000]) && FLAG==0

An argument vector has no such problem: the list goes to the task, one element per
argument, and nothing in between reinterprets it. ``pysas`` is therefore never imported
here -- see :func:`has_sas` for why it is not even used as a probe.
"""

import os
import shutil
import subprocess
import threading

from .utils import get_logger


def has_sas():
    """
    Whether a real SAS is available to call.

    Two questions, and each of them has been the answer on somebody's machine.
    ``SAS_DIR`` is what ``setsas.sh`` sets and what the tasks read; a task on ``PATH`` is
    the only proof that the initialisation actually reached *this* process, and a stale
    ``SAS_DIR`` left over from another shell is a real failure mode.

    A third question used to be asked -- whether ``import pysas`` succeeds -- and it was
    the wrong one. ESA's pysas ships inside a SAS installation, so importing it does
    prove one is there; but failing to import it proves nothing, because it pulls in
    third-party packages this pipeline never touches. On the first machine this was tried
    on, a complete SAS 22.1.0 with every task on ``PATH`` was reported as "no SAS"
    because ``beautifultable`` was missing from the environment. Since :func:`run`
    reaches the tasks through :func:`subprocess.run`, pysas is not on the path between
    this package and a reduction, and it has no say in whether one can happen.

    Returns
    -------
    bool
    """
    return bool(os.environ.get("SAS_DIR")) and shutil.which("evselect") is not None


#: Whether SAS can be called. Resolved once, at import, as ``heasoft.HAS_HEASOFT`` is.
HAS_SAS = has_sas()

#: Held while any SAS task runs in this process. Re-entrant, so a task invoked from inside
#: another lock-holding call cannot deadlock. The twin of ``heasoft.HEASOFT_LOCK``, for a
#: weaker reason: SAS has no ``PFILES`` to corrupt, but its tasks write scratch files into
#: the working directory and they are minutes-long subprocesses, so nothing is gained by
#: overlapping two of them within one observation and a readable log is lost.
SAS_LOCK = threading.RLock()

#: Log files this process has already started. The first call of a run truncates and the
#: rest append, so that the dozen ``evselect`` calls of one observation read in the order
#: they ran without a rerun inheriting the last run's output. Twin of ``heasoft._LOG_STARTED``.
_LOG_STARTED = set()


class IN_PLACE:
    """
    Marker for an output a task edits rather than creates.

    ``barycen`` rewrites the event list it is given, so "the file exists and is not empty"
    is still the right check -- but the file existed before the call too, and saying so at
    the call site keeps the intent readable. Twin of ``heasoft.IN_PLACE``.

    Parameters
    ----------
    path : str
        The file the task edits.

    Examples
    --------
    >>> IN_PLACE("/tmp/events.ds").path
    '/tmp/events.ds'
    """

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return f"IN_PLACE({self.path!r})"


def _argument(key, value):
    """
    One ``keyword=value`` argument, in the spelling SAS expects.

    Only booleans need translating: SAS writes them ``yes`` and ``no``, and Python's
    ``True`` would arrive as the string ``True``, which a task reads as an error or, worse,
    as false. Everything else is its own plain text, and in particular an expression is
    passed through untouched -- see the module docstring.

    ``heasoft.run`` needs no equivalent, because ``heasoftpy`` converts the values itself.

    Examples
    --------
    >>> _argument("withfilteredset", True)
    'withfilteredset=yes'
    >>> _argument("timebinsize", 10.0)
    'timebinsize=10.0'
    >>> _argument("expression", "#XMMEA_EP && (PATTERN<=4)")
    'expression=#XMMEA_EP && (PATTERN<=4)'
    """
    if isinstance(value, bool):
        value = "yes" if value else "no"
    return f"{key}={value}"


def _outputs_to_check(produces):
    """
    Normalise ``produces`` to a list of paths.

    ``heasoft._outputs_to_check`` also strips a leading ``!``, which is how a HEASOFT tool
    is told to overwrite. SAS has no such convention -- it takes ``clobber=yes`` as an
    ordinary parameter -- so a name here is only ever a name.

    Examples
    --------
    >>> _outputs_to_check("a.fits")
    ['a.fits']
    >>> _outputs_to_check(["a.fits", IN_PLACE("b.ds")])
    ['a.fits', 'b.ds']
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
    returned 0, wrote nothing, and the failure surfaced one step later as a message about
    a different tool. Checking here names the task that actually failed. Twin of
    ``heasoft._check_outputs``.

    Parameters
    ----------
    name : str
        Task name, for the message.
    produces : str or IN_PLACE or list
        What the call was supposed to leave behind: a file that must exist and be
        non-empty, a directory that must exist and hold at least one entry, or an
        :class:`IN_PLACE` file the task only edited. An empty list checks nothing, which is
        what a test double wants and what a task with no file output gets --
        ``ecoordconv`` answers on standard output.

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

    Twin of ``heasoft._log_file``, doing by hand what ``heasoftpy`` does through its
    ``logfile`` parameter. SAS tasks are chatty and are called repeatedly -- ``evselect``
    runs once per exposure -- so one file per task per observation is what makes a failed
    reduction legible, and a file each would only scatter it.

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


def sas_environment(ccf=None, odf=None, ccfpath=None, verbosity=None):
    """
    A copy of this process's environment with the SAS variables one observation needs.

    The variables are returned rather than set, and this is the point of the function.
    ``SAS_CCF`` and ``SAS_ODF`` name *one observation's* calibration index and raw data,
    while a worker process reduces several observations one after another
    (:func:`heasarc_retrieve_pipeline.core.prepare_worker`). Setting them in
    ``os.environ`` would leave the second observation quietly reading the first one's
    calibration -- the ``PFILES`` lesson, applied before it bites rather than after.

    What is not named here is inherited unchanged, which is how ``SAS_DIR``, ``SAS_PATH``
    and a machine-wide ``SAS_CCFPATH`` set by ``setsas.sh`` reach the task.

    This takes paths rather than an OBSID and a config, so that the SAS layer stays
    mission-neutral in the way the HEASOFT layer is: building XMM's paths is
    :mod:`heasarc_retrieve_pipeline.xmm`'s business.

    Parameters
    ----------
    ccf : str, optional
        Calibration index file for this observation -- ``SAS_CCF``. On the PPS route this
        is the ``CALIND`` product, which is the index the SOC itself used.
    odf : str, optional
        Directory holding the observation's ODF -- ``SAS_ODF``.
    ccfpath : str, optional
        Where the calibration constituents live -- ``SAS_CCFPATH``. Normally set once for
        the whole machine, so leaving it out keeps whatever the user set.
    verbosity : int, optional
        ``SAS_VERBOSITY``, 0 to 10.

    Returns
    -------
    dict
        A new environment mapping. ``os.environ`` is not touched.

    Examples
    --------
    >>> env = sas_environment(ccf="/data/0153950401/pps/ccf.cif")
    >>> env["SAS_CCF"]
    '/data/0153950401/pps/ccf.cif'
    """
    environment = dict(os.environ)
    for variable, value in (
        ("SAS_CCF", ccf),
        ("SAS_ODF", odf),
        ("SAS_CCFPATH", ccfpath),
        ("SAS_VERBOSITY", verbosity),
    ):
        if value is not None:
            environment[variable] = str(value)
    return environment


def run(name, *, produces, log_to=None, env=None, **params):
    """
    Run one SAS task, one at a time in this process.

    Parameters
    ----------
    name : str
        Task name, as SAS installs it -- ``"evselect"``, ``"especget"``, ``"barycen"``.
    produces : str or IN_PLACE or list, keyword-only, required
        What the call must leave behind -- see :func:`_check_outputs`. Mandatory on
        purpose: a caller who has to write the output down cannot forget that a zero
        return code proves nothing. Pass ``[]`` for a task that writes no file.
    log_to : str, optional
        Send the task's output to this file instead of the screen -- see
        :func:`_log_stream`. Standard error is merged into it, because SAS writes its
        warnings there and they belong beside the lines they refer to.
    env : dict, optional
        Environment for the task, normally from :func:`sas_environment`. ``None``, the
        default, inherits this process's own.
    **params
        Task parameters, passed as ``keyword=value`` -- see :func:`_argument`.

    Returns
    -------
    subprocess.CompletedProcess

    Raises
    ------
    ImportError
        If no SAS installation was found.
    RuntimeError
        If the task is not on ``PATH``, exits with a non-zero return code, or does not
        produce what it said it would.
    """
    if not HAS_SAS:
        raise ImportError(
            "No SAS installation found. SAS cannot be installed with pip or conda; "
            "initialise one with `. $SAS_DIR/setsas.sh` -- see the module docstring of "
            "heasarc_retrieve_pipeline.sas."
        )

    argv = [name] + [_argument(key, value) for key, value in params.items()]
    get_logger().info(f"Running {' '.join(argv)}")

    stream = _log_stream(name, log_to) if log_to is not None else None
    try:
        with SAS_LOCK:
            try:
                result = subprocess.run(
                    argv,
                    env=env,
                    stdout=stream,
                    stderr=subprocess.STDOUT if stream is not None else None,
                    check=False,
                )
            except FileNotFoundError as error:
                raise RuntimeError(
                    f"{name} is not on PATH. Has SAS been initialised in this process?"
                ) from error
    finally:
        if stream is not None:
            stream.close()

    if result.returncode != 0:
        where = f" See {os.path.abspath(log_to)}." if log_to is not None else ""
        raise RuntimeError(f"{name} failed with return code {result.returncode}.{where}")

    _check_outputs(name, produces)
    return result
