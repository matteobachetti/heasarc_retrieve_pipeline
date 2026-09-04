"""
The one place where HEASOFT tools are invoked.

Every HEASOFT call in the package goes through here, and they are serialised within a
process by a lock. This is not caution: it is measured. ``heasoftpy`` reads and rewrites
``<PFILES>/<tool>.par`` around each call, so two calls at once in the same process delete
the parameter file under each other. Running ``ftlist`` 200 times, eight at a time from
threads of one process, 19 of them failed with ``parameter file .../ftlist.par not found``.

``PFILES`` is an environment variable, so it cannot be made per-thread: within a process
the only fix is to let one tool run at a time. Across processes the fix is a private
``PFILES`` directory each, which :func:`heasarc_retrieve_pipeline.core.prepare_worker`
sets up. Parallelism therefore lives between observations, not inside one.

The lock costs nothing real: a HEASOFT tool is an external subprocess doing seconds to
minutes of work, and two of them in one observation have nothing to gain from overlapping.

Every call here also has to say what it produces, and the file is checked before the call
returns. A zero return code is not evidence that anything was written: ``ftmgtime`` handed
an empty list of input GTIs exits 0, writes nothing, and lets the next tool take the blame.
"""

import os
import threading

from .utils import get_logger

try:
    import heasoftpy as hsp

    HAS_HEASOFT = True
except ImportError:
    hsp = None
    HAS_HEASOFT = False


def _hsp_failure():
    """
    What ``heasoftpy`` raises for a tool that exited non-zero, if anything.

    Whether it raises at all is :func:`_calling`'s business; this is only about being able
    to recognise it when it does. A ``heasoftpy`` that renames the exception would leave
    nothing to catch, so say so once rather than let the first failed tool discover it.

    Returns
    -------
    type or tuple
        The exception class, or an empty tuple -- which an ``except`` clause accepts and
        never matches -- when there is nothing to catch.
    """
    if not HAS_HEASOFT:
        return ()
    failure = getattr(hsp, "HSPTaskException", None)
    if failure is None:
        get_logger().warning(
            "heasoftpy is installed but has no HSPTaskException. If it reports a failed "
            "tool by raising, that exception will now reach callers unchanged instead of "
            "as a RuntimeError naming the tool. heasarc_retrieve_pipeline.heasoft needs "
            "to learn the new name."
        )
        return ()
    return failure


#: Caught in :func:`_calling` and translated. Resolved once, at import.
HSP_FAILURE = _hsp_failure()

#: Held while any HEASOFT tool runs in this process. Re-entrant, so a tool that is invoked
#: from inside another lock-holding call cannot deadlock.
HEASOFT_LOCK = threading.RLock()


#: The ``PFILES`` value this process claimed through :func:`use_private_pfiles`. ``None``
#: means nobody claimed one -- a plain script, or a test -- and the environment is then
#: left exactly as the user set it.
_EXPECTED_PFILES = None

#: Whether the repair below has already been reported. Once is enough for the log.
_PFILES_REPAIRED = False


def use_private_pfiles(directory):
    """
    Point ``PFILES`` at a directory of this process's own, and remember the value.

    The first entry is where parameters are written; after the ``;`` is the read-only
    system copy the tools fall back on.

    Parameters
    ----------
    directory : str
        This process's private parameter directory. Must already exist.

    Returns
    -------
    str
        The ``PFILES`` value that was set.

    Raises
    ------
    KeyError
        If ``HEADAS`` is not in the environment, so there is no system copy to fall back
        on and no HEASOFT to run anyway.
    """
    global _EXPECTED_PFILES
    system = os.path.join(os.environ["HEADAS"], "syspfiles")
    _EXPECTED_PFILES = f"{directory};{system}"
    os.environ["PFILES"] = _EXPECTED_PFILES
    return _EXPECTED_PFILES


def _hold_on_to_private_pfiles():
    """
    Put ``PFILES`` back to this process's own directory if something has changed it.

    Private parameter directories are what stop several worker processes deleting one
    another's ``<tool>.par``, and they only work while ``PFILES`` still names them. In the
    2026 reprocessing of 56 NuSTAR observations of M82 they did not hold: of 1016
    ``fthedit`` calls a handful resolved their parameter file to the shared
    ``$HOME/pfiles`` instead, under all four worker PIDs rather than one bad process, and
    seven observations were lost to a parameter file being deleted mid-read.

    What puts it back has not been pinned down. ``$HEADAS/BUILD_DIR/headas-setup`` forces
    ``$HOME/pfiles`` to the front of ``PFILES`` whenever HEASOFT is initialised, and
    ``heasoftpy`` splits ``PFILES`` on ``:`` as well as ``;``, so anything that
    re-initialises HEASOFT in this process would do it.

    Rather than guess, repair it where the damage shows. ``heasoftpy`` re-reads
    ``os.environ["PFILES"]`` on every call, in ``HSPTask.find_pfile``, so restoring it
    immediately before the call is enough. The cost is a string comparison against a
    subprocess that runs for seconds; the first repair is logged, with the value that was
    found, so the next run says what did it.

    Returns
    -------
    bool
        True if ``PFILES`` had to be put back.
    """
    global _PFILES_REPAIRED
    if _EXPECTED_PFILES is None:
        return False
    found = os.environ.get("PFILES")
    if found == _EXPECTED_PFILES:
        return False

    if not _PFILES_REPAIRED:
        _PFILES_REPAIRED = True
        get_logger().warning(
            f"PFILES had been changed to {found!r}; putting it back to "
            f"{_EXPECTED_PFILES!r}. Sharing a parameter directory between processes "
            "deletes parameter files mid-read. This message appears once per process."
        )
    os.environ["PFILES"] = _EXPECTED_PFILES
    return True


def _checked(name, result):
    """
    Return ``result``, or raise if the tool reported failure.

    :func:`_calling` asks ``heasoftpy`` to return rather than raise: a tool that exits
    non-zero comes back as an ordinary result object with a non-zero ``returncode``, and a
    caller that does not look carries on with a file that was never written. That is not
    hypothetical -- a real run had ``fappend`` fail quietly, and the merged event file
    travelled several steps downstream before anything noticed it had no GTI extension.

    Raises
    ------
    RuntimeError
        If the tool returned a non-zero return code.
    """
    returncode = getattr(result, "returncode", 0)
    if returncode:
        output = getattr(result, "stdout", "") or ""
        stderr = getattr(result, "stderr", "") or ""
        raise RuntimeError(
            f"{name} failed with return code {returncode}:\n{output}\n{stderr}".strip()
        )
    return result


def _calling(name, task, *args, **kwargs):
    """
    Call one ``heasoftpy`` task, reporting a failed tool the way this module promises.

    Two things happen here, and the second is the one that has to hold.

    ``allow_failure`` decides what ``heasoftpy`` does with a non-zero exit: ``True`` hands
    back an ordinary result carrying the return code, ``False`` raises
    ``HSPTaskException``. The unset default is ``True`` in ``heasoftpy`` 1.5 -- with a
    deprecation warning on every call announcing that it will become ``False`` -- and is
    already ``False`` in the build on the HEASARC conda channel. Neither this module nor
    its tests should depend on which one is installed, so it is asked for explicitly, on
    every call. Not through ``heasoftpy.Config``, which is process-wide: that would also
    stop anyone else sharing this interpreter from getting the exceptions they expect.

    ``True`` is the one worth asking for. The result object carries the return code, the
    output and the stderr separately, which is what :func:`_checked` turns into a message
    naming the tool; ``HSPTaskException`` carries the same text but never says which task
    raised it.

    Asking is not enough, though. ``heasoftpy`` only forwards the keywords that name real
    tool parameters and discards the rest, so a version that drops or renames this one
    would ignore it in silence and go back to raising. Translating the exception here as
    well means the promise in :func:`run` holds whatever ``heasoftpy`` does next.

    Raises
    ------
    RuntimeError
        If ``heasoftpy`` raised rather than returning a failed result.
    """
    kwargs.setdefault("allow_failure", True)
    try:
        return task(*args, **kwargs)
    except HSP_FAILURE as error:
        raise RuntimeError(f"{name} failed:\n{error}".strip()) from error


class IN_PLACE:
    """
    Marker for an output a tool edits rather than creates.

    ``fthedit`` and ``fappend`` rewrite a file that is already there, so "the file exists
    and is not empty" is still the right check -- but the file existed before the call
    too, and saying so at the call site keeps the intent readable.

    Parameters
    ----------
    path : str
        The file the tool edits.

    Examples
    --------
    >>> IN_PLACE("/tmp/some.gti").path
    '/tmp/some.gti'
    """

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return f"IN_PLACE({self.path!r})"


def _outputs_to_check(produces):
    """
    Normalise ``produces`` to a list of paths, stripping HEASOFT's clobber marker.

    A leading ``!`` means "overwrite this" to a HEASOFT tool; it is not part of the name.

    Examples
    --------
    >>> _outputs_to_check("a.fits")
    ['a.fits']
    >>> _outputs_to_check(["!a.fits", IN_PLACE("b.gti")])
    ['a.fits', 'b.gti']
    >>> _outputs_to_check([])
    []
    """
    items = produces if isinstance(produces, (list, tuple)) else [produces]
    paths = []
    for item in items:
        path = str(item.path if isinstance(item, IN_PLACE) else item)
        paths.append(path[1:] if path.startswith("!") else path)
    return paths


def _check_outputs(name, produces):
    """
    Raise unless every file the tool promised is there and has something in it.

    A zero return code is not evidence that a file was written. Measured on a real run:
    ``ftmgtime`` was handed an empty list of input GTIs, returned 0, wrote nothing at all,
    and the failure only surfaced one step later as ``ftsort failed with return code 33``
    -- a message that says nothing about where the trouble started. Checking here names
    the tool that actually failed.

    Parameters
    ----------
    name : str
        Tool name, for the message.
    produces : str or IN_PLACE or list
        What the call was supposed to leave behind: a file that must exist and be
        non-empty, a directory that must exist and hold at least one entry, or an
        :class:`IN_PLACE` file the tool only edited. An empty list checks nothing, which
        is what a test double wants.

    Raises
    ------
    RuntimeError
        Naming the tool and the path that is missing or empty.
    """
    for path in _outputs_to_check(produces):
        if not os.path.exists(path):
            raise RuntimeError(f"{name} returned success but did not create {path}")
        if os.path.isdir(path):
            if not os.listdir(path):
                raise RuntimeError(f"{name} returned success but left {path} empty")
        elif os.path.getsize(path) == 0:
            raise RuntimeError(f"{name} returned success but {path} is empty")


def run(name, *args, produces, **kwargs):
    """
    Run one HEASOFT tool through ``heasoftpy``, one at a time in this process.

    Parameters
    ----------
    name : str
        Tool name, as ``heasoftpy`` exposes it -- ``"ftmerge"``, ``"barycorr"``.
    *args, **kwargs
        Passed to the tool unchanged, except that ``allow_failure=True`` is added when the
        caller has not asked for something else -- see :func:`_calling`.
    produces : str or IN_PLACE or list, keyword-only, required
        What the call must leave behind -- see :func:`_check_outputs`. Mandatory on
        purpose: every tool in this package has a nameable output, and a caller that has
        to write it down cannot forget that a zero return code proves nothing.

    Returns
    -------
    heasoftpy.HSPResult
        Whatever the tool returned.

    Raises
    ------
    ImportError
        If ``heasoftpy`` is not installed.
    RuntimeError
        If the tool exits with a non-zero return code, or does not produce what it said
        it would.
    """
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy not installed")
    with HEASOFT_LOCK:
        _hold_on_to_private_pfiles()
        result = _checked(name, _calling(name, getattr(hsp, name), *args, **kwargs))
        _check_outputs(name, produces)
        return result


def run_task(name, *, produces, **params):
    """
    Run a HEASOFT tool through ``heasoftpy``'s ``HSPTask``, one at a time in this process.

    ``HSPTask`` reads the parameter file when it is built, so the construction is inside
    the lock too, not only the call.

    Parameters
    ----------
    name : str
        Tool name, for example ``"nupipeline"``.
    produces : str or IN_PLACE or list, keyword-only, required
        What the call must leave behind -- see :func:`_check_outputs`.
    **params
        Tool parameters.

    Returns
    -------
    heasoftpy.HSPResult
        Whatever the tool returned.

    Raises
    ------
    ImportError
        If ``heasoftpy`` is not installed.
    RuntimeError
        If the tool exits with a non-zero return code, or does not produce what it said
        it would.
    """
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy not installed")
    with HEASOFT_LOCK:
        _hold_on_to_private_pfiles()
        result = _checked(name, _calling(name, hsp.HSPTask(name), **params))
        _check_outputs(name, produces)
        return result
