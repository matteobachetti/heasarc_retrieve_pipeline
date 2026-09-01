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

    ``heasoftpy`` defaults to ``allow_failure=True``: a tool that exits non-zero comes back
    as an ordinary result object with a non-zero ``returncode``, and a caller that does not
    look carries on with a file that was never written. That is not hypothetical -- a real
    run had ``fappend`` fail quietly, and the merged event file travelled several steps
    downstream before anything noticed it had no GTI extension.

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


def run(name, *args, **kwargs):
    """
    Run one HEASOFT tool through ``heasoftpy``, one at a time in this process.

    Parameters
    ----------
    name : str
        Tool name, as ``heasoftpy`` exposes it -- ``"ftmerge"``, ``"barycorr"``.
    *args, **kwargs
        Passed to the tool unchanged.

    Returns
    -------
    heasoftpy.HSPResult
        Whatever the tool returned.

    Raises
    ------
    ImportError
        If ``heasoftpy`` is not installed.
    RuntimeError
        If the tool exits with a non-zero return code.
    """
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy not installed")
    with HEASOFT_LOCK:
        _hold_on_to_private_pfiles()
        return _checked(name, getattr(hsp, name)(*args, **kwargs))


def run_task(name, **params):
    """
    Run a HEASOFT tool through ``heasoftpy``'s ``HSPTask``, one at a time in this process.

    ``HSPTask`` reads the parameter file when it is built, so the construction is inside
    the lock too, not only the call.

    Parameters
    ----------
    name : str
        Tool name, for example ``"nupipeline"``.
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
        If the tool exits with a non-zero return code.
    """
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy not installed")
    with HEASOFT_LOCK:
        _hold_on_to_private_pfiles()
        return _checked(name, hsp.HSPTask(name)(**params))
