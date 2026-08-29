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

import threading

try:
    import heasoftpy as hsp

    HAS_HEASOFT = True
except ImportError:
    hsp = None
    HAS_HEASOFT = False

#: Held while any HEASOFT tool runs in this process. Re-entrant, so a tool that is invoked
#: from inside another lock-holding call cannot deadlock.
HEASOFT_LOCK = threading.RLock()


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
        return _checked(name, hsp.HSPTask(name)(**params))
