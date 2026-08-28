"""
Small helpers shared across the package.
"""

import logging
import os

import numpy as np
from prefect import get_run_logger

__all__ = ["get_logger", "splitext_improved"]


def get_logger():
    """
    Prefect's run logger inside a flow or task run, a plain one outside.

    ``prefect.get_run_logger`` raises when there is no active run, which makes any task
    that logs impossible to call through ``.fn`` from a test. Falling back to a standard
    library logger keeps the tasks unit-testable offline.

    Returns
    -------
    logging.Logger or logging.LoggerAdapter
        A logger that is safe to use in either context.
    """
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger("heasarc_retrieve_pipeline")


def splitext_improved(path):
    """
    Split a path into root and extension, keeping compression suffixes attached.

    ``os.path.splitext`` treats ``a.evt.gz`` as ``("a.evt", ".gz")``, which is the wrong
    split for the archive's file names: almost every FITS file in a HEASARC observation is
    gzipped, and the useful root is ``a``. This version recognises ``.gz``, ``.Z``, ``.zip``
    and ``.bz2`` and folds them into the extension.

    Parameters
    ----------
    path : str
        File path, with or without directories.

    Returns
    -------
    root : str
        The path with its extension removed, directories preserved.
    ext : str
        The extension, including the compression suffix if there was one.

    Examples
    --------
    >>> assert np.all(splitext_improved("a.tar.gz") ==  ('a', '.tar.gz'))
    >>> assert np.all(splitext_improved("a.tar") ==  ('a', '.tar'))
    >>> path_with_dirs = os.path.join("a.f", "a.tar")
    >>> path_without_ext = os.path.join("a.f", "a")
    >>> assert np.all(splitext_improved(path_with_dirs) ==  (path_without_ext, '.tar'))
    >>> path_with_dirs = os.path.join("a.a.a.f", "a.tar.gz")
    >>> path_without_ext = os.path.join("a.a.a.f", "a")
    >>> assert np.all(splitext_improved(path_with_dirs) ==  (path_without_ext, '.tar.gz'))
    >>> path_with_dirs = os.path.join("a.a.a.f", "a.1.tar.gz")
    >>> path_without_ext = os.path.join("a.a.a.f", "a.1")
    >>> assert np.all(splitext_improved(path_with_dirs) ==  (path_without_ext, '.tar.gz'))
    """

    dir, file = os.path.split(path)
    ENDS_WITH_GZ = False
    gz_ext = None
    for ext in [".gz", ".Z", ".zip", ".bz2"]:
        if file.endswith(ext):
            ENDS_WITH_GZ = True
            gz_ext = ext
            file = file[: -len(ext)]
            break

    froot, ext = os.path.splitext(file)

    if ENDS_WITH_GZ:
        ext += gz_ext
    return os.path.join(dir, froot), ext
