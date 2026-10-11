"""
XRISM/Resolve reduction for timing: the archive's cleaned events, merged, then barycentred.

Resolve is a 6x6 microcalorimeter array covering about 3 arcmin, with event times
assigned to 5 microseconds. It has no source-separation step to speak of: everything
inside the array is "the source", as for NICER. Xtend, the CCD camera, is not reduced:
its frames are seconds long, too slow for most pulsars.

The reduction does not rerun ``xapipeline``. The archive's ``resolve/event_cl`` files
are already calibrated, clock-corrected (``CLOCKAPP = T``) and screened, and are what this
module starts from.

See ``docs/technical_details.rst`` for more detail.
"""

import glob
import os

from .utils import get_logger

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")

#: What of an XRISM observation directory the reduction reads, matched on the whole
#: remote path so that the directory anchors hold.
#:
#: * ``auxil/xa<OBSID>.orb``: the orbit file, which ``barycen`` reads.
#: * ``resolve/event_cl/xa<OBSID>rsl_*_cl.evt``: every cleaned Resolve event file. An
#:   observation can have several, one per filter-wheel position: 101002010 (the Crab,
#:   2024-10-03) has a 10.3 ks ``px1000`` file with ``FILTER = OPEN`` and a 16 s ``px0000``
#:   one with ``FILTER = UNDEF``, taken while the wheel moved, which fills a gap between
#:   the first file's good time intervals.
#:
#: Measured on 101002010: 62 MB of the observation's several hundred. The rest is
#: unfiltered events, housekeeping, the 33 MB attitude file and the Xtend data.
DOWNLOAD_RE = (
    r"(?:/auxil/xa[0-9]{9}\.orb(?:\.gz)?$"
    r"|/resolve/event_cl/xa[0-9]{9}rsl_[^/]*_cl\.evt(?:\.gz)?$)"
)


def xrism_download_filter(config):
    """
    What of an XRISM observation directory to download.

    Called through :func:`~heasarc_retrieve_pipeline.core.mission_download_filter`. There
    is one route, so the configuration is not read.

    Parameters
    ----------
    config : dict
        The run's configuration.

    Returns
    -------
    dict
        ``re_include`` for :func:`~heasarc_retrieve_pipeline.core.recursive_download`.

    Examples
    --------
    >>> sorted(xrism_download_filter({}))
    ['re_include']
    """
    return {"re_include": DOWNLOAD_RE}


def xrism_base_output_path(config, obsid):
    """
    Top-level output directory of an observation.

    Parameters
    ----------
    config : dict
        Must contain ``out_data_path``.
    obsid : str
        Observation identifier.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>``, which is also where the raw data were downloaded.
    """
    return os.path.join(config["out_data_path"], obsid)


def _archive_files(root, *parts):
    """Files matching a glob under ``root``, sorted, without macOS ``._`` sidecars."""
    found = glob.glob(os.path.join(root, *parts))
    return sorted(f for f in found if not os.path.basename(f).startswith("._"))


def find_xrism_inputs(raw_data_dir):
    """
    The files of a downloaded observation that the reduction reads.

    Parameters
    ----------
    raw_data_dir : str
        The observation directory, with the archive's ``auxil`` and ``resolve``
        subdirectories.

    Returns
    -------
    dict
        ``event_files``, a sorted list of the cleaned Resolve event files (empty if there
        are none), and ``orbit_file``, a path or ``None``.
    """
    event_files = _archive_files(raw_data_dir, "resolve", "event_cl", "xa*rsl_*_cl.evt*")
    orbits = _archive_files(raw_data_dir, "auxil", "xa*.orb*")
    if len(orbits) > 1:
        get_logger().warning(f"Several orbit files in {raw_data_dir}; using {orbits[0]}")
    return {"event_files": event_files, "orbit_file": orbits[0] if orbits else None}
