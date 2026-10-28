"""
What the ECLAIRs and GRM reductions share: finding an ObsID's files, the geometric cuts
of :mod:`~heasarc_retrieve_pipeline.svom_geometry` put together, and barycentring.
"""

import glob
import os
from collections import defaultdict

import numpy as np
from astropy.io import fits

from . import svom_geometry as geo
from .barycenter import (
    OFFICIAL_TOOL,
    barycenter_ephemeris,
    barycenter_tool,
    barycenter_with_package,
    barycentered_file_name,
)
from .utils import get_logger

#: Orbit and attitude extensions.
ORBIT_EXTNAME = "SVO-ORB-CNV"
ATTITUDE_EXTNAME = "SVO-ATT-CNV"

#: The SAA table for each value of the ``*_saa`` configuration keys.
SAA_EXTNAMES = {"deep": "SAA-DEEP-ECL", "ext": "SAA-EXT-ECL"}

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")


def is_data_file(path):
    """A FITS file, not one of the ``._*`` files macOS leaves on exFAT disks."""
    name = os.path.basename(path)
    return not name.startswith("._") and name.endswith((".fits", ".fits.gz"))


def find_inputs(raw_data_dir):
    """
    The observation's files, by the name of their first extension.

    E.g. ``ECL-EVT-CAL``, ``ECL-GTI-CAL-GRP``, ``EBOUNDS`` (GRM events), ``SVO-ORB-CNV``,
    ``SVO-ATT-CNV``, ``SAA-DEEP-ECL``, ``SAA-EXT-ECL``; each a sorted list, empty if there
    are none.
    """
    found = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(raw_data_dir, "*"))):
        if os.path.isfile(path) and is_data_file(path):
            found[fits.getheader(path, 1).get("EXTNAME", "").strip()].append(path)
    return found


def total_time(gti):
    """The total length of a list of intervals."""
    gti = np.asarray(gti, dtype=float).reshape(-1, 2)
    return float(np.sum(gti[:, 1] - gti[:, 0]))


def geometry_cuts(inputs, source=None, axis="POINTING", max_offaxis_deg=None, saa=None):
    """
    The geometric cuts of an observation, in the order they are applied.

    ``orbit``, where the orbit is measured; with a ``source``, ``visible`` (above the
    Earth's limb), ``attitude`` (where the attitude is known) and ``offaxis`` (within
    ``max_offaxis_deg`` of ``axis``); with ``saa`` (``"deep"`` or ``"ext"``), ``saa``.

    Parameters
    ----------
    inputs : dict
        From :func:`find_inputs`.
    axis : str
        ``"POINTING"`` (ECLAIRs) or ``"GRDX_n"`` (GRM detector ``n``).

    Returns
    -------
    cuts : dict
    orbit : astropy.table.Table
        The joined orbit, for the report.
    """
    orbit, _ = geo.read_aux_series(inputs[ORBIT_EXTNAME])
    times = np.asarray(orbit["TIME"], dtype=float)
    cuts = dict(orbit=geo.orbit_coverage_gti(orbit))
    if source is not None:
        cuts["visible"] = geo.visibility_gti(orbit, source)
        attitude, _ = geo.read_aux_series(inputs[ATTITUDE_EXTNAME])
        cuts["attitude"] = geo.coverage_gti(attitude["TIME"])
        ra, dec = geo.axis_columns(attitude, axis)
        cuts["offaxis"] = geo.offaxis_gti(attitude["TIME"], ra, dec, source, max_offaxis_deg)
    if saa is not None:
        table = geo.read_saa(inputs[SAA_EXTNAMES[saa]])
        cuts["saa"] = geo.saa_gti(table, times[0], times[-1])
    return cuts, orbit


def discontinuity_counts(orbit):
    """How many ``ORIGIN`` and ``FLAG_PVT`` changes and position jumps the orbit has."""
    found = geo.orbit_discontinuities(orbit)
    return {kind: int((found["kind"] == kind).sum()) for kind in ("ORIGIN", "FLAG_PVT", "jump")}


def barycentre_events(infile, orbit_files, ra, dec, config=None):
    """
    Barycentre one event file with the ``barycenter`` package, from every orbit file given.

    There is no official tool for SVOM (``barycorr`` refuses ``TELESCOP = SVOM``), so
    ``barycenter_tool: official`` is overridden, with a warning.
    """
    config = config or {}
    if barycenter_tool(config) == OFFICIAL_TOOL:
        get_logger().warning("No official barycentring tool for SVOM: using the barycenter package")
    outfile = barycentered_file_name(infile)
    return barycenter_with_package(
        infile,
        list(orbit_files),
        outfile,
        ra=float(ra),
        dec=float(dec),
        ephem=barycenter_ephemeris(config),
    )
