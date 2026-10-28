"""
SVOM geometry from the orbit and attitude files: the good time intervals both ECLAIRs and
GRM need.

Plain numpy and astropy: no ECPI, no archive. The inputs are the files
:func:`heasarc_retrieve_pipeline.svom.svom_download` fetched beside the events:

* ``SVO-ORB-CNV``: ``TIME``, ``POSITION`` and ``VELOCITY`` (J2000, metres), ``FLAG_PVT``
  (``0x08``: telemetry gap), ``ORIGIN`` (which system gave the position). 1-s samples.
* ``SVO-ATT-CNV``: same grid; ``POINTING`` (ECLAIRs axis RA, Dec, roll) and ``GRDX_1..3``
  (RA, Dec of each GRM detector's axis), in degrees.
* ``SAA-DEEP-ECL`` / ``SAA-EXT-ECL``: ``TIME_ENTRY``, ``TIME_EXIT`` of each SAA (South
  Atlantic Anomaly) crossing.

Each ``*_gti`` function returns an ``(N, 2)`` array of good time intervals, in the
files' own time (TT seconds since MJDREF). The reductions intersect them. Edges that fall
between two samples are placed by linear interpolation of the quantity being cut on, so
they are good to far better than the 1-s sampling.
"""

import os

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table, vstack

from .utils import good_intervals, merge_intervals

#: Equatorial radius of the Earth (WGS84). The Earth is taken as a sphere: at SVOM's
#: altitude the flattening moves the limb by a fraction of a degree, far less than the
#: atmosphere's uncertainty.
R_EARTH_M = 6378137.0

#: Height of the atmosphere taken as opaque, above :data:`R_EARTH_M`.
ATMOSPHERE_M = 100e3

#: Longest hole between orbit samples that is bridged. The orbit is never extended past
#: its first or last sample.
MAX_HOLE_S = 2.0

#: ``FLAG_PVT`` bit marking a telemetry gap: the sample is filler, not a measurement.
TELEMETRY_GAP = 0x08

#: Smallest step in position, beyond what the velocity explains, reported as a jump.
JUMP_M = 1000.0


def read_aux_series(files, hdu=1):
    """
    Join orbit or attitude files into one time series, one row per sample time.

    Files of consecutive passes abut and may share samples. Each time is kept once, from
    the first file in time order; macOS ``._*`` metadata files are skipped.

    Parameters
    ----------
    files : list of str
    hdu : int or str
        The extension to read from each file.

    Returns
    -------
    joined : astropy.table.Table
        Sorted by ``TIME``.
    n_conflicts : int
        Repeated samples whose values differ from the one kept.
    """
    files = [f for f in files if not os.path.basename(f).startswith("._")]
    tables = [Table.read(f, hdu=hdu) for f in files]
    tables.sort(key=lambda table: float(table["TIME"][0]) if len(table) else np.inf)
    table = vstack(tables, metadata_conflicts="silent")
    table = table[np.argsort(table["TIME"], kind="stable")]

    times = np.asarray(table["TIME"])
    first = np.r_[True, np.diff(times) != 0]
    group = np.cumsum(first) - 1
    kept = table[first]
    differs = np.zeros((~first).sum(), bool)
    for name in table.colnames:
        repeat = np.asarray(table[name][~first])
        original = np.asarray(kept[name][group[~first]])
        same = repeat == original
        if repeat.dtype.kind == "f":
            same |= np.isnan(repeat) & np.isnan(original)
        differs |= ~same.reshape(len(same), -1).all(axis=1)
    return kept, int(differs.sum())


def coverage_gti(times, max_hole=MAX_HOLE_S):
    """
    The intervals covered by samples no more than ``max_hole`` apart.

    Examples
    --------
    >>> coverage_gti([0, 1, 2, 3, 10, 11]).tolist()
    [[0.0, 3.0], [10.0, 11.0]]
    """
    times = np.sort(np.asarray(times, dtype=float))
    if times.size < 2:
        return np.zeros((0, 2))
    close = np.diff(times) <= max_hole
    return merge_intervals(np.column_stack([times[:-1][close], times[1:][close]]))


def orbit_coverage_gti(orbit, max_hole=MAX_HOLE_S):
    """Where the orbit is measured: samples flagged as telemetry gaps count as missing."""
    good = (np.asarray(orbit["FLAG_PVT"]) & TELEMETRY_GAP) == 0
    return coverage_gti(np.asarray(orbit["TIME"])[good], max_hole=max_hole)


def orbit_discontinuities(orbit, jump_m=JUMP_M):
    """
    Changes of ``ORIGIN`` or ``FLAG_PVT``, and jumps in position, for the report.

    A jump is a step between consecutive samples that the mean of their velocities does
    not explain by more than ``jump_m``. Nothing is cut here: whether these matter for
    timing is to be seen.

    Returns
    -------
    astropy.table.Table
        ``time`` (the first sample after the change), ``kind`` (``"ORIGIN"``,
        ``"FLAG_PVT"`` or ``"jump"``), ``value`` (the new value, or the step in metres).
    """
    times = np.asarray(orbit["TIME"], dtype=float)
    rows = []
    for name in ("ORIGIN", "FLAG_PVT"):
        values = np.asarray(orbit[name])
        for i in np.flatnonzero(values[1:] != values[:-1]) + 1:
            rows.append((times[i], name, float(values[i])))
    position = np.asarray(orbit["POSITION"], dtype=float)
    velocity = np.asarray(orbit["VELOCITY"], dtype=float)
    predicted = position[:-1] + 0.5 * (velocity[:-1] + velocity[1:]) * np.diff(times)[:, None]
    step = np.linalg.norm(position[1:] - predicted, axis=1)
    for i in np.flatnonzero(step > jump_m):
        rows.append((times[i + 1], "jump", step[i]))
    rows.sort(key=lambda row: row[0])
    return Table(rows=rows, names=["time", "kind", "value"], dtype=[float, str, float])


def intervals_where_positive(times, values):
    """
    The intervals where a sampled quantity is at or above zero, edges interpolated.

    Examples
    --------
    >>> intervals_where_positive([0, 1, 2, 3], [-1, 1, 1, -3]).tolist()
    [[0.5, 2.25]]
    """
    times = np.asarray(times, dtype=float)
    values = np.asarray(values, dtype=float)
    good = values >= 0
    if not good.any():
        return np.zeros((0, 2))

    def crossing(i):
        """Where the line between samples i and i + 1 goes through zero."""
        return times[i] + (times[i + 1] - times[i]) * values[i] / (values[i] - values[i + 1])

    edges = np.diff(good.astype(int))
    starts = [times[0]] if good[0] else []
    stops = []
    for i in np.flatnonzero(edges):
        (starts if edges[i] > 0 else stops).append(crossing(i))
    if good[-1]:
        stops.append(times[-1])
    return merge_intervals(np.column_stack([starts, stops]))


def _unit_vectors(ra_deg, dec_deg):
    ra, dec = np.radians(ra_deg), np.radians(dec_deg)
    return np.column_stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)])


def _source_vector(source):
    source = source.icrs if isinstance(source, SkyCoord) else source
    return _unit_vectors(source.ra.deg, source.dec.deg)[0]


def earth_limb_margin(position_m, source, atmosphere_m=ATMOSPHERE_M):
    """
    How far the source is above the Earth's limb, in degrees; negative when hidden.

    The limb is the edge of a sphere of radius :data:`R_EARTH_M` plus ``atmosphere_m``.

    Examples
    --------
    >>> overhead = [[7000e3, 0.0, 0.0]]
    >>> float(earth_limb_margin(overhead, SkyCoord(180, 0, unit="deg"))[0]) < 0
    True
    """
    position = np.asarray(position_m, dtype=float).reshape(-1, 3)
    distance = np.linalg.norm(position, axis=1)
    to_earth = -position / distance[:, None]
    separation = np.arccos(np.clip(to_earth @ _source_vector(source), -1, 1))
    earth_radius = np.arcsin(np.clip((R_EARTH_M + atmosphere_m) / distance, -1, 1))
    return np.degrees(separation - earth_radius)


def visibility_gti(orbit, source, atmosphere_m=ATMOSPHERE_M):
    """Where the source is not hidden by the Earth, from the orbit's positions."""
    margin = earth_limb_margin(orbit["POSITION"], source, atmosphere_m=atmosphere_m)
    return intervals_where_positive(orbit["TIME"], margin)


def offaxis_angle(axis_ra, axis_dec, source):
    """
    Angle between an instrument axis and the source, in degrees.

    Examples
    --------
    >>> offaxis_angle([0.0, 30.0], [0.0, 0.0], SkyCoord(0, 0, unit="deg")).round(6).tolist()
    [0.0, 30.0]
    """
    cosine = _unit_vectors(axis_ra, axis_dec) @ _source_vector(source)
    return np.degrees(np.arccos(np.clip(cosine, -1, 1)))


def offaxis_gti(times, axis_ra, axis_dec, source, max_deg):
    """
    Where the source is within ``max_deg`` of an axis.

    For ECLAIRs the axis is ``POINTING[:, 0:2]``; for GRM detector ``n``, ``GRDX_n``.
    """
    angle = offaxis_angle(axis_ra, axis_dec, source)
    return intervals_where_positive(times, max_deg - angle)


def axis_columns(attitude, column):
    """RA and Dec of an axis from the attitude table: ``"POINTING"`` or ``"GRDX_n"``."""
    values = np.asarray(attitude[column], dtype=float)
    return values[:, 0], values[:, 1]


def read_saa(files):
    """The entry and exit times of every SAA table given, joined."""
    files = [f for f in files if not os.path.basename(f).startswith("._")]
    return vstack([Table.read(f, hdu=1) for f in files], metadata_conflicts="silent")


def saa_gti(saa, tstart, tstop):
    """Outside the SAA crossings, inside ``[tstart, tstop]``. Rows with NaN are dropped."""
    entry = np.asarray(saa["TIME_ENTRY"], dtype=float)
    exit_ = np.asarray(saa["TIME_EXIT"], dtype=float)
    known = np.isfinite(entry) & np.isfinite(exit_)
    return good_intervals(np.column_stack([entry[known], exit_[known]]), tstart, tstop)
