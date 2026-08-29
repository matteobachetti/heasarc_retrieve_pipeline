"""
Small helpers shared across the package.
"""

import logging
import os

import numpy as np
from prefect import get_run_logger

__all__ = [
    "apply_gti",
    "binned_lightcurve",
    "get_logger",
    "good_intervals",
    "gti_to_array",
    "intersect_intervals",
    "intervals_above_threshold",
    "intervals_removed",
    "merge_intervals",
    "mask_from_gti",
    "read_gti",
    "splitext_improved",
]


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


def merge_intervals(intervals, tolerance=0.0):
    """
    Sort a set of intervals and merge the ones that overlap or touch.

    Intervals of zero or negative length are dropped, so the result always satisfies the
    three properties a good time interval list is expected to have: positive length,
    sorted, disjoint.

    Parameters
    ----------
    intervals : iterable of (float, float)
        Can also be an ``(N, 2)`` array. Any order.
    tolerance : float, optional
        Intervals separated by less than this are merged as well. Use it when the interval
        edges carry numerical noise -- sample times that jitter by a fraction of a
        microsecond, say -- and a gap that small cannot mean anything. Keep it far below
        the shortest gap that would be real. Zero, the default, merges only intervals that
        genuinely overlap or touch.

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``.

    Examples
    --------
    >>> merge_intervals([[20, 30], [0, 10], [5, 15]]).tolist()
    [[0.0, 15.0], [20.0, 30.0]]
    >>> merge_intervals([[0, 10], [10.5, 20]], tolerance=1).tolist()
    [[0.0, 20.0]]
    """
    intervals = np.asarray(intervals, dtype=float).reshape(-1, 2)

    merged = []
    order = np.argsort(intervals[:, 0]) if intervals.size else []
    for start, stop in intervals[order]:
        if stop <= start:
            continue
        if merged and start <= merged[-1][1] + tolerance:
            merged[-1][1] = max(merged[-1][1], stop)
        else:
            merged.append([start, stop])

    return np.array(merged, dtype=float).reshape(-1, 2)


def intervals_above_threshold(times, values, threshold, cadence=None):
    """
    The intervals over which a sampled light curve sits at or above a threshold.

    Written for the GOES X-ray flux, where it complements the HEK flare catalogue: the
    catalogue says when a *solar* flare began and ended, the flux says when the Sun was
    actually bright. The two disagree in ways that matter -- a flare's decay tail keeps
    NuSTAR's background elevated well past the catalogued end time, and a rise that was
    never catalogued at the requested class is invisible to the catalogue entirely.

    Each sample at or above the threshold is taken to cover its own cadence bin,
    ``[t - cadence/2, t + cadence/2]``, and the resulting intervals are merged. Samples
    that are ``NaN`` never contribute: a gap in the coverage is missing information, not a
    flare.

    Parameters
    ----------
    times : array-like
        Sample times.
    values : array-like
        Sampled quantity, same length as ``times``. ``NaN`` is allowed.
    threshold : float
        Samples at or above this count as bad.
    cadence : float, optional
        Width of one sample. Defaults to the median spacing of ``times``. Gaps narrower
        than a thousandth of it are treated as sample-time jitter and merged over.

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``, sorted and disjoint.

    Examples
    --------
    >>> times = [0.0, 60.0, 120.0, 180.0]
    >>> flux = [1e-7, 1e-5, 1e-5, 1e-7]
    >>> intervals_above_threshold(times, flux, 5e-6).tolist()
    [[30.0, 150.0]]
    """
    times = np.asarray(times, dtype=float)
    values = np.asarray(values, dtype=float)
    if times.size == 0:
        return np.zeros((0, 2))

    if cadence is None:
        cadence = float(np.median(np.diff(times))) if times.size > 1 else 0.0

    hot = times[np.isfinite(values) & (values >= threshold)]
    # Sample times jitter: the real GOES 1-minute series wanders by around a microsecond
    # about its own cadence. Without a tolerance, consecutive bright samples come back as
    # separate intervals divided by slivers of "good" time a few tens of nanoseconds long.
    return merge_intervals(
        np.column_stack([hot - cadence / 2, hot + cadence / 2]), tolerance=cadence / 1000
    )


def intersect_intervals(first, second):
    """
    The times covered by both interval lists.

    Parameters
    ----------
    first, second : array-like or table
        Intervals, as accepted by :func:`gti_to_array`.

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``, sorted and disjoint.

    Examples
    --------
    >>> intersect_intervals([[0, 100], [200, 300]], [[50, 250]]).tolist()
    [[50.0, 100.0], [200.0, 250.0]]
    """
    second = gti_to_array(second)
    overlaps = []
    for start, stop in gti_to_array(first):
        for other_start, other_stop in second:
            low, high = max(start, other_start), min(stop, other_stop)
            if high > low:
                overlaps.append([low, high])
    return merge_intervals(overlaps)


def good_intervals(bad, tstart, tstop):
    """
    The complement of a set of bad intervals inside ``[tstart, tstop]``.

    Given the stretches of time to throw away -- solar flares, in this package's only
    caller -- return the stretches to keep. The bad intervals may arrive in any order,
    may overlap each other, and may stick out of ``[tstart, tstop]`` at either end; all of
    that is handled here so that the caller does not have to think about it.

    The result always satisfies the three properties a good time interval (GTI) list is
    expected to have: every interval has positive length, the intervals are sorted, and
    they do not overlap. Nothing sticks out of ``[tstart, tstop]``.

    Parameters
    ----------
    bad : iterable of (float, float)
        Intervals to exclude. Can also be an ``(N, 2)`` array.
    tstart, tstop : float
        Bounds of the observation, in the same time units as ``bad``.

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``, the intervals to keep. Empty if the bad intervals cover
        everything.

    Examples
    --------
    >>> good_intervals([(40, 60)], 0, 100).tolist()
    [[0.0, 40.0], [60.0, 100.0]]

    A flare that started before the observation did only trims the beginning:

    >>> good_intervals([(-10, 30)], 0, 100).tolist()
    [[30.0, 100.0]]

    Overlapping exclusions are merged rather than double-counted:

    >>> good_intervals([(40, 60), (50, 70)], 0, 100).tolist()
    [[0.0, 40.0], [70.0, 100.0]]
    """
    bad = np.asarray(bad, dtype=float).reshape(-1, 2)
    merged = merge_intervals(np.clip(bad, tstart, tstop) if bad.size else bad)

    good = []
    current = tstart
    for start, stop in merged:
        if start > current:
            good.append([current, start])
        current = stop
    if tstop > current:
        good.append([current, tstop])

    return np.array(good, dtype=float).reshape(-1, 2)


def gti_to_array(gti):
    """
    Normalise a good time interval list to an ``(N, 2)`` array of floats.

    Accepts what the various sources in this package hand around: an ``(N, 2)`` array or
    list of pairs, or a FITS table (or record array, or ``astropy`` table) with ``START``
    and ``STOP`` columns, in any capitalisation.

    Parameters
    ----------
    gti : array-like or table
        The intervals.

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``.
    """
    dtype = getattr(gti, "dtype", None)
    names = getattr(dtype, "names", None)
    if names:
        by_name = {name.upper(): name for name in names}
        return np.column_stack(
            [np.asarray(gti[by_name["START"]]), np.asarray(gti[by_name["STOP"]])]
        ).astype(float)
    return np.asarray(gti, dtype=float).reshape(-1, 2)


def _extension_index(hdul, wanted, fallback, suffix=None):
    """Index of the first extension whose ``EXTNAME`` matches, else ``fallback``."""
    extnames = [str(hdu.header.get("EXTNAME", "")).upper() for hdu in hdul]
    for index, extname in enumerate(extnames):
        if extname in wanted:
            return index
    if suffix is not None:
        for index, extname in enumerate(extnames):
            if extname.endswith(suffix):
                return index
    return fallback


def mask_from_gti(times, gti):
    """
    Boolean mask selecting the times that fall inside a GTI.

    Interval edges count as inside, so an event recorded exactly at a ``START`` or a
    ``STOP`` is kept.

    Parameters
    ----------
    times : array-like
        Times, in the same scale as ``gti``.
    gti : array-like or table
        Good time intervals, as accepted by :func:`gti_to_array`.

    Returns
    -------
    numpy.ndarray
        Boolean array, the same length as ``times``.
    """
    times = np.asarray(times, dtype=float)
    mask = np.zeros(times.size, dtype=bool)
    for start, stop in gti_to_array(gti):
        mask |= (times >= start) & (times <= stop)
    return mask


def intervals_removed(before, after):
    """
    The stretches of time that ``before`` covers and ``after`` no longer does.

    Used by the flare diagnostic to shade exactly what the filtering threw away, rather
    than every gap in the light curve: the complement of ``after`` on its own would also
    pick up Earth occultations and the other gaps that were never good time to begin with.

    Parameters
    ----------
    before, after : array-like or table
        Good time intervals, as accepted by :func:`gti_to_array`. ``after`` is expected to
        be contained in ``before``.

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``.

    Examples
    --------
    >>> intervals_removed([[0, 100]], [[0, 40], [60, 100]]).tolist()
    [[40.0, 60.0]]
    >>> intervals_removed([[0, 10], [20, 30]], [[0, 10]]).tolist()
    [[20.0, 30.0]]
    """
    after = gti_to_array(after)
    removed = []
    for start, stop in gti_to_array(before):
        removed.extend(good_intervals(after, start, stop).tolist())
    return np.array(removed, dtype=float).reshape(-1, 2)


def read_gti(hdul):
    """
    The good time intervals of an open event file, on the ``TIME + TIMEZERO`` scale.

    The GTI extension is found by ``EXTNAME`` -- ``GTI`` or ``STDGTI``, or anything ending
    in ``GTI`` -- falling back to index 2.

    Parameters
    ----------
    hdul : astropy.io.fits.HDUList
        Open event file.

    Returns
    -------
    numpy.ndarray
        Shape ``(N, 2)``.
    """
    hdu = hdul[_extension_index(hdul, ("GTI", "STDGTI"), 2, suffix="GTI")]
    return gti_to_array(hdu.data) + float(hdu.header.get("TIMEZERO", 0.0))


def apply_gti(hdul, gti):
    """
    Filter an open event file on a new GTI, table *and* header.

    Replacing only the GTI extension -- which is what this package used to do -- leaves an
    event file that still contains the events it claims to have excluded, and still
    advertises the exposure it had before. Anything that reads rates from the header, or
    reads the event table without applying the GTI, then gets the wrong answer. This
    function does the whole job:

    * events outside the new intervals are dropped from the event table;
    * the new intervals replace the GTI extension;
    * ``ONTIME`` becomes the exact total of the new intervals;
    * ``LIVETIME`` and ``EXPOSURE`` are scaled by the ratio of the new GTI total to the
      old one.

    Note where that ratio comes from: the **file's own GTI**, not its ``ONTIME`` keyword.
    ``ONTIME`` is by definition the GTI total, but the two disagree in practice --
    HEASOFT's ``ftmerge`` copies the keyword from the first input instead of recomputing
    it, so a merged NuSTAR file can claim ``ONTIME = 36058 s`` over a GTI totalling
    58889 s. Trusting the keyword there would scale ``LIVETIME`` by 1.58 and make the
    filtered file claim *more* live time than the unfiltered one. The GTI is the authority,
    because it is what the events were selected on.

    The extensions are located by ``EXTNAME`` (``EVENTS``, and ``GTI`` or ``STDGTI``),
    falling back to indices 1 and 2. Times are compared on the ``TIME + TIMEZERO`` scale,
    and ``gti`` is taken to be on that same scale.

    On the ``LIVETIME`` scaling. The exact quantity is the integral of the instrument's
    live fraction over the surviving intervals, which needs the housekeeping file.
    Measured on NuSTAR observation 80002092008: integrating the housekeeping live fraction
    over the full GTI gives 33675.99 s against a header ``LIVETIME`` of 33646.06 s, a
    0.089% difference, which is the accuracy of the integration itself. Over the
    flare-free GTI, exact integration gives 32725.75 s against 32694.29 s for the
    proportional scaling used here -- 0.096%, the same order. Scaling is therefore good
    enough, and it keeps this function independent of any mission's housekeeping file.
    Note the sign: dead time is worse during a flare, so removing flare intervals and
    scaling proportionally very slightly *under*estimates the surviving live time.

    Parameters
    ----------
    hdul : astropy.io.fits.HDUList
        Open event file. Modified in place.
    gti : array-like or table
        The intervals to keep, as accepted by :func:`gti_to_array`.

    Returns
    -------
    dict
        ``nevents_before``, ``nevents_after``, ``ontime_before``, ``ontime_after``,
        ``livetime_before`` and ``livetime_after`` -- what the filtering cost, for logging
        and for the diagnostic plot.
    """
    from astropy.io import fits

    gti = gti_to_array(gti)

    events_index = _extension_index(hdul, ("EVENTS",), 1)
    gti_index = _extension_index(hdul, ("GTI", "STDGTI"), 2, suffix="GTI")
    events = hdul[events_index]
    gti_hdu = hdul[gti_index]

    events_timezero = float(events.header.get("TIMEZERO", 0.0))
    gti_timezero = float(gti_hdu.header.get("TIMEZERO", 0.0))

    old_gti = read_gti(hdul)
    ontime_before = float(np.sum(old_gti[:, 1] - old_gti[:, 0]))
    livetime_before = float(events.header.get("LIVETIME", ontime_before))
    exposure_before = float(events.header.get("EXPOSURE", livetime_before))

    times = np.asarray(events.data["TIME"], dtype=float) + events_timezero
    mask = mask_from_gti(times, gti)

    nevents_before = times.size
    events.data = events.data[mask]

    ontime_after = float(np.sum(gti[:, 1] - gti[:, 0])) if gti.size else 0.0
    scale = ontime_after / ontime_before if ontime_before > 0 else 0.0

    hdul[gti_index] = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=gti[:, 0] - gti_timezero),
            fits.Column(name="STOP", format="D", array=gti[:, 1] - gti_timezero),
        ],
        header=gti_hdu.header,
    )

    new_values = {
        "ONTIME": ontime_after,
        "LIVETIME": livetime_before * scale,
        "EXPOSURE": exposure_before * scale,
    }
    for hdu in hdul:
        for keyword, value in new_values.items():
            if keyword in hdu.header:
                hdu.header[keyword] = value

    return {
        "nevents_before": nevents_before,
        "nevents_after": int(np.count_nonzero(mask)),
        "ontime_before": ontime_before,
        "ontime_after": ontime_after,
        "livetime_before": livetime_before,
        "livetime_after": livetime_before * scale,
    }


def binned_lightcurve(times, gti, dt, min_fraction=0.5):
    """
    A binned light curve that knows about good time intervals.

    A plain histogram of event times divided by the bin width is wrong wherever a bin is
    only partly inside a GTI: the instrument was not collecting for the whole bin, so the
    rate comes out too low and the light curve grows spurious dips at every GTI edge. Here
    each bin's exposure is the actual overlap between the bin and the intervals, and bins
    with too little good time are dropped instead of being reported as near-zero rates.

    Parameters
    ----------
    times : array-like
        Event times, in the same scale as ``gti``.
    gti : array-like or table
        Good time intervals, as accepted by :func:`gti_to_array`.
    dt : float
        Bin width, in the same units.
    min_fraction : float, optional
        Keep a bin only if at least this fraction of it is inside the GTI. The default,
        0.5, is the usual compromise: enough exposure for the Poisson error to mean
        something, without throwing away half the edges.

    Returns
    -------
    dict of numpy.ndarray
        ``time`` (bin centres), ``counts``, ``exposure``, ``rate`` and ``rate_err``.
        All arrays have the same length, and are empty when the GTI is.
    """
    gti = gti_to_array(gti)
    times = np.asarray(times, dtype=float)

    empty = {
        key: np.array([]) for key in ("time", "counts", "exposure", "rate", "rate_err")
    }
    if gti.size == 0:
        return empty

    edges = np.arange(gti[0, 0], gti[-1, 1] + dt, dt)
    if edges.size < 2:
        return empty
    low, high = edges[:-1], edges[1:]

    exposure = np.zeros(low.size)
    for start, stop in gti:
        exposure += np.clip(np.minimum(high, stop) - np.maximum(low, start), 0, None)

    counts = np.histogram(times, bins=edges)[0].astype(float)

    keep = exposure >= min_fraction * dt
    counts, exposure = counts[keep], exposure[keep]
    centres = (low + high)[keep] / 2

    return {
        "time": centres,
        "counts": counts,
        "exposure": exposure,
        "rate": counts / exposure,
        "rate_err": np.sqrt(counts) / exposure,
    }
