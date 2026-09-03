"""
The record each reduction step leaves behind, for the report to be built from.

A reduction that has finished says very little about itself. The products are on disk and
the log has scrolled past, and nothing remembers how long a step took, why a step decided
to do nothing, or which numbers went into a decision. This module is where a step writes
that down.

What lives where
----------------

Three things record what happened to an observation, and they do not overlap:

``skipped_inputs.txt``
    Which *inputs* the reduction had to skip, and why. Written by
    :func:`~heasarc_retrieve_pipeline.utils.record_skipped_input`, and the source of truth
    for that question.

``<OBSID>/diagnostics/``
    This module. Per *step*: how it ended, how long it took, the scalars behind its
    decisions, and the arrays its figures are drawn from.

step stamps
    What a step *produced*, and the CALDB and pipeline versions it produced it with. These
    drive the decision to re-run a step, so they are written only when a step succeeds --
    which is exactly why they cannot also carry failures, and why this module exists
    alongside them rather than inside them.

Why files
---------

Observations are reduced in separate processes (``ProcessPoolTaskRunner``), so no
in-memory accumulator can span them, and a task's return value has to pickle. Writing to
disk keeps every task's return type exactly as it was.

**One writer, one file name.** Within a worker some tasks run concurrently -- the two
``join_source_data`` calls, for instance -- so every record goes to its own
``<step>__<key>.json`` and nothing is ever read, modified and written back. That is what
lets this module do without the lock that ``record_skipped_input`` needs for its single
shared file.

The dataset outlives the run
----------------------------

The arrays are a measurement of the observation, not of the run that happened to take it.
A step that skips because its output was already on disk measures nothing, and the record
it writes must therefore *keep* what the run that did the work wrote: the ``.npz`` beside
it, and the values in it. Otherwise a rerun of a finished observation quietly replaces a
full record with an empty one, orphans the payload on disk, and takes the figure off the
page -- which is precisely what it used to do.

So a record inherits. On entry it picks up the values of the record it is about to
replace, and on every write it keeps pointing at the payload beside it unless this run
measured a new one. ``arrays_from_earlier_run`` says which of the two happened, so the
report can draw the figure and still say plainly that the numbers are not from this run.
Inheritance reads only the file this record already owns, so the one-writer-one-file-name
invariant above is untouched.

Statuses
--------

``running``
    Written when the step is entered, and replaced when it leaves. A run that is killed
    mid-step therefore leaves behind a record naming the step it died in, which is the
    most useful thing a crashed run can say and costs nothing to keep.
``done``
    It finished.
``skipped``
    It decided there was nothing to do, and ``reason`` says why in words.
``failed``
    It raised. ``error`` and ``traceback`` say what. The exception is always re-raised:
    the caller still has to fail.

Examples
--------
>>> import tempfile
>>> directory = tempfile.mkdtemp()
>>> with record_step(directory, "90101", "separate_sources", key="nu90101A01") as rec:
...     rec.value(n_peaks=3, threshold=41.5)
...     rec.array(image=np.zeros((4, 4)))
>>> record = read_records(directory)[0]
>>> record["step"], record["status"], record["values"]["n_peaks"]
('separate_sources', 'done', 3)
>>> read_arrays(directory, record)["image"].shape
(4, 4)
"""

import json
import os
import re
import tempfile
import time
import traceback as traceback_module
from datetime import datetime, timezone

import numpy as np

from .utils import get_logger

__all__ = [
    "canonical_metadata",
    "catalogue_row",
    "diagnostics_path",
    "read_arrays",
    "read_manifest",
    "read_records",
    "record_step",
    "write_manifest",
]

#: Bumped when the shape of a record changes in a way a reader has to know about.
#: 2 added ``arrays_from_earlier_run``. Readers use ``.get()``, so schema-1 records
#: written before that -- a whole reduction's worth, for anyone who has already run this
#: -- still read back fine.
SCHEMA = 2

#: The manifest is a record of the observation, not of a step, so
#: :func:`read_records` leaves it alone.
MANIFEST = "manifest.json"

#: Anything else in a file name would put the record somewhere unexpected, or make it
#: unopenable on a case-insensitive filesystem.
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")

#: The catalogue calls the same quantity different things depending on the mission and on
#: which query answered. The first name present wins; see ``MISSION_CONFIG`` in
#: :mod:`heasarc_retrieve_pipeline.core` for where the three schemas come from.
CANONICAL_COLUMNS = {
    "source_name": ("source_name", "name", "target_name"),
    "exposure": ("exposure", "exposure_a"),
    "time": ("time",),
    "ra": ("ra",),
    "dec": ("dec",),
    "public_date": ("public_date",),
    "cycle": ("cycle",),
    "prnb": ("prnb",),
    "solar_activity": ("solar_activity",),
}


def diagnostics_path(obsid, config):
    """
    Directory holding one observation's step records.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/diagnostics``.

    Examples
    --------
    >>> diagnostics_path("90202038002", {"out_data_path": "out"})
    'out/90202038002/diagnostics'
    """
    return os.path.join(config["out_data_path"], obsid, "diagnostics")


def _jsonable(value):
    """
    The nearest thing to ``value`` that :mod:`json` will write.

    Everything in this package is numpy, and three quarters of it is not JSON. A masked
    catalogue cell -- ``public_date`` is routinely masked -- raises outright, as do
    ``np.float32``, ``np.int64`` and ``np.bool_``; ``np.float64`` happens to be a subclass
    of ``float`` and survives, which makes the failure look intermittent. Non-finite
    floats are written by :mod:`json` as ``NaN`` and ``Infinity``, which no strict JSON
    reader accepts, so they become ``None``.

    Examples
    --------
    >>> _jsonable(np.float32(1.5)), _jsonable(np.int64(3)), _jsonable(np.ma.masked)
    (1.5, 3, None)
    >>> _jsonable(float("nan")) is None
    True
    >>> _jsonable({"a": np.array([1, 2])})
    {'a': [1, 2]}
    """
    if value is None or isinstance(value, str):
        return value
    if value is np.ma.masked or isinstance(value, np.ma.core.MaskedConstant):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, np.ndarray):
        return _jsonable(value.tolist())
    if isinstance(value, np.generic):
        return _jsonable(value.item())
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    if isinstance(value, int):
        return value
    return str(value)


def _replace_atomically(path, write):
    """
    Write a file through a temporary name, so a reader never sees it half-written.

    ``write`` is called with an open binary file object. The same pattern as
    :func:`~heasarc_retrieve_pipeline.utils.record_skipped_input`.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle, temporary = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as fobj:
            write(fobj)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _stem(step, key):
    """``<step>__<key>``, with anything awkward in ``key`` flattened to an underscore."""
    if not key:
        return step
    return f"{step}__{_UNSAFE.sub('_', str(key))}"


class StepRecord:
    """
    One step's record, written on entry and rewritten on exit.

    Made by :func:`record_step`; not constructed directly. See the module docstring for
    the statuses and what each means.
    """

    def __init__(self, directory, obsid, step, key=""):
        self.directory = directory
        self.obsid = obsid
        self.step = step
        self.key = key
        self.stem = _stem(step, key)
        if self.stem + ".json" == MANIFEST:
            # It would overwrite the manifest and then be skipped when read back, so the
            # step would simply not appear in the report. Better to say so now.
            raise ValueError(f"{step!r} is reserved; a step may not be called that")
        self.status = "running"
        self.reason = None
        self.values = {}
        self.arrays = {}
        self._error = None
        self._traceback = None
        self._started = None
        self._monotonic = None
        self._inherited_values = {}
        self._earlier = False

    @property
    def path(self):
        """Where the record itself goes."""
        return os.path.join(self.directory, self.stem + ".json")

    @property
    def arrays_path(self):
        """Where the array payload goes, when there is one."""
        return os.path.join(self.directory, self.stem + ".npz")

    def value(self, **values):
        """Record scalars. Anything :func:`_jsonable` can carry is fine."""
        self.values.update(values)

    def array(self, **arrays):
        """Record arrays, for a figure to be drawn from. They go in the ``.npz``."""
        self.arrays.update(arrays)

    def from_earlier_outputs(self):
        """
        Mark the arrays as describing work this run did not do.

        A record that inherits its payload is marked by itself. This is for
        :mod:`heasarc_retrieve_pipeline.recover`, which measures *now* but measures the
        output of a run that finished some time ago -- so the arrays are fresh and the
        work behind them is not, and the page has to say the second thing.
        """
        self._earlier = True

    def skip(self, reason):
        """
        Mark the step as having decided to do nothing, and say why.

        Does not raise: the caller goes on to return normally, as it always did.
        """
        self.status = "skipped"
        self.reason = reason

    def as_dict(self):
        """The record as it will be written."""
        inherited = self._earlier or (
            not self.arrays and os.path.exists(self.arrays_path)
        )
        duration = None
        if self._monotonic is not None:
            duration = round(time.monotonic() - self._monotonic, 3)
        return {
            "schema": SCHEMA,
            "obsid": self.obsid,
            "step": self.step,
            "key": self.key,
            "status": self.status,
            "reason": self.reason,
            "error": self._error,
            "traceback": self._traceback,
            "started": self._started,
            "started_iso": (
                None
                if self._started is None
                else datetime.fromtimestamp(self._started, timezone.utc).isoformat()
            ),
            "duration_s": duration,
            "values": _jsonable({**self._inherited_values, **self.values}),
            "arrays": (
                os.path.basename(self.arrays_path)
                if self.arrays or inherited
                else None
            ),
            "arrays_from_earlier_run": inherited,
        }

    def write(self):
        """Write the record, and its arrays if it has any."""
        if self.arrays:
            _replace_atomically(
                self.arrays_path,
                lambda fobj: np.savez_compressed(fobj, **self.arrays),
            )
        payload = self.as_dict()
        _replace_atomically(
            self.path,
            lambda fobj: fobj.write(json.dumps(payload, indent=1).encode("utf-8")),
        )

    def _inherit(self):
        """
        Pick up the values of the record this one is about to replace.

        The payload needs no such care: it is picked up in :meth:`as_dict` by looking for
        the file, which also adopts a ``.npz`` left orphaned by a rerun that ran before
        any of this was fixed.
        """
        try:
            with open(self.path, "rb") as fobj:
                previous = json.load(fobj)
        except (OSError, ValueError):
            # No earlier run, or one whose record did not survive. Either way there is
            # nothing to inherit, and refusing to record now would be the worse failure.
            return
        if isinstance(previous, dict) and isinstance(previous.get("values"), dict):
            self._inherited_values = previous["values"]

    def __enter__(self):
        self._started = time.time()
        self._monotonic = time.monotonic()
        os.makedirs(self.directory, exist_ok=True)
        self._inherit()
        self.write()
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.status = "failed"
            self._error = f"{exc_type.__name__}: {exc}"
            self._traceback = "".join(traceback_module.format_exception(exc_type, exc, tb))
        elif self.status == "running":
            self.status = "done"
        try:
            self.write()
        except Exception as error:  # pragma: no cover - reporting must not mask the work
            get_logger().warning(f"Could not record the {self.step} step: {error}")
        # Never swallow: process_observations counts a failure by catching it.
        return False


class _NoRecord:
    """
    What :func:`record_step` gives back when there is nowhere to write.

    Every recording call site takes ``diagnostics_dir=None`` to mean "record nothing", so
    that callers which have no output directory -- and every test written before any of
    this existed -- go on working untouched.
    """

    status = "running"
    reason = None

    def value(self, **values):
        pass

    def array(self, **arrays):
        pass

    def skip(self, reason):
        self.reason = reason

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def record_step(directory, obsid, step, key=""):
    """
    Record one step of a reduction, as a context manager.

    Parameters
    ----------
    directory : str or None
        Where to write, normally :func:`diagnostics_path`. ``None`` records nothing.
    obsid : str
        Observation identifier.
    step : str
        Stable short name of the step, e.g. ``"separate_sources"``.
    key : str, optional
        What within the step this record is about -- usually an event file's root name.
        Together with ``step`` it makes the file name, so it must be unique among the
        records written concurrently for one observation.

    Returns
    -------
    StepRecord or _NoRecord
        Use it as a context manager; see the module docstring.

    Examples
    --------
    >>> with record_step(None, "90101", "join_source_data") as rec:
    ...     rec.value(files=2)
    """
    if directory is None:
        return _NoRecord()
    return StepRecord(directory, obsid, step, key=key)


def no_record():
    """
    A record that goes nowhere.

    For a function that takes a record from its caller and has to work when it is given
    none -- called from a test, or from a context that has no output directory.

    Returns
    -------
    _NoRecord
        Accepts every recording call and does nothing with it.

    Examples
    --------
    >>> rec = no_record()
    >>> rec.value(radius=42)
    >>> rec.skip("nothing to do")
    """
    return _NoRecord()


def read_records(directory):
    """
    Every step record in a diagnostics directory, oldest first.

    A record that will not parse is logged and left out rather than taken as fatal: the
    report has to render whatever survived a crashed run.

    Parameters
    ----------
    directory : str
        A :func:`diagnostics_path`.

    Returns
    -------
    list of dict
        Sorted by when the step started. Empty if the directory does not exist.
    """
    if not os.path.isdir(directory):
        return []

    records = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".json") or name == MANIFEST:
            continue
        path = os.path.join(directory, name)
        try:
            with open(path) as fobj:
                record = json.load(fobj)
        except (OSError, ValueError) as error:
            get_logger().warning(f"Ignoring unreadable step record {path}: {error}")
            continue
        if not isinstance(record, dict) or "step" not in record:
            continue
        record.setdefault("_file", name)
        records.append(record)

    return sorted(records, key=lambda record: (record.get("started") or 0, record["_file"]))


def read_arrays(directory, record):
    """
    The array payload of one record, or ``None`` if it has none.

    Parameters
    ----------
    directory : str
        A :func:`diagnostics_path`.
    record : dict
        One of :func:`read_records`.

    Returns
    -------
    dict of numpy.ndarray or None
        Keyed as they were recorded.
    """
    name = record.get("arrays")
    if not name:
        return None
    path = os.path.join(directory, name)
    try:
        with np.load(path) as payload:
            return {key: payload[key] for key in payload.files}
    except (OSError, ValueError) as error:
        get_logger().warning(f"Ignoring unreadable array payload {path}: {error}")
        return None


def catalogue_row(row):
    """
    Every column of a catalogue row, as plain JSON values.

    ``__row`` is astroquery's internal row identifier and is left out: it is meaningless
    outside the query that produced it.

    Parameters
    ----------
    row : astropy.table.Row or dict
        One row of a HEASARC master-catalogue answer.

    Returns
    -------
    dict

    Examples
    --------
    >>> catalogue_row({"obsid": "1", "exposure_a": np.float32(1000.0), "__row": 7})
    {'obsid': '1', 'exposure_a': 1000.0}
    """
    names = getattr(row, "colnames", None)
    if names is None:
        names = list(row.keys())
    return {name: _jsonable(row[name]) for name in names if name != "__row"}


def canonical_metadata(catalogue):
    """
    The handful of observation parameters the report shows, whatever they are called.

    Three missions and two query paths give the same quantity three different names --
    ``source_name``, ``name`` and ``target_name`` for the target, ``exposure`` and
    ``exposure_a`` for the exposure -- and only some carry ``public_date`` or ``cycle``.
    Rather than branch on the mission, take the first name that is present.

    Parameters
    ----------
    catalogue : dict
        As returned by :func:`catalogue_row`.

    Returns
    -------
    dict
        Only the keys whose column was present. A column that was present but masked is
        kept, with value ``None``.

    Examples
    --------
    >>> canonical_metadata({"target_name": "M82", "exposure_a": 1000.0, "cycle": 5})
    {'source_name': 'M82', 'exposure': 1000.0, 'cycle': 5}
    >>> canonical_metadata({"name": "Her X-1", "public_date": None})
    {'source_name': 'Her X-1', 'public_date': None}
    """
    metadata = {}
    for canonical, candidates in CANONICAL_COLUMNS.items():
        for candidate in candidates:
            if candidate in catalogue:
                metadata[canonical] = catalogue[candidate]
                break
    return metadata


def write_manifest(directory, obsid, catalogue=None, **extra):
    """
    Record what the catalogue said about an observation, before it is reduced.

    Written before the observation is handed to a worker, so that a run killed part way
    through still has one of these for every observation it meant to do -- which is what
    lets the index list an observation that never started.

    Parameters
    ----------
    directory : str or None
        A :func:`diagnostics_path`. ``None`` writes nothing.
    obsid : str
        Observation identifier.
    catalogue : dict, optional
        Every catalogue column, from :func:`catalogue_row`. Kept verbatim, with
        :func:`canonical_metadata` stored beside it.
    **extra
        Anything else worth keeping -- ``url``, ``mission``, the position actually used.

    Returns
    -------
    str or None
        Where it was written.
    """
    if directory is None:
        return None

    catalogue = catalogue or {}
    payload = {
        "schema": SCHEMA,
        "obsid": obsid,
        "written_iso": datetime.now(timezone.utc).isoformat(),
        "catalogue": _jsonable(catalogue),
        "metadata": _jsonable(canonical_metadata(catalogue)),
    }
    payload.update(_jsonable(extra))

    path = os.path.join(directory, MANIFEST)
    _replace_atomically(
        path, lambda fobj: fobj.write(json.dumps(payload, indent=1).encode("utf-8"))
    )
    return path


def read_manifest(directory):
    """
    What the catalogue said about an observation, or ``None`` if nothing was recorded.

    Parameters
    ----------
    directory : str
        A :func:`diagnostics_path`.

    Returns
    -------
    dict or None
    """
    path = os.path.join(directory, MANIFEST)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as fobj:
            return json.load(fobj)
    except (OSError, ValueError) as error:
        get_logger().warning(f"Ignoring unreadable manifest {path}: {error}")
        return None
