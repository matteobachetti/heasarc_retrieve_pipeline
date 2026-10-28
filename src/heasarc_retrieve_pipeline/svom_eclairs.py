"""
SVOM/ECLAIRs reduction for timing: one event file per downlink pass, then barycentred.

An ObsID of ECLAIRs data is a day or so of survey, a few hundred million events, in
files that come one per burst (``CP``, Core Programme) or per stretch of survey (``GP``)
and per downlink pass. Each event file ``ECL-EVT-CAL`` has a GTI file ``ECL-GTI-CAL`` of
its own, with the same ``OBS_ID``, ``BURST_ID`` and ``PASS_ID`` in its primary header, and
one extension per criterion (``GTICAL-<name>``):

========  =========================================================
``STA``   stable attitude
``TLM``   no telemetry gap
``EGP``   instrument in a good state
``NSA``   outside the South Atlantic Anomaly
``NEO``   no Earth in the field of view
``PEO``   partial Earth occultation
``TEO``   total Earth occultation
========  =========================================================

``NEO``, ``PEO`` and ``TEO`` split the time between them, and the archive leaves out the
extension of a state the file never was in (``NEO`` was missing from 6 of 14 files of the
ObsID surveyed). ``TLM`` and ``EGP`` were in every file.

Each pass is reduced on its own -- a pass is about 35 million events, an ObsID ten times
that -- in :func:`reduce_pass`:

1. Each event file is paired with the GTI file of its own burst and pass. An event file
   without one is skipped and reported, never given another file's GTIs.
2. Events with ``FLAG != 0`` are dropped, as ECPI's own imaging does: bit 16 is a pixel
   disabled in that minute, bit 32 an energy below threshold (16 % of all events), bit 64
   one out of range.
3. Each file's events are cut on its own archive GTI, :func:`archive_gti`: ``TLM`` and
   ``EGP``, and ``NEO`` or ``PEO`` (so not total occultation), clipped to the file's own
   ``TSTART``-``TSTOP``.
4. The pass's files are joined, sorted by time (not every file is), and rows identical in
   every column are kept once: a burst file and the survey file it overlaps were seen to
   share no events, but two bursts could.
5. The pass's GTI, the union of the files' archive GTIs, is cut on the geometry of
   :mod:`~heasarc_retrieve_pipeline.svom_geometry` -- orbit and attitude coverage, the
   source above the Earth's limb, the source within :data:`DEFAULT_MAX_OFFAXIS_DEG` of the
   axis and, if asked for, outside the SAA -- and the events outside it go.

The file is ``l2_files/<obsid>_<pass>_ecl.evt``; what each step removed, and the exposure
after each cut, go in ``l2_files/<obsid>_ecl.ecsv``, one row per pass.

Configuration keys:

``eclairs_gti_criteria``
    Archive criteria required on top of the default ones, e.g. ``["STA", "NSA"]``. A file
    without one of them is skipped: a missing ``NSA`` is not known to mean "never in the
    SAA".
``eclairs_pi_band``
    ``[pimin, pimax]``, both kept, in ``PI`` channels. Default: all.
``eclairs_max_offaxis_deg``
    Default :data:`DEFAULT_MAX_OFFAXIS_DEG`.
``eclairs_saa``
    ``"deep"`` or ``"ext"``: also cut on that SAA table. Default: no SAA cut.
"""

import os
from collections import defaultdict

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Table
from prefect import flow, task

from .svom_common import (
    COMMON_STALE_KEYWORDS,
    DEFAULT_CONFIG,
    apply_cuts,
    barycentre_units,
    count_removed,
    discontinuity_counts,
    find_inputs,
    geometry_cuts,
    is_data_file,
    join_events,
    record_screening,
    write_events,
)
from .diagnostics import diagnostics_path, record_step
from .utils import (
    absolute_config,
    get_logger,
    intersect_intervals,
    mask_from_gti,
    merge_intervals,
    record_skipped_input,
)

#: The event extension, and the first extension of an event file.
EVENTS_EXTNAME = "ECL-EVT-CAL"

#: The first extension of a GTI file, which lists the criteria.
GTI_GROUP_EXTNAME = "ECL-GTI-CAL-GRP"

#: Prefix of each criterion's extension in a GTI file.
GTI_PREFIX = "GTICAL-"

#: Criteria every file must have, and that always apply.
REQUIRED_CRITERIA = ("TLM", "EGP")

#: Earth occultation states that are good: none, or partial. Joined, not intersected.
OCCULTATION_CRITERIA = ("NEO", "PEO")

#: The only ``FLAG`` value kept.
GOOD_FLAG = 0

#: Largest angle between the ECLAIRs axis and the source, in degrees. The coded field of
#: view reaches about 45 degrees off axis at zero response.
DEFAULT_MAX_OFFAXIS_DEG = 40.0

#: Header keywords of the input files that do not describe a joined pass, and are dropped.
STALE_KEYWORDS = COMMON_STALE_KEYWORDS + (
    "GTISTART",
    "GTISTOP",
    "SJDSTART",
    "SJDSTOP",
    "EARTH_FR",
    "EARTHMIN",
    "EARTHMAX",
)

#: The counts in a pass's summary, in the order they are taken.
COUNTS = (
    "files",
    "read",
    "flagged",
    "out_of_band",
    "outside_archive",
    "duplicates",
    "outside_gti",
    "written",
)


class MissingCriterion(ValueError):
    """A GTI file lacks a criterion that has to be applied."""


def file_identity(path):
    """``(OBS_ID, BURST_ID, PASS_ID)`` from the primary header, as stripped strings."""
    header = fits.getheader(path, 0)
    return tuple(str(header[key]).strip() for key in ("OBS_ID", "BURST_ID", "PASS_ID"))


def pair_event_and_gti_files(paths):
    """
    Pair each event file with the GTI file of the same burst and pass, by pass.

    Parameters
    ----------
    paths : list of str
        Event and GTI files, in any order; others and ``._*`` files are ignored.

    Returns
    -------
    passes : dict
        ``{pass_id: [(event_file, gti_file), ...]}``, sorted by pass and by event file.
    unpaired : list of tuple
        ``(event_file, reason)`` for each event file without a GTI file of its own.
    """
    events, gtis = {}, {}
    for path in filter(is_data_file, paths):
        extname = fits.getheader(path, 1).get("EXTNAME", "").strip()
        kind = {EVENTS_EXTNAME: events, GTI_GROUP_EXTNAME: gtis}.get(extname)
        if kind is None:
            continue
        key = file_identity(path)
        if key in kind:
            raise ValueError(
                f"{os.path.basename(path)} and {os.path.basename(kind[key])}: same burst and pass"
            )
        kind[key] = path

    passes, unpaired = defaultdict(list), []
    for key, event_file in sorted(events.items(), key=lambda item: item[1]):
        if key in gtis:
            passes[key[2]].append((event_file, gtis[key]))
        else:
            unpaired.append((event_file, "no ECL-GTI-CAL file of the same burst and pass"))
    return dict(sorted(passes.items())), unpaired


def archive_gti(gti_file, extra_criteria=()):
    """
    A file's good time by the archive's criteria: ``TLM`` and ``EGP``, ``NEO`` or ``PEO``.

    Parameters
    ----------
    gti_file : str
    extra_criteria : list of str
        Criteria also required, e.g. ``["STA"]``.

    Raises
    ------
    MissingCriterion
        If ``TLM``, ``EGP`` or one of ``extra_criteria`` has no extension in the file.
    """
    with fits.open(gti_file) as hdul:
        available = {
            hdu.name[len(GTI_PREFIX) :]: merge_intervals(
                np.column_stack([hdu.data["START"], hdu.data["STOP"]])
            )
            for hdu in hdul
            if hdu.name.startswith(GTI_PREFIX)
        }
    required = list(REQUIRED_CRITERIA) + [c for c in extra_criteria if c not in REQUIRED_CRITERIA]
    missing = [name for name in required if name not in available]
    if missing:
        names = ", ".join(GTI_PREFIX + name for name in missing)
        raise MissingCriterion(f"{os.path.basename(gti_file)} has no {names}")

    occultation = [available[name] for name in OCCULTATION_CRITERIA if name in available]
    gti = merge_intervals(np.concatenate(occultation)) if occultation else np.zeros((0, 2))
    for name in required:
        gti = intersect_intervals(gti, available[name])
    return gti


def _read_file(event_file, gti_file, extra_criteria, pi_band, counts):
    """One file's events left after the flag, band and archive cuts; its GTI and headers."""
    with fits.open(event_file) as hdul:
        primary = hdul[0].header.copy()
        header = hdul[EVENTS_EXTNAME].header.copy()
    table = Table.read(event_file, hdu=EVENTS_EXTNAME)
    gti = intersect_intervals(
        archive_gti(gti_file, extra_criteria), [[header["TSTART"], header["TSTOP"]]]
    )

    counts["read"] += len(table)
    good = np.asarray(table["FLAG"]) == GOOD_FLAG
    counts["flagged"] += int((~good).sum())
    table = table[good]
    if pi_band is not None:
        pi = np.asarray(table["PI"])
        good = (pi >= pi_band[0]) & (pi <= pi_band[1])
        counts["out_of_band"] += int((~good).sum())
        table = table[good]
    good = mask_from_gti(table["TIME"], gti)
    counts["outside_archive"] += int((~good).sum())
    return table[good], gti, primary, header


def reduce_pass(pairs, outfile, cuts=None, extra_criteria=(), pi_band=None):
    """
    Reduce one pass: screen each file on its own GTIs, join, cut on ``cuts``, write.

    Parameters
    ----------
    pairs : list of tuple
        ``(event_file, gti_file)`` of the pass, from :func:`pair_event_and_gti_files`.
    outfile : str
    cuts : dict, optional
        ``{name: gti}``, applied in order to the union of the files' archive GTIs.
    extra_criteria : list of str
        See :func:`archive_gti`.
    pi_band : [int, int], optional
        ``PI`` channels kept, both ends included.

    Returns
    -------
    outfile : str or None
        ``None`` if no event is left.
    summary : dict
        The :data:`COUNTS`; ``exposure_archive`` and ``exposure_<name>``, the good time left
        after each cut, in seconds; ``removed_<name>``, the events each cut removed; and,
        not scalars, ``skipped``, ``(file, reason)`` for each file left out for a missing
        criterion, and ``steps``, from
        :func:`~heasarc_retrieve_pipeline.svom_common.apply_cuts`.
    """
    logger = get_logger()
    cuts = cuts or {}
    summary = dict.fromkeys(COUNTS, 0)
    summary["skipped"] = []
    tables, gtis, headers = [], [], None
    for event_file, gti_file in pairs:
        try:
            table, gti, primary, header = _read_file(
                event_file, gti_file, extra_criteria, pi_band, summary
            )
        except MissingCriterion as error:
            logger.warning(f"{os.path.basename(event_file)} skipped: {error}")
            summary["skipped"].append((event_file, str(error)))
            continue
        summary["files"] += 1
        tables.append(table)
        gtis.append(gti)
        headers = headers or (primary, header)

    gti = merge_intervals(np.concatenate(gtis)) if gtis else np.zeros((0, 2))
    summary["steps"] = apply_cuts(gti, cuts, "archive", summary)
    gti = summary["steps"][-1][1]
    if not tables:
        return None, summary

    table, summary["duplicates"] = join_events(tables)
    table = table[count_removed(table["TIME"], summary["steps"], summary)]
    summary["written"] = len(table)
    if not len(table):
        logger.warning(f"{os.path.basename(outfile)}: no event left")
        return None, summary
    return write_events(table, gti, *headers, outfile, EVENTS_EXTNAME, STALE_KEYWORDS), summary


@task(name="reduce_eclairs_obsid")
def reduce_observation(raw_data_dir: str, obsid: str, config: dict, source=None):
    """
    Reduce every pass of one ObsID; see the module docstring.

    Returns
    -------
    written : dict
        ``{pass: event file}``, for each pass with events left.
    orbit_files : list of str
    """
    logger = get_logger()
    inputs = find_inputs(raw_data_dir)
    passes, unpaired = pair_event_and_gti_files(inputs[EVENTS_EXTNAME] + inputs[GTI_GROUP_EXTNAME])
    for path, reason in unpaired:
        logger.warning(f"{obsid}: {os.path.basename(path)} skipped: {reason}")
        record_skipped_input(obsid, config, path, reason)
    if not passes:
        logger.warning(f"{obsid}: no ECLAIRs event file with its GTI file")
        return {}, []

    orbit_files = inputs["SVO-ORB-CNV"]
    cuts, meta, orbit, angles = {}, {}, None, None
    max_offaxis = config.get("eclairs_max_offaxis_deg", DEFAULT_MAX_OFFAXIS_DEG)
    if orbit_files:
        (cuts,), orbit, (angles,) = geometry_cuts(
            inputs,
            source=source,
            axes=["POINTING"],
            max_offaxis_deg=max_offaxis,
            saa=config.get("eclairs_saa"),
        )
        meta = discontinuity_counts(orbit)
    else:
        logger.warning(f"{obsid}: no orbit file; only the archive GTIs apply")

    l2_dir = os.path.join(raw_data_dir, "l2_files")
    os.makedirs(l2_dir, exist_ok=True)
    written, rows = {}, []
    for pass_id, pairs in passes.items():
        with record_step(diagnostics_path(obsid, config), obsid, "svom_screening", pass_id) as rec:
            outfile, summary = reduce_pass(
                pairs,
                os.path.join(l2_dir, f"{obsid}_{pass_id}_ecl.evt"),
                cuts=cuts,
                extra_criteria=config.get("eclairs_gti_criteria", ()),
                pi_band=config.get("eclairs_pi_band"),
            )
            for path, reason in summary.pop("skipped"):
                record_skipped_input(obsid, config, path, reason)
            steps = summary.pop("steps")
            record_screening(rec, summary, steps, orbit, angles, max_offaxis)
        rows.append(dict(summary, **{"pass": pass_id}))
        if outfile is not None:
            written[pass_id] = outfile
    table = Table(rows=rows)
    table = table[["pass"] + [name for name in table.colnames if name != "pass"]]
    table.meta["orbit_discontinuities"] = meta
    table.write(os.path.join(l2_dir, f"{obsid}_ecl.ecsv"), overwrite=True)
    return written, orbit_files


@flow
def process_eclairs_obsid(obsid: str, config=None, flags=None, ra: float = None, dec: float = None):
    """
    Reduce one SVOM/ECLAIRs ObsID for timing: one file per pass, barycentred.

    Parameters
    ----------
    obsid : str
    config : dict, optional
        Pipeline configuration; see the module docstring for the ECLAIRs keys.
    flags : dict, optional
        Accepted for signature compatibility with the other missions, and ignored.
    ra, dec : float, optional
        The searched position, in degrees. Without it the source cannot be placed, so
        neither the Earth nor the off-axis cut is made, and nothing is barycentred.

    Returns
    -------
    list of str or None
        The barycentred files, the unbarycentred ones if there is no position or orbit,
        or ``None`` if no pass has events left.
    """
    current_config = absolute_config(config, DEFAULT_CONFIG)
    logger = get_logger()
    logger.info(f"Processing SVOM/ECLAIRs observation {obsid}")
    raw_data_dir = os.path.join(current_config["out_data_path"], obsid)
    source = None if ra is None or dec is None else SkyCoord(ra, dec, unit="deg")

    written, orbit_files = reduce_observation(raw_data_dir, obsid, current_config, source=source)
    if not written:
        return None
    return barycentre_units(written, orbit_files, ra, dec, obsid, current_config)
