"""
Co-adding several observations of the same source.

Two short observations taken days apart, each too faint to fit on its own. This module
combines what the pipeline produced for each of them into one dataset, in a new
observation-shaped directory beside them::

    <out_data_path>/<NAME>/products/<NAME>_A.pha    co-added source spectrum, FPMA
    <out_data_path>/<NAME>/products/<NAME>_A.bak    co-added background
    <out_data_path>/<NAME>/products/<NAME>_A.rsp    combined response
    <out_data_path>/<NAME>/products/<NAME>_A_grp.pha   grouped, the one you fit
    <out_data_path>/<NAME>/products/<NAME>_A_inputs.lis  what went into it
    <out_data_path>/<NAME>/<NAME>A_src1_bary.evt    merged event list
    <out_data_path>/<NAME>/diagnostics/             what this run did

A directory of its own is what gets the merged dataset a page in the report for free:
:func:`~heasarc_retrieve_pipeline.report.observation_directories` counts any subdirectory
holding a ``diagnostics`` as an observation.

Spectra
    HEASOFT ``addspec``, per focal-plane module, through
    :func:`~heasarc_retrieve_pipeline.coadd.run_addspec`. It does the exposure weighting,
    the background rescaling and the response combination itself. The ARF is folded into
    the output ``.rsp``, so the merged spectrum has no separate ``ANCRFILE``.

    Merging observations is **case A** of the two ``addspec`` says it may be used for:
    "adding data from the same detector at different times". Its convention of adding the
    inputs' exposure times is therefore the right one here, and nothing is rescaled
    afterwards. That is not true of the other use this package makes of the same tool --
    co-adding FPMA with FPMB, which is case B, different detectors at the *same* time, and
    needs the exposure and effective area put right afterwards. The distinction, and the
    measurement behind it, are in :mod:`heasarc_retrieve_pipeline.coadd`.

    ``addspec`` also warns that adding PHA datasets at all is a dangerous exercise. For
    case A the danger is a source that varies, or a detector whose response changed between
    the epochs: a merged spectrum is a time average, and nothing in the file says so.

Event lists
    :func:`~heasarc_retrieve_pipeline.nustar.merge_event_files`, unchanged. It has always
    taken an arbitrary list of paths; only its callers were tied to one OBSID. The GTIs
    are combined with ``OR``, so the days between two observations show up as a gap, which
    is what any downstream timing has to see.

Why the inputs are staged
    ``addspec`` resolves the files a spectrum names **relative to the current working
    directory**, so the inputs have to be gathered into one directory and run from there.
    Since the spectra of a merge live one directory per OBSID, there is no such directory
    until :func:`~heasarc_retrieve_pipeline.coadd.stage_inputs` builds it. The bug that
    forces this, and the measurement of exactly how far the workaround has to go, are
    documented there.

Command line::

    hrp-merge-obsids <out_data_path> <OBSID> <OBSID> [...] [--name NAME]

See ``docs/technical_details.rst`` for the scientific rationale, and
``docs/known_issues.rst`` for known defects.
"""

import argparse
import glob
import os
import re
import sys

from .coadd import (
    GROUPING_COMMAND,
    run_addspec,
    stage_inputs,
    working_directory,
)
from .diagnostics import diagnostics_path, record_step, write_manifest
from .nustar import (
    merge_event_files,
    nu_base_output_path,
    nu_product_output_path,
)
from .utils import get_logger

__all__ = [
    "GROUPING_COMMAND",
    "MERGE_NAME_RE",
    "SPECTRUM_RE",
    "main",
    "merge_event_lists",
    "merge_name",
    "merge_obsids",
    "merge_spectra",
    "source_spectra",
    "stage_inputs",
    "working_directory",
]

#: Matches a source spectrum ``nuproducts`` wrote, picking out the module it belongs to.
#: The ``_sr.pha`` anchor at the end is what keeps the segment spectra of
#: :mod:`heasarc_retrieve_pipeline.segments` -- ``..._sr_seg1.pha`` -- out of a merge:
#: co-adding a segment with the whole observation it came from would count its events
#: twice.
SPECTRUM_RE = re.compile(r"^nu(?P<obsid>\d+)(?P<fpm>[AB])(?P<mode>\d\d)(?P<rest>.*)_sr\.pha$")

#: What a merged dataset may be called. ``mathpha``, which ``addspec`` spawns, parses its
#: input as an arithmetical expression, so a name carrying ``+``, ``-``, ``*`` or brackets
#: is refused rather than left to fail three tools deep.
MERGE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


def merge_name(obsids, name=None):
    """
    What to call a merged dataset.

    Parameters
    ----------
    obsids : list of str
        The observations being merged.
    name : str, optional
        An explicit name. Checked against :data:`MERGE_NAME_RE`.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        If ``name`` holds anything but letters, digits and underscores. ``mathpha`` would
        read a ``+`` or a ``-`` in it as arithmetic.

    Examples
    --------
    >>> merge_name(["80002092002", "80002092004"])
    'merged_80002092002_80002092004'
    >>> merge_name(["1", "2", "3"])
    'merged_1_3'
    """
    if name is not None:
        if not MERGE_NAME_RE.match(name):
            raise ValueError(
                f"{name!r} is not a usable name: HEASOFT's mathpha, which addspec "
                "spawns, reads +, -, * and brackets in a file name as arithmetic. "
                "Use letters, digits and underscores."
            )
        return name
    ordered = sorted(obsids)
    if len(ordered) == 1:
        return f"merged_{ordered[0]}"
    return f"merged_{ordered[0]}_{ordered[-1]}"


def source_spectra(obsid, config, mode01_only=False):
    """
    An observation's source spectra, with the module each belongs to.

    Segment spectra written by :mod:`heasarc_retrieve_pipeline.segments` are excluded:
    they are parts of the same events as the whole-observation spectrum beside them, and
    co-adding both would count those events twice.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    mode01_only : bool, optional
        Keep only the normal-science spectra, leaving the mode-06 CHU subsets out.

    Returns
    -------
    list of tuple
        ``(fpm, path)``, sorted by path.
    """
    products = nu_product_output_path(obsid, config=config)
    found = []
    for path in sorted(glob.glob(os.path.join(products, "nu*_sr.pha"))):
        match = SPECTRUM_RE.match(os.path.basename(path))
        if match is None or match.group("obsid") != obsid:
            continue
        if mode01_only and match.group("mode") != "01":
            continue
        found.append((match.group("fpm"), path))
    return found


def merge_spectra(obsids, config, name, mode01_only=False, rec=None, spectra=None):
    """
    Co-add the observations' spectra with ``addspec``, one output per module.

    Parameters
    ----------
    obsids : list of str
        Observations to merge. Used to find the inputs, unless ``spectra`` names them,
        and to report how many observations a lone spectrum was found across.
    config : dict
        Must contain ``out_data_path``.
    name : str
        Name of the merged dataset.
    mode01_only : bool, optional
        Leave the mode-06 CHU spectra out. Ignored when ``spectra`` is given.
    rec : StepRecord, optional
        Diagnostics record to write into.
    spectra : list of tuple, optional
        ``(fpm, path)`` pairs to co-add, in place of everything :func:`source_spectra`
        would find in ``obsids``. The caller then owns the choice entirely, which is the
        point: :data:`SPECTRUM_RE` refuses a ``_seg<N>`` spectrum on purpose, so that
        merging observations cannot double-count one that was previously split, and that
        guard has to stay. Naming the files instead is how the segment round trip -- split
        an observation, co-add the pieces, compare with the parent -- is reached without
        weakening it. See :mod:`heasarc_retrieve_pipeline.roundtrip`.

    Returns
    -------
    dict
        Module to the base name of the grouped spectrum written for it.
    """
    logger = get_logger()
    outdir = nu_product_output_path(name, config=config)
    os.makedirs(outdir, exist_ok=True)

    by_module = {}
    if spectra is None:
        spectra = [
            pair
            for obsid in obsids
            for pair in source_spectra(obsid, config, mode01_only=mode01_only)
        ]
    for fpm, path in spectra:
        by_module.setdefault(fpm, []).append(path)

    written = {}
    inputs = {}
    for fpm, spectra in sorted(by_module.items()):
        inputs[fpm] = [os.path.basename(path) for path in spectra]
        if len(spectra) < 2:
            logger.warning(
                f"FPM{fpm} has only {len(spectra)} spectrum across {len(obsids)} "
                "observation(s); nothing to co-add"
            )
            continue

        root = f"{name}_{fpm}"
        # Case A -- the same detector at different times -- so addspec's own convention of
        # adding the exposures is the right one and nothing is rescaled afterwards.
        run_addspec(spectra, outdir, root, f"_inputs_FPM{fpm}")
        written[fpm] = root + "_grp.pha"

    if rec is not None:
        rec.value(spectra=written, inputs=inputs)
    return written


def merge_event_lists(obsids, config, name, rec=None):
    """
    Concatenate the observations' barycentred event lists, per module and per product.

    The GTIs are combined with ``OR``: the days between two observations become a gap,
    which is what they are.

    Parameters
    ----------
    obsids : list of str
        Observations to merge.
    config : dict
        Must contain ``out_data_path``.
    name : str
        Name of the merged dataset.
    rec : StepRecord, optional
        Diagnostics record to write into.

    Returns
    -------
    list of str
        Base names of the event files written.
    """
    logger = get_logger()
    outdir = nu_base_output_path(name, config=config)
    os.makedirs(outdir, exist_ok=True)

    # Group by what the file is -- FPMA's first source, FPMB's background -- across the
    # observations, by stripping the OBSID out of the name.
    groups = {}
    for obsid in obsids:
        basedir = nu_base_output_path(obsid, config=config)
        prefix = f"nu{obsid}"
        for path in sorted(glob.glob(os.path.join(basedir, prefix + "*_bary.evt*"))):
            # What the file is, with the observation it came from taken off the front:
            # "A_src1_bary.evt". That is the key two observations have in common.
            key = os.path.basename(path)[len(prefix):]
            groups.setdefault(key, []).append(path)

    written = []
    for key, paths in sorted(groups.items()):
        if len(paths) < 2:
            logger.warning(f"Only {len(paths)} file(s) for {key}; nothing to merge")
            continue
        outfile = os.path.join(outdir, "nu" + name + key)
        logger.info(f"Merging {len(paths)} event files into {os.path.basename(outfile)}")
        merge_event_files.fn(paths, outfile, gti_operation="OR")
        written.append(os.path.basename(outfile))

    if rec is not None:
        rec.value(event_files=written, groups={k: len(v) for k, v in groups.items()})
    return written


def merge_obsids(obsids, config, name=None, mode01_only=False, spectra=True, events=True):
    """
    Co-add several observations into one dataset.

    Parameters
    ----------
    obsids : list of str
        Observations to merge. Each must already be reduced.
    config : dict
        Must contain ``out_data_path``.
    name : str, optional
        What to call the result; see :func:`merge_name`.
    mode01_only : bool, optional
        Leave the mode-06 CHU spectra out of the co-addition.
    spectra, events : bool, optional
        Which products to merge.

    Returns
    -------
    dict
        ``name``, ``spectra`` and ``event_files``.
    """
    logger = get_logger()
    name = merge_name(obsids, name)
    os.makedirs(nu_base_output_path(name, config=config), exist_ok=True)
    logger.info(f"Merging {', '.join(obsids)} into {name}")

    with record_step(diagnostics_path(name, config), name, "merge_obsids") as rec:
        rec.value(obsids=list(obsids), mode01_only=bool(mode01_only))
        written_spectra = (
            merge_spectra(obsids, config, name, mode01_only=mode01_only) if spectra else {}
        )
        written_events = merge_event_lists(obsids, config, name) if events else []
        rec.value(spectra=written_spectra, event_files=written_events)

    write_manifest(diagnostics_path(name, config), name, merged_from=list(obsids))
    return {"name": name, "spectra": written_spectra, "event_files": written_events}


def main(argv=None):
    """
    ``hrp-merge-obsids <out_data_path> <OBSID> <OBSID> [...]``.

    Parameters
    ----------
    argv : list of str, optional
        Defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        Process exit status.
    """
    parser = argparse.ArgumentParser(
        prog="hrp-merge-obsids",
        description=__doc__.strip().splitlines()[0],
    )
    parser.add_argument("out_data_path", help="the pipeline's output directory")
    parser.add_argument(
        "obsid", nargs="+", help="observations to merge; each must already be reduced"
    )
    parser.add_argument(
        "--name",
        default=None,
        help="what to call the result (letters, digits and underscores only)",
    )
    parser.add_argument(
        "--mode01-only",
        action="store_true",
        help="co-add only the normal-science spectra, leaving the mode-06 subsets out",
    )
    parser.add_argument(
        "--no-spectra", action="store_true", help="do not co-add the spectra"
    )
    parser.add_argument(
        "--no-events", action="store_true", help="do not merge the event lists"
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    if len(args.obsid) < 2:
        parser.error("give at least two observations to merge")

    import logging

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = {"out_data_path": os.path.abspath(args.out_data_path)}
    result = merge_obsids(
        args.obsid,
        config,
        name=args.name,
        mode01_only=args.mode01_only,
        spectra=not args.no_spectra,
        events=not args.no_events,
    )
    print(
        f"{result['name']}: {len(result['spectra'])} co-added spectrum/spectra, "
        f"{len(result['event_files'])} merged event file(s)"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
