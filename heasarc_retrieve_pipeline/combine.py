"""
Co-adding several observations of the same source.

Two short observations taken days apart, each too faint to fit on its own. This module
combines what the pipeline produced for each of them into one dataset, in a new
observation-shaped directory beside them::

    <out_data_path>/<NAME>/products/<NAME>_A01.pha   co-added source spectrum, FPMA
    <out_data_path>/<NAME>/products/<NAME>_A01.bak   co-added background
    <out_data_path>/<NAME>/products/<NAME>_A01.rsp   combined response
    <out_data_path>/<NAME>/products/<NAME>_A01_grp.pha   grouped, the one you fit
    <out_data_path>/<NAME>/products/<NAME>_A01_inputs.lis  what went into it
    <out_data_path>/<NAME>/products/<NAME>_comb01.pha    both modules co-added
    <out_data_path>/<NAME>/<NAME>A_src1_bary.evt    merged event list
    <out_data_path>/<NAME>/diagnostics/             what this run did

A directory of its own is what gets the merged dataset a page in the report for free:
:func:`~heasarc_retrieve_pipeline.report.observation_directories` counts any subdirectory
holding a ``diagnostics`` as an observation.

Spectra
    HEASOFT ``addspec``, per focal-plane module **and per observing mode**, through
    :func:`~heasarc_retrieve_pipeline.coadd.run_addspec`. It does the exposure weighting,
    the background rescaling and the response combination itself. The ARF is folded into
    the output ``.rsp``, so the merged spectrum has no separate ``ANCRFILE``.

    The mode is part of the grouping because mode 01 and mode 06 are not the same
    measurement: mode 06 has a reconstructed aspect solution, and its spectra are worth
    being able to fit -- or to leave out -- on their own. Until this was so, ``<NAME>_A``
    was silently whichever mixture of the two the observations happened to contain. See
    ``docs/known_issues.rst``: the merged products are named differently as a result.

    :func:`combine_merged_modules` then co-adds FPMA with FPMB across the merged products,
    giving ``<NAME>_comb01``, ``<NAME>_comb06`` and ``<NAME>_comb0106`` exactly as
    :func:`~heasarc_retrieve_pipeline.nustar.combine_module_spectra` does for a single
    observation, with the same case-B correction.

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
    apply_case_b_scaling,
    run_addspec,
    stage_inputs,
    working_directory,
)
from .diagnostics import diagnostics_path, record_step, write_manifest
from .nustar import (
    COMBINED_PRODUCTS,
    merge_event_files,
    nu_base_output_path,
    nu_product_output_path,
)
from .utils import get_logger

__all__ = [
    "GROUPING_COMMAND",
    "MERGE_NAME_RE",
    "SPECTRUM_MODE_RE",
    "SPECTRUM_RE",
    "combine_merged_modules",
    "main",
    "merge_event_lists",
    "merge_name",
    "merge_obsids",
    "merge_spectra",
    "source_spectra",
    "spectrum_mode",
    "stage_inputs",
    "working_directory",
]

#: Matches a source spectrum ``nuproducts`` wrote, picking out the module it belongs to.
#: The ``_sr.pha`` anchor at the end is what keeps the segment spectra of
#: :mod:`heasarc_retrieve_pipeline.segments` -- ``..._sr_seg1.pha`` -- out of a merge:
#: co-adding a segment with the whole observation it came from would count its events
#: twice.
SPECTRUM_RE = re.compile(r"^nu(?P<obsid>\d+)(?P<fpm>[AB])(?P<mode>\d\d)(?P<rest>.*)_sr\.pha$")

#: The observing mode a spectrum's file name declares: the two digits after the module
#: letter. Deliberately looser than :data:`SPECTRUM_RE` -- it matches a segment spectrum
#: too, because the round trip co-adds segments and they have to be grouped by mode just
#: the same.
SPECTRUM_MODE_RE = re.compile(r"^nu\d+[AB](?P<mode>\d\d)")

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


def spectrum_mode(path):
    """
    Which observing mode a spectrum came from.

    Parameters
    ----------
    path : str
        Path or base name of a spectrum.

    Returns
    -------
    str or None
        ``"01"``, ``"06"``, or ``None`` for a name that does not carry a mode at all.

    Examples
    --------
    >>> spectrum_mode("nu80002092006A01_sr.pha")
    '01'
    >>> spectrum_mode("x/nu80002092006B06_chu12_N_sr_seg2.pha")
    '06'
    >>> spectrum_mode("something_else.pha") is None
    True
    """
    match = SPECTRUM_MODE_RE.match(os.path.basename(path))
    return match.group("mode") if match else None


def combine_merged_modules(name, config, roots, rec=None):
    """
    Co-add FPMA with FPMB across a merged dataset's per-module spectra.

    The merge-level counterpart of
    :func:`~heasarc_retrieve_pipeline.nustar.combine_module_spectra`, and it works the same
    way: one product per flavour of :data:`~heasarc_retrieve_pipeline.nustar.COMBINED_PRODUCTS`,
    built only from modes where both modules are there, with
    :func:`~heasarc_retrieve_pipeline.coadd.apply_case_b_scaling` putting the exposure right
    afterwards. The inputs are the merged per-module spectra rather than one observation's,
    which changes nothing about the arithmetic: mode 01 and mode 06 are disjoint in time and
    add, while FPMA and FPMB are simultaneous and do not.

    Parameters
    ----------
    name : str
        Name of the merged dataset.
    config : dict
        Must contain ``out_data_path``.
    roots : dict
        Group key -- module letter followed by mode, as :func:`merge_spectra` builds it --
        to the base name of the merged spectrum written for it.
    rec : StepRecord, optional
        Diagnostics record to write into.

    Returns
    -------
    dict
        Flavour to the base name of the grouped spectrum written for it.
    """
    logger = get_logger()
    outdir = nu_product_output_path(name, config=config)

    by_mode = {}
    for key, root in roots.items():
        fpm, mode = key[0], key[1:]
        if mode not in ("01", "06"):
            # A group that carried no mode at all, so there is no telling what it is.
            continue
        by_mode.setdefault(mode, {})[fpm] = os.path.join(outdir, root + ".pha")
    paired = {
        mode: [modules["A"], modules["B"]]
        for mode, modules in by_mode.items()
        if len(modules) == 2
    }

    written = {}
    inputs = {}
    for suffix, modes in COMBINED_PRODUCTS.items():
        if not all(mode in paired for mode in modes):
            continue
        spectra = [path for mode in modes for path in paired[mode]]
        root = f"{name}_{suffix}"
        inputs[suffix] = [os.path.basename(path) for path in spectra]
        logger.info(f"Co-adding the modules of {name} into {root}.pha")
        run_addspec(spectra, outdir, root, f"_inputs_{suffix}")
        # addspec added the two modules' exposures as though they had observed one after
        # the other. They did not. See coadd.apply_case_b_scaling.
        apply_case_b_scaling(
            [os.path.join(outdir, root + end) for end in (".pha", "_grp.pha")], 2
        )
        written[suffix] = root + "_grp.pha"

    if rec is not None:
        rec.value(combined=written, combined_inputs=inputs)
    return written


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
        Group key -- the module letter followed by the observing mode, ``"A01"`` -- to the
        base name of the grouped spectrum written for it, and one entry per flavour
        :func:`combine_merged_modules` went on to build.
    """
    logger = get_logger()
    outdir = nu_product_output_path(name, config=config)
    os.makedirs(outdir, exist_ok=True)

    by_group = {}
    if spectra is None:
        spectra = [
            pair
            for obsid in obsids
            for pair in source_spectra(obsid, config, mode01_only=mode01_only)
        ]
    for fpm, path in spectra:
        # Module and mode together. Mode 01 and mode 06 are different measurements of the
        # same source -- the second has a reconstructed aspect solution -- so a merged
        # product that mixed them could be neither fitted nor unpicked afterwards.
        by_group.setdefault(f"{fpm}{spectrum_mode(path) or ''}", []).append(path)

    roots = {}
    written = {}
    inputs = {}
    for key, group in sorted(by_group.items()):
        inputs[key] = [os.path.basename(path) for path in group]
        if len(group) < 2:
            logger.warning(
                f"{key} has only {len(group)} spectrum across {len(obsids)} "
                "observation(s); nothing to co-add"
            )
            continue

        root = f"{name}_{key}"
        # Case A -- the same detector at different times -- so addspec's own convention of
        # adding the exposures is the right one and nothing is rescaled afterwards.
        run_addspec(group, outdir, root, f"_inputs_{key}")
        roots[key] = root
        written[key] = root + "_grp.pha"

    # Case B, on top of case A: the same two modules, now merged, still observed at once.
    written.update(combine_merged_modules(name, config, roots, rec=rec))

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
