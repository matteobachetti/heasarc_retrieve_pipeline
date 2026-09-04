"""
Checking a split against its parent, on a copy, without re-running the pipeline.

:mod:`~heasarc_retrieve_pipeline.segments` cuts a reduced observation into time segments
and :mod:`~heasarc_retrieve_pipeline.combine` co-adds spectra. The obvious question is
whether the two undo each other: split an observation in half, put the halves back
together, and see whether the parent comes back. This module asks that question on its
own, in minutes rather than the hour a reduction takes, and without touching the tree it
is checking.

Three things make it cheap.

**A copy, not the original.** :func:`stage_observation` copies one observation's directory
into a scratch tree and every later step works there, so a failed check cannot leave
segment products scattered through a real reduction. The copy is a plain ``cp -R`` of one
OBSID, a few hundred megabytes for a short observation.

**Mode 01 only, by default.** ``spectral_input_files`` takes normal-science data from
``event_pipe/`` and the CHU-resolved spacecraft-science subsets from ``split/``. Leaving
``split/`` out of the copy therefore reduces the work from eight ``nuproducts`` calls per
segment to two, without a flag or a special case anywhere in the split itself: there is
simply nothing there to find. A mode-06 observation splits in about 16 minutes per cut and
a mode-01-only copy of it in about four. Pass ``--with-mode06`` when the CHU subsets are
what is being checked.

**Two checks, only one of which needs HEASOFT.**

*The partition check* is pure astropy and takes seconds. The segments were extracted by
``nuproducts`` over ``intersect_intervals(parent GTI, segment bounds)``, so they divide the
parent's good time between them with nothing left over and nothing counted twice. Channel
for channel, the segment spectra must therefore sum to the parent's, and their good times
must sum to the parent's. This is the stronger of the two checks -- it is exact, it is per
channel, and it fails loudly on an off-by-one in the bounds.

*The addspec round trip* co-adds the segments back with the same ``addspec`` call a merge
of observations uses, and compares its output with the parent. It is the weaker check,
being a comparison of one summed spectrum with another, but it is the one that exercises
the tool a user would actually reach for.

The round trip cannot go through ``hrp-merge-obsids``, and that is deliberate.
``combine.SPECTRUM_RE`` anchors on ``_sr.pha`` so that a ``_seg<N>`` spectrum is never
swept into a merge of observations -- without it, merging an observation that had been
split would count it twice, once whole and once in pieces. Rather than weaken that guard,
this module names the segment files explicitly through ``merge_spectra(spectra=...)``.

Command line::

    hrp-check-roundtrip <out_data_path> <OBSID> <MJD> [<MJD> ...]

The exit status is 0 when every check passed and 1 when any did not.
"""

import argparse
import glob
import os
import re
import shutil
import sys

import numpy as np
from astropy.io import fits

from .combine import merge_spectra
from .nustar import nu_base_output_path, nu_product_output_path
from .segments import SEGMENT_RE, split_obsid
from .utils import get_logger, gti_to_array, read_gti, rootname

__all__ = [
    "COUNTS_TOLERANCE",
    "SEGMENT_SPECTRUM_RE",
    "addspec_roundtrip",
    "compare_events",
    "compare_spectra",
    "check_roundtrip",
    "main",
    "segment_families",
    "stage_observation",
]


#: A segment source spectrum, with the stem it belongs to and its segment number. The
#: mirror image of ``combine.SPECTRUM_RE``, which matches everything this does not.
SEGMENT_SPECTRUM_RE = re.compile(r"^(?P<stem>nu\d+[AB].*)_sr_seg(?P<number>\d+)\.pha$")

#: How far a summed exposure may sit from the parent's before it is called a failure.
#: ``nuproducts`` computes each segment's live time independently, so the parts need not
#: add up to the last bit; a real off-by-one in the bounds is orders of magnitude larger.
EXPOSURE_TOLERANCE = 1e-6


def stage_observation(obsid, config, workdir, with_mode06=False):
    """
    Copy one reduced observation into a scratch tree, and return a config naming it.

    Parameters
    ----------
    obsid : str
        Observation to copy. Must already be reduced.
    config : dict
        Configuration of the real tree. Must contain ``out_data_path``.
    workdir : str
        Where to put the copy. Created if it is not there. Keep it short: HEASOFT's older
        tools truncate file-name parameters, see ``docs/known_issues.rst``.
    with_mode06 : bool, optional
        Copy the ``split/`` directory too, so that the CHU-resolved spacecraft-science
        files are split as well. Off by default, which is what makes the check quick.

    Returns
    -------
    dict
        A configuration whose ``out_data_path`` is ``workdir``.

    Raises
    ------
    FileNotFoundError
        If the observation is not in ``config["out_data_path"]``.
    """
    logger = get_logger()
    source = nu_base_output_path(obsid, config=config)
    if not os.path.isdir(source):
        raise FileNotFoundError(f"{obsid} is not reduced in {config['out_data_path']}")

    os.makedirs(workdir, exist_ok=True)
    destination = os.path.join(workdir, obsid)
    if os.path.exists(destination):
        logger.info(f"Removing the copy of {obsid} left by an earlier check")
        shutil.rmtree(destination)

    ignore = None if with_mode06 else shutil.ignore_patterns("split")
    logger.info(f"Copying {source} to {destination}")
    shutil.copytree(source, destination, ignore=ignore, symlinks=True)

    return dict(config, out_data_path=os.path.abspath(workdir))


def segment_families(obsid, config):
    """
    Group an observation's segment spectra by the parent spectrum they came from.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    dict
        Stem -- ``nu<OBSID>A01``, say -- to ``(parent path, [segment paths in number
        order])``. Stems whose parent spectrum is missing are left out.

    Examples
    --------
    >>> SEGMENT_SPECTRUM_RE.match("nu80002092008A01_sr_seg2.pha").group("stem")
    'nu80002092008A01'
    >>> SEGMENT_SPECTRUM_RE.match("nu80002092008A01_sr.pha") is None
    True
    """
    products = nu_product_output_path(obsid, config=config)
    families = {}
    for path in sorted(glob.glob(os.path.join(products, "nu*_sr_seg*.pha"))):
        match = SEGMENT_SPECTRUM_RE.match(os.path.basename(path))
        if match is None:
            continue
        stem = match.group("stem")
        parent = os.path.join(products, stem + "_sr.pha")
        if not os.path.exists(parent):
            continue
        families.setdefault(stem, (parent, []))[1].append((int(match.group("number")), path))

    return {
        stem: (parent, [path for _, path in sorted(numbered)])
        for stem, (parent, numbered) in families.items()
    }


#: How far a channel may be from its expected value before it is called wrong. Counts are
#: integers, so this is exact equality for them; it exists for the ``RATE`` case below,
#: where a spectrum has been through a division and a multiplication.
COUNTS_TOLERANCE = 1e-6


def _spectrum(path):
    """
    A spectrum's counts, exposure and good time, as plain numbers.

    ``nuproducts`` writes ``COUNTS``, and that is what the segments and their parent carry.
    ``addspec`` may hand back a ``RATE`` instead, depending on what it was asked to do with
    the background, so that is converted back with the file's own ``EXPOSURE`` rather than
    failing -- which is why the comparisons carry a tolerance instead of testing equality.
    """
    with fits.open(path) as hdul:
        spectrum = hdul["SPECTRUM"]
        exposure = float(spectrum.header["EXPOSURE"])
        names = spectrum.columns.names
        if "COUNTS" in names:
            counts = np.asarray(spectrum.data["COUNTS"], dtype=float)
        elif "RATE" in names:
            counts = np.asarray(spectrum.data["RATE"], dtype=float) * exposure
        else:
            raise KeyError(f"{path} has neither a COUNTS nor a RATE column: {names}")
        try:
            gti = gti_to_array(read_gti(hdul))
        except (KeyError, IndexError):
            gti = np.zeros((0, 2))
    return counts, exposure, float(np.sum(gti[:, 1] - gti[:, 0])) if gti.size else 0.0


def compare_spectra(parent, segments):
    """
    Check that a parent spectrum is the channel-by-channel sum of its segments.

    The exact statement of what a split is supposed to be. ``nuproducts`` extracted each
    segment over the parent's own GTI intersected with that segment's bounds, so the
    segments partition the parent's good time: every event is in exactly one of them, and
    the counts in each of the 4096 channels must add up.

    Parameters
    ----------
    parent : str
        Path of the whole-observation source spectrum.
    segments : list of str
        Paths of its segment spectra.

    Returns
    -------
    dict
        ``counts_match``, ``parent_counts``, ``segment_counts``, ``channels_wrong``,
        ``exposure_match``, ``parent_exposure``, ``segment_exposure``, ``gti_match``,
        ``parent_gti`` and ``segment_gti``.
    """
    parent_counts, parent_exposure, parent_gti = _spectrum(parent)
    pieces = [_spectrum(path) for path in segments]

    summed = np.sum([counts for counts, _, _ in pieces], axis=0)
    segment_exposure = float(np.sum([exposure for _, exposure, _ in pieces]))
    segment_gti = float(np.sum([gti for _, _, gti in pieces]))

    def close(a, b):
        return abs(a - b) <= EXPOSURE_TOLERANCE * max(abs(a), abs(b), 1.0)

    wrong = np.abs(summed - parent_counts) > COUNTS_TOLERANCE

    return {
        "counts_match": not bool(wrong.any()),
        "channels_wrong": int(np.count_nonzero(wrong)),
        "parent_counts": int(round(float(parent_counts.sum()))),
        "segment_counts": int(round(float(summed.sum()))),
        "exposure_match": close(parent_exposure, segment_exposure),
        "parent_exposure": parent_exposure,
        "segment_exposure": segment_exposure,
        "gti_match": close(parent_gti, segment_gti),
        "parent_gti": parent_gti,
        "segment_gti": segment_gti,
    }


def compare_events(parent, segments):
    """
    Check that a parent event list is the union of its segments, event for event.

    The event split is pure astropy -- :func:`~heasarc_retrieve_pipeline.utils.apply_gti`
    on the parent's own table -- so this is the cheap half of the round trip and it can be
    exact: the segments' event times, concatenated and sorted, must be the parent's.

    Parameters
    ----------
    parent : str
        Path of the whole-observation event file.
    segments : list of str
        Paths of its segment event files.

    Returns
    -------
    dict
        ``times_match``, ``parent_events``, ``segment_events``, ``gti_match``,
        ``parent_gti`` and ``segment_gti``.
    """

    def times_and_gti(path):
        with fits.open(path) as hdul:
            times = np.asarray(hdul["EVENTS"].data["TIME"], dtype=float)
            gti = gti_to_array(read_gti(hdul))
        return times, float(np.sum(gti[:, 1] - gti[:, 0])) if gti.size else 0.0

    parent_times, parent_gti = times_and_gti(parent)
    pieces = [times_and_gti(path) for path in segments]

    joined = np.sort(np.concatenate([times for times, _ in pieces])) if pieces else []
    segment_gti = float(np.sum([gti for _, gti in pieces]))

    return {
        "times_match": bool(
            len(joined) == parent_times.size and np.array_equal(joined, np.sort(parent_times))
        ),
        "parent_events": int(parent_times.size),
        "segment_events": int(len(joined)),
        "gti_match": abs(parent_gti - segment_gti) <= EXPOSURE_TOLERANCE * max(parent_gti, 1.0),
        "parent_gti": parent_gti,
        "segment_gti": segment_gti,
    }


def addspec_roundtrip(obsid, config, families, name=None):
    """
    Co-add each family's segments back together with ``addspec``, and compare.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``. This should be the *staged* configuration: the
        merged products are written into a dataset of their own beside the copy.
    families : dict
        As :func:`segment_families` returns.
    name : str, optional
        Prefix of the merged datasets, one per family. Kept short on purpose: it becomes a
        directory name inside the staging tree, and HEASOFT's file-name limit is measured
        from the output root outwards.

    Returns
    -------
    dict
        Stem to the comparison :func:`compare_spectra` would make between the parent and
        the single co-added spectrum, plus ``merged`` naming the file. A stem whose merge
        produced nothing is mapped to ``None``; one with a single segment, which there is
        nothing to co-add, to the string ``"single segment"``.

    Notes
    -----
    One merge per family rather than one merge for the whole observation. ``addspec``
    groups its output by focal-plane module, and mode 01 and the mode-06 CHU subsets of the
    same module are different pointings with different responses -- co-adding them would be
    answering a different question than "does the split undo itself".
    """
    logger = get_logger()
    name = "roundtrip" if name is None else name
    results = {}

    for stem, (parent, segments) in sorted(families.items()):
        fpm = stem[len(f"nu{obsid}")]
        merged_name = f"{name}_{stem}"

        if len(segments) < 2:
            # A cut outside the observation leaves one segment, which is a legitimate
            # split and nothing to co-add. addspec on a single file is a slow, lossy copy,
            # and merge_spectra refuses it; that is not a failure of the round trip.
            logger.info(f"{stem} has one segment, so there is nothing to co-add")
            results[stem] = "single segment"
            continue

        logger.info(f"Co-adding {len(segments)} segment(s) of {stem} back together")

        written = merge_spectra(
            [obsid],
            config,
            merged_name,
            spectra=[(fpm, path) for path in segments],
        )
        if not written:
            logger.warning(f"{stem} produced no co-added spectrum")
            results[stem] = None
            continue

        # Every segment here is one stem, so one module and one mode: merge_spectra groups
        # by exactly that and leaves a single entry, whatever the key is called.
        key = min(written)
        merged = os.path.join(
            nu_product_output_path(merged_name, config=config), f"{merged_name}_{key}.pha"
        )
        comparison = compare_spectra(parent, [merged])
        comparison["merged"] = merged
        results[stem] = comparison

    return results


def check_roundtrip(
    obsid,
    config,
    mjds,
    workdir,
    scale=None,
    with_mode06=False,
    addspec=True,
):
    """
    Split a copy of an observation, put it back together, and report what came back.

    Parameters
    ----------
    obsid : str
        Observation to check. Must already be reduced.
    config : dict
        Configuration of the real tree. Must contain ``out_data_path``.
    mjds : list of float
        Where to cut, as :func:`~heasarc_retrieve_pipeline.segments.split_obsid` takes them.
    workdir : str
        Scratch directory for the copy.
    scale : str, optional
        Time scale of ``mjds``; ``None`` means the files' own ``TIMESYS``.
    with_mode06 : bool, optional
        Include the CHU-resolved spacecraft-science files.
    addspec : bool, optional
        Run the ``addspec`` half of the check. Off makes the whole thing pure astropy.

    Returns
    -------
    dict
        ``config`` for the staged copy, ``split`` as ``split_obsid`` returned it,
        ``spectra`` and ``events`` mapping each stem to its comparison, and ``merged`` from
        :func:`addspec_roundtrip` or ``None``.
    """
    staged = stage_observation(obsid, config, workdir, with_mode06=with_mode06)
    split = split_obsid(obsid, staged, mjds, scale=scale)

    families = segment_families(obsid, staged)
    spectra = {
        stem: compare_spectra(parent, segments)
        for stem, (parent, segments) in sorted(families.items())
    }

    # Paired up by name rather than by asking the split what it wrote, so that a check can
    # be run over segments an earlier run left. The compression suffix is not assumed:
    # ``insert_tag`` keeps whatever the parent had, ``.evt`` or ``.evt.gz``.
    base = nu_base_output_path(obsid, config=staged)
    found = {}
    for path in sorted(glob.glob(os.path.join(base, "*_seg[0-9]*.evt*"))):
        root = rootname(os.path.basename(path))
        if SEGMENT_RE.search(root) is None:
            continue
        parents = glob.glob(os.path.join(base, SEGMENT_RE.sub("", root) + ".evt*"))
        if parents:
            found.setdefault(sorted(parents)[0], []).append(path)
    events = {
        os.path.basename(parent): compare_events(parent, sorted(paths))
        for parent, paths in sorted(found.items())
    }

    merged = addspec_roundtrip(obsid, staged, families) if addspec else None

    return {
        "config": staged,
        "split": split,
        "spectra": spectra,
        "events": events,
        "merged": merged,
    }


def _report(result):
    """Print the comparisons, and say whether everything held. Returns an exit status."""
    ok = True

    print("\nSpectra -- do the segments sum to the parent, channel for channel?")
    for stem, c in result["spectra"].items():
        good = c["counts_match"] and c["exposure_match"]
        ok = ok and good
        print(
            f"  {'PASS' if good else 'FAIL'}  {stem}: "
            f"{c['segment_counts']} counts against {c['parent_counts']}, "
            f"{c['channels_wrong']} channel(s) wrong, "
            f"exposure {c['segment_exposure']:.3f} against {c['parent_exposure']:.3f}"
        )

    print("\nEvent lists -- are the segments' events exactly the parent's?")
    for stem, c in result["events"].items():
        good = c["times_match"] and c["gti_match"]
        ok = ok and good
        print(
            f"  {'PASS' if good else 'FAIL'}  {stem}: "
            f"{c['segment_events']} events against {c['parent_events']}, "
            f"good time {c['segment_gti']:.3f} against {c['parent_gti']:.3f}"
        )

    if result["merged"] is not None:
        print("\naddspec -- does co-adding the segments give the parent back?")
        for stem, c in result["merged"].items():
            if c is None:
                ok = False
                print(f"  FAIL  {stem}: no co-added spectrum was written")
                continue
            if not isinstance(c, dict):
                print(f"  ----  {stem}: {c}, nothing to co-add")
                continue
            good = c["counts_match"]
            ok = ok and good
            print(
                f"  {'PASS' if good else 'FAIL'}  {stem}: "
                f"{c['segment_counts']} counts against {c['parent_counts']}, "
                f"{c['channels_wrong']} channel(s) wrong, "
                f"exposure {c['segment_exposure']:.3f} against {c['parent_exposure']:.3f}"
            )

    print(f"\n{'Everything held.' if ok else 'Something did not hold.'}")
    return 0 if ok else 1


def main(argv=None):
    """
    ``hrp-check-roundtrip <out_data_path> <OBSID> <MJD> [<MJD> ...]``.

    Parameters
    ----------
    argv : list of str, optional
        Defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        Process exit status: 0 if every check passed.
    """
    parser = argparse.ArgumentParser(
        prog="hrp-check-roundtrip",
        description=__doc__.strip().splitlines()[0],
    )
    parser.add_argument("out_data_path", help="the pipeline's output directory")
    parser.add_argument("obsid", help="observation to check; must already be reduced")
    parser.add_argument(
        "mjd", nargs="+", type=float, help="where to cut; N of them give N + 1 segments"
    )
    parser.add_argument(
        "--workdir",
        default=None,
        help=(
            "where to put the copy the check works on; defaults to a directory beside "
            "the output tree. Keep it short -- HEASOFT truncates long file names"
        ),
    )
    parser.add_argument(
        "--utc",
        action="store_true",
        help="read the MJDs as UTC rather than in the files' own TIMESYS",
    )
    parser.add_argument(
        "--with-mode06",
        action="store_true",
        help=(
            "copy the split/ directory too, so the CHU-resolved spacecraft-science files "
            "are checked as well. Four times the nuproducts calls"
        ),
    )
    parser.add_argument(
        "--no-addspec",
        action="store_true",
        help="skip the co-adding half, leaving a check that needs no HEASOFT",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    import logging

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    out_data_path = os.path.abspath(args.out_data_path)
    workdir = args.workdir or os.path.join(os.path.dirname(out_data_path), "roundtrip")

    result = check_roundtrip(
        args.obsid,
        {"out_data_path": out_data_path},
        args.mjd,
        workdir,
        scale="utc" if args.utc else None,
        with_mode06=args.with_mode06,
        addspec=not args.no_addspec,
    )
    print(f"\nWorked in {result['config']['out_data_path']}")
    return _report(result)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
