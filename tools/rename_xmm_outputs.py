"""
Rename an existing XMM reduction to the ``xmm<OBSID>_<camera>_<expid>_<mode>`` convention.

A one-off migration for trees reduced before commit f8534d9. Three things move together,
and doing only the first would leave the tree broken:

1. The files in ``event_cl/`` and ``products/`` are renamed.
2. ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE`` in every spectrum are rewritten to the new
   names. These are pointers XSPEC follows; a rename without this step gives spectra that
   load with no background and no response.
3. The file names recorded in ``diagnostics/*.json`` are rewritten.

Deliberately *not* touched: SAS's own ``XPROC*``, ``XDAL*`` and ``SLCTEXPR`` provenance
cards, which record the command line that made each file. Those are history -- nothing
reads them to find a file, and they already name the scratch directory the original run
used. Rewriting them would claim ``evselect`` ran with a name that did not yet exist.

Also not touched: the diagnostics *keys* (``pnS003_imaging``), the log file names built
from them, and the ``spec_pnS003_imaging_src_rate`` array names. Under the new convention
those stay short on purpose -- they are read inside the observation's own directory.
"""

import argparse
import glob
import json
import os
import re

#: Basenames in ``event_cl/`` and ``products/`` that belong to one exposure.
OLD_NAME_RE = re.compile(
    r"^(?P<instrument>pn|mos1|mos2)(?P<expid>[SU]\d{3})_(?P<mode>imaging|timing)(?P<rest>[._].*)$"
)

#: Keywords that point at another file by name, and so must follow a rename.
POINTER_KEYWORDS = ("BACKFILE", "RESPFILE", "ANCRFILE")

#: Diagnostics record fields holding a file name. Not ``key`` or ``arrays``: those are the
#: short in-observation identifier, which the new convention keeps as it is.
DIAGNOSTICS_NAME_FIELDS = (
    "arf",
    "rmf",
    "source_spectrum",
    "background_spectrum",
    "grouped_spectrum",
    "barycentered_file",
    "barycentered_source_file",
    "plot",
)


def new_name(obsid, basename):
    """The new basename, or ``None`` when this file is not one exposure's product."""
    match = OLD_NAME_RE.match(basename)
    if match is None:
        return None
    part = match.groupdict()
    return f"xmm{obsid}_{part['instrument']}_{part['expid']}_{part['mode']}{part['rest']}"


def planned_renames(root):
    """``{obsid: {old basename: new basename}}`` over every observation under *root*."""
    plan = {}
    for obsid in sorted(os.listdir(root)):
        if not os.path.isdir(os.path.join(root, obsid)):
            continue
        names = {}
        for directory in ("event_cl", "products"):
            path = os.path.join(root, obsid, directory)
            if not os.path.isdir(path):
                continue
            for basename in sorted(os.listdir(path)):
                if not os.path.isfile(os.path.join(path, basename)):
                    continue
                renamed = new_name(obsid, basename)
                if renamed is not None:
                    names[basename] = renamed
        if names:
            plan[obsid] = names
    return plan


def rename_files(root, obsid, names, dry_run):
    """Move the files. Refuses to land on a name that already exists."""
    moved = 0
    for directory in ("event_cl", "products"):
        path = os.path.join(root, obsid, directory)
        if not os.path.isdir(path):
            continue
        for old, new in names.items():
            source = os.path.join(path, old)
            target = os.path.join(path, new)
            if not os.path.exists(source):
                continue
            if os.path.exists(target):
                raise FileExistsError(target)
            if not dry_run:
                os.rename(source, target)
            moved += 1
    return moved


def rewrite_pointers(root, obsid, names, dry_run):
    """Point every spectrum's ``BACKFILE``/``RESPFILE``/``ANCRFILE`` at the new names."""
    from astropy.io import fits

    changed = 0
    for path in sorted(glob.glob(os.path.join(root, obsid, "products", "*.pi"))):
        updates = {}
        with fits.open(path) as hdul:
            for index, hdu in enumerate(hdul):
                for keyword in POINTER_KEYWORDS:
                    value = hdu.header.get(keyword)
                    if isinstance(value, str) and value in names:
                        updates[(index, keyword)] = names[value]
        if not updates:
            continue
        changed += len(updates)
        if dry_run:
            for (index, keyword), value in updates.items():
                print(f"      {os.path.basename(path)} HDU{index} {keyword} -> {value}")
            continue
        with fits.open(path, mode="update") as hdul:
            for (index, keyword), value in updates.items():
                hdul[index].header[keyword] = value
    return changed


def rewrite_diagnostics(root, obsid, names, dry_run):
    """Rewrite the file names recorded in the diagnostics, leaving the keys alone."""
    changed = 0
    for path in sorted(glob.glob(os.path.join(root, obsid, "diagnostics", "*.json"))):
        with open(path) as handle:
            record = json.load(handle)
        values = record.get("values")
        if not isinstance(values, dict):
            continue
        touched = False
        for field in DIAGNOSTICS_NAME_FIELDS:
            value = values.get(field)
            if isinstance(value, str) and value in names:
                values[field] = names[value]
                touched = True
                changed += 1
        if touched and not dry_run:
            with open(path, "w") as handle:
                json.dump(record, handle, indent=1)
    return changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", help="Directory holding the observation directories.")
    parser.add_argument("--dry-run", action="store_true", help="Say what would happen.")
    args = parser.parse_args()

    plan = planned_renames(args.root)
    totals = [0, 0, 0]
    for obsid, names in plan.items():
        # Pointers and records are read before the move and written after, so both see the
        # old names as keys either way.
        pointers = rewrite_pointers(args.root, obsid, names, dry_run=True) if args.dry_run else 0
        moved = rename_files(args.root, obsid, names, args.dry_run)
        if not args.dry_run:
            pointers = rewrite_pointers(args.root, obsid, names, args.dry_run)
        records = rewrite_diagnostics(args.root, obsid, names, args.dry_run)
        totals = [totals[0] + moved, totals[1] + pointers, totals[2] + records]
        print(f"{obsid}: {moved} files, {pointers} header pointers, {records} diagnostics fields")

    verb = "would rename" if args.dry_run else "renamed"
    print(
        f"\n{len(plan)} observations: {verb} {totals[0]} files, "
        f"{totals[1]} header pointers, {totals[2]} diagnostics fields"
    )


if __name__ == "__main__":
    main()
