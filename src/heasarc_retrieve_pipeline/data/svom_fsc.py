"""
Search and download SVOM products at the French Science Center (FSC).

Run by :mod:`heasarc_retrieve_pipeline.svom`, with the Python of the environment that has
the SVOM team's ``svom.messaging`` (the ``eclair`` environment, Python 3.12), never
imported by the package. It imports nothing from the package, so it works in an
environment where the package is not installed. Credentials and ``FSC_URL`` come from the
environment, set up by the caller.

Two commands, both printing JSON on standard output:

``search ACRONYM [KEY=VALUE ...] [--tmin T] [--tmax T]``
    Every product with this acronym whose primary header matches, and, with
    ``--tmin``/``--tmax``, whose ``TSTART``-``TSTOP`` overlaps that interval (SVOM
    seconds). A list of ``{"acronym", "url", "header"}``.
``get ACRONYM OUTDIR FILENAME KEY=VALUE [...]``
    Download the latest product matching the criteria to ``OUTDIR/FILENAME``. Prints
    ``{"path": ...}``.
"""

import argparse
import json
import os
import sys

#: Primary-header keywords worth passing back. The rest of the header is not needed.
KEYWORDS = (
    "CARD",
    "OBS_ID",
    "BURST_ID",
    "PASS_ID",
    "TSTART",
    "TSTOP",
    "PROGRAM",
    "PROC_VER",
    "INSTRUME",
)

#: Products per page of a search. The archive's own default is 100; every page is asked
#: for until one comes back short.
PAGE_SIZE = 100


def criteria(pairs, tmin=None, tmax=None):
    """``KEY=VALUE`` pairs, and the overlap with ``[tmin, tmax]``, as archive criteria."""
    crit = [f"PrimaryHDU,{pair}" for pair in pairs]
    if tmax is not None:
        crit.append(f"PrimaryHDU,TSTART<={tmax}")
    if tmin is not None:
        crit.append(f"PrimaryHDU,TSTOP>={tmin}")
    return crit


def client():
    from svom.messaging.sdbio import SdbIo

    return SdbIo(fsc_url=os.environ.get("FSC_URL"), max_tries=5)


def search(acronym, crit):
    sdb = client()
    found, page = [], 1
    while True:
        answer = sdb.search(acronym=acronym, criteria=crit, size=PAGE_SIZE, page=page).json()
        for product in answer:
            header = product.get("criteria", {}).get("PrimaryHDU", {})
            found.append(
                dict(
                    acronym=acronym,
                    url=product.get("url"),
                    header={key: header.get(key) for key in KEYWORDS},
                )
            )
        if len(answer) < PAGE_SIZE:
            return found
        page += 1


def get(acronym, outdir, filename, crit):
    os.makedirs(outdir, exist_ok=True)
    client().save_latest_fits_file(
        dirname=outdir, filename=filename, acronym=acronym, criteria=crit, timeout=1800
    )
    return dict(path=os.path.join(outdir, filename))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    commands = parser.add_subparsers(dest="command", required=True)
    find = commands.add_parser("search")
    find.add_argument("acronym")
    find.add_argument("pairs", nargs="*")
    find.add_argument("--tmin", type=float, default=None)
    find.add_argument("--tmax", type=float, default=None)
    fetch = commands.add_parser("get")
    fetch.add_argument("acronym")
    fetch.add_argument("outdir")
    fetch.add_argument("filename")
    fetch.add_argument("pairs", nargs="+")
    args = parser.parse_args(argv)

    if args.command == "search":
        result = search(args.acronym, criteria(args.pairs, args.tmin, args.tmax))
    else:
        result = get(args.acronym, args.outdir, args.filename, criteria(args.pairs))
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()
