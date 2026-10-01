#!/usr/bin/env python
"""
Compare the ``barycenter`` package with each mission's own tool, on real reductions.

For every case below an event file was barycentered by the pipeline with the mission's
own tool, and the settings it used are known from the output header's parameter history
or from the step's diagnostics record. This script barycenters the same input with the
``barycenter`` package and the same settings, through the pipeline's own
:func:`heasarc_retrieve_pipeline.barycenter.barycenter_with_package`, and reports how far
apart the two outputs are. The inputs are only read; outputs go to ``--outdir``.

Usage::

    python validate.py --outdir <scratch directory>

See README.md in this directory for what was found.
"""

import argparse
import os

import numpy as np
from astropy.io import fits

from heasarc_retrieve_pipeline.barycenter import barycenter_with_package

HOME = os.path.expanduser("~")
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
NUSTAR = os.path.join(REPO, "out_flares", "90901333002")
RXTE = os.path.join(HOME, "tmp", "m82_rxte", "scout")
XMM = os.path.join(HOME, "tmp", "m82_xmm", "0657801901")
CHANDRA = os.path.join(HOME, "tmp", "m82_acis", "5644")

#: M82 X-2, the position the XMM, Chandra and RXTE references were made at.
M82_X2 = (148.96267, 69.67931)

#: name: (input, orbit, reference, (ra, dec), ephemeris, clock file or None)
CASES = {
    "NuSTAR barycorr": (
        os.path.join(NUSTAR, "nu90901333002A01_cl_src1.evt"),
        os.path.join(NUSTAR, "event_pipe", "nu90901333002A.attorb"),
        os.path.join(NUSTAR, "nu90901333002A01_cl_src1_bary.evt"),
        # From barycorr's parameter history in the reference header.
        (148.95644714300073, 69.67831142068086),
        "DE430",
        # The CALDB clock file barycorr read, so that the clock is not the difference.
        os.path.join(HOME, "devel/CALDB/data/nustar/fpm/bcf/clock/nuCclock20100101v230.fits.gz"),
    ),
    "RXTE barycorr": (
        os.path.join(RXTE, "bary_test", "gx.evt"),
        os.path.join(RXTE, "94123-01-19-00", "orbit", "FPorbit_Day5510"),
        # The default-clock run. gx_bary.evt beside it was made with clockfile=NONE, which
        # for RXTE does skip tdc.dat, and so sits 59.6 us away from both.
        os.path.join(RXTE, "bary_test", "gx_bary_caldb.evt"),
        M82_X2,
        "DE430",
        None,
    ),
    "XMM barycen": (
        os.path.join(XMM, "event_cl", "xmm0657801901_pn_S003_imaging_cl.evt"),
        os.path.join(XMM, "PPS", "P0657801901OBX000ORBTSR0000.FTZ"),
        os.path.join(XMM, "event_cl", "xmm0657801901_pn_S003_imaging_cl_bary.evt"),
        M82_X2,
        "DE430",
        None,
    ),
    "Chandra axbary": (
        os.path.join(CHANDRA, "event_cl", "chandra05644_aciss_timed_cl.evt"),
        os.path.join(CHANDRA, "primary", "orbitf240581100N001_eph1.fits.gz"),
        os.path.join(CHANDRA, "event_cl", "chandra05644_aciss_timed_cl_bary.evt.gz"),
        M82_X2,
        "DE405",  # all axbary can do with refframe=ICRS
        None,
    ),
}


#: Extensions whose times the mission tools leave on spacecraft time and the package
#: corrects: NuSTAR's bad-pixel list and XMM's housekeeping. Nothing downstream reads them.
AUXILIARY = ("BADPIX", "HKAUX")


def _kind(key):
    """``STDGTI07/START`` -> ``STDGTI/START``: one row per kind of extension, not per CCD."""
    name, column = key.split("/")
    return name.rstrip("0123456789") + "/" + column


def _times(path):
    """Event times and GTI boundaries of a file, as one dictionary of arrays."""
    out = {}
    with fits.open(path) as hdul:
        for hdu in hdul[1:]:
            names = [n.upper() for n in (hdu.columns.names if hdu.columns else [])]
            if "TIME" in names:
                out[f"{hdu.name}/TIME"] = np.asarray(hdu.data.field("TIME"), dtype=float)
            if "START" in names and "STOP" in names:
                out[f"{hdu.name}/START"] = np.asarray(hdu.data.field("START"), dtype=float)
                out[f"{hdu.name}/STOP"] = np.asarray(hdu.data.field("STOP"), dtype=float)
    return out


def compare(ours, reference):
    """Differences, ours minus reference, in nanoseconds, column by column."""
    a, b = _times(ours), _times(reference)
    grouped, notes = {}, []
    for key in sorted(set(a) & set(b)):
        if key.startswith(AUXILIARY):
            notes.append(key)
            continue
        if len(a[key]) != len(b[key]):
            notes.append(f"{key} lengths {len(a[key])} vs {len(b[key])}")
            continue
        grouped.setdefault(_kind(key), []).append((a[key] - b[key]) * 1e9)
    rows = []
    for kind, diffs in grouped.items():
        diff = np.concatenate(diffs)
        diff = diff[np.isfinite(diff)]
        if len(diff):
            rows.append((kind, len(diff), np.mean(diff), np.max(np.abs(diff)), ""))
    if notes:
        rows.append(("(not compared)", "", None, None, "; ".join(n.split("/")[0] for n in notes)))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    print("| case | column | n | mean (ns) | max abs (ns) | note |")
    print("|---|---|---|---|---|---|")
    outputs = {}
    for name, (infile, orbit, reference, (ra, dec), ephem, clock) in CASES.items():
        missing = [p for p in (infile, orbit, reference) if not os.path.exists(p)]
        if missing:
            print(f"| {name} | | | | | missing: {', '.join(missing)} |")
            continue
        outfile = os.path.join(args.outdir, name.replace(" ", "_") + "_bary.evt")
        outputs[name] = barycenter_with_package(
            infile, orbit, outfile, ra=ra, dec=dec, ephem=ephem, clockfile=clock
        )
        for key, n, mean, worst, note in compare(outfile, reference):
            mean_s = "" if mean is None else f"{mean:+.1f}"
            worst_s = "" if worst is None else f"{worst:.1f}"
            print(f"| {name} | {key} | {n} | {mean_s} | {worst_s} | {note} |")

    # The figure the Chandra module quotes for staying on axbary's DE405: is DE430 minus
    # DE405 really a constant 0.377 us on this observation?
    if "Chandra axbary" in outputs:
        infile, orbit, _, (ra, dec), _, _ = CASES["Chandra axbary"]
        de430 = barycenter_with_package(
            infile, orbit, os.path.join(args.outdir, "Chandra_DE430_bary.evt"), ra, dec, "DE430"
        )
        key = "EVENTS/TIME"
        diff = (_times(de430)[key] - _times(outputs["Chandra axbary"])[key]) * 1e6
        print(
            f"\nChandra 5644, DE430 minus DE405: mean {np.mean(diff):+.4f} us, "
            f"peak-to-peak {np.ptp(diff):.4f} us over {len(diff)} events"
        )


if __name__ == "__main__":
    main()
