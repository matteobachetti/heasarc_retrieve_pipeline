"""SVOM/ECLAIRs reduction: pairing, screening and joining one downlink pass's files.

All files are synthetic, written with the extensions, columns and header keywords of the
archive's ``ECL-EVT-CAL`` and ``ECL-GTI-CAL`` products.
"""

import os

import numpy as np
import pytest
from astropy.io import fits
from astropy.table import Table

from heasarc_retrieve_pipeline import svom_common
from heasarc_retrieve_pipeline import svom_eclairs as ecl
from heasarc_retrieve_pipeline.diagnostics import diagnostics_path, read_arrays, read_records
from heasarc_retrieve_pipeline.tests.test_svom_geometry import circular_orbit

OBS_ID = 1000000001

TIMING = dict(
    TELESCOP="SVOM",
    INSTRUME="ECL",
    TIMESYS="TT",
    TIMEREF="LOCAL",
    TIMEUNIT="s",
    MJDREFI=57754,
    MJDREFF=0.000800740741,
)


def identity(burst, pass_id):
    return dict(TIMING, OBS_ID=OBS_ID, BURST_ID=burst, PASS_ID=pass_id)


def write_events(
    path, times, burst="n.a.", pass_id="000001_HBX", flag=None, pi=None, span=None, blank=()
):
    """An ECLAIRs event file; ``span`` is (TSTART, TSTOP), the events' extent by default;
    the keywords in ``blank`` are written without a value, as the archive sometimes does."""
    times = np.asarray(times, dtype=float)
    n = times.size
    table = Table(
        dict(
            TIME=times,
            JDET=((2 * times).astype(int) % 80).astype(np.uint8),
            IDET=np.zeros(n, np.uint8),
            PHA=np.full(n, 100, np.uint16),
            PI=np.full(n, 50, np.uint16) if pi is None else np.asarray(pi, np.uint16),
            FLAG=np.zeros(n, np.uint8) if flag is None else np.asarray(flag, np.uint8),
        )
    )
    tstart, tstop = span if span is not None else (times.min(), times.max())
    header = dict(identity(burst, pass_id), TSTART=tstart, TSTOP=tstop, ONTIME=tstop - tstart)
    hdu = fits.table_to_hdu(table)
    hdu.name = ecl.EVENTS_EXTNAME
    hdu.header.update(header)
    for key in blank:
        hdu.header[key] = None
    fits.HDUList([fits.PrimaryHDU(header=fits.Header(header)), hdu]).writeto(path)
    return str(path)


def write_gtis(path, criteria, burst="n.a.", pass_id="000001_HBX"):
    """An ECLAIRs GTI file with one ``GTICAL-<name>`` extension per criterion given."""
    header = identity(burst, pass_id)
    hdus = [fits.PrimaryHDU(header=fits.Header(header))]
    group = fits.BinTableHDU(Table(dict(GTI_NAME=list(criteria))), name="ECL-GTI-CAL-GRP")
    hdus.append(group)
    for name, intervals in criteria.items():
        intervals = np.asarray(intervals, dtype=float).reshape(-1, 2)
        gti = fits.BinTableHDU(Table(dict(START=intervals[:, 0], STOP=intervals[:, 1])))
        gti.name = f"GTICAL-{name}"
        gti.header.update(header)
        hdus.append(gti)
    fits.HDUList(hdus).writeto(path)
    return str(path)


def all_good(start, stop, **others):
    """The criteria every archive file carries, all good over ``[start, stop]``."""
    good = [[start, stop]]
    return dict(dict(STA=good, TLM=good, EGP=good, PEO=good), **others)


def test_files_pair_by_burst_and_pass_and_an_orphan_is_reported(tmp_path):
    """Each event file goes with the GTI file of its own burst and pass, grouped by pass;
    an event file with no GTI file of its own is reported, and ``._`` files are ignored."""
    paths = [
        write_events(tmp_path / "e1.fits", [1.0], pass_id="P1"),
        write_gtis(tmp_path / "g1.fits", all_good(0, 2), pass_id="P1"),
        write_events(tmp_path / "e2.fits", [1.0], burst="sb1", pass_id="P1"),
        write_gtis(tmp_path / "g2.fits", all_good(0, 2), burst="sb1", pass_id="P1"),
        write_events(tmp_path / "e3.fits", [5.0], pass_id="P2"),
        write_gtis(tmp_path / "g3.fits", all_good(4, 6), pass_id="P2"),
        write_events(tmp_path / "e4.fits", [5.0], burst="sb2", pass_id="P2"),
    ]
    (tmp_path / "._e1.fits").write_bytes(b"\x00\x05\x16\x07")
    paths.append(str(tmp_path / "._e1.fits"))

    passes, unpaired = ecl.pair_event_and_gti_files(paths)

    names = {p: sorted(os.path.basename(e) for e, _ in pairs) for p, pairs in passes.items()}
    assert names == {"P1": ["e1.fits", "e2.fits"], "P2": ["e3.fits"]}
    assert dict(passes["P1"])[paths[2]] == paths[3]
    assert [os.path.basename(path) for path, _ in unpaired] == ["e4.fits"]


def test_archive_gti_is_telemetry_and_instrument_and_either_occultation_state(tmp_path):
    """Partial and no Earth occultation are joined, then cut by telemetry and instrument;
    stable attitude counts only when asked for."""
    path = write_gtis(
        tmp_path / "g.fits",
        dict(
            STA=[[0, 50]],
            TLM=[[0, 100]],
            EGP=[[10, 100]],
            NEO=[[0, 30]],
            PEO=[[30, 60], [80, 100]],
        ),
    )

    assert ecl.archive_gti(path).tolist() == [[10.0, 60.0], [80.0, 100.0]]
    assert ecl.archive_gti(path, extra_criteria=["STA"]).tolist() == [[10.0, 50.0]]


def test_a_gti_file_without_the_telemetry_criterion_is_refused(tmp_path):
    """Never seen missing; if it ever is, the file is skipped rather than guessed about."""
    path = write_gtis(tmp_path / "g.fits", dict(EGP=[[0, 10]], PEO=[[0, 10]]))

    with pytest.raises(ecl.MissingCriterion, match="TLM"):
        ecl.archive_gti(path)


def test_a_pass_keeps_unflagged_events_once_and_only_inside_every_cut(tmp_path):
    """Two burst files overlapping by 20 s and sharing those events are joined, unsorted
    input is sorted, flagged events and events outside the cuts go, and every removal is
    counted; the output's GTI lies inside the events' span."""
    first = np.r_[np.arange(0.5, 60.0), 5.25]
    flags = np.zeros(first.size, np.uint8)
    flags[[3, 4]] = 32
    second = np.arange(40.5, 100.0)[::-1]
    pairs = [
        (
            write_events(tmp_path / "e1.fits", first, burst="sb1", flag=flags, span=(0, 60)),
            write_gtis(tmp_path / "g1.fits", all_good(0, 60), burst="sb1"),
        ),
        (
            write_events(tmp_path / "e2.fits", second, burst="sb2", span=(40, 100)),
            write_gtis(tmp_path / "g2.fits", all_good(40, 100, PEO=[[40, 90]]), burst="sb2"),
        ),
    ]
    outfile = str(tmp_path / "pass.evt")

    written, summary = ecl.reduce_pass(pairs, outfile, cuts=dict(orbit=[[-50.0, 80.0]]))

    with fits.open(written) as hdul:
        times = hdul[ecl.EVENTS_EXTNAME].data["TIME"]
        gti = np.column_stack([hdul["GTI"].data["START"], hdul["GTI"].data["STOP"]])
        header = hdul[ecl.EVENTS_EXTNAME].header
    expected = np.setdiff1d(np.arange(0.5, 80.0), [3.5, 4.5])
    assert np.array_equal(times, np.sort(np.r_[expected, 5.25]))
    assert gti.tolist() == [[0.0, 80.0]]
    assert (header["TSTART"], header["TSTOP"], header["ONTIME"]) == (0.0, 80.0, 80.0)
    assert "BURST_ID" not in header
    assert summary["read"] == first.size + second.size
    assert summary["flagged"] == 2
    assert summary["duplicates"] == 20
    assert summary["outside_archive"] == 10
    assert summary["outside_gti"] == 10
    assert summary["written"] == times.size
    assert summary["exposure_archive"] == 90.0
    assert summary["exposure_orbit"] == 80.0


def test_blank_dead_time_keywords_give_an_exposure_equal_to_the_good_time(tmp_path):
    """Archive files can carry ``DEADC`` and ``EXPOSURE`` with no value; a blank ``DEADC``
    counts as 1, like a missing one, so ``EXPOSURE`` becomes the total good time."""
    pairs = [
        (
            write_events(
                tmp_path / "e.fits", np.arange(0.5, 10.0), span=(0, 10), blank=("DEADC", "EXPOSURE")
            ),
            write_gtis(tmp_path / "g.fits", all_good(0, 10)),
        )
    ]

    written, _ = ecl.reduce_pass(pairs, str(tmp_path / "pass.evt"))

    assert fits.getheader(written, ecl.EVENTS_EXTNAME)["EXPOSURE"] == 10.0


def test_without_a_position_the_observation_stops_before_barycentring(tmp_path, monkeypatch):
    """The flow writes one file per pass and a summary table, and only barycentres, with
    every orbit file of the observation, when it is given a position. Each pass leaves a
    screening record (events removed per cut, off-axis angles) and a barycentring one."""
    obsdir = tmp_path / str(OBS_ID)
    obsdir.mkdir()
    for pass_id, start in (("P1", 0.0), ("P2", 1000.0)):
        times = np.arange(start + 0.5, start + 500.0)
        write_events(obsdir / f"e{pass_id}.fits", times, pass_id=pass_id)
        write_gtis(obsdir / f"g{pass_id}.fits", all_good(start, start + 500), pass_id=pass_id)
    orbit = circular_orbit(np.arange(-10.0, 1600.0))
    attitude = Table(dict(TIME=orbit["TIME"], POINTING=np.zeros((len(orbit), 3))))
    for name, part in (("o1.fits", orbit[:800]), ("o2.fits", orbit[790:])):
        hdu = fits.table_to_hdu(part)
        hdu.name, hdu.header["TELESCOP"] = "SVO-ORB-CNV", "SVOM"
        fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(obsdir / name)
    hdu = fits.table_to_hdu(attitude)
    hdu.name = "SVO-ATT-CNV"
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(obsdir / "a.fits")
    calls = []

    def fake_barycentre(infile, orbit, outfile, ra=None, dec=None, ephem=None, clockfile=None):
        calls.append(dict(orbit=sorted(os.path.basename(o) for o in orbit), ra=ra))
        return outfile

    monkeypatch.setattr(svom_common, "barycenter_with_package", fake_barycentre)
    config = {"out_data_path": str(tmp_path)}

    diagnostics = diagnostics_path(str(OBS_ID), config)

    def records():
        return {(r["step"], r["key"]): r for r in read_records(diagnostics)}

    unbarycentred = ecl.process_eclairs_obsid.fn(str(OBS_ID), config=config)
    assert records()["barycenter", "P1"]["reason"] == "no position given"
    barycentred = ecl.process_eclairs_obsid.fn(str(OBS_ID), config=config, ra=0.0, dec=0.0)

    assert [os.path.basename(f) for f in unbarycentred] == [
        f"{OBS_ID}_P1_ecl.evt",
        f"{OBS_ID}_P2_ecl.evt",
    ]
    assert [os.path.basename(f) for f in barycentred] == [
        f"{OBS_ID}_P1_ecl_bary.evt",
        f"{OBS_ID}_P2_ecl_bary.evt",
    ]
    assert calls == [dict(orbit=["o1.fits", "o2.fits"], ra=0.0)] * 2
    summary = Table.read(os.path.join(os.path.dirname(unbarycentred[0]), f"{OBS_ID}_ecl.ecsv"))
    assert summary["pass"].tolist() == ["P1", "P2"]
    assert summary["written"].tolist() == [500, 500]
    screening = records()["svom_screening", "P1"]
    values = screening["values"]
    assert screening["status"] == "done"
    assert values["cuts"] == ["archive", "orbit", "orbit_jumps", "visible", "attitude", "offaxis"]
    assert sum(values[f"removed_{name}"] for name in values["cuts"][1:]) == values["outside_gti"]
    assert values["offaxis_max_deg"] == 0.0
    arrays = read_arrays(diagnostics, screening)
    assert {"gti_archive", "gti_offaxis"} <= set(arrays)
    assert np.allclose(np.diff(arrays["offaxis_time"]), 10.0)
    assert records()["barycenter", "P1"]["values"]["ephem"] == "DE430"
