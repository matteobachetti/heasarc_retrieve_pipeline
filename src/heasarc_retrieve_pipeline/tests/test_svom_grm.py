"""SVOM/GRM reduction: one file per detector, selected, cut and dead-time corrected.

All files are synthetic, written with the extensions, columns and header keywords of the
archive's ``GRM-EVT`` product.
"""

import os

import numpy as np
from astropy.io import fits
from astropy.table import Table

from heasarc_retrieve_pipeline import svom_common
from heasarc_retrieve_pipeline import svom_grm as grm
from heasarc_retrieve_pipeline.tests.test_svom_geometry import circular_orbit

OBS_ID = 1000000001

TIMING = dict(
    TELESCOP="SVOM",
    INSTRUME="GRM",
    TIMESYS="TT",
    TIMEREF="LOCAL",
    TIMEUNIT="s",
    MJDREFI=57754,
    MJDREFF=0.000800740741,
    OBS_ID=OBS_ID,
    BURST_ID="n.a.",
)

#: Five channels; 20-300 keV is channels 1 and 2.
E_MIN = [5.0, 20.0, 100.0, 300.0, 1000.0]
E_MAX = [20.0, 100.0, 300.0, 1000.0, 20000.0]


def write_grm(path, times, span, gti=None, columns=None):
    """A GRM event file: the same events in all three detectors unless ``columns`` (a dict
    of column overrides per detector number) says otherwise."""
    times = np.asarray(times, dtype=float)
    n = times.size
    header = dict(TIMING, TSTART=span[0], TSTOP=span[1])
    ebounds = Table(dict(CHANNEL=np.arange(5, dtype=np.int16), E_MIN=E_MIN, E_MAX=E_MAX))
    hdus = [
        fits.PrimaryHDU(header=fits.Header(header)),
        fits.BinTableHDU(ebounds, name="EBOUNDS"),
    ]
    gti = np.asarray([span] if gti is None else gti, dtype=float)
    hdus.append(fits.BinTableHDU(Table(dict(START=gti[:, 0], STOP=gti[:, 1])), name="GTI"))
    for detector in (1, 2, 3):
        table = Table(
            dict(
                TIME=times,
                PI=np.ones(n, np.int16),
                GAIN_TYPE=np.zeros(n, np.uint8),
                DEAD_TIME=np.full(n, 10.0, np.float32),
                EVT_TYPE=np.zeros(n, np.uint8),
                ANTI_COIN=np.zeros(n, np.uint8),
                FLAG=np.zeros(n, np.uint8),
            )
        )
        for name, values in (columns or {}).get(detector, {}).items():
            table[name] = np.asarray(values, dtype=table[name].dtype)
        hdu = fits.table_to_hdu(table)
        hdu.name = f"EVENTS{detector:02d}"
        hdu.header.update(header)
        hdu.header["DETNAM"] = f"GRD_{detector:02d}"
        hdus.append(hdu)
    for hdu in hdus[1:3]:
        hdu.header.update(header)
    fits.HDUList(hdus).writeto(path)
    return str(path)


def test_the_band_is_the_channels_entirely_inside_it():
    """A channel counts only if both its edges are inside the band."""
    ebounds = Table(dict(CHANNEL=np.arange(5), E_MIN=E_MIN, E_MAX=E_MAX))

    assert grm.band_channels(ebounds, (20.0, 300.0)).tolist() == [1, 2]
    assert grm.band_channels(ebounds, (15.0, 400.0)).tolist() == [1, 2]


def test_a_detector_keeps_good_in_band_events_and_counts_every_events_dead_time(tmp_path):
    """Flagged, overflow, calibration and out-of-band events are dropped, but their dead
    time still counts against the live time inside the final GTI."""
    times = np.arange(0.5, 100.0)
    pi = np.ones(times.size)
    pi[[0, 1]] = 0, 4
    flags = dict(FLAG=np.zeros(times.size), EVT_TYPE=np.zeros(times.size))
    flags["FLAG"][2], flags["EVT_TYPE"][3] = 1, 1
    anti = np.zeros(times.size)
    anti[4] = 1
    path = write_grm(
        tmp_path / "grm.fits",
        times,
        span=(0.0, 100.0),
        columns={1: dict(PI=pi, ANTI_COIN=anti, **flags)},
    )
    outfile = str(tmp_path / "grd1.evt")

    written, summary = grm.reduce_detector([path], 1, outfile, cuts=dict(orbit=[[0.0, 50.0]]))

    with fits.open(written) as hdul:
        events = hdul["EVENTS"]
        assert events.header["DETNAM"] == "GRD_01"
        assert np.array_equal(events.data["TIME"], times[5:50])
        assert [hdul["GTI"].data["START"][0], hdul["GTI"].data["STOP"][0]] == [0.0, 50.0]
        assert events.header["ONTIME"] == 50.0
        assert np.isclose(events.header["LIVETIME"], 50.0 - 50 * 10e-6)
        assert "EBOUNDS" in hdul
    assert (summary["read"], summary["rejected"], summary["out_of_band"]) == (100, 3, 2)
    assert (summary["outside_gti"], summary["written"]) == (50, 45)
    assert np.isclose(summary["dead_time"], 50 * 10e-6)


def test_overlapping_files_give_each_event_and_its_dead_time_once(tmp_path):
    """Two files sharing 20 s of identical events: the shared events, and their dead
    time, are counted once."""
    first = write_grm(tmp_path / "a.fits", np.arange(0.5, 60.0), span=(0.0, 60.0))
    second = write_grm(tmp_path / "b.fits", np.arange(40.5, 100.0), span=(40.0, 100.0))

    written, summary = grm.reduce_detector([first, second], 2, str(tmp_path / "grd2.evt"))

    assert np.array_equal(fits.getdata(written, "EVENTS")["TIME"], np.arange(0.5, 100.0))
    assert summary["duplicates"] == 20
    assert np.isclose(summary["dead_time"], 100 * 10e-6)


def test_each_detector_gets_its_own_offaxis_cut_and_is_barycentred(tmp_path, monkeypatch):
    """Detectors 1 and 3 face the source and detector 2 faces away: two files, each
    barycentred with every orbit file, and a summary row per detector."""
    obsdir = tmp_path / str(OBS_ID)
    obsdir.mkdir()
    write_grm(obsdir / "grm.fits", np.arange(0.5, 1000.0), span=(0.0, 1000.0))
    orbit = circular_orbit(np.arange(-10.0, 1100.0))
    n = len(orbit)
    facing, away = np.zeros((n, 2)), np.column_stack([np.full(n, 180.0), np.zeros(n)])
    attitude = Table(dict(TIME=orbit["TIME"], GRDX_1=facing, GRDX_2=away, GRDX_3=facing))
    for name, extname, table in (
        ("o1.fits", "SVO-ORB-CNV", orbit[:600]),
        ("o2.fits", "SVO-ORB-CNV", orbit[590:]),
        ("a.fits", "SVO-ATT-CNV", attitude),
    ):
        hdu = fits.table_to_hdu(table)
        hdu.name, hdu.header["TELESCOP"] = extname, "SVOM"
        fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(obsdir / name)
    calls = []

    def fake_barycentre(infile, orbit, outfile, ra=None, dec=None, ephem=None, clockfile=None):
        calls.append(sorted(os.path.basename(o) for o in orbit))
        return outfile

    monkeypatch.setattr(svom_common, "barycenter_with_package", fake_barycentre)

    out = grm.process_grm_obsid.fn(
        str(OBS_ID), config={"out_data_path": str(tmp_path)}, ra=0.0, dec=0.0
    )

    assert [os.path.basename(f) for f in out] == [
        f"{OBS_ID}_grd1_bary.evt",
        f"{OBS_ID}_grd3_bary.evt",
    ]
    assert calls == [["o1.fits", "o2.fits"]] * 2
    summary = Table.read(os.path.join(os.path.dirname(out[0]), f"{OBS_ID}_grm.ecsv"))
    assert summary["detector"].tolist() == [1, 2, 3]
    assert summary["written"].tolist() == [1000, 0, 1000]
