import os
import re

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import xrism
from heasarc_retrieve_pipeline.xrism import (
    barycentre_resolve,
    find_xrism_inputs,
    process_xrism_obsid,
    reduce_observation,
    resolve_screened_events,
    xrism_base_output_path,
    xrism_download_filter,
)


CONFIG = {"out_data_path": "out"}
OBSID = "101002010"

# The whole archive listing of 101002010 (the Crab, 2024-10-03) except log/ and xtend/,
# recorded on 2026-10-11.
LISTING_101002010 = [
    "auxil/xa101002010.att.gz",
    "auxil/xa101002010.cat.gz",
    "auxil/xa101002010.com.gz",
    "auxil/xa101002010.ehk.gz",
    "auxil/xa101002010.mkf.gz",
    "auxil/xa101002010.orb.gz",
    "auxil/xa101002010.tim.gz",
    "auxil/xa101002010_fff.cat",
    "auxil/xa101002010_gen.gti.gz",
    "auxil/xa101002010gen_a0.hk.gz",
    "resolve/event_cl/xa101002010rsl_p0px0000_cl.evt.gz",
    "resolve/event_cl/xa101002010rsl_p0px1000_cl.evt.gz",
    "resolve/event_uf/xa101002010rsl_a0pxpr_uf.evt.gz",
    "resolve/event_uf/xa101002010rsl_el.gti.gz",
    "resolve/event_uf/xa101002010rsl_p0px1000_uf.evt.gz",
    "resolve/event_uf/xa101002010rsl_px1000_exp.gti.gz",
    "resolve/hk/xa101002010rsl_a0.hk1.gz",
    "resolve/products/xa101002010rsl_p0px1000_src.lc.gz",
    "resolve/products/xa101002010rsl_p0px1000_sky.img.gz",
    "xtend/event_cl/xa101002010xtd_p031200010_cl.evt.gz",
]
KEPT_101002010 = [
    "auxil/xa101002010.orb.gz",
    "resolve/event_cl/xa101002010rsl_p0px0000_cl.evt.gz",
    "resolve/event_cl/xa101002010rsl_p0px1000_cl.evt.gz",
]


def what_a_filter_keeps(arguments, entries, prefix="xrism/data/obs/1/101002010/"):
    """
    Run a download filter over a recorded listing, the way a transport would: against the
    whole remote name, as an HTTPS URL and as an S3 key, which must agree. This is
    test_rxte.py's helper of the same name, over XRISM's archive paths.
    """
    include = re.compile(arguments["re_include"])
    kept = {}
    for flavour, base in [
        ("https", "https://heasarc.gsfc.nasa.gov/FTP/" + prefix),
        ("s3", prefix),
    ]:
        kept[flavour] = [entry for entry in entries if include.search(base + entry)]
    assert kept["https"] == kept["s3"], "the filter reads an S3 key and a URL differently"
    return kept["s3"]


class TestXrismPaths:
    def test_the_output_directory_is_the_obsid_under_out_data_path(self):
        assert xrism_base_output_path(config=CONFIG, obsid=OBSID) == os.path.join("out", OBSID)


class TestTheDownloadFilter:
    def test_it_keeps_exactly_what_the_reduction_reads(self):
        """The orbit file and every cleaned Resolve event file; not the unfiltered events,
        the 33 MB attitude file, the products or anything from Xtend."""
        kept = what_a_filter_keeps(xrism_download_filter({}), LISTING_101002010)

        assert kept == KEPT_101002010

    def test_uncompressed_copies_are_kept_too(self):
        """A local or SciServer copy may have been gunzipped."""
        entries = ["auxil/xa101002010.orb", "resolve/event_cl/xa101002010rsl_p0px1000_cl.evt"]

        assert what_a_filter_keeps(xrism_download_filter({}), entries) == entries


class TestFindingTheInputs:
    def make_observation(self, tmp_path, names=KEPT_101002010):
        root = tmp_path / OBSID
        for name in names:
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"")
        return root

    def test_every_cleaned_file_and_the_orbit_are_found(self, tmp_path):
        found = find_xrism_inputs(str(self.make_observation(tmp_path)))

        assert [os.path.basename(f) for f in found["event_files"]] == [
            "xa101002010rsl_p0px0000_cl.evt.gz",
            "xa101002010rsl_p0px1000_cl.evt.gz",
        ]
        assert os.path.basename(found["orbit_file"]) == "xa101002010.orb.gz"

    def test_an_observation_without_resolve_data_is_reported_not_raised(self, tmp_path):
        """Calibration pointings such as 000001150 have only unfiltered Resolve events."""
        found = find_xrism_inputs(
            str(self.make_observation(tmp_path, ["auxil/xa000001150.orb.gz"]))
        )

        assert found["event_files"] == []

    def test_the_macos_sidecar_files_are_not_mistaken_for_data(self, tmp_path):
        """Copies on an exFAT drive get a ``._`` sidecar next to every file."""
        root = self.make_observation(tmp_path)
        (root / "resolve" / "event_cl" / "._xa101002010rsl_p0px1000_cl.evt.gz").write_bytes(b"")
        (root / "auxil" / "._xa101002010.orb.gz").write_bytes(b"")

        found = find_xrism_inputs(str(root))

        assert len(found["event_files"]) == 2
        assert os.path.basename(found["orbit_file"]) == "xa101002010.orb.gz"


def write_resolve_file(path, times, itypes, pixels, gti, filt="OPEN", exposure=None, pis=None):
    """A minimal cleaned Resolve event file: the columns and keywords the merge reads.
    PI defaults to 4000, 2 keV."""
    pis = np.full(len(times), 4000) if pis is None else pis
    status = np.zeros((len(times), 16), dtype=bool)
    status[:, 3] = True
    events = fits.BinTableHDU.from_columns(
        [
            fits.Column("TIME", "D", unit="s", array=times),
            fits.Column("ITYPE", "B", array=itypes),
            fits.Column("PIXEL", "B", array=pixels),
            fits.Column("PI", "J", array=pis),
            fits.Column("STATUS", "16X", array=status),
        ],
        name="EVENTS",
    )
    gti = np.asarray(gti, dtype=float)
    gti_hdu = fits.BinTableHDU.from_columns(
        [fits.Column("START", "D", array=gti[:, 0]), fits.Column("STOP", "D", array=gti[:, 1])],
        name="GTI",
    )
    for header in (events.header, gti_hdu.header):
        header.update(TELESCOP="XRISM", INSTRUME="RESOLVE", TIMESYS="TT", MJDREFI=58484)
        header.update(MJDREFF=0.000800740740741, TIMEUNIT="s", TSTART=gti[0, 0])
        header["TSTOP"] = gti[-1, 1]
    events.header["FILTER"] = filt
    events.header["EXPOSURE"] = exposure if exposure is not None else float(np.ptp(gti))
    events.header["CHECKSUM"] = "stale"
    fits.HDUList([fits.PrimaryHDU(), events, gti_hdu]).writeto(path)
    return str(path)


class TestMergingTheCleanedFiles:
    def make_crab_like(self, tmp_path):
        """Two files shaped like 101002010's: a science one with a gap, and the short
        filter-in-motion one that fills it exactly."""
        science = write_resolve_file(
            tmp_path / "px1000.evt",
            [1.0, 2.0, 30.0, 31.0],
            [0, 4, 3, 1],
            [0, 35, 12, 5],
            [[0.0, 10.0], [20.0, 40.0]],
            exposure=29.0,
        )
        undef = write_resolve_file(
            tmp_path / "px0000.evt", [15.0], [2], [7], [[10.0, 20.0]], filt="UNDEF"
        )
        return [science, undef]

    def test_events_are_merged_in_time_order_without_the_calibration_pixel(self, tmp_path):
        out = resolve_screened_events(self.make_crab_like(tmp_path), str(tmp_path / "cl.evt"))

        with fits.open(out) as hdul:
            assert list(hdul["EVENTS"].data["TIME"]) == [1.0, 2.0, 15.0, 31.0]
            assert list(hdul["GTI"].data["START"]) == [0.0]
            assert list(hdul["GTI"].data["STOP"]) == [40.0]

    def test_bit_array_columns_survive_the_merge(self, tmp_path):
        """A plain numpy concatenation turns the 16X STATUS flags into raw bytes."""
        out = resolve_screened_events(self.make_crab_like(tmp_path), str(tmp_path / "cl.evt"))

        status = fits.getdata(out, "EVENTS")["STATUS"]
        assert status.shape == (4, 16)
        assert status[:, 3].all() and not status[:, 4].any()

    def test_the_exposure_keywords_describe_the_merged_file(self, tmp_path):
        """EXPOSURE adds up the inputs', ONTIME is the joined intervals, and no stale
        checksum is carried over from the first file."""
        out = resolve_screened_events(self.make_crab_like(tmp_path), str(tmp_path / "cl.evt"))

        header = fits.getheader(out, "EVENTS")
        assert header["EXPOSURE"] == pytest.approx(29.0 + 10.0)
        assert header["ONTIME"] == pytest.approx(40.0)
        assert (header["TSTART"], header["TSTOP"]) == (0.0, 40.0)
        assert header["FILTER"] == "OPEN,UNDEF"
        assert "CHECKSUM" not in header
        assert fits.getheader(out, "GTI")["TIMESYS"] == "TT"

    def test_grades_can_be_chosen(self, tmp_path):
        out = resolve_screened_events(
            self.make_crab_like(tmp_path), str(tmp_path / "cl.evt"), itypes=[0, 1]
        )

        assert list(fits.getdata(out, "EVENTS")["TIME"]) == [1.0, 31.0]
        assert fits.getheader(out, "EVENTS")["ITYPES"] == "0,1"

    def test_an_energy_band_keeps_the_events_inside_it(self, tmp_path):
        """PI counts 0.5 eV per channel; the band includes its lower edge, excludes its upper
        one, and is recorded in the header."""
        a = write_resolve_file(
            tmp_path / "a.evt",
            [1.0, 2.0, 3.0, 4.0],
            [0] * 4,
            [0] * 4,
            [[0.0, 10.0]],
            pis=[3000, 4000, 10000, 16000],
        )

        out = resolve_screened_events([a], str(tmp_path / "cl.evt"), energy_band=(2.0, 8.0))

        assert list(fits.getdata(out, "EVENTS")["PI"]) == [4000, 10000]
        header = fits.getheader(out, "EVENTS")
        assert (header["EMIN_KEV"], header["EMAX_KEV"]) == (2.0, 8.0)

    def test_an_empty_energy_band_is_refused(self, tmp_path):
        """A band given upside down would otherwise silently keep nothing."""
        a = write_resolve_file(tmp_path / "a.evt", [1.0], [0], [0], [[0.0, 10.0]])

        with pytest.raises(ValueError, match="band"):
            resolve_screened_events([a], str(tmp_path / "cl.evt"), energy_band=(8.0, 2.0))

    def test_files_that_overlap_in_time_are_refused(self, tmp_path):
        """Merging them would count the shared stretch twice."""
        a = write_resolve_file(tmp_path / "a.evt", [1.0], [0], [0], [[0.0, 10.0]])
        b = write_resolve_file(tmp_path / "b.evt", [9.0], [0], [0], [[9.0, 20.0]])

        with pytest.raises(ValueError, match="overlap"):
            resolve_screened_events([a, b], str(tmp_path / "cl.evt"))

    def test_nothing_surviving_is_reported_as_none(self, tmp_path):
        a = write_resolve_file(tmp_path / "a.evt", [1.0], [0], [12], [[0.0, 10.0]])

        assert resolve_screened_events([a], str(tmp_path / "cl.evt")) is None


def fake_barycen(calls, timesys="TDB"):
    """Stand-in for heasoft.run: records the call and writes a file on ``timesys``."""

    def run(name, *, produces, **params):
        calls.append((name, params))
        write_resolve_file(params["outfile"], [1.0], [0], [0], [[0.0, 2.0]])
        with fits.open(params["outfile"], mode="update") as hdul:
            hdul["EVENTS"].header["TIMESYS"] = timesys

    return run


def fake_package(calls, timesys="TDB"):
    """Stand-in for barycenter_with_package: records the call and writes a file on ``timesys``."""

    def run(infile, orbit, outfile, ra=None, dec=None, ephem=None, clockfile=None):
        calls.append(dict(infile=infile, orbit=orbit, ra=ra, dec=dec, ephem=ephem))
        fake_barycen([], timesys)("barycenter", produces=outfile, outfile=outfile)
        return outfile

    return run


OFFICIAL = {"barycenter_tool": "official"}


class TestBarycentring:
    def test_the_package_gets_the_position_and_the_configured_ephemeris(
        self, tmp_path, monkeypatch
    ):
        """By default the package barycentres, at the searched position and on DE430, the
        ephemeris every other mission uses."""
        calls = []
        monkeypatch.setattr(xrism, "barycenter_tool", lambda config: "barycenter")
        monkeypatch.setattr(xrism, "barycenter_with_package", fake_package(calls))

        out = barycentre_resolve(
            "in_cl.evt", "xa1.orb.gz", 83.633218, 22.014464, outfile=str(tmp_path / "bary.evt")
        )

        (call,) = calls
        assert (call["ra"], call["dec"]) == (83.633218, 22.014464)
        assert call["orbit"] == "xa1.orb.gz"
        assert call["ephem"] == "DE430"
        assert out == str(tmp_path / "bary.evt")

    def test_barycen_gets_the_position_and_the_fk5_frame(self, tmp_path, monkeypatch):
        """The fallback runs barycen in FK5, which is DE200: the only frame whose output says
        truthfully which ephemeris it used."""
        calls = []
        monkeypatch.setattr(xrism.heasoft, "run", fake_barycen(calls))

        barycentre_resolve(
            "in_cl.evt",
            "xa1.orb.gz",
            83.633218,
            22.014464,
            outfile=str(tmp_path / "b.evt"),
            config=OFFICIAL,
        )

        ((name, params),) = calls
        assert name == "barycen"
        assert (params["ra"], params["dec"]) == (83.633218, 22.014464)
        assert params["orbfile"] == "xa1.orb.gz"
        assert params["refframe"] == "FK5"

    @pytest.mark.parametrize("tool", ["barycenter", "official"])
    def test_a_file_left_on_spacecraft_time_is_an_error(self, tmp_path, monkeypatch, tool):
        """Either tool reporting success on a file still on TT must not pass for barycentred."""
        monkeypatch.setattr(xrism, "barycenter_tool", lambda config: tool)
        monkeypatch.setattr(xrism, "barycenter_with_package", fake_package([], timesys="TT"))
        monkeypatch.setattr(xrism.heasoft, "run", fake_barycen([], timesys="TT"))

        with pytest.raises(RuntimeError, match="TT"):
            barycentre_resolve("in.evt", "o.orb", 0.0, 0.0, outfile=str(tmp_path / "b.evt"))

    def test_asking_barycen_for_another_ephemeris_warns(self, tmp_path, monkeypatch, caplog):
        """barycen cannot do DE430, and a run that asked for it should say so, not fail."""
        monkeypatch.setattr(xrism.heasoft, "run", fake_barycen([]))
        config = dict(OFFICIAL, barycenter_ephemeris="DE430")

        barycentre_resolve(
            "in.evt", "o.orb", 0.0, 0.0, outfile=str(tmp_path / "b.evt"), config=config
        )

        assert "not DE430" in caplog.text


class TestTheFlow:
    def test_without_a_position_it_stops_at_the_merged_file(self, tmp_path, monkeypatch):
        """No position means no barycentring: never fall back to the header target."""
        root = tmp_path / OBSID
        (root / "resolve" / "event_cl").mkdir(parents=True)
        (root / "auxil").mkdir()
        (root / "auxil" / f"xa{OBSID}.orb.gz").write_bytes(b"")
        write_resolve_file(
            root / "resolve" / "event_cl" / f"xa{OBSID}rsl_p0px1000_cl.evt",
            [1.0],
            [0],
            [0],
            [[0.0, 2.0]],
        )
        monkeypatch.setattr(xrism, "barycentre_resolve", lambda *a, **k: pytest.fail("called"))

        out = process_xrism_obsid.fn(OBSID, config={"out_data_path": str(tmp_path)})

        assert out == os.path.join(str(root), "l2_files", f"{OBSID}_rsl_cl.evt")

    def test_the_energy_band_comes_from_the_configuration(self, tmp_path):
        """resolve_energy_band reaches the merge; without it every energy is kept."""
        root = tmp_path / OBSID
        (root / "resolve" / "event_cl").mkdir(parents=True)
        write_resolve_file(
            root / "resolve" / "event_cl" / f"xa{OBSID}rsl_p0px1000_cl.evt",
            [1.0, 2.0],
            [0, 0],
            [0, 0],
            [[0.0, 10.0]],
            pis=[3000, 4000],
        )

        out = reduce_observation.fn(str(root), OBSID, {"resolve_energy_band": [2, 8]})

        assert list(fits.getdata(out, "EVENTS")["PI"]) == [4000]
