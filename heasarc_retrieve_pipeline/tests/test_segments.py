"""
Offline tests for :mod:`heasarc_retrieve_pipeline.segments`.

No HEASOFT and no network. ``nuproducts`` is stubbed and its parameters inspected -- what
matters about the spectral split is precisely which parameters it is called with, since
HEASOFT does the extraction itself. The event-list split is exercised for real on
synthetic files.
"""

import glob
import os

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import segments
from heasarc_retrieve_pipeline.utils import read_gti

from .test_nustar import make_synthetic_event_file

OBSID = "80002092006"


def make_reduced_tree(base, tstart=0.0, tstop=1000.0, nevents=500, with_regions=True):
    """
    An observation tree shaped like one the pipeline has finished with.

    One mode-01 event file per module under ``event_pipe``, with the region files and the
    flare-free GTI the parent run leaves next to it, an empty ``products`` directory, and
    a merged source and background event file at the top level.
    """
    obsdir = os.path.join(base, OBSID)
    pipedir = os.path.join(obsdir, "event_pipe")
    for directory in (pipedir, os.path.join(obsdir, "products"), os.path.join(obsdir, "split")):
        os.makedirs(directory, exist_ok=True)

    for fpm in "AB":
        root = f"nu{OBSID}{fpm}01_cl"
        path = os.path.join(pipedir, root + ".evt")
        make_synthetic_event_file(path, tstart, tstop, nevents, seed=ord(fpm))
        if with_regions:
            for suffix in ("_src.reg", "_bkg.reg"):
                with open(os.path.join(pipedir, root + suffix), "w") as fobj:
                    fobj.write("circle(500,500,30)\n")
        # What calculate_spectra leaves behind: the file's own GTI with the flares out.
        # A gap in the middle, so that the segments are not trivially the bounds.
        gti = fits.BinTableHDU.from_columns(
            [
                fits.Column(name="START", format="D", array=np.array([tstart, 600.0])),
                fits.Column(name="STOP", format="D", array=np.array([400.0, tstop])),
            ],
            name="GTI",
        )
        gti.header["TIMEZERO"] = 0.0
        gti.header["MJDREFI"] = 55197
        gti.header["MJDREFF"] = 0.00076601852
        gti.header["TIMESYS"] = "TT"
        fits.HDUList([fits.PrimaryHDU(), gti]).writeto(
            os.path.join(pipedir, root + "_noflares.gti"), overwrite=True
        )

    for name in (f"nu{OBSID}A_src1_bary.evt", f"nu{OBSID}A_back_bary.evt"):
        make_synthetic_event_file(os.path.join(obsdir, name), tstart, tstop, nevents)

    return {"out_data_path": str(base)}


class StubNuproducts:
    """
    ``nuproducts``, recorded rather than run.

    The files it claims to produce are written, because ``heasoft.run`` checks that a tool
    produced what it said it would, and because the code then edits the spectra's headers.
    """

    def __init__(self):
        self.calls = []

    def __call__(self, name, *args, **kwargs):
        params = args[0] if args else kwargs
        self.calls.append((name, dict(params)))
        for key in ("phafile", "bkgphafile", "grpphafile"):
            path = params.get(key)
            if path in (None, "NONE"):
                continue
            spectrum = fits.BinTableHDU.from_columns(
                [
                    fits.Column(name="CHANNEL", format="J", array=np.arange(10)),
                    fits.Column(name="COUNTS", format="J", array=np.arange(10)),
                ],
                name="SPECTRUM",
            )
            # What nuproducts writes when it was told not to make a response.
            spectrum.header["RESPFILE"] = "none"
            spectrum.header["ANCRFILE"] = "none"
            fits.HDUList([fits.PrimaryHDU(), spectrum]).writeto(path, overwrite=True)
        # The plot it writes whether or not it was asked to.
        with open(os.path.join(params["outdir"], params["stemout"] + "_ph.gif"), "w") as f:
            f.write("gif")


@pytest.fixture
def tree(tmp_path):
    return make_reduced_tree(tmp_path)


@pytest.fixture
def stub(monkeypatch):
    stub = StubNuproducts()
    monkeypatch.setattr(segments.heasoft, "run", stub)
    return stub


class TestSegmentNaming:
    def test_the_tag_counts_from_one(self):
        assert segments.segment_tag(1) == "seg1"
        assert segments.segment_tag(12) == "seg12"

    def test_the_tag_goes_before_the_extension(self):
        assert (
            segments.insert_tag("nu1A01_sr.pha", "seg1") == "nu1A01_sr_seg1.pha"
        )

    def test_a_compression_suffix_is_kept(self):
        assert (
            segments.insert_tag("nu1A_src1.evt.gz", "seg2") == "nu1A_src1_seg2.evt.gz"
        )

    @pytest.mark.parametrize(
        "name, tagged",
        [
            ("nu1A_src1_bary", False),
            ("nu1A_src1_bary_seg1", True),
            ("nu1A_back_seg12", True),
            ("nu1A06_chu123_N", False),
        ],
    )
    def test_the_regex_recognises_its_own_output(self, name, tagged):
        assert bool(segments.SEGMENT_RE.search(name)) is tagged


class TestWriteGtiFile:
    """A segment GTI file has to look exactly like the one it came from."""

    def test_only_the_rows_change(self, tree, tmp_path):
        template = os.path.join(
            tmp_path, OBSID, "event_pipe", f"nu{OBSID}A01_cl_noflares.gti"
        )
        out = str(tmp_path / "segment.gti")
        segments.write_gti_file(template, out, [[0.0, 200.0]])

        with fits.open(out) as hdul, fits.open(template) as original:
            assert hdul[1].header["EXTNAME"] == "GTI"
            for keyword in ("MJDREFI", "MJDREFF", "TIMESYS", "TIMEZERO"):
                assert hdul[1].header[keyword] == original[1].header[keyword]
            assert hdul[1].data["START"].tolist() == [0.0]
            assert hdul[1].data["STOP"].tolist() == [200.0]

    def test_timezero_is_taken_back_out(self, tmp_path):
        """The rows are TIME-column values; the caller works in TIME + TIMEZERO."""
        gti = fits.BinTableHDU.from_columns(
            [
                fits.Column(name="START", format="D", array=np.array([0.0])),
                fits.Column(name="STOP", format="D", array=np.array([100.0])),
            ],
            name="GTI",
        )
        gti.header["TIMEZERO"] = -1.0
        template = str(tmp_path / "template.gti")
        fits.HDUList([fits.PrimaryHDU(), gti]).writeto(template)

        out = str(tmp_path / "segment.gti")
        segments.write_gti_file(template, out, [[20.0, 50.0]])

        with fits.open(out) as hdul:
            assert hdul[1].data["START"].tolist() == [21.0]
            assert read_gti(hdul).tolist() == [[20.0, 50.0]]


class TestSplitSpectra:
    """What matters is which parameters nuproducts is called with."""

    def calls(self, tree, stub, bounds=((0.0, 500.0), (500.0, 1000.0))):
        segments.split_spectra(OBSID, tree, np.array(bounds))
        return [params for name, params in stub.calls if name == "nuproducts"]

    def test_one_call_per_file_per_segment(self, tree, stub):
        assert len(self.calls(tree, stub)) == 4

    def test_the_responses_are_not_regenerated(self, tree, stub):
        """The whole point: they are the slow part and they do not depend on the cut."""
        for params in self.calls(tree, stub):
            assert params["runmkarf"] == "no"
            assert params["runmkrmf"] == "no"

    def test_each_call_gets_its_own_segment_gti(self, tree, stub):
        gtis = [params["usrgtifile"] for params in self.calls(tree, stub)]
        assert sorted(os.path.basename(path) for path in gtis) == [
            f"nu{OBSID}A01_cl_seg1.gti",
            f"nu{OBSID}A01_cl_seg2.gti",
            f"nu{OBSID}B01_cl_seg1.gti",
            f"nu{OBSID}B01_cl_seg2.gti",
        ]

    def test_the_segment_gti_is_the_parent_gti_cut(self, tree, stub):
        """The parent's flare-free GTI is 0-400 and 600-1000, cut at 500."""
        self.calls(tree, stub)
        base = os.path.join(tree["out_data_path"], OBSID, "event_pipe")
        with fits.open(os.path.join(base, f"nu{OBSID}A01_cl_seg1.gti")) as hdul:
            assert read_gti(hdul).tolist() == [[0.0, 400.0]]
        with fits.open(os.path.join(base, f"nu{OBSID}A01_cl_seg2.gti")) as hdul:
            assert read_gti(hdul).tolist() == [[600.0, 1000.0]]

    def test_the_output_names_carry_the_segment_last(self, tree, stub):
        params = self.calls(tree, stub)[0]
        assert os.path.basename(params["phafile"]) == f"nu{OBSID}A01_sr_seg1.pha"
        assert os.path.basename(params["bkgphafile"]) == f"nu{OBSID}A01_bk_seg1.pha"
        assert os.path.basename(params["grpphafile"]) == f"nu{OBSID}A01_grp_seg1.pha"

    def test_stemout_is_tagged_so_the_parents_plot_survives(self, tree, stub):
        """
        nuproducts names its own extra outputs from stemout and has no way to be told
        not to write them. An untagged stem would overwrite the parent's.
        """
        for params in self.calls(tree, stub):
            assert params["stemout"].endswith(("_seg1", "_seg2"))

    def test_the_light_curves_and_image_are_not_made(self, tree, stub):
        for params in self.calls(tree, stub):
            assert params["lcfile"] == "NONE"
            assert params["bkglcfile"] == "NONE"
            assert params["imagefile"] == "NONE"

    def test_the_parents_regions_are_reused(self, tree, stub):
        params = self.calls(tree, stub)[0]
        assert params["srcregionfile"].endswith(f"nu{OBSID}A01_cl_src.reg")
        assert params["bkgregionfile"].endswith(f"nu{OBSID}A01_cl_bkg.reg")

    def test_the_grouping_matches_the_pipeline(self, tree, stub):
        params = self.calls(tree, stub)[0]
        assert params["rungrppha"] == "yes"
        assert params["grpmincounts"] == 20
        assert (params["grppibadlow"], params["grppibadhigh"]) == (35, 1909)

    def test_the_spectra_point_at_the_parents_response(self, tree, stub):
        self.calls(tree, stub)
        products = os.path.join(tree["out_data_path"], OBSID, "products")
        for name in (f"nu{OBSID}A01_sr_seg1.pha", f"nu{OBSID}A01_grp_seg1.pha"):
            with fits.open(os.path.join(products, name)) as hdul:
                header = hdul["SPECTRUM"].header
                assert header["RESPFILE"] == f"nu{OBSID}A01_sr.rmf"
                assert header["ANCRFILE"] == f"nu{OBSID}A01_sr.arf"

    def test_the_response_names_are_bare(self, tree, stub):
        """They resolve relative to the spectrum's directory, which is the parent's."""
        self.calls(tree, stub)
        products = os.path.join(tree["out_data_path"], OBSID, "products")
        with fits.open(os.path.join(products, f"nu{OBSID}A01_sr_seg1.pha")) as hdul:
            assert os.sep not in hdul["SPECTRUM"].header["RESPFILE"]

    def test_the_plot_is_removed(self, tree, stub):
        self.calls(tree, stub)
        products = os.path.join(tree["out_data_path"], OBSID, "products")
        assert [n for n in os.listdir(products) if n.endswith(".gif")] == []

    def test_an_empty_segment_is_skipped_not_extracted(self, tree, stub):
        """The parent GTI has a gap at 400-600; a segment inside it has no good time."""
        calls = self.calls(tree, stub, bounds=((420.0, 580.0), (600.0, 1000.0)))
        assert len(calls) == 2
        assert all(params["usrgtifile"].endswith("_seg2.gti") for params in calls)

    def test_a_file_without_regions_is_skipped_and_recorded(self, tmp_path, stub):
        config = make_reduced_tree(tmp_path, with_regions=False)
        segments.split_spectra(OBSID, config, np.array([[0.0, 1000.0]]))

        assert stub.calls == []
        report = os.path.join(tmp_path, OBSID, "skipped_inputs.txt")
        assert "region" in open(report).read()


class TestSplitEventFiles:
    """The events have to be partitioned, not sampled."""

    def parent_and_segments(self, tree, bounds=((0.0, 500.0), (500.0, 1000.0))):
        written = segments.split_event_files(OBSID, tree, np.array(bounds))
        obsdir = os.path.join(tree["out_data_path"], OBSID)
        return obsdir, written

    def test_one_file_per_input_per_segment(self, tree):
        _, written = self.parent_and_segments(tree)
        assert sorted(written) == [
            f"nu{OBSID}A_back_bary_seg1.evt",
            f"nu{OBSID}A_back_bary_seg2.evt",
            f"nu{OBSID}A_src1_bary_seg1.evt",
            f"nu{OBSID}A_src1_bary_seg2.evt",
        ]

    def test_the_events_are_partitioned(self, tree):
        obsdir, _ = self.parent_and_segments(tree)
        with fits.open(os.path.join(obsdir, f"nu{OBSID}A_src1_bary.evt")) as hdul:
            parent = np.asarray(hdul["EVENTS"].data["TIME"])

        pieces = []
        for tag in ("seg1", "seg2"):
            path = os.path.join(obsdir, f"nu{OBSID}A_src1_bary_{tag}.evt")
            with fits.open(path) as hdul:
                pieces.append(np.asarray(hdul["EVENTS"].data["TIME"]))

        assert len(pieces[0]) + len(pieces[1]) == len(parent)
        assert np.array_equal(np.concatenate(pieces), parent)

    def test_the_exposures_add_up(self, tree):
        obsdir, _ = self.parent_and_segments(tree)
        with fits.open(os.path.join(obsdir, f"nu{OBSID}A_src1_bary.evt")) as hdul:
            parent = hdul["EVENTS"].header["EXPOSURE"]

        total = 0.0
        for tag in ("seg1", "seg2"):
            path = os.path.join(obsdir, f"nu{OBSID}A_src1_bary_{tag}.evt")
            with fits.open(path) as hdul:
                total += hdul["EVENTS"].header["EXPOSURE"]

        assert total == pytest.approx(parent, rel=1e-9)

    def test_the_time_bounds_are_narrowed(self, tree):
        obsdir, _ = self.parent_and_segments(tree)
        path = os.path.join(obsdir, f"nu{OBSID}A_src1_bary_seg2.evt")
        with fits.open(path) as hdul:
            header = hdul["EVENTS"].header
            assert header["TSTART"] == 500.0
            assert header["TSTOP"] == 1000.0
            assert read_gti(hdul).tolist() == [[500.0, 1000.0]]

    def test_a_rerun_does_not_split_its_own_output(self, tree):
        obsdir, first = self.parent_and_segments(tree)
        second = segments.split_event_files(
            OBSID, tree, np.array([[0.0, 500.0], [500.0, 1000.0]])
        )
        assert sorted(second) == sorted(first)

    def test_an_empty_segment_writes_nothing(self, tree):
        _, written = self.parent_and_segments(
            tree, bounds=((0.0, 0.0), (0.0, 1000.0))
        )
        assert all("seg1" not in name for name in written)


class TestResolveSplitTimes:
    """The MJD the user typed, turned into a time in the file."""

    def test_a_split_at_the_start_is_met_zero(self, tree):
        from heasarc_retrieve_pipeline.utils import mjd_from_met

        path = os.path.join(tree["out_data_path"], OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        with fits.open(path) as hdul:
            mjd = mjd_from_met(0.0, hdul)

        mets, _, timesys = segments.resolve_split_times(OBSID, tree, [mjd])
        assert mets[0] == pytest.approx(0.0, abs=1e-3)
        assert timesys == "tt"

    def test_reading_the_mjd_as_utc_moves_it(self, tree):
        from heasarc_retrieve_pipeline.utils import mjd_from_met

        path = os.path.join(tree["out_data_path"], OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        with fits.open(path) as hdul:
            mjd = mjd_from_met(500.0, hdul)

        as_file, _, _ = segments.resolve_split_times(OBSID, tree, [mjd])
        as_utc, _, _ = segments.resolve_split_times(OBSID, tree, [mjd], scale="utc")
        assert as_utc[0] - as_file[0] == pytest.approx(66.184, abs=1e-3)

    def test_an_unreduced_observation_says_so(self, tmp_path):
        with pytest.raises(ValueError, match="no cleaned event file"):
            segments.resolve_split_times("99999999999", {"out_data_path": str(tmp_path)}, [1.0])


class TestSplitObsid:
    """The whole thing, end to end with nuproducts stubbed."""

    def test_it_records_what_it_did(self, tree, stub):
        from heasarc_retrieve_pipeline.diagnostics import diagnostics_path, read_records
        from heasarc_retrieve_pipeline.utils import mjd_from_met

        path = os.path.join(tree["out_data_path"], OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        with fits.open(path) as hdul:
            mjd = mjd_from_met(500.0, hdul)

        result = segments.split_obsid(OBSID, tree, [mjd])

        assert len(result["bounds"]) == 2
        assert len(result["spectra"]) == 4
        assert len(result["event_files"]) == 4

        records = read_records(diagnostics_path(OBSID, tree))
        record = [r for r in records if r["step"] == "split_obsid"][0]
        assert record["status"] == "done"
        assert record["values"]["split_mets"][0] == pytest.approx(500.0, abs=1e-3)
        assert record["values"]["timesys"] == "tt"

    def test_the_pieces_can_be_asked_for_separately(self, tree, stub):
        from heasarc_retrieve_pipeline.utils import mjd_from_met

        path = os.path.join(tree["out_data_path"], OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        with fits.open(path) as hdul:
            mjd = mjd_from_met(500.0, hdul)

        result = segments.split_obsid(OBSID, tree, [mjd], spectra=False)
        assert result["spectra"] == []
        assert result["event_files"] != []


class TestCommandLine:
    def test_it_splits_and_reports(self, tree, stub, capsys):
        from heasarc_retrieve_pipeline.utils import mjd_from_met

        path = os.path.join(tree["out_data_path"], OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        with fits.open(path) as hdul:
            mjd = mjd_from_met(500.0, hdul)

        status = segments.main([tree["out_data_path"], OBSID, str(mjd)])

        assert status == 0
        assert "2 segment(s)" in capsys.readouterr().out


class TestSegmentsSpanEachFile:
    """
    A mode-06 file may run past the ends of the mode-01 observation span.

    ``nusplitsc`` writes the per-CHU files from the whole of the mode-06 data, and on a real
    observation (90901333002) those reach both before the first mode-01 event and after the
    last: 150 s early on one CHU combination, 800 s late on two others. Clamping every
    file's segments to one file's span silently threw that good time away -- 720 s of it on
    the worst file -- so the first and last segment are open-ended and each file's own GTI
    decides where its data start and stop.
    """

    @staticmethod
    def _overrunning_mode_06(base, tstart, tstop):
        """A CHU file whose good time reaches past both ends of the mode-01 span."""
        splitdir = os.path.join(base, OBSID, "split")
        root = f"nu{OBSID}A06_chu13_N_cl"
        path = os.path.join(splitdir, root + ".evt")
        make_synthetic_event_file(path, tstart - 200.0, tstop + 300.0, 200, seed=7)
        for suffix in ("_src.reg", "_bkg.reg"):
            with open(os.path.join(splitdir, root + suffix), "w") as fobj:
                fobj.write("circle(500,500,30)\n")
        gti = fits.BinTableHDU.from_columns(
            [
                fits.Column(name="START", format="D", array=np.array([tstart - 200.0, 600.0])),
                fits.Column(name="STOP", format="D", array=np.array([400.0, tstop + 300.0])),
            ],
            name="GTI",
        )
        gti.header["TIMEZERO"] = 0.0
        gti.header["MJDREFI"] = 55197
        gti.header["MJDREFF"] = 0.00076601852
        gti.header["TIMESYS"] = "TT"
        fits.HDUList([fits.PrimaryHDU(), gti]).writeto(
            os.path.join(splitdir, root + "_noflares.gti"), overwrite=True
        )
        return os.path.join(splitdir, root)

    def test_good_time_outside_the_observation_span_survives(self, tree, stub):
        from heasarc_retrieve_pipeline.utils import mjd_from_met

        root = self._overrunning_mode_06(tree["out_data_path"], 0.0, 1000.0)

        path = os.path.join(tree["out_data_path"], OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        with fits.open(path) as hdul:
            mjd = mjd_from_met(500.0, hdul)

        segments.split_obsid(OBSID, tree, [mjd], events=False)

        def ontime(name):
            with fits.open(name) as hdul:
                data = hdul["GTI"].data
                return float(np.sum(data["STOP"] - data["START"]))

        parent = ontime(root + "_noflares.gti")
        pieces = sum(ontime(root + f"_seg{n}.gti") for n in (1, 2))
        assert pieces == pytest.approx(parent, abs=1e-6)

    def test_the_open_edges_are_recorded_as_json_null(self, tree, stub):
        """
        ``json.dumps`` writes a bare ``Infinity`` for an infinite float. Python reads that
        back without complaint, so a broken diagnostics file would only show up in
        something else -- ``jq``, a browser, any other language. ``null`` is both valid and
        the honest word for an open end.
        """
        import json

        from heasarc_retrieve_pipeline.diagnostics import diagnostics_path
        from heasarc_retrieve_pipeline.utils import mjd_from_met

        path = os.path.join(tree["out_data_path"], OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        with fits.open(path) as hdul:
            mjd = mjd_from_met(500.0, hdul)

        segments.split_obsid(OBSID, tree, [mjd], spectra=False, events=False)

        def strict(name):
            raise AssertionError(f"{name} is not valid JSON")

        written = glob.glob(os.path.join(diagnostics_path(OBSID, tree), "*.json"))
        bounds = None
        for name in written:
            with open(name) as fobj:
                payload = json.loads(fobj.read(), parse_constant=strict)
            for record in payload if isinstance(payload, list) else [payload]:
                if record.get("step") == "split_obsid":
                    bounds = record["values"]["bounds"]
        assert bounds == [[None, pytest.approx(500.0, abs=1e-3)], [pytest.approx(500.0, abs=1e-3), None]]
