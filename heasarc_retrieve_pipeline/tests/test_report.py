"""
Offline tests for the diagnostics report.

Three layers, none of which needs a browser:

* **structure** -- the page is parsed and asserted on: one script tag pointing at the
  shared bundle by the right relative path, one graph div per figure, the OBSID in the
  title;
* **content** -- asserted on the ``go.Figure`` objects rather than on their HTML, which is
  where the numbers actually are;
* **robustness** -- an empty directory, a manifest and nothing else, a failed step, a step
  that never finished, and an observation that raised all have to render rather than
  raise.
"""

import json
import os

import numpy as np
import pytest

from heasarc_retrieve_pipeline import report
from heasarc_retrieve_pipeline.diagnostics import (
    diagnostics_path,
    read_arrays,
    read_records,
    record_step,
    write_manifest,
)
from heasarc_retrieve_pipeline.utils import record_skipped_input

pytest.importorskip("plotly")
pytest.importorskip("bs4")

from bs4 import BeautifulSoup  # noqa: E402


OBSID = "90901333002"


def soup(path):
    """The written page, parsed."""
    with open(path) as fobj:
        return BeautifulSoup(fobj.read(), "html.parser")


def observation(tmp_path, obsid=OBSID):
    """The diagnostics directory of one observation under a run root."""
    return diagnostics_path(obsid, dict(out_data_path=str(tmp_path)))


def a_manifest(tmp_path, obsid=OBSID, **extra):
    """The manifest ``process_observations`` writes before anything is submitted."""
    catalogue = dict(name="M82", exposure_a=42350.0, time="2026-03-14", cycle=8)
    catalogue.update(extra)
    write_manifest(
        observation(tmp_path, obsid),
        obsid,
        catalogue,
        url=f"s3://nasa-heasarc/nustar/{obsid}",
        mission="nustar",
        ra=148.9575,
        dec=69.6794,
    )


def a_separation(tmp_path, obsid=OBSID, key="nu123A01_cl", n_peaks=2):
    """A ``separate_sources`` record with an image and two peaks, one of them faint."""
    rng = np.random.default_rng(5)
    image = rng.uniform(0, 3, (99, 99)).astype(np.float32)
    image[70, 20] = 90.0
    image[30, 60] = 12.0
    with record_step(observation(tmp_path, obsid), obsid, "separate_sources", key=key) as rec:
        rec.value(
            n_events=6206,
            n_events_no_sky_position=206,
            n_events_used=5400,
            background_median=8.0,
            background_mad=2.0,
            acceptance_threshold=10.0,
            n_peaks=n_peaks,
            n_sources=1,
            sources=[dict(source=1, x=603.0, y=401.0, flux=90.0)],
        )
        rec.array(
            image=image,
            image_x=np.linspace(300, 700, 100, dtype=np.float32),
            image_y=np.linspace(300, 700, 100, dtype=np.float32),
            peaks=np.array([[603.0, 401.0], [420.0, 540.0]])[:n_peaks],
            peak_fluxes=np.array([90.0, 4.0])[:n_peaks],
        )


def a_region(tmp_path, obsid=OBSID, key="nu123A01_cl"):
    """A ``source_region`` record with the radial profile the radius came from."""
    radius = np.linspace(5, 200, 40)
    with record_step(observation(tmp_path, obsid), obsid, "source_region", key=key) as rec:
        rec.value(rlimit=48.0, rlimit_snr=48.0, max_radius=80, ra=148.95, dec=69.68)
        rec.array(
            radius=radius,
            profile=1000 * np.exp(-radius / 30),
            profile_error=np.sqrt(1000 * np.exp(-radius / 30)),
            psf_profile=900 * np.exp(-radius / 32),
        )


def a_join(tmp_path, obsid=OBSID):
    """A ``join_source_data`` record with the intervals of both modules."""
    with record_step(observation(tmp_path, obsid), obsid, "join_source_data", key="src1") as rec:
        rec.value(
            inputs_A=[f"nu{obsid}A01_src1.evt"],
            inputs_B=[f"nu{obsid}B01_src1.evt"],
            modules=[f"nu{obsid}A_src1.evt", f"nu{obsid}B_src1.evt"],
            combined=f"nu{obsid}_src1.evt",
        )
        rec.array(
            gti_A_in_0=np.array([[0.0, 400.0], [600.0, 1000.0]]),
            gti_A_out=np.array([[0.0, 400.0], [600.0, 1000.0]]),
            gti_B_in_0=np.array([[50.0, 1000.0]]),
            gti_B_out=np.array([[50.0, 1000.0]]),
            gti_combined=np.array([[50.0, 400.0], [600.0, 1000.0]]),
        )


def a_flare_filtering(tmp_path, obsid=OBSID, goes=True):
    """A ``flare_filtering`` record with both bands before and after."""
    time = np.arange(0, 1000, 100.0)
    with record_step(observation(tmp_path, obsid), obsid, "flare_filtering", key="nu_src1") as rec:
        rec.value(
            nevents_before=5000,
            nevents_after=4100,
            livetime_before=900.0,
            livetime_after=740.0,
            n_intervals_removed=1,
            chi2_dof_3_10=[8.4, 1.1],
            chi2_dof_10_79=[1.2, 1.1],
            goes_light_curve=f"nu{obsid}_goes.fits" if goes else None,
        )
        arrays = dict(
            removed=np.array([[400.0, 600.0]]),
            gti_before=np.array([[0.0, 1000.0]]),
            gti_after=np.array([[0.0, 400.0], [600.0, 1000.0]]),
        )
        for band in ("3_10", "10_79"):
            for when in ("before", "after"):
                arrays[f"lc_{band}_{when}_time"] = time
                arrays[f"lc_{band}_{when}_rate"] = np.full_like(time, 0.5)
                arrays[f"lc_{band}_{when}_rate_err"] = np.full_like(time, 0.05)
        if goes:
            arrays["goes_time"] = time
            arrays["goes_xrsb"] = np.full_like(time, 1e-6)
            arrays["goes_xrsa"] = np.full_like(time, 1e-7)
        rec.array(**arrays)


def a_full_observation(tmp_path, obsid=OBSID):
    """One observation with a record of every kind, as a finished reduction leaves."""
    a_manifest(tmp_path, obsid)
    with record_step(observation(tmp_path, obsid), obsid, "l2_pipeline") as rec:
        rec.value(cleaned_event_files=[f"nu{obsid}A01_cl.evt"])
    a_separation(tmp_path, obsid)
    a_region(tmp_path, obsid)
    a_join(tmp_path, obsid)
    a_flare_filtering(tmp_path, obsid)


class TestTheShapeOfThePage:
    """What has to be in the file for a browser to render it at all."""

    def test_the_page_names_the_observation(self, tmp_path):
        a_full_observation(tmp_path)

        path = report.write_observation_page(OBSID, str(tmp_path))

        assert path == os.path.join(str(tmp_path), OBSID, "diagnostics.html")
        assert OBSID in soup(path).title.text

    def test_the_bundle_is_loaded_once_by_relative_path(self, tmp_path):
        """The page sits one directory below the bundle, and is opened from disk."""
        a_full_observation(tmp_path)

        path = report.write_observation_page(OBSID, str(tmp_path))

        scripts = [tag for tag in soup(path).find_all("script") if tag.get("src")]
        assert [tag["src"] for tag in scripts] == ["../plotly.min.js"]

    def test_no_figure_carries_its_own_copy_of_plotly(self, tmp_path):
        """4.8 MB per figure would be the difference between a page and a download."""
        a_full_observation(tmp_path)

        path = report.write_observation_page(OBSID, str(tmp_path))

        assert os.path.getsize(path) < 2_000_000

    def test_there_is_one_plot_div_per_figure(self, tmp_path):
        """Timeline, separation, radial profile, joining, flares."""
        a_full_observation(tmp_path)

        path = report.write_observation_page(OBSID, str(tmp_path))

        divs = soup(path).find_all("div", class_="plotly-graph-div")
        assert len(divs) == 5

    def test_the_observation_parameters_are_on_the_page(self, tmp_path):
        a_full_observation(tmp_path)

        path = report.write_observation_page(OBSID, str(tmp_path))
        text = soup(path).get_text()

        assert "M82" in text
        assert "42350" in text
        assert "148.95750" in text

    def test_a_skipped_input_is_named_with_its_reason(self, tmp_path):
        a_full_observation(tmp_path)
        record_skipped_input(
            OBSID,
            dict(out_data_path=str(tmp_path)),
            f"nu{OBSID}A06_chu1_cl.evt",
            "no usable extraction region could be measured",
        )

        path = report.write_observation_page(OBSID, str(tmp_path))
        text = soup(path).get_text()

        assert "chu1" in text
        assert "no usable extraction region could be measured" in text

    def test_the_script_tag_resolves_to_the_bundle_that_was_written(self, tmp_path):
        """A relative path that points at nothing is a page with no plots in it."""
        a_full_observation(tmp_path)
        report.write_plotly_bundle(str(tmp_path))
        path = report.write_observation_page(OBSID, str(tmp_path))

        (script,) = [tag for tag in soup(path).find_all("script") if tag.get("src")]

        assert os.path.exists(os.path.join(os.path.dirname(path), script["src"]))

    def test_the_bundle_is_written_at_the_run_root(self, tmp_path):
        path = report.write_plotly_bundle(str(tmp_path))

        assert path == os.path.join(str(tmp_path), "plotly.min.js")
        assert os.path.getsize(path) > 1_000_000


class TestWhatTheFiguresContain:
    """Asserted on the figures, not on their HTML. The numbers are in the figures."""

    def records(self, tmp_path, step):
        directory = observation(tmp_path)
        (record,) = [r for r in read_records(directory) if r["step"] == step]
        return record, read_arrays(directory, record)

    def test_the_timeline_has_one_bar_per_step(self, tmp_path):
        a_full_observation(tmp_path)
        records = read_records(observation(tmp_path))

        fig = report.timeline_figure(records)

        assert sum(len(trace.y) for trace in fig.data) == len(records)

    def test_the_timeline_separates_the_statuses(self, tmp_path):
        with record_step(observation(tmp_path), OBSID, "l2_pipeline") as rec:
            rec.skip("PIPELINE_DONE.TXT already exists")
        with record_step(observation(tmp_path), OBSID, "join_source_data") as rec:
            rec.value(combined="x.evt")

        fig = report.timeline_figure(read_records(observation(tmp_path)))

        assert {trace.name for trace in fig.data} == {"done", "skipped"}

    def test_the_sky_image_keeps_the_axis_convention(self, tmp_path):
        """The horizontal axis is sky Y and the vertical one sky X. See image_from_table."""
        a_separation(tmp_path)
        record, arrays = self.records(tmp_path, "separate_sources")

        fig = report.separation_figure(record, arrays)

        heatmap = fig.data[0]
        assert heatmap.z.shape == (99, 99)
        assert len(heatmap.x) == 99, "bin edges must become centres"
        assert fig.layout.xaxis.title.text == "sky Y (pixels)"
        assert fig.layout.yaxis.title.text == "sky X (pixels)"

    def test_a_rejected_peak_is_drawn_differently_from_an_accepted_one(self, tmp_path):
        a_separation(tmp_path, n_peaks=2)
        record, arrays = self.records(tmp_path, "separate_sources")

        fig = report.separation_figure(record, arrays)

        names = {trace.name for trace in fig.data if trace.name}
        assert names == {"accepted", "below the threshold"}

    def test_the_radial_profile_marks_the_radius_that_was_chosen(self, tmp_path):
        a_region(tmp_path)
        record, arrays = self.records(tmp_path, "source_region")

        fig = report.radial_profile_figure(record, arrays)

        assert {trace.name for trace in fig.data} == {"measured", "expected PSF"}
        assert [shape.x0 for shape in fig.layout.shapes] == [48.0]

    def test_the_joining_shows_every_input_and_both_merges(self, tmp_path):
        a_join(tmp_path)
        record, arrays = self.records(tmp_path, "join_source_data")

        fig = report.gti_figure(record, arrays)

        rows = [trace.y[0] for trace in fig.data]
        assert rows == [
            f"nu{OBSID}A01_src1.evt",
            "FPMA merged (OR)",
            f"nu{OBSID}B01_src1.evt",
            "FPMB merged (OR)",
            f"nu{OBSID}_src1.evt",
        ]

    def test_the_flare_figure_shades_what_was_removed(self, tmp_path):
        a_flare_filtering(tmp_path)
        record, arrays = self.records(tmp_path, "flare_filtering")

        fig = report.flare_figure(record, arrays)

        shaded = [(shape.x0, shape.x1) for shape in fig.layout.shapes]
        assert shaded == [(400.0, 600.0)] * 3, "one band per panel"

    def test_the_flare_figure_leaves_the_goes_panel_out_when_there_is_none(self, tmp_path):
        a_flare_filtering(tmp_path, goes=False)
        record, arrays = self.records(tmp_path, "flare_filtering")

        fig = report.flare_figure(record, arrays)

        assert not [trace for trace in fig.data if "GOES" in (trace.name or "")]
        assert len(fig.data) == 4, "both bands, before and after"

    def test_a_region_read_back_from_disk_has_no_profile_to_draw(self, tmp_path):
        """A rerun measures nothing, so there is nothing to plot. That is not a failure."""
        with record_step(observation(tmp_path), OBSID, "source_region", key="A01") as rec:
            rec.value(rlimit=30.0, read_back=True)
            rec.skip("the region files were already there")
        record, arrays = self.records(tmp_path, "source_region")

        assert report.radial_profile_figure(record, arrays) is None

    def test_the_figures_are_serialised_without_the_default_theme(self, tmp_path):
        """The theme is 6.6 kB of boilerplate per figure, and 100 kB per page."""
        a_separation(tmp_path)
        record, arrays = self.records(tmp_path, "separate_sources")

        fig = report.separation_figure(record, arrays)

        assert fig.layout.template.layout.plot_bgcolor is None


class TestPagesThatCouldGoWrong:
    """Every one of these has happened, or will. None of them may raise."""

    def test_an_observation_that_recorded_nothing_still_gets_a_page(self, tmp_path):
        os.makedirs(os.path.join(tmp_path, OBSID))

        path = report.write_observation_page(OBSID, str(tmp_path))

        assert OBSID in soup(path).get_text()
        assert "recorded no steps" in soup(path).get_text()

    def test_an_observation_with_only_a_manifest_renders(self, tmp_path):
        """A run killed before the first step still wrote one of these for every item."""
        a_manifest(tmp_path)

        path = report.write_observation_page(OBSID, str(tmp_path))

        assert "M82" in soup(path).get_text()
        assert report.observation_summary(OBSID, str(tmp_path))["outcome"] == "no records"

    def test_a_failed_step_puts_its_traceback_on_the_page(self, tmp_path):
        a_manifest(tmp_path)
        with pytest.raises(ValueError):
            with record_step(observation(tmp_path), OBSID, "join_source_data") as rec:
                rec.value(inputs_A=[])
                raise ValueError("source separation produced nothing for FPMA")

        path = report.write_observation_page(OBSID, str(tmp_path))
        text = soup(path).get_text()

        assert "source separation produced nothing for FPMA" in text
        assert "Traceback" in text

    def test_a_step_that_never_finished_names_itself(self, tmp_path):
        """A killed run leaves its last step as running. That is the point of writing it."""
        rec = record_step(observation(tmp_path), OBSID, "calculate_spectra")
        rec.__enter__()  # entered, never exited: the process died here

        summary = report.observation_summary(OBSID, str(tmp_path))

        assert summary["outcome"] == "running"
        assert "Spectral extraction" in soup(
            report.write_observation_page(OBSID, str(tmp_path))
        ).get_text()

    def test_an_unreadable_array_payload_does_not_lose_the_page(self, tmp_path):
        a_separation(tmp_path)
        directory = observation(tmp_path)
        with open(os.path.join(directory, "separate_sources__nu123A01_cl.npz"), "w") as f:
            f.write("this is not an npz")

        path = report.write_observation_page(OBSID, str(tmp_path))

        assert "Source separation" in soup(path).get_text()

    def test_a_figure_that_raises_does_not_lose_the_page(self, tmp_path, monkeypatch):
        a_full_observation(tmp_path)

        def explode(*args, **kwargs):
            raise RuntimeError("plotly said no")

        monkeypatch.setattr(report, "separation_figure", explode)

        path = report.write_observation_page(OBSID, str(tmp_path))

        assert len(soup(path).find_all("div", class_="plotly-graph-div")) == 4

    def test_the_page_is_written_whole_or_not_at_all(self, tmp_path):
        """A page half written by a killed process would not open in a browser."""
        a_full_observation(tmp_path)
        report.write_observation_page(OBSID, str(tmp_path))

        before = os.path.getsize(os.path.join(tmp_path, OBSID, "diagnostics.html"))
        report.write_observation_page(OBSID, str(tmp_path))

        assert os.path.getsize(os.path.join(tmp_path, OBSID, "diagnostics.html")) == before
        assert not [f for f in os.listdir(os.path.join(tmp_path, OBSID)) if f.endswith(".tmp")]


class TestJoiningTheOtherTwoRecords:
    """The report reads the other records the pipeline keeps; it does not duplicate them."""

    def test_step_stamps_are_shown_when_they_exist(self, tmp_path):
        """The completion model is not implemented yet. The page renders either way."""
        a_manifest(tmp_path)
        steps = os.path.join(tmp_path, OBSID, ".steps")
        os.makedirs(steps)
        with open(os.path.join(steps, "l2_pipeline.json"), "w") as fobj:
            json.dump(dict(outputs=["nu90901333002A01_cl.evt"]), fobj)

        path = report.write_observation_page(OBSID, str(tmp_path))

        assert "nu90901333002A01_cl.evt" in soup(path).get_text()

    def test_an_unreadable_stamp_is_ignored(self, tmp_path):
        a_manifest(tmp_path)
        steps = os.path.join(tmp_path, OBSID, ".steps")
        os.makedirs(steps)
        with open(os.path.join(steps, "broken.json"), "w") as fobj:
            fobj.write("{not json")

        assert report.read_step_stamps(OBSID, str(tmp_path)) == []

    def test_the_observations_of_a_run_are_found_on_disk(self, tmp_path):
        a_manifest(tmp_path, obsid="90901333002")
        a_manifest(tmp_path, obsid="80002092008")
        os.makedirs(os.path.join(tmp_path, "not_an_observation"))

        assert report.observation_directories(str(tmp_path)) == [
            "80002092008",
            "90901333002",
        ]


class TestTheRunIndex:
    """One page for the run, one row per observation, every link resolving."""

    def a_run(self, tmp_path):
        """Three observations: one finished, one failed, one that never started."""
        a_full_observation(tmp_path, obsid="90901333002")
        with record_step(
            observation(tmp_path, "90901333002"), "90901333002", "observation"
        ) as rec:
            rec.value(mission="nustar")

        a_manifest(tmp_path, obsid="80002092008", name="NGC 253")
        try:
            with record_step(
                observation(tmp_path, "80002092008"), "80002092008", "observation"
            ):
                raise ValueError("nupipeline died")
        except ValueError:
            pass

        a_manifest(tmp_path, obsid="30202022003", name="M82")
        return ["30202022003", "80002092008", "90901333002"]

    def test_every_observation_is_listed_with_its_outcome(self, tmp_path):
        obsids = self.a_run(tmp_path)

        path = report.write_index(str(tmp_path), obsids)
        text = soup(path).get_text()

        assert path == os.path.join(str(tmp_path), "index.html")
        for obsid in obsids:
            assert obsid in text
        assert "no records" in text, "the one that never started"
        assert "failed" in text
        assert "nupipeline died" in text

    def test_every_link_resolves_to_a_page_that_exists(self, tmp_path):
        obsids = self.a_run(tmp_path)
        for obsid in obsids:
            report.write_observation_page(obsid, str(tmp_path))

        path = report.write_index(str(tmp_path), obsids)

        links = [tag["href"] for tag in soup(path).find_all("a")]
        assert links == [f"{obsid}/diagnostics.html" for obsid in obsids]
        for link in links:
            assert os.path.exists(os.path.join(str(tmp_path), link))

    def test_the_index_loads_the_bundle_from_its_own_directory(self, tmp_path):
        """The index is at the root, the observation pages one level down."""
        obsids = self.a_run(tmp_path)

        path = report.write_index(str(tmp_path), obsids)

        scripts = [tag for tag in soup(path).find_all("script") if tag.get("src")]
        assert [tag["src"] for tag in scripts] == ["plotly.min.js"]

    def test_the_run_timeline_has_a_bar_per_observation_that_ran(self, tmp_path):
        obsids = self.a_run(tmp_path)
        summaries = [report.observation_summary(o, str(tmp_path)) for o in obsids]

        fig = report.run_timeline_figure(summaries)

        assert sum(len(trace.y) for trace in fig.data) == 2, "the third never started"
        assert {trace.name for trace in fig.data} == {"done", "failed"}

    def test_a_run_where_nothing_started_has_no_timeline_but_still_lists(self, tmp_path):
        a_manifest(tmp_path, obsid="30202022003")

        path = report.write_index(str(tmp_path), ["30202022003"])

        assert not soup(path).find_all("div", class_="plotly-graph-div")
        assert "30202022003" in soup(path).get_text()

    def test_the_index_finds_the_observations_by_itself(self, tmp_path):
        """A crashed run is rebuilt from disk with no list of what it meant to do."""
        obsids = self.a_run(tmp_path)

        path = report.write_index(str(tmp_path))

        assert [tag["href"] for tag in soup(path).find_all("a")] == [
            f"{obsid}/diagnostics.html" for obsid in obsids
        ]

    def test_the_target_and_the_exposure_are_in_the_row(self, tmp_path):
        obsids = self.a_run(tmp_path)

        path = report.write_index(str(tmp_path), obsids)
        text = soup(path).get_text()

        assert "NGC 253" in text
        assert "42350" in text

    def test_the_command_line_rebuilds_everything(self, tmp_path):
        """python -m heasarc_retrieve_pipeline.report <outdir>, after a crashed run."""
        self.a_run(tmp_path)

        assert report.main([str(tmp_path)]) == 0

        assert os.path.exists(os.path.join(tmp_path, "index.html"))
        assert os.path.exists(os.path.join(tmp_path, "plotly.min.js"))
        assert os.path.exists(
            os.path.join(tmp_path, "90901333002", "diagnostics.html")
        )

    def test_the_command_line_says_how_to_use_it(self, tmp_path, capsys):
        assert report.main([]) == 2
        assert "usage" in capsys.readouterr().out
