"""Offline tests for the NuSTAR module.

These need neither the network nor a HEASOFT installation: they exercise the pure
file-selection helpers that decide which event files the pipeline works on.
"""

import os

# These tests call Prefect tasks through ``.fn``, outside any flow run. Prefect's API log
# handler warns about that on every call; it has nothing to report to.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

import glob  # noqa: E402
import numpy as np  # noqa: E402
import pytest  # noqa: E402

from astropy.coordinates import SkyCoord  # noqa: E402
import astropy.units as u  # noqa: E402

from heasarc_retrieve_pipeline import heasoft, nustar  # noqa: E402
from heasarc_retrieve_pipeline.nustar import (  # noqa: E402
    chi2_dof_against_a_constant,
    flare_filtered_event_file_name,
    nu_base_output_path,
    nu_local_raw_data_path,
    nu_pipeline_done_file,
    nu_pipeline_output_path,
    nu_product_output_path,
    nu_longest_output_name,
    split_path,
    get_best_source_regions,
    goes_class_to_flux,
    join_source_data,
    plot_flare_filtering,
    mode_01_input_files,
    nu_goes_gti_file,
    nu_goes_lc_file,
    position_is_consistent,
    spectral_input_files,
)


OBSID = "80002092008"


def make_obsid_tree(base, pipe_files=(), split_files=(), obsid=OBSID):
    """Create an empty ``event_pipe``/``split`` tree and return a matching config."""
    pipedir = os.path.join(base, obsid, "event_pipe")
    splitdir = os.path.join(base, obsid, "split")
    os.makedirs(pipedir, exist_ok=True)
    os.makedirs(splitdir, exist_ok=True)
    for d, names in ((pipedir, pipe_files), (splitdir, split_files)):
        for name in names:
            open(os.path.join(d, name), "w").close()
    return dict(out_data_path=str(base), input_data_path=str(base), max_radius=80)


@pytest.fixture
def full_observation(tmp_path):
    """An observation with mode 01 and mode 06 data for both modules, as 80002092008 has."""
    config = make_obsid_tree(
        tmp_path,
        pipe_files=[
            f"nu{OBSID}A01_cl.evt",
            f"nu{OBSID}B01_cl.evt",
            f"nu{OBSID}A04_cl.evt",
            f"nu{OBSID}B04_cl.evt",
            f"nu{OBSID}A06_cl.evt",
            f"nu{OBSID}B06_cl.evt",
            f"nu{OBSID}_att.fits",
        ],
        split_files=[
            f"nu{OBSID}A06_chu1_N_cl.evt",
            f"nu{OBSID}A06_chu12_N_cl.evt",
            f"nu{OBSID}B06_chu1_N_cl.evt",
        ],
    )
    return config


def test_mode_01_files_are_found_for_both_modules(full_observation):
    found = [(fpm, f) for fpm, f in spectral_input_files(OBSID, full_observation)
             if "01_cl.evt" in f]
    assert len(found) == 2
    assert {fpm for fpm, _ in found} == {"A", "B"}
    for fpm, f in found:
        assert os.path.basename(f) == f"nu{OBSID}{fpm}01_cl.evt"


def test_other_observing_modes_are_ignored(full_observation):
    found = [f for _, f in spectral_input_files(OBSID, full_observation)]
    assert not [f for f in found if "04_cl.evt" in f]


def test_unsplit_mode_06_file_is_ignored(full_observation):
    """The mode-06 file in event_pipe has no usable aspect until nusplitsc has run."""
    found = [os.path.basename(f) for _, f in spectral_input_files(OBSID, full_observation)]
    assert f"nu{OBSID}A06_cl.evt" not in found


def test_encrypted_files_are_ignored(tmp_path):
    config = make_obsid_tree(
        tmp_path,
        pipe_files=[f"nu{OBSID}A01_cl.evt.gpg", f"nu{OBSID}B01_cl.evt"],
    )
    found = [os.path.basename(f) for _, f in spectral_input_files(OBSID, config)]
    assert found == [f"nu{OBSID}B01_cl.evt"]


def test_uncompressed_file_is_preferred_over_gzipped(tmp_path):
    config = make_obsid_tree(
        tmp_path, pipe_files=[f"nu{OBSID}A01_cl.evt.gz", f"nu{OBSID}A01_cl.evt"]
    )
    found = [os.path.basename(f) for _, f in spectral_input_files(OBSID, config)]
    assert found[0] == f"nu{OBSID}A01_cl.evt"


def test_observation_without_mode_01_data(tmp_path):
    """80002092003 has no mode-01 data at all. This used to leave ``infile`` unbound."""
    config = make_obsid_tree(
        tmp_path,
        pipe_files=["nu80002092003A03_cl.evt", "nu80002092003A06_cl.evt"],
        obsid="80002092003",
    )
    found = [f for _, f in spectral_input_files("80002092003", config)]
    assert not [f for f in found if "01_cl.evt" in f]


def test_empty_observation_yields_nothing(tmp_path):
    config = make_obsid_tree(tmp_path)
    assert list(spectral_input_files(OBSID, config)) == []


def write_region_files(directory, root, ra, dec, radius_arcsec):
    """Write the pair of ds9 region files ``get_best_source_region`` produces."""
    with open(os.path.join(directory, root + "_src.reg"), "w") as fobj:
        print(f'icrs\ncircle({ra}, {dec}, {radius_arcsec}")', file=fobj)
    with open(os.path.join(directory, root + "_bkg.reg"), "w") as fobj:
        print(f'icrs\n-circle({ra}, {dec}, 100")\ncircle({ra}, {dec}, 250")', file=fobj)


def test_existing_regions_are_read_back_on_a_rerun(tmp_path):
    """A rerun must return the positions it already measured, not ``(0, 0, 0)``.

    ``process_nustar_obsid`` feeds this straight into barycentring, so returning zeros
    would barycentre the data to RA=0, Dec=0.
    """
    pytest.importorskip("regions")
    config = make_obsid_tree(
        tmp_path, pipe_files=[f"nu{OBSID}A01_cl.evt", f"nu{OBSID}B01_cl.evt"]
    )
    pipedir = os.path.join(tmp_path, OBSID, "event_pipe")
    write_region_files(pipedir, f"nu{OBSID}A01_cl", 148.90, 69.66, 30.0)
    write_region_files(pipedir, f"nu{OBSID}B01_cl", 149.10, 69.70, 40.0)

    ra, dec, rlimit = get_best_source_regions.fn(OBSID, config)

    assert ra == pytest.approx(149.0, abs=1e-6)
    assert dec == pytest.approx(69.68, abs=1e-6)
    assert rlimit == pytest.approx(35.0, abs=1e-3)


def test_no_event_files_gives_no_position(tmp_path):
    config = make_obsid_tree(tmp_path)
    assert get_best_source_regions.fn(OBSID, config) == (0.0, 0.0, 0.0)


def test_mode_01_input_files_excludes_mode_06(full_observation):
    """The averaged position must come from mode 01 alone.

    Each CHU combination has its own aspect reconstruction, so mode-06 detections scatter
    by about 2 arcmin around the true position.
    """
    found = [os.path.basename(f) for _, f in mode_01_input_files(OBSID, full_observation)]
    assert found == [f"nu{OBSID}A01_cl.evt", f"nu{OBSID}B01_cl.evt"]
    assert not any("chu" in f for f in found)


def test_chu_positions_do_not_move_the_barycentring_position(tmp_path):
    """Regression: mode-06 regions must not be averaged into the returned position.

    Measured on 80002092008, averaging the eight CHU positions in alongside the two mode-01
    ones moved the mean by 63 arcsec. The barycentric delay is the Earth-Sun vector -- about
    499 light-seconds -- projected on the source direction, so an error of 63 arcsec is
    worth roughly 150 ms of delay, which ruins any timing analysis downstream.

    The numbers below reproduce that geometry: the mode-01 pair straddles the true position,
    and every CHU region sits about 2 arcmin north of it.
    """
    pytest.importorskip("regions")
    truth_ra, truth_dec = 148.9575, 69.6794
    chu_dec = truth_dec + 2 / 60  # the CHU aspect scatter, all in one direction

    split_files = [f"nu{OBSID}A06_chu{c}_N_cl.evt" for c in ("2", "3", "12", "23")]
    split_files += [f"nu{OBSID}B06_chu{c}_N_cl.evt" for c in ("2", "3", "12", "23")]
    config = make_obsid_tree(
        tmp_path,
        pipe_files=[f"nu{OBSID}A01_cl.evt", f"nu{OBSID}B01_cl.evt"],
        split_files=split_files,
    )

    pipedir = os.path.join(tmp_path, OBSID, "event_pipe")
    write_region_files(pipedir, f"nu{OBSID}A01_cl", truth_ra, truth_dec - 0.0005, 80.0)
    write_region_files(pipedir, f"nu{OBSID}B01_cl", truth_ra, truth_dec + 0.0005, 80.0)

    splitdir = os.path.join(tmp_path, OBSID, "split")
    for name in split_files:
        write_region_files(splitdir, name[: -len(".evt")], truth_ra, chu_dec, 80.0)

    ra, dec, _ = get_best_source_regions.fn(OBSID, config)

    assert dec == pytest.approx(truth_dec, abs=1e-6)
    measured = SkyCoord(ra, dec, unit="deg")
    assert measured.separation(SkyCoord(truth_ra, truth_dec, unit="deg")) < 1 * u.arcsec


def test_mode_06_chu_files_are_found(full_observation):
    found = [os.path.basename(f) for _, f in spectral_input_files(OBSID, full_observation)]
    assert f"nu{OBSID}A06_chu1_N_cl.evt" in found
    assert f"nu{OBSID}A06_chu12_N_cl.evt" in found
    assert f"nu{OBSID}B06_chu1_N_cl.evt" in found


def test_strict_split_files_are_found_too(tmp_path):
    """``nusplitsc splitmode=STRICT`` names its output ``_S_`` rather than ``_N_``."""
    config = make_obsid_tree(tmp_path, split_files=[f"nu{OBSID}A06_chu2_S_cl.evt"])
    found = [os.path.basename(f) for _, f in spectral_input_files(OBSID, config)]
    assert found == [f"nu{OBSID}A06_chu2_S_cl.evt"]


def test_mode_01_comes_before_mode_06(full_observation):
    """Mode 01 defines the reference position, so it must be processed first."""
    per_fpm = {}
    for fpm, infile in spectral_input_files(OBSID, full_observation):
        per_fpm.setdefault(fpm, []).append(os.path.basename(infile))
    for fpm, files in per_fpm.items():
        modes = ["01" if f"{fpm}01" in f else "06" for f in files]
        assert modes == sorted(modes)


def test_observation_without_mode_01_still_yields_chu_files(tmp_path):
    """80002092003 has no mode-01 data, but its recovered mode-06 data is usable."""
    config = make_obsid_tree(
        tmp_path,
        pipe_files=["nu80002092003A06_cl.evt"],
        split_files=["nu80002092003A06_chu3_N_cl.evt"],
        obsid="80002092003",
    )
    found = [os.path.basename(f) for _, f in spectral_input_files("80002092003", config)]
    assert found == ["nu80002092003A06_chu3_N_cl.evt"]


class TestPositionIsConsistent:
    """The mode-06 region must never be far from the mode-01 one.

    Each CHU combination has its own aspect reconstruction, scattered by about 2 arcmin
    (nusplitsc documentation), so a detection further away than that is a different object.
    """

    reference = SkyCoord(148.95, 69.68, unit="deg")

    def test_nearby_position_is_accepted(self):
        nearby = self.reference.directional_offset_by(0 * u.deg, 1 * u.arcmin)
        assert position_is_consistent(nearby, self.reference, 3 * u.arcmin)

    def test_distant_position_is_rejected(self):
        far = self.reference.directional_offset_by(0 * u.deg, 5 * u.arcmin)
        assert not position_is_consistent(far, self.reference, 3 * u.arcmin)

    def test_position_just_inside_the_limit_is_accepted(self):
        edge = self.reference.directional_offset_by(90 * u.deg, 2.9 * u.arcmin)
        assert position_is_consistent(edge, self.reference, 3 * u.arcmin)

    def test_without_a_reference_everything_is_accepted(self):
        anywhere = SkyCoord(12.3, -45.6, unit="deg")
        assert position_is_consistent(anywhere, None, 3 * u.arcmin)


def _shadowing_imports(path):
    """Function-local imports that rebind a name already imported at module level.

    Python decides at compile time that a name assigned anywhere in a function is local
    to the *whole* function. An ``import x`` inside one branch therefore makes ``x``
    unbound on every path that does not run that branch, even though the module imports
    it at the top -- an ``UnboundLocalError`` that only fires on the untaken branch.
    """
    import ast

    tree = ast.parse(path.read_text())
    module_level = {
        alias.asname or alias.name.split(".")[0]
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    found = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(func):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            for alias in node.names:
                name = alias.asname or alias.name.split(".")[0]
                if name in module_level:
                    found.append(f"{path.name}:{node.lineno} {func.name}() rebinds '{name}'")
    return found


def test_no_function_local_import_shadows_a_module_level_one():
    """Guard the bug that made ``u`` unbound in ``get_best_source_region``.

    ``get_best_source_region`` imported ``astropy.units as u`` inside its
    already-have-the-regions branch, which shadowed the module-level import and made
    ``u.arcmin`` raise on the branch that actually measures a region -- the path no
    offline test can reach, because it needs ``nustar_gen`` and a real image.
    """
    import pathlib

    package = pathlib.Path(__file__).parent.parent
    offenders = []
    for path in sorted(package.glob("*.py")):
        offenders.extend(_shadowing_imports(path))
    assert offenders == []


class TestJoinSourceData:
    """The list of files the join step hands to the rest of the pipeline.

    ``process_nustar_obsid`` flare-filters and barycentres whatever comes back from
    :func:`join_source_data`, so returning a different set on a rerun than on a fresh run
    changes what the pipeline does the second time it is run. See issue 6 in
    ``docs/known_issues.rst``.
    """

    @staticmethod
    def _tree(base, label="_src1", extra=()):
        """An output directory as it looks after a successful join."""
        outdir = os.path.join(base, OBSID)
        os.makedirs(outdir, exist_ok=True)
        names = [
            f"nu{OBSID}{label}.evt",  # the combined FPMA+FPMB file: the only real product
            f"nu{OBSID}A{label}.evt",  # per-module intermediates
            f"nu{OBSID}B{label}.evt",
            f"nu{OBSID}A01{label}.evt",  # per-mode intermediates, copied in by the join
            f"nu{OBSID}B01{label}.evt",
        ]
        for name in list(names) + list(extra):
            open(os.path.join(outdir, name), "w").close()
        return outdir, dict(out_data_path=str(base), input_data_path=str(base))

    def test_a_rerun_returns_only_the_combined_file(self, tmp_path):
        outdir, config = self._tree(tmp_path)
        open(os.path.join(outdir, "JOIN_DONE_SRC1.TXT"), "w").close()

        files = join_source_data.fn(OBSID, [], config)

        assert files == [os.path.join(outdir, f"nu{OBSID}_src1.evt")]

    def test_a_rerun_of_the_background_returns_only_its_combined_file(self, tmp_path):
        outdir, config = self._tree(tmp_path, label="_back")
        open(os.path.join(outdir, "JOIN_DONE_SRC0.TXT"), "w").close()

        files = join_source_data.fn(OBSID, [], config, src_num=0)

        assert files == [os.path.join(outdir, f"nu{OBSID}_back.evt")]

    def test_a_rerun_without_the_combined_file_returns_nothing(self, tmp_path):
        """A sentinel with no product behind it is not a reason to hand back a path."""
        outdir = os.path.join(tmp_path, OBSID)
        os.makedirs(outdir)
        open(os.path.join(outdir, "JOIN_DONE_SRC1.TXT"), "w").close()

        assert join_source_data.fn(OBSID, [], dict(out_data_path=str(tmp_path))) == []

    def test_a_rerun_returns_what_the_fresh_run_returned(self, tmp_path, monkeypatch):
        """The two code paths must agree; today the cached one returns five files."""
        merged = []

        def fake_merge(files, outfile, gti_operation="OR"):
            merged.append((list(files), outfile, gti_operation))
            open(outfile, "w").close()

        monkeypatch.setattr(nustar, "merge_event_files", fake_merge)

        pipedir = os.path.join(tmp_path, OBSID, "event_pipe")
        os.makedirs(pipedir)
        for fpm in "A", "B":
            for mode in "01", "06":
                open(os.path.join(pipedir, f"nu{OBSID}{fpm}{mode}_src1.evt"), "w").close()
        config = dict(out_data_path=str(tmp_path), input_data_path=str(tmp_path))

        fresh = join_source_data.fn(OBSID, [pipedir], config)
        rerun = join_source_data.fn(OBSID, [pipedir], config)

        assert fresh == rerun

    def test_the_fpmb_file_is_not_derived_by_replacing_every_a_in_the_path(
        self, tmp_path, monkeypatch
    ):
        """``a_file.replace("A", "B")`` rewrites directory names as well as the module."""
        merged = []

        def fake_merge(files, outfile, gti_operation="OR"):
            merged.append((list(files), outfile, gti_operation))
            open(outfile, "w").close()

        monkeypatch.setattr(nustar, "merge_event_files", fake_merge)

        base = os.path.join(tmp_path, "ARCHIVE")  # a capital A in the output path
        pipedir = os.path.join(base, OBSID, "event_pipe")
        os.makedirs(pipedir)
        for fpm in "A", "B":
            open(os.path.join(pipedir, f"nu{OBSID}{fpm}01_src1.evt"), "w").close()
        config = dict(out_data_path=base, input_data_path=base)

        join_source_data.fn(OBSID, [pipedir], config)

        combined = [call for call in merged if call[2] == "AND"]
        assert len(combined) == 1
        inputs = combined[0][0]
        assert inputs == [
            os.path.join(base, OBSID, f"nu{OBSID}A_src1.evt"),
            os.path.join(base, OBSID, f"nu{OBSID}B_src1.evt"),
        ]


def make_synthetic_event_file(path, tstart=0.0, tstop=1000.0, nevents=500, seed=42):
    """A NuSTAR-shaped event file: EVENTS with TIME and PI, plus a GTI extension."""
    from astropy.io import fits

    rng = np.random.default_rng(seed)
    times = np.sort(rng.uniform(tstart, tstop, nevents))
    # PI 35 is 3 keV and PI 1935 is 79 keV, via E = 0.04 * PI + 1.6.
    pi = rng.integers(35, 1935, nevents)

    events = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="TIME", format="D", array=times),
            fits.Column(name="PI", format="J", array=pi),
        ],
        name="EVENTS",
    )
    events.header["TIMEZERO"] = 0.0
    events.header["TSTART"] = tstart
    events.header["TSTOP"] = tstop
    # NuSTAR's reference epoch, 2010-01-01 00:00:00 UTC.
    events.header["MJDREFI"] = 55197
    events.header["MJDREFF"] = 0.00076601852
    events.header["ONTIME"] = tstop - tstart
    events.header["LIVETIME"] = 0.9 * (tstop - tstart)
    events.header["EXPOSURE"] = 0.9 * (tstop - tstart)

    gti = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=np.array([tstart])),
            fits.Column(name="STOP", format="D", array=np.array([tstop])),
        ],
        name="GTI",
    )
    fits.HDUList([fits.PrimaryHDU(), events, gti]).writeto(path, overwrite=True)
    return str(path)


class TestObservationTimeSpan:
    """The interval GOES is asked about.

    It has to be the whole observation. A mode-06 CHU subset can be a few minutes long,
    and GOES samples once a minute with gaps: 90201037002 asked about ``A06_chu3`` alone,
    got a time series with no rows, and died in astropy with "cannot guess format from
    input values with zero-size array".
    """

    def observation(self, tmp_path, spans, modes=("A01", "B01")):
        config = make_obsid_tree(tmp_path)
        pipedir = os.path.join(tmp_path, OBSID, "event_pipe")
        for mode, (tstart, tstop) in zip(modes, spans):
            make_synthetic_event_file(
                os.path.join(pipedir, f"nu{OBSID}{mode}_cl.evt"), tstart=tstart, tstop=tstop
            )
        return config

    def test_it_covers_every_mode_01_file(self, tmp_path):
        config = self.observation(tmp_path, [(100.0, 900.0), (300.0, 1500.0)])

        tstart, tstop, gti, mjdref = nustar.observation_time_span(OBSID, config)

        assert tstart == pytest.approx(100.0)
        assert tstop == pytest.approx(1500.0)
        assert mjdref == pytest.approx(55197.00076601852)

    def test_the_gti_is_the_union_of_the_files(self, tmp_path):
        config = self.observation(tmp_path, [(0.0, 400.0), (600.0, 1000.0)])

        _, _, gti, _ = nustar.observation_time_span(OBSID, config)

        assert gti.tolist() == [[0.0, 400.0], [600.0, 1000.0]]

    def test_a_gti_wider_than_the_header_wins(self, tmp_path):
        """ftmerge copies TSTART/TSTOP from its first input, so on a merged file the
        header can be narrower than the data. Issue 35 in known_issues.rst."""
        config = self.observation(tmp_path, [(0.0, 1000.0)], modes=("A01",))
        path = os.path.join(tmp_path, OBSID, "event_pipe", f"nu{OBSID}A01_cl.evt")
        from astropy.io import fits

        with fits.open(path, mode="update") as hdul:
            hdul[1].header["TSTART"] = 400.0
            hdul[1].header["TSTOP"] = 600.0

        tstart, tstop, _, _ = nustar.observation_time_span(OBSID, config)

        assert tstart == pytest.approx(0.0)
        assert tstop == pytest.approx(1000.0)

    def test_without_mode_01_every_cleaned_file_is_used(self, tmp_path):
        """80002092003 has no mode-01 data at all, and still has to be asked about."""
        config = self.observation(tmp_path, [(50.0, 800.0)], modes=("A06",))

        tstart, tstop, _, _ = nustar.observation_time_span(OBSID, config)

        assert tstart == pytest.approx(50.0)
        assert tstop == pytest.approx(800.0)

    def test_an_observation_with_no_event_file_says_so(self, tmp_path):
        config = make_obsid_tree(tmp_path)

        with pytest.raises(ValueError, match=OBSID):
            nustar.observation_time_span(OBSID, config)


class TestRequireGoesCoverage:
    """Fatal on purpose: keeping all the good time would turn the flare filtering off
    without saying so, which is not the pipeline's decision to make."""

    def test_one_measurement_is_enough(self):
        assert nustar.require_goes_coverage(1, OBSID, 0.0, 100.0) is None

    def test_no_measurement_raises_naming_the_observation(self):
        with pytest.raises(nustar.NoGoesCoverage) as excinfo:
            nustar.require_goes_coverage(0, OBSID, 0.0, 100.0)

        assert OBSID in str(excinfo.value)


class TestPlotFlareFiltering:
    """The diagnostic figure. A smoke test, not a rendering test.

    What is checked is that it runs headless, writes a non-empty file, and leaves no
    figure behind in pyplot's registry -- the leak that issue 31 is about.
    """

    def test_it_writes_a_figure_and_leaks_none(self, tmp_path):
        plt = pytest.importorskip("matplotlib.pyplot")
        event_file = make_synthetic_event_file(tmp_path / f"nu{OBSID}_src1.evt")

        outfile = plot_flare_filtering.fn(event_file, [[0, 1000]], [[0, 400], [600, 1000]])

        assert outfile == str(tmp_path / f"nu{OBSID}_src1_flares.jpg")
        assert os.path.getsize(outfile) > 0
        assert len(plt.get_fignums()) == 0, "a figure was left open"

    def test_the_goes_panel_is_used_when_the_light_curve_is_there(self, tmp_path):
        """The panel is filled from the observation's light curve, no second download."""
        pytest.importorskip("matplotlib")
        from astropy.table import Table

        event_file = make_synthetic_event_file(tmp_path / f"nu{OBSID}_src1.evt")
        times = np.linspace(0, 1000, 100)
        flux = np.full_like(times, 1e-7)
        flux[40:60] = 1e-5  # an M-class flare in the middle
        goes_lc_file = str(tmp_path / f"nu{OBSID}_goes.fits")
        Table({"TIME": times, "XRSA": flux / 10, "XRSB": flux}).write(goes_lc_file)

        outfile = plot_flare_filtering.fn(
            event_file, [[0, 1000]], [[0, 400], [600, 1000]], goes_lc_file=goes_lc_file
        )

        assert os.path.getsize(outfile) > 0

    def test_it_works_without_a_goes_light_curve(self, tmp_path):
        """A rerun skips the download, so the file may legitimately be missing."""
        pytest.importorskip("matplotlib")
        event_file = make_synthetic_event_file(tmp_path / f"nu{OBSID}_src1.evt")

        outfile = plot_flare_filtering.fn(
            event_file, [[0, 1000]], [[0, 1000]], goes_lc_file=str(tmp_path / "gone.fits")
        )

        assert os.path.getsize(outfile) > 0

    def test_it_works_when_no_light_curve_was_named_at_all(self, tmp_path):
        pytest.importorskip("matplotlib")
        event_file = make_synthetic_event_file(tmp_path / f"nu{OBSID}_src1.evt")

        outfile = plot_flare_filtering.fn(event_file, [[0, 1000]], [[0, 1000]])

        assert os.path.getsize(outfile) > 0

    def test_nothing_removed_still_draws(self, tmp_path):
        pytest.importorskip("matplotlib")
        event_file = make_synthetic_event_file(tmp_path / f"nu{OBSID}_back.evt")

        outfile = plot_flare_filtering.fn(event_file, [[0, 1000]], [[0, 1000]])

        assert os.path.exists(outfile)


class TestChi2DofAgainstAConstant:
    def test_a_constant_light_curve_gives_about_one(self):
        rng = np.random.default_rng(0)
        counts = rng.poisson(100, 200).astype(float)
        lc = dict(
            counts=counts,
            exposure=np.full(200, 10.0),
            rate=counts / 10.0,
            time=np.arange(200) * 10.0,
        )

        assert chi2_dof_against_a_constant(lc) == pytest.approx(1.0, abs=0.2)

    def test_a_flaring_light_curve_gives_much_more(self):
        counts = np.full(200, 100.0)
        counts[100:110] = 1000.0
        lc = dict(counts=counts, exposure=np.full(200, 10.0), rate=counts / 10.0)

        assert chi2_dof_against_a_constant(lc) > 10

    def test_empty_bins_do_not_divide_by_zero(self):
        counts = np.array([0.0, 0.0, 5.0])
        lc = dict(counts=counts, exposure=np.full(3, 10.0), rate=counts / 10.0)

        assert np.isfinite(chi2_dof_against_a_constant(lc))

    def test_too_few_bins_to_say_anything(self):
        lc = dict(counts=np.array([1.0]), exposure=np.array([10.0]), rate=np.array([0.1]))

        assert np.isnan(chi2_dof_against_a_constant(lc))


class TestGoesClassToFlux:
    def test_the_class_letters_are_decades(self):
        assert goes_class_to_flux("C1.0") == pytest.approx(1e-6)
        assert goes_class_to_flux("M1.0") == pytest.approx(1e-5)
        assert goes_class_to_flux("X1.0") == pytest.approx(1e-4)

    def test_the_multiplier_scales_within_a_decade(self):
        assert goes_class_to_flux("C5.0") == pytest.approx(5e-6)
        assert goes_class_to_flux("M2.5") == pytest.approx(2.5e-5)

    def test_a_lower_case_letter_works(self):
        assert goes_class_to_flux("c5.0") == pytest.approx(5e-6)

    def test_the_default_cut_is_where_it_should_be(self):
        """C5.0 must sit above the quiescent 1--8 A flux, not below it."""
        assert goes_class_to_flux("C5.0") > 1.5e-6  # Feb 2014 quiescent level


class TestPlotFlareFilteringWithoutTheFluxCut:
    def test_flux_class_none_still_draws(self, tmp_path):
        """The flux criterion can be turned off; the figure must still be produced."""
        pytest.importorskip("matplotlib")
        from astropy.table import Table

        event_file = make_synthetic_event_file(tmp_path / f"nu{OBSID}_src1.evt")
        times = np.linspace(0, 1000, 100)
        goes_lc_file = str(tmp_path / f"nu{OBSID}_goes.fits")
        Table({"TIME": times, "XRSA": np.full(100, 1e-8), "XRSB": np.full(100, 1e-7)}).write(
            goes_lc_file
        )

        outfile = plot_flare_filtering.fn(
            event_file,
            [[0, 1000]],
            [[0, 400], [600, 1000]],
            goes_lc_file=goes_lc_file,
            flux_class=None,
        )

        assert os.path.getsize(outfile) > 0


class TestNustarPaths:
    """The local layout one NuSTAR observation maps to.

    These are plain functions, not Prefect tasks: they are one-line string joins, and
    wrapping them made them 43% of the task runs in a real observation.
    """

    CONFIG = {"out_data_path": "out", "input_data_path": "raw"}

    def test_the_output_directory_is_the_obsid_under_out_data_path(self):
        assert nu_base_output_path(OBSID, self.CONFIG) == os.path.join("out", OBSID)

    def test_the_level_2_products_go_in_event_pipe(self):
        assert nu_pipeline_output_path(OBSID, self.CONFIG) == os.path.join(
            "out", OBSID, "event_pipe"
        )

    def test_the_spectral_products_go_in_products(self):
        assert nu_product_output_path(OBSID, self.CONFIG) == os.path.join(
            "out", OBSID, "products"
        )

    def test_no_directory_ends_in_a_slash(self):
        """Measured in a nuproducts log: given ``.../event_pipe/`` as ``indir``, the tool
        built ``.../event_pipe//nu<OBSID>A_fpm.hk``, one character wasted out of 128."""
        for path in (
            nu_pipeline_output_path(OBSID, self.CONFIG),
            nu_product_output_path(OBSID, self.CONFIG),
            split_path(OBSID, self.CONFIG),
            nu_base_output_path(OBSID, self.CONFIG),
        ):
            assert not path.endswith("/")

    def test_the_per_chu_files_go_in_split(self):
        """No trailing slash: nusplitsc adds one, and "split//" wastes a character of a
        budget that is only 128 wide on some HEASOFT builds."""
        assert split_path(OBSID, self.CONFIG) == os.path.join("out", OBSID, "split")

    def test_the_sentinel_sits_beside_the_level_2_products(self):
        done = nu_pipeline_done_file(OBSID, self.CONFIG)

        assert done == os.path.join("out", OBSID + "/event_pipe/", "PIPELINE_DONE.TXT")

    def test_the_raw_data_directory_comes_from_input_data_path(self):
        assert nu_local_raw_data_path(OBSID, self.CONFIG) == os.path.join("raw", OBSID)

    def test_the_derived_names_hang_off_the_event_file_root(self):
        event_file = "out/x/nu123A01_cl.evt"

        assert flare_filtered_event_file_name(event_file) == "out/x/nu123A01_cl_noflares.evt"

    def test_the_goes_files_are_per_observation_not_per_event_file(self):
        """The Sun does not care which module or CHU subset the events came from, and a
        CHU subset a few minutes long can fall entirely inside a gap in GOES sampling."""
        assert nu_goes_lc_file(OBSID, self.CONFIG) == os.path.join(
            "out", OBSID, f"nu{OBSID}_goes.fits"
        )
        assert nu_goes_gti_file(OBSID, self.CONFIG) == os.path.join(
            "out", OBSID, f"nu{OBSID}_goes.gti"
        )

    def test_the_goes_files_sit_in_the_observation_directory(self):
        for path in (
            nu_goes_lc_file(OBSID, self.CONFIG),
            nu_goes_gti_file(OBSID, self.CONFIG),
        ):
            assert os.path.dirname(path) == nu_base_output_path(OBSID, self.CONFIG)


class StubHeasoft:
    """The three HEASOFT calls ``merge_event_files`` makes, recorded, not run.

    The files it leaves behind are not empty: ``heasoft.run`` now checks that each tool
    produced what it said it would, and a real ``ftmerge`` writes a real event file.
    """

    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def _record(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if name == self.fail_on:
            raise RuntimeError(f"{name} failed")
        if name == "ftmerge":
            with open(kwargs["outfile"].lstrip("!"), "w") as fobj:
                fobj.write("not a real event file, but not an empty one either")

    def ftmerge(self, **kwargs):
        self._record("ftmerge", **kwargs)

    def ftsort(self, **kwargs):
        self._record("ftsort", **kwargs)

    def fappend(self, **kwargs):
        self._record("fappend", **kwargs)


class TestMergeGtisCallsHeasoftCorrectly:
    """Every HEASOFT call in ``merge_gtis`` has to say which extension it means.

    Measured in the HEASOFT x86 environment: ``ftsort infile=<gti file>`` with no extension
    lands on the primary header and fails with ``CFITSIO ERROR NOT_TABLE``, return code 235,
    single-threaded and every time. It went unnoticed for as long as it did because
    ``heasoftpy`` defaults to ``allow_failure=True`` and the pipeline never looked at the
    return code, so the merged GTIs were simply never sorted.
    """

    def calls(self, monkeypatch, tmp_path):
        recorded = []

        def stub_run(name, *args, **kwargs):
            recorded.append((name, kwargs))
            return None

        monkeypatch.setattr(nustar.heasoft, "run", stub_run)
        out = os.path.join(tmp_path, "merged.gti")
        nustar.merge_gtis.fn(["a.evt", "b.evt"], out)
        return out, dict(recorded), [name for name, _ in recorded]

    def test_the_tools_run_in_order(self, tmp_path, monkeypatch):
        _, _, names = self.calls(monkeypatch, tmp_path)

        assert names == ["ftmgtime", "ftsort", "fthedit"]

    def test_ftsort_names_the_table_extension(self, tmp_path, monkeypatch):
        out, calls, _ = self.calls(monkeypatch, tmp_path)

        assert calls["ftsort"]["infile"] == out + "[1]"

    def test_ftsort_writes_back_over_the_same_file(self, tmp_path, monkeypatch):
        out, calls, _ = self.calls(monkeypatch, tmp_path)

        assert calls["ftsort"]["outfile"] == "!" + out
        assert calls["ftsort"]["columns"] == "START"

    def test_ftmgtime_asks_for_the_gti_extensions(self, tmp_path, monkeypatch):
        out, calls, _ = self.calls(monkeypatch, tmp_path)

        assert calls["ftmgtime"]["ingtis"] == "a.evt[GTI],b.evt[GTI]"
        assert calls["ftmgtime"]["outgti"] == out


class TestMergeEventFilesTemporary:
    """The intermediate GTI file must be predictable, and must not survive."""

    def merge(self, tmp_path, monkeypatch, fail_on=None):
        """Run the merge with HEASOFT and the GTI merge stubbed out."""
        gti_names = []

        def stub_merge_gtis(files_to_join, outfile_gti, gti_operation="OR"):
            gti_names.append(outfile_gti)
            with open(outfile_gti, "w") as fobj:
                fobj.write("not a real GTI file, but not an empty one either")

        monkeypatch.setattr(nustar, "merge_gtis", stub_merge_gtis)
        monkeypatch.setattr(heasoft, "hsp", StubHeasoft(fail_on=fail_on), raising=False)
        monkeypatch.setattr(heasoft, "HAS_HEASOFT", True)

        outfile = os.path.join(tmp_path, "nu123A_src1.evt")
        nustar.merge_event_files.fn(["a.evt", "b.evt"], outfile)
        return gti_names[0]

    def test_no_gti_file_is_left_behind(self, tmp_path, monkeypatch):
        self.merge(tmp_path, monkeypatch)

        assert glob.glob(os.path.join(tmp_path, "*.gti")) == []

    def test_the_intermediate_name_is_the_same_every_run(self, tmp_path, monkeypatch):
        first = self.merge(tmp_path, monkeypatch)
        second = self.merge(tmp_path, monkeypatch)

        assert first == second

    def test_the_intermediate_sits_beside_its_output(self, tmp_path, monkeypatch):
        name = self.merge(tmp_path, monkeypatch)

        assert os.path.dirname(name) == str(tmp_path)
        assert name.endswith(".gti")
        assert "nu123A_src1" in os.path.basename(name)

    def test_a_failed_merge_still_cleans_up(self, tmp_path, monkeypatch):
        with pytest.raises(RuntimeError):
            self.merge(tmp_path, monkeypatch, fail_on="fappend")

        assert glob.glob(os.path.join(tmp_path, "*.gti")) == []


class TestGoesDownloadPath:
    """Where the raw GOES files land.

    Sunpy's default is one shared download directory. Two observations from the same day
    ask for the same file, and with several reductions running at once one of them can be
    handed a file the other is still writing. See issue 26 in ``docs/known_issues.rst``.
    """

    CONFIG = {"out_data_path": "/data"}

    def test_the_files_land_in_the_observation_directory(self):
        path = nustar.goes_download_path("90901333002", self.CONFIG)

        assert path == os.path.join("/data", "90901333002", "{file}")

    def test_two_observations_do_not_share_a_directory(self):
        first = nustar.goes_download_path("90901333002", self.CONFIG)
        second = nustar.goes_download_path("80002092008", self.CONFIG)

        assert first != second


class TestLongestOutputName:
    """What the length check has to be measured against.

    Some HEASOFT builds truncate file names at 128 characters, so the flow refuses to
    start when the longest name the reduction would build does not fit. That is only
    meaningful if the name it checks really is the longest one. See issue 39 in
    ``docs/known_issues.rst``.
    """

    CONFIG = {"out_data_path": "/scratch/out", "input_data_path": "/scratch/raw"}

    def other_names_the_reduction_builds(self, obsid, config):
        """Every long name any step constructs, for comparison.

        Taken from two complete reductions on disk, not from reading the code: the walk
        of a finished output tree is what showed that the mode-06 image beats the
        ``nusplitsc`` temporary this function used to return.
        """
        pid = "9" * 7
        split = split_path(obsid, config)
        events = nu_pipeline_output_path(obsid, config)
        base = nu_base_output_path(obsid, config)
        products = nu_product_output_path(obsid, config)
        # The worst case for a mode-06 stem: all three star trackers in the solution.
        stem6 = f"nu{obsid}A06_chu123_N"
        return [
            # nusplitsc, per CHU combination
            os.path.join(split, f"nu{obsid}_chu123_merge_{pid}.fits"),
            os.path.join(split, f"nu{obsid}_chu123_gti_{pid}.fits"),
            os.path.join(split, stem6 + "_cl.evt"),
            os.path.join(split, f"xselect_chu123_{pid}.xco"),
            # nustar_gen's make_image, which is xselect: the mode-06 case is the longest
            # name the whole reduction builds
            os.path.join(split, stem6 + "_cl_3to80keV.log"),
            os.path.join(events, f"nu{obsid}A01_cl_3to80keV.fits"),
            # regions, flare filtering and its GTIs
            os.path.join(split, stem6 + "_cl_src.reg"),
            os.path.join(split, stem6 + "_cl_noflares.gti"),
            # merge_gtis re-sorts in place, and CFITSIO's clobber prefix is a character
            # of the name as the tool sees it
            "!" + os.path.join(split, stem6 + "_cl_noflares.gti"),
            os.path.join(split, stem6 + "_cl_goes.fits"),
            os.path.join(split, stem6 + "_cl_src1.evt"),
            # nupipeline and nuscreen
            os.path.join(events, f"nu{obsid}A01_gti.fits"),
            os.path.join(events, f"nu{obsid}A01_cl.evt"),
            os.path.join(events, f"nu{obsid}A_uf.evt"),
            # merging, flare filtering, barycentring
            os.path.join(base, f"nu{obsid}A01_cl_noflares_bary.evt"),
            os.path.join(base, f"nu{obsid}_src1_noflares_bary.evt"),
            os.path.join(base, f"nu{obsid}A01_cl_goes.fits"),
            # nuproducts, including its per-CHU mode-06 products
            os.path.join(products, f"nu{obsid}A01_sr.pha"),
            os.path.join(products, stem6 + "_grp.pha"),
            os.path.join(products, stem6 + "_sr.rmf"),
            os.path.join(products, f"{pid}_skymap_vign.img"),
        ]

    def test_nothing_the_reduction_builds_is_longer(self):
        longest = nu_longest_output_name(OBSID, self.CONFIG)

        for name in self.other_names_the_reduction_builds(OBSID, self.CONFIG):
            assert len(name) <= len(longest), f"{name} is longer than {longest}"

    def test_it_is_the_mode_06_image(self):
        """An xselect ``save image`` output, which is the write side that truncates."""
        longest = nu_longest_output_name(OBSID, self.CONFIG)

        assert longest.startswith(split_path(OBSID, self.CONFIG))
        assert "A06_chu123_N_cl_" in longest
        assert longest.endswith("keV.fits")

    def test_it_allows_for_all_three_star_trackers(self):
        """``nusplitsc`` splits by CHU combination; ``chu123`` is the longest of them."""
        longest = nu_longest_output_name(OBSID, self.CONFIG)

        assert "chu123" in longest

    def test_the_band_is_the_one_the_image_step_actually_uses(self):
        """``make_image`` names its output ``<stem>_<elow>to<ehigh>keV.fits``, so a change
        to the default band changes the longest file name the reduction builds."""
        import inspect

        defaults = inspect.signature(nustar.get_best_source_region).parameters

        assert defaults["elow"].default == nustar.IMAGE_ELOW
        assert defaults["ehigh"].default == nustar.IMAGE_EHIGH
        assert f"{nustar.IMAGE_ELOW}to{nustar.IMAGE_EHIGH}keV" in nu_longest_output_name(
            OBSID, self.CONFIG
        )

    def test_it_grows_with_the_output_root(self):
        short = nu_longest_output_name(OBSID, {"out_data_path": "/a"})
        long = nu_longest_output_name(OBSID, {"out_data_path": "/aaaaaaaaaa"})

        assert len(long) == len(short) + 9

    def test_the_pipeline_adds_61_characters_after_the_output_root(self):
        """The number quoted in the docs and in the commit messages: an output root of
        more than 67 characters cannot work against a 128-character limit."""
        root = "/scratch/out"
        longest = nu_longest_output_name(OBSID, {"out_data_path": root})

        assert len(longest) - len(root) == 61


class TestRecoverSpacecraftScienceWithoutMode06:
    """An observation with no mode-06 data must still finish the step.

    Not every observation has spacecraft-science data: CHU4, the star tracker on the
    optics bench, only loses its solution when the Sun or the Moon blinds it. Four of the
    56 M82 observations reprocessed in 2026 had good mode-01 science and no mode-06 at
    all -- 30202022003, 30202022007, 90202038001 and 90901332001 -- and every one of them
    failed the whole observation on ``FileNotFoundError`` for the sentinel, because
    ``nusplitsc`` never ran and so never created the directory it was to be written in.
    """

    def config(self, tmp_path):
        return dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path))

    def raw_and_pipeline_dirs(self, tmp_path, modes=()):
        """Lay out the input tree, with a cleaned event file for each mode given."""
        config = self.config(tmp_path)
        ev_dir = nu_pipeline_output_path(OBSID, config)
        hk_dir = os.path.join(nu_local_raw_data_path(OBSID, config), "hk")
        os.makedirs(ev_dir, exist_ok=True)
        os.makedirs(hk_dir, exist_ok=True)
        for mode in modes:
            for fpm in "AB":
                open(os.path.join(ev_dir, f"nu{OBSID}{fpm}{mode}_cl.evt"), "w").close()
                open(os.path.join(ev_dir, f"nu{OBSID}{fpm}_fpm.hk"), "w").close()
        open(os.path.join(hk_dir, f"nu{OBSID}_chu123.fits"), "w").close()
        return config

    def run(self, tmp_path, monkeypatch, modes=()):
        called = []

        def stub_run(name, *args, **kwargs):
            called.append((name, kwargs))
            os.makedirs(kwargs["outdir"], exist_ok=True)
            return None

        monkeypatch.setattr(nustar.heasoft, "run", stub_run)
        config = self.raw_and_pipeline_dirs(tmp_path, modes=modes)
        splitdir = nustar.recover_spacecraft_science_data.fn(OBSID, config)
        return splitdir, called

    def test_it_does_not_raise_when_there_is_no_mode_06(self, tmp_path, monkeypatch):
        splitdir, called = self.run(tmp_path, monkeypatch, modes=("01", "02", "03"))

        assert splitdir == split_path(OBSID, self.config(tmp_path))
        assert called == []

    def test_the_sentinel_is_written_even_with_nothing_to_split(self, tmp_path, monkeypatch):
        splitdir, _ = self.run(tmp_path, monkeypatch, modes=("01",))

        assert os.path.exists(os.path.join(splitdir, "RECOVER_DONE.TXT"))

    def test_a_second_run_skips_the_work(self, tmp_path, monkeypatch):
        self.run(tmp_path, monkeypatch, modes=("01",))
        _, called_again = self.run(tmp_path, monkeypatch, modes=("01", "06"))

        assert called_again == [], "the sentinel should have stopped it"

    def test_mode_06_still_gets_split(self, tmp_path, monkeypatch):
        _, called = self.run(tmp_path, monkeypatch, modes=("01", "06"))

        assert [name for name, _ in called] == ["nusplitsc", "nusplitsc"]


class TestObservingModesPresent:
    """Which observing modes Level 2 produced, and whether any of them is science.

    Measured on the 56 M82 observations reprocessed in 2026: every one of the 32 that
    reduced successfully produced both mode 01 and mode 06. The four slews produced
    neither -- 30202022001 had only mode 03, and 30502020001, 30502020003 and 30502022001
    had only modes 02 and 03.
    """

    def tree(self, tmp_path, *names):
        config = dict(input_data_path=str(tmp_path), out_data_path=str(tmp_path))
        pipedir = nu_pipeline_output_path(OBSID, config)
        os.makedirs(pipedir, exist_ok=True)
        for name in names:
            open(os.path.join(pipedir, name), "w").close()
        return config

    def test_it_lists_the_modes_it_finds(self, tmp_path):
        config = self.tree(
            tmp_path,
            f"nu{OBSID}A01_cl.evt",
            f"nu{OBSID}B01_cl.evt",
            f"nu{OBSID}A06_cl.evt",
        )

        assert nustar.observing_modes_present(OBSID, config) == ["01", "06"]

    def test_compressed_files_count_too(self, tmp_path):
        config = self.tree(tmp_path, f"nu{OBSID}A01_cl.evt.gz")

        assert nustar.observing_modes_present(OBSID, config) == ["01"]

    def test_it_ignores_files_of_other_observations(self, tmp_path):
        config = self.tree(tmp_path, "nu99999999999A01_cl.evt", f"nu{OBSID}A03_cl.evt")

        assert nustar.observing_modes_present(OBSID, config) == ["03"]

    def test_it_ignores_unfiltered_and_uncleaned_files(self, tmp_path):
        config = self.tree(
            tmp_path, f"nu{OBSID}A01_uf.evt", f"nu{OBSID}A_fpm.hk", f"nu{OBSID}A06_cl.evt"
        )

        assert nustar.observing_modes_present(OBSID, config) == ["06"]

    def test_a_slew_has_no_science_data(self, tmp_path):
        config = self.tree(tmp_path, f"nu{OBSID}A02_cl.evt", f"nu{OBSID}A03_cl.evt")

        assert nustar.observing_modes_present(OBSID, config) == ["02", "03"]
        assert not nustar.has_science_data(OBSID, config)

    def test_mode_01_alone_is_science(self, tmp_path):
        config = self.tree(tmp_path, f"nu{OBSID}A01_cl.evt", f"nu{OBSID}A03_cl.evt")

        assert nustar.has_science_data(OBSID, config)

    def test_mode_06_alone_is_science(self, tmp_path):
        config = self.tree(tmp_path, f"nu{OBSID}A06_cl.evt")

        assert nustar.has_science_data(OBSID, config)

    def test_an_empty_pipeline_directory_is_not_science(self, tmp_path):
        config = self.tree(tmp_path)

        assert nustar.observing_modes_present(OBSID, config) == []
        assert not nustar.has_science_data(OBSID, config)


class TestSnrOptimisedRadius:
    """A file too faint to place a region on must not take the observation down.

    ``nustar_gen``'s ``optimize_radius_snr`` binds ``best_radius`` only inside
    ``if snr > old_snr``, and ``old_snr`` starts at zero. On a flat radial profile the
    condition never holds and the return statement raises ``UnboundLocalError``.

    Reproduced against nustar_gen 0.8.dev9 with a flat profile, with and without counts.
    It cost three of the 56 M82 observations reprocessed in 2026: 30202022008, 30702012004
    and 90101005002.
    """

    def test_a_radius_comes_straight_back(self):
        def optimize(rind, rad_profile, radial_err, psf_profile, show=True):
            return 42.0

        assert nustar.snr_optimised_radius(optimize, [1], [1], [1], [1]) == 42.0

    def test_the_optimisation_is_asked_not_to_plot(self):
        seen = {}

        def optimize(rind, rad_profile, radial_err, psf_profile, show=True):
            seen["show"] = show
            return 1.0

        nustar.snr_optimised_radius(optimize, [1], [1], [1], [1])

        assert seen["show"] is False

    def test_no_best_radius_becomes_none(self):
        def optimize(rind, rad_profile, radial_err, psf_profile, show=True):
            """Exactly what nustar_gen does when the SNR never rises above zero."""
            if False:
                best_radius = 1
            return best_radius  # noqa: F821

        assert nustar.snr_optimised_radius(optimize, [1], [1], [1], [1]) is None

    def test_other_failures_are_not_swallowed(self):
        """Only the missing best_radius is tolerated; a real error must still be seen."""

        def optimize(rind, rad_profile, radial_err, psf_profile, show=True):
            raise ValueError("the profile is nonsense")

        with pytest.raises(ValueError, match="nonsense"):
            nustar.snr_optimised_radius(optimize, [1], [1], [1], [1])
