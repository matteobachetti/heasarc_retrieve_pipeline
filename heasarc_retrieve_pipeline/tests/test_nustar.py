"""Offline tests for the NuSTAR module.

These need neither the network nor a HEASOFT installation: they exercise the pure
file-selection helpers that decide which event files the pipeline works on.
"""

import os

# These tests call Prefect tasks through ``.fn``, outside any flow run. Prefect's API log
# handler warns about that on every call; it has nothing to report to.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

import numpy as np  # noqa: E402
import pytest  # noqa: E402

from astropy.coordinates import SkyCoord  # noqa: E402
import astropy.units as u  # noqa: E402

from heasarc_retrieve_pipeline import nustar  # noqa: E402
from heasarc_retrieve_pipeline.nustar import (  # noqa: E402
    chi2_dof_against_a_constant,
    get_best_source_regions,
    goes_class_to_flux,
    join_source_data,
    plot_flare_filtering,
    mode_01_input_files,
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
        """The panel is filled from ``<root>_goes.fits``, with no second download."""
        pytest.importorskip("matplotlib")
        from astropy.table import Table

        event_file = make_synthetic_event_file(tmp_path / f"nu{OBSID}_src1.evt")
        times = np.linspace(0, 1000, 100)
        flux = np.full_like(times, 1e-7)
        flux[40:60] = 1e-5  # an M-class flare in the middle
        Table({"TIME": times, "XRSA": flux / 10, "XRSB": flux}).write(
            tmp_path / f"nu{OBSID}_src1_goes.fits"
        )

        outfile = plot_flare_filtering.fn(event_file, [[0, 1000]], [[0, 400], [600, 1000]])

        assert os.path.getsize(outfile) > 0

    def test_it_works_without_a_goes_light_curve(self, tmp_path):
        """A rerun skips the download, so the file may legitimately be missing."""
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
        Table({"TIME": times, "XRSA": np.full(100, 1e-8), "XRSB": np.full(100, 1e-7)}).write(
            tmp_path / f"nu{OBSID}_src1_goes.fits"
        )

        outfile = plot_flare_filtering.fn(
            event_file, [[0, 1000]], [[0, 400], [600, 1000]], flux_class=None
        )

        assert os.path.getsize(outfile) > 0
