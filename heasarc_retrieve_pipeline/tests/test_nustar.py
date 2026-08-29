"""Offline tests for the NuSTAR module.

These need neither the network nor a HEASOFT installation: they exercise the pure
file-selection helpers that decide which event files the pipeline works on.
"""

import os

# These tests call Prefect tasks through ``.fn``, outside any flow run. Prefect's API log
# handler warns about that on every call; it has nothing to report to.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

import pytest  # noqa: E402

from astropy.coordinates import SkyCoord  # noqa: E402
import astropy.units as u  # noqa: E402

from heasarc_retrieve_pipeline.nustar import (  # noqa: E402
    get_best_source_regions,
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
