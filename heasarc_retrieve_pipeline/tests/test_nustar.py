"""Offline tests for the NuSTAR module.

These need neither the network nor a HEASOFT installation: they exercise the pure
file-selection helpers that decide which event files the pipeline works on.
"""

import os

# These tests call Prefect tasks through ``.fn``, outside any flow run. Prefect's API log
# handler warns about that on every call; it has nothing to report to.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

import pytest  # noqa: E402

from heasarc_retrieve_pipeline.nustar import (  # noqa: E402
    get_best_source_regions,
    spectral_input_files,
)


OBSID = "80002092008"


def make_obsid_tree(base, pipe_files=(), split_files=()):
    """Create an empty ``event_pipe``/``split`` tree and return a matching config."""
    pipedir = os.path.join(base, OBSID, "event_pipe")
    splitdir = os.path.join(base, OBSID, "split")
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
    )
    assert list(spectral_input_files("80002092003", config)) == []


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
