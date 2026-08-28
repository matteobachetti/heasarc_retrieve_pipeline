"""Offline tests for the NuSTAR module.

These need neither the network nor a HEASOFT installation: they exercise the pure
file-selection helpers that decide which event files the pipeline works on.
"""

import os

import pytest

from heasarc_retrieve_pipeline.nustar import spectral_input_files


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
