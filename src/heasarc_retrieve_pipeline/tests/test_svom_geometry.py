"""SVOM geometry: joining orbit and attitude files, and the good time intervals they give.

Everything runs on a synthetic circular equatorial orbit sampled every second, where
occultation and off-axis times are known analytically.
"""

import numpy as np
import pytest
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Table

from heasarc_retrieve_pipeline import svom_geometry as geo

GM_EARTH = 3.986004418e14
RADIUS_M = geo.R_EARTH_M + 625e3
PERIOD_S = 2 * np.pi * np.sqrt(RADIUS_M**3 / GM_EARTH)


def circular_orbit(times):
    """Equatorial circular orbit, at RA 0 at t = 0, moving towards RA 90."""
    phase = 2 * np.pi * times / PERIOD_S
    speed = 2 * np.pi * RADIUS_M / PERIOD_S
    position = RADIUS_M * np.column_stack([np.cos(phase), np.sin(phase), 0 * phase])
    velocity = speed * np.column_stack([-np.sin(phase), np.cos(phase), 0 * phase])
    return Table(
        dict(
            TIME=times,
            POSITION=position.astype(np.float32),
            VELOCITY=velocity.astype(np.float32),
            FLAG_PVT=np.zeros(times.size, np.uint8),
            ORIGIN=np.zeros(times.size, np.uint8),
        )
    )


SOURCE = SkyCoord(0.0, 0.0, unit="deg")


def test_the_source_is_hidden_for_the_angular_radius_of_the_earth_each_orbit():
    """In the orbital plane, the source sets for a fraction rho/pi of the orbit, centred
    on the far side, with rho the angular radius of Earth plus atmosphere."""
    orbit = circular_orbit(np.arange(0.0, 2 * PERIOD_S))
    rho = np.arcsin((geo.R_EARTH_M + geo.ATMOSPHERE_M) / RADIUS_M)

    gti = geo.visibility_gti(orbit, SOURCE)

    hidden = np.column_stack([gti[:-1, 1], gti[1:, 0]])
    assert len(hidden) == 2
    assert np.allclose(np.diff(hidden, axis=1), rho / np.pi * PERIOD_S, atol=0.05)
    assert np.allclose(hidden.mean(axis=1), [0.5 * PERIOD_S, 1.5 * PERIOD_S], atol=0.05)


def test_off_axis_cut_ends_where_a_uniform_slew_crosses_the_limit():
    """An axis slewing away from the source at 0.3 deg/s crosses 40 deg at 133.33 s,
    between two samples."""
    times = np.arange(0.0, 300.0)

    gti = geo.offaxis_gti(times, 0.3 * times, 0 * times, SOURCE, max_deg=40.0)

    assert gti.shape == (1, 2)
    assert gti[0] == pytest.approx([0.0, 40.0 / 0.3], abs=1e-6)


def test_coverage_excludes_holes_and_telemetry_gap_samples_beyond_two_seconds():
    """An 11 s hole is cut; a single flagged sample leaves a 2 s hole, which is kept."""
    orbit = circular_orbit(np.arange(0.0, 1000.0))
    orbit = orbit[(orbit["TIME"] < 300) | (orbit["TIME"] > 309)]
    orbit["FLAG_PVT"][orbit["TIME"] == 600] = geo.TELEMETRY_GAP

    assert geo.orbit_coverage_gti(orbit).tolist() == [[0.0, 299.0], [310.0, 999.0]]


def test_position_jumps_and_origin_changes_are_reported():
    """A 5 km step in position and a change of position source, at their times."""
    orbit = circular_orbit(np.arange(0.0, 200.0))
    orbit["POSITION"][120:] += np.float32(5e3)
    orbit["ORIGIN"][50:] = 1

    found = geo.orbit_discontinuities(orbit)

    assert [(row["time"], row["kind"]) for row in found] == [(50.0, "ORIGIN"), (120.0, "jump")]


def test_files_are_joined_once_per_sample_skipping_macos_metadata(tmp_path):
    """Two abutting files that share 50 samples give each second once, sorted; the
    shared samples that disagree are counted; ``._`` files are ignored."""
    orbit = circular_orbit(np.arange(0.0, 300.0))
    later, earlier = orbit[150:], orbit[:200].copy()
    earlier["ORIGIN"][180] = 1
    paths = []
    for name, part in (("b.fits", later), ("a.fits", earlier)):
        hdu = fits.BinTableHDU(part, name="SVO-ORB-CNV")
        fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(tmp_path / name)
        paths.append(str(tmp_path / name))
    (tmp_path / "._a.fits").write_bytes(b"\x00\x05\x16\x07")
    paths.append(str(tmp_path / "._a.fits"))

    joined, n_conflicts = geo.read_aux_series(paths)

    assert np.array_equal(joined["TIME"], np.arange(0.0, 300.0))
    assert n_conflicts == 1


def test_saa_gti_is_the_complement_of_entry_and_exit_times():
    """Inside the requested span; rows with missing times are dropped."""
    saa = Table(dict(TIME_ENTRY=[100.0, 500.0, np.nan], TIME_EXIT=[200.0, 700.0, 900.0]))

    assert geo.saa_gti(saa, 0.0, 600.0).tolist() == [[0.0, 100.0], [200.0, 500.0]]
