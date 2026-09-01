"""
Offline tests for the image-based source separation.

The diagnostic figures these draw used to go through ``pyplot``, whose figure registry is
one global object per process. That is the same class of shared state as the working
directory and the HEASOFT parameter files -- see issue 26 in ``docs/known_issues.rst`` --
and the figures were the last of it left in the package.
"""

import glob
import os

import numpy as np
import pytest

from astropy.io import fits

from heasarc_retrieve_pipeline.image_utils import (
    filter_sources_in_images,
    filter_table_outside_regions,
    get_random_fluxes_in_img,
    has_sky_position,
    valid_table,
)

pytest.importorskip("skimage")
pytest.importorskip("statsmodels")

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def _add_unplaced(x, y, n_unplaced, rng):
    """Append events the aspect reconstruction could not place on the sky.

    NuSTAR marks those by setting ``X`` and ``Y`` to zero, and many observations carry a
    large pile-up of them at the origin. Real files also hold a few with only one of the
    two coordinates set, which is what told the three predicates in ``image_utils`` apart.
    """
    if not n_unplaced:
        return x, y
    return (
        np.concatenate([x, np.zeros(n_unplaced), np.zeros(3), rng.uniform(300, 700, 3)]),
        np.concatenate([y, np.zeros(n_unplaced), rng.uniform(300, 700, 3), np.zeros(3)]),
    )


def event_table(n_placed=500, n_unplaced=200, seed=99):
    """An event table, as read out of a FITS file, with a pile-up of unplaced events."""
    rng = np.random.default_rng(seed)
    x, y = _add_unplaced(
        rng.uniform(300, 700, n_placed), rng.uniform(300, 700, n_placed), n_unplaced, rng
    )
    return fits.BinTableHDU.from_columns(
        [
            fits.Column(name="X", format="E", array=x),
            fits.Column(name="Y", format="E", array=y),
            fits.Column(name="PI", format="J", array=rng.integers(40, 1900, x.size)),
        ],
        name="EVENTS",
    ).data


def event_file(path, n_source=4000, n_background=2000, n_unplaced=0, seed=1234):
    """A NuSTAR-like cleaned event file with one bright source in a flat field."""
    rng = np.random.default_rng(seed)

    x = np.concatenate([rng.normal(500, 4, n_source), rng.uniform(300, 700, n_background)])
    y = np.concatenate([rng.normal(500, 4, n_source), rng.uniform(300, 700, n_background)])
    x, y = _add_unplaced(x, y, n_unplaced, rng)
    # PI 35 to 1900 is 3 to 79 keV, the band the function keeps.
    pi = rng.integers(40, 1900, x.size)

    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="X", format="E", array=x),
            fits.Column(name="Y", format="E", array=y),
            fits.Column(name="PI", format="J", array=pi),
            fits.Column(name="TIME", format="D", array=np.linspace(0, 1000, x.size)),
        ],
        name="EVENTS",
    )
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


class TestFilterSourcesInImages:
    def test_the_source_and_background_files_are_written(self, tmp_path):
        path = event_file(tmp_path / "nu123A01_cl.evt")

        assert filter_sources_in_images(path) is True
        assert os.path.exists(tmp_path / "nu123A01_cl_src1.evt")
        assert os.path.exists(tmp_path / "nu123A01_cl_back.evt")

    def test_the_brightest_peak_is_the_first_source(self, tmp_path):
        path = event_file(tmp_path / "nu123A01_cl.evt")

        filter_sources_in_images(path)

        with fits.open(tmp_path / "nu123A01_cl_src1.evt") as hdul:
            assert np.median(hdul[1].data["X"]) == pytest.approx(500, abs=10)
            assert np.median(hdul[1].data["Y"]) == pytest.approx(500, abs=10)

    def test_the_diagnostic_images_are_drawn(self, tmp_path):
        path = event_file(tmp_path / "nu123A01_cl.evt")

        filter_sources_in_images(path)

        assert sorted(os.path.basename(f) for f in glob.glob(str(tmp_path / "*.jpg"))) == [
            "nu123A01_cl.jpg",
            "nu123A01_cl_back.jpg",
            "nu123A01_cl_src1.jpg",
        ]

    def test_nothing_is_left_in_the_global_figure_registry(self, tmp_path):
        """A figure held by pyplot is shared process state, and a leak besides."""
        plt.close("all")
        path = event_file(tmp_path / "nu123A01_cl.evt")

        filter_sources_in_images(path)

        assert plt.get_fignums() == []

    def test_no_unplaced_event_reaches_any_output(self, tmp_path):
        """The whole point: a pile-up at the origin must not survive into a product."""
        path = event_file(tmp_path / "nu123A01_cl.evt", n_unplaced=1000)

        filter_sources_in_images(path)

        for product in glob.glob(str(tmp_path / "nu123A01_cl_*.evt")):
            with fits.open(product) as hdul:
                assert has_sky_position(hdul[1].data).all(), product

    def test_a_file_with_too_few_events_is_left_alone(self, tmp_path):
        path = event_file(tmp_path / "nu123A01_cl.evt", n_source=5, n_background=5)

        assert filter_sources_in_images(path) is None
        assert glob.glob(str(tmp_path / "*_src*.evt")) == []

class TestHasSkyPosition:
    """One predicate, because ``image_utils`` used to hold three that disagreed."""

    def test_an_event_at_the_origin_is_not_placed(self):
        assert not has_sky_position({"X": np.array([0]), "Y": np.array([0])})[0]

    def test_an_event_with_only_one_coordinate_set_is_not_placed(self):
        """``valid_table`` used an OR, so these survived it and nothing else."""
        table = {"X": np.array([0, 7]), "Y": np.array([7, 0])}

        assert not has_sky_position(table).any()

    def test_a_negative_coordinate_is_not_placed(self):
        assert not has_sky_position({"X": np.array([-1]), "Y": np.array([5])})[0]

    def test_an_ordinary_event_is_placed(self):
        assert has_sky_position({"X": np.array([500]), "Y": np.array([500])})[0]


class TestValidTable:
    def test_the_pile_up_at_the_origin_is_dropped(self):
        placed = valid_table(event_table(n_placed=500, n_unplaced=200))

        assert len(placed) == 500

    def test_every_surviving_event_has_both_coordinates(self):
        placed = valid_table(event_table())

        assert (placed["X"] > 0).all()
        assert (placed["Y"] > 0).all()


class TestFilterTableOutsideRegions:
    """The background file is what this feeds, and it used to swallow the whole pile-up.

    The predicate here was ``X < 0 or Y < 0``, which does not consider zero a null
    marker, so every unplaced event landed in the background events.
    """

    def test_the_unplaced_events_do_not_reach_the_background(self):
        table = event_table(n_placed=500, n_unplaced=200)

        background = filter_table_outside_regions(table, [[500, 500]], region_size=50)

        assert (background["X"] > 0).all()
        assert (background["Y"] > 0).all()

    def test_events_inside_a_region_are_excluded(self):
        table = event_table(n_unplaced=0)

        background = filter_table_outside_regions(table, [[500, 500]], region_size=50)

        assert len(background) < len(table)
        distance = np.hypot(background["X"] - 500, background["Y"] - 500)
        assert (distance >= 50).all()

    def test_events_far_from_every_region_are_kept(self):
        table = event_table(n_placed=500, n_unplaced=0)

        background = filter_table_outside_regions(table, [[0, 0]], region_size=10)

        assert len(background) == 500


class TestGetRandomFluxesInImg:
    """The apertures have to be thrown over the field, not over the origin."""

    def test_the_unplaced_events_do_not_move_the_bounding_box(self):
        """Same seed, same apertures: the pile-up must make no difference at all.

        It used to stretch the box from the field down to (0, 0), so most apertures
        landed on empty sky.
        """
        table = event_table(n_placed=500, n_unplaced=200)

        np.random.seed(0)
        with_pile_up = get_random_fluxes_in_img(table, region_size=30, n_rand=50)
        np.random.seed(0)
        without_pile_up = get_random_fluxes_in_img(valid_table(table), region_size=30, n_rand=50)

        assert with_pile_up == without_pile_up

    def test_the_apertures_land_on_the_field(self):
        table = event_table(n_placed=2000, n_unplaced=2000)

        fluxes = get_random_fluxes_in_img(table, region_size=30, n_rand=100)

        assert np.median(fluxes) > 0
