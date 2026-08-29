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

from heasarc_retrieve_pipeline.image_utils import filter_sources_in_images

pytest.importorskip("skimage")
pytest.importorskip("statsmodels")

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def event_file(path, n_source=4000, n_background=2000, seed=1234):
    """A NuSTAR-like cleaned event file with one bright source in a flat field."""
    rng = np.random.default_rng(seed)

    x = np.concatenate([rng.normal(500, 4, n_source), rng.uniform(300, 700, n_background)])
    y = np.concatenate([rng.normal(500, 4, n_source), rng.uniform(300, 700, n_background)])
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

    def test_a_file_with_too_few_events_is_left_alone(self, tmp_path):
        path = event_file(tmp_path / "nu123A01_cl.evt", n_source=5, n_background=5)

        assert filter_sources_in_images(path) is None
        assert glob.glob(str(tmp_path / "*_src*.evt")) == []
