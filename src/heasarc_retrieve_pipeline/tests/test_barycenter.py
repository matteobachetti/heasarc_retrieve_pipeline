import os

from heasarc_retrieve_pipeline.barycenter import barycentered_file_name


class TestBarycenteredFileName:
    """``_bary`` goes before the extension, whatever the extension is."""

    def test_a_nustar_event_file(self):
        assert barycentered_file_name("nu123A01_cl.evt") == "nu123A01_cl_bary.evt"

    def test_a_gzipped_file_stays_gzipped(self):
        """The compression suffix stays last: a gzipped input gives a gzipped output."""
        assert barycentered_file_name("nu123A01_cl.evt.gz") == "nu123A01_cl_bary.evt.gz"

    def test_a_fits_file(self):
        """Missions that call their event files something else are the reason this
        function exists: ``str.replace(".evt", "_bary.evt")`` leaves these untouched, and
        an output name equal to the input is worse than an ugly one."""
        assert barycentered_file_name("obs_events.fits") == "obs_events_bary.fits"

    def test_a_chandra_style_extension(self):
        assert barycentered_file_name("acisf_evt2.fits") == "acisf_evt2_bary.fits"

    def test_an_xmm_style_extension(self):
        assert barycentered_file_name("P0123_events.ds") == "P0123_events_bary.ds"

    def test_a_gzipped_fits_file(self):
        assert barycentered_file_name("obs.fits.gz") == "obs_bary.fits.gz"

    def test_directories_are_preserved(self):
        name = barycentered_file_name(os.path.join("out", "90901333002", "x.evt"))

        assert name == os.path.join("out", "90901333002", "x_bary.evt")

    def test_a_dot_in_a_directory_name_is_not_an_extension(self):
        """``str.replace`` would rename the directory instead of the file."""
        name = barycentered_file_name(os.path.join("out.evt", "x.evt"))

        assert name == os.path.join("out.evt", "x_bary.evt")

    def test_a_file_with_no_extension(self):
        assert barycentered_file_name("events") == "events_bary"
