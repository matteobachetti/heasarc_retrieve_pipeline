import os

from heasarc_retrieve_pipeline.nicer import (
    ni_base_output_path,
    ni_pipeline_done_file,
    ni_pipeline_output_path,
)


CONFIG = {"out_data_path": "out"}
OBSID = "1104010106"


class TestNicerPaths:
    """The local layout and the archive layout a NICER observation maps to."""

    def test_the_output_directory_is_the_obsid_under_out_data_path(self):
        assert ni_base_output_path(config=CONFIG, obsid=OBSID) == os.path.join("out", OBSID)

    def test_the_level_2_products_go_in_l2files(self):
        path = ni_pipeline_output_path(config=CONFIG, obsid=OBSID)

        assert path == os.path.join("out", OBSID + "/l2files/")

    def test_the_sentinel_sits_beside_the_level_2_products(self):
        """This is the first call this function has ever had: it passed its arguments
        positionally to ni_pipeline_output_path, whose signature is (config, obsid), so
        it raised TypeError for every input."""
        done = ni_pipeline_done_file(obsid=OBSID, config=CONFIG)

        assert done == os.path.join("out", OBSID + "/l2files/", "PIPELINE_DONE.TXT")
