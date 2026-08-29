import os

from heasarc_retrieve_pipeline.rxte import rxte_base_output_path


CONFIG = {"out_data_path": "out"}
OBSID = "10408-01-05-000"


class TestRxtePaths:
    def test_the_output_directory_is_the_obsid_under_out_data_path(self):
        assert rxte_base_output_path(config=CONFIG, obsid=OBSID) == os.path.join("out", OBSID)
