import pytest
from heasarc_retrieve_pipeline.core import retrieve_heasarc_data_by_source_name, recursive_download


@pytest.mark.remote_data
@pytest.mark.parametrize(
    "mission",
    ["nustar", "nicer"],
)
def test_retrieve_heasarc_data_by_source_name(mission):
    results = retrieve_heasarc_data_by_source_name("M82 X-2", mission=mission, test=True)
    assert len(results) > 0


@pytest.mark.remote_data
def test_recursive_download():
    import shutil
    import re

    results = recursive_download(
        "https://heasarc.gsfc.nasa.gov/FTP/nustar/data/obs/00/8/80002092003/",
        "out_test",
        cut_ndirs=0,
        test_str=".",
        test=False,
        re_include=r"[AB]0.*evt",
        re_exclude=r"[AB]0[2-5]",
    )
    assert len(results) == 2
    shutil.rmtree("out_test")
