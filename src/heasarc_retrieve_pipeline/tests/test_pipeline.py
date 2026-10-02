import pytest
from heasarc_retrieve_pipeline.core import (
    retrieve_heasarc_data_by_source_name,
    recursive_download,
    retrieve_heasarc_data_by_obsid,
)


def kwargs_from_host(host):
    """
    Helper function to return kwargs based on the host.
    """
    if host == "aws":
        return {"force_s3": True}
    elif host == "heasarc":
        return {"force_heasarc": True}


@pytest.mark.remote_data
@pytest.mark.parametrize("mission", ["nustar", "nicer"])
@pytest.mark.parametrize("host", ["heasarc", "aws"])
def test_retrieve_heasarc_data_by_source_name(mission, host):

    results = retrieve_heasarc_data_by_source_name(
        "M82 X-2", mission=mission, test=True, **kwargs_from_host(host)
    )
    assert len(results) > 0


@pytest.mark.remote_data
@pytest.mark.parametrize("host", ["heasarc", "aws"])
def test_retrieve_heasarc_data_by_obsid_nustar(host):
    results = retrieve_heasarc_data_by_obsid(
        "90101005001", mission="nustar", test=True, **kwargs_from_host(host)
    )
    assert len(results) > 0


@pytest.mark.remote_data
@pytest.mark.parametrize("host", ["heasarc", "aws"])
def test_retrieve_heasarc_data_by_obsid_nicer(host):
    results = retrieve_heasarc_data_by_obsid(
        "1104010106", mission="nicer", test=True, **kwargs_from_host(host)
    )
    assert len(results) > 0


@pytest.mark.remote_data
@pytest.mark.parametrize("host", ["aws", "heasarc"])
def test_recursive_download(host):
    import shutil

    if host == "aws":
        path = "s3://nasa-heasarc"
    else:
        path = "https://heasarc.gsfc.nasa.gov/FTP"

    results = recursive_download(
        f"{path}/nustar/data/obs/00/8/80002092003/",
        "out_test",
        cut_ndirs=0,
        test=False,
        test_str=".",
        re_include=r"[AB]0.*evt",
        re_exclude=r"[AB]0[2-5]",
    )
    assert len(results) == 2
    shutil.rmtree("out_test")


@pytest.mark.remote_data
def test_gbm_days_are_found_by_date():
    """The live fermigdays answer: 2024-03-22 runs from 23:59 the day before, so a
    window inside that day finds it and nothing else."""
    from heasarc_retrieve_pipeline.core import retrieve_heasarc_table_by_time

    results = retrieve_heasarc_table_by_time(60391.2, 60391.8, "fermi_gbm")

    assert list(results["obsid"]) == ["20240322"]
    assert "__row" in results.colnames
