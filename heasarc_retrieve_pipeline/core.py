import os
import sys
import glob
import traceback
from prefect import flow, task


@task
def read_config(config_file):
    import yaml

    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config


def retrieve_heasarc_data_by_position(ra_deg, dec_deg, table="numaster", radius_deg=0.1):
    import astropy.coordinates as coord
    import pyvo as vo

    tap_services = vo.regsearch(servicetype="table", keywords=["heasarc"])

    query = f"""SELECT name, cycle, obsid, time, exposure_a, ra, dec, public_date, solar_activity
        FROM public.{table} as cat
        where
        contains(point('ICRS',cat.ra,cat.dec),circle('ICRS',{ra_deg},{dec_deg},{radius_deg}))=1
        and
        cat.exposure_a > 0 order by cat.time
        """

    results = tap_services[0].search(query).to_table()
    return results


@task
def retrieve_heasarc_data_by_source_name(source, table="numaster", radius_deg=0.1):
    import astropy.coordinates as coord

    pos = coord.SkyCoord.from_name(f"{source}")

    return retrieve_heasarc_data_by_position(
        pos.ra.deg, pos.ra.deg, table="numaster", radius_deg=0.1
    )


# test that the retrieval works
def test_retrieve_heasarc_data_by_source_name():
    results = retrieve_heasarc_data_by_source_name("M82")
    assert len(results) > 0
