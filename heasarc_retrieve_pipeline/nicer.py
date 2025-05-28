import os
import glob
from prefect import flow, task


OUT_DATA_DIR = "./"


@task
def process_nicer_obsid(obsid, config=None, ra="NONE", dec="NONE"):
    raise NotImplementedError("process_nicer_obsid is not yet implemented.")


@task
def ni_raw_data_path(obsid, time):
    from astropy.time import Time

    mjd = Time(time.data, format="mjd")
    mjd_dt = mjd.to_datetime()

    return os.path.normpath(f"/FTP/nicer/data/obs/{mjd_dt.year}_{mjd_dt.month:02d}//{obsid}/")


@task
def ni_base_output_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid)


@task
def ni_pipeline_output_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid + "/event_cl/")


@task
def ni_run_l2_pipeline(obsid):
    import heasoftpy as hsp

    pass
