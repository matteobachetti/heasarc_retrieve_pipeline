import os
import glob
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from .image_utils import filter_sources_in_images

try:
    HAS_HEASOFT = True
    import heasoftpy as hsp
except ImportError:
    HAS_HEASOFT = False

OUT_DATA_DIR = "./"


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def nu_raw_data_path(obsid, **kwargs):
    return obsid
    return os.path.normpath(f"/FTP/nustar/data/obs/{obsid[1:3]}/{obsid[0]}/{obsid}/")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def nu_base_output_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def nu_pipeline_output_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid + "/event_cl/")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def split_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid + "/split/")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def separate_sources(directories):
    for d in directories:
        logger = get_run_logger()
        logger.info(f"Separating sources in {d}")
        for event_file in glob.glob(os.path.join(d, "nu*_cl.evt*")):
            if os.path.exists(event_file.replace(".evt", "_src1.evt")):
                continue
            filter_sources_in_images(event_file)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def nu_run_l2_pipeline(obsid):
    logger = get_run_logger()
    nupipeline = hsp.HSPTask("nupipeline")
    logger.info("Running NuSTAR L2 pipeline")
    datadir = nu_raw_data_path.fn(obsid)
    ev_dir = nu_pipeline_output_path.fn(obsid)
    os.makedirs(ev_dir, exist_ok=True)
    stem = "nu" + obsid
    for instr in ["FPMA", "FPMB"]:
        nupipeline(
            indir=datadir,
            outdir=ev_dir,
            clobber="yes",
            steminputs=stem,
            instrument=instr,
            noprompt=True,
            verbose=True,
        )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def recover_spacecraft_science_data(obsid):
    logger = get_run_logger()
    logger.info("Squeezing every photon from spacecraft science data")
    datadir = nu_raw_data_path.fn(obsid)
    ev_dir = nu_pipeline_output_path.fn(obsid)
    splitdir = split_path.fn(obsid)

    hk_dir = os.path.join(datadir, "hk")

    evfiles_06 = glob.glob(os.path.join(ev_dir, "*[AB]06_cl.evt*"))

    if os.path.exists(splitdir):
        logger.info("Output directory exists. Assuming processing done")
        return splitdir

    for evfile in evfiles_06:
        evfile_base = os.path.split(evfile)[1]
        chu123hkfile = [
            f
            for f in glob.glob(os.path.join(hk_dir, f"nu{obsid}_chu123.fits*"))
            if "gpg" not in f
        ][0]
        hkfile = [
            f
            for f in glob.glob(os.path.join(ev_dir, f"{evfile_base[:14]}_fpm.hk*"))
            if "gpg" not in f
        ][0]

        hsp.nusplitsc(
            infile=evfile,
            chu123hkfile=chu123hkfile,
            hkfile=hkfile,
            outdir=splitdir,
            clobber="yes",
        )

    return splitdir


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def join_source_data(obsid, directories, src_num=1):
    logger = get_run_logger()
    outdir = nu_base_output_path(obsid)
    outfiles = []
    for fpm in "A", "B":
        outfile = os.path.join(outdir, f"{obsid}{fpm}_src{src_num}.evt")
        outfile_gti = os.path.join(outdir, f"{obsid}{fpm}.gti")
        logger.info(outfile)
        files_to_join = []
        for d in directories:
            files_to_join.extend(
                glob.glob(os.path.join(d, f"nu{obsid}{fpm}0[16]*_src{src_num}.evt*"))
            )
        hsp.ftmgtime(
            ingtis=",".join([f + "[GTI]" for f in files_to_join]),
            outgti=outfile_gti,
            merge="OR",
        )
        hsp.ftsort(infile=outfile_gti, outfile="!" + outfile_gti, columns="START")

        hsp.ftmerge(infile=",".join(files_to_join), outfile=outfile, copyall="NO")
        hsp.ftsort(infile=outfile, outfile="!" + outfile, columns="TIME")
        hsp.fappend(infile=f"{outfile_gti}[GTI]", outfile=outfile)

        outfiles.append(outfile)
    return outfiles


@flow
def process_nustar_obsid(obsid, config, ra="NONE", dec="NONE"):
    os.makedirs(os.path.join(nu_base_output_path(obsid)), exist_ok=True)
    outdir = nu_run_l2_pipeline(obsid)
    splitdir = recover_spacecraft_science_data(obsid)
    separate_sources([outdir, splitdir])
    join_source_data(obsid, [outdir, splitdir])
