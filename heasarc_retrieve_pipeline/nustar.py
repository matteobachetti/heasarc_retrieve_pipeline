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


# @task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
# def nu_run_l2_pipeline_on_single_fpm(obsid, fpm):
#     logger = get_run_logger()
#     nupipeline = hsp.HSPTask("nupipeline")
#     logger.info(f"Running NuSTAR L2 pipeline on fpm {fpm}")
#     datadir = nu_raw_data_path.fn(obsid)
#     ev_dir = nu_pipeline_output_path.fn(obsid)
#     os.makedirs(ev_dir, exist_ok=True)
#     stem = "nu" + obsid
#     nupipeline(
#         indir=datadir,
#         outdir=ev_dir,
#         clobber="yes",
#         steminputs=stem,
#         instrument=fpm,
#         noprompt=True,
#         verbose=True,
#     )
#     return True


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def nu_run_l2_pipeline(obsid):
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy not installed")
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

    return ev_dir


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


# @task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
@task
def join_source_data(obsid, directories, src_num=1):
    logger = get_run_logger()
    outdir = nu_base_output_path.fn(obsid)
    outfiles = []
    for fpm in "A", "B":
        logger.info(f"Joining source data for fpm {fpm}")
        outfile = os.path.join(outdir, f"{obsid}{fpm}_src{src_num}.evt")
        outfile_gti = os.path.join(outdir, f"{obsid}{fpm}.gti")
        logger.info(outfile)
        files_to_join = []
        for d in directories:
            logger.info(f"Adding data from {d}")
            new_files = glob.glob(
                os.path.join(d, f"nu{obsid}{fpm}0[16]*_src{src_num}.evt*")
            )
            files_to_join.extend(new_files)
            for nf in new_files:
                if f"{fpm}01" in nf:
                    logger.info(f"Copying {nf} to {outdir}")
                    os.system(f"cp {nf} {outdir}/")

        logger.info(f"Creating GTI file {outfile_gti} from {files_to_join}")

        hsp.ftmgtime(
            ingtis=",".join([f + "[GTI]" for f in files_to_join]),
            outgti=outfile_gti,
            merge="OR",
        )
        hsp.ftsort(infile=outfile_gti, outfile="!" + outfile_gti, columns="START")

        logger.info(f"Changing extension name to GTI in {outfile_gti}")

        hsp.fthedit(
            infile=outfile_gti + "+1", keyword="EXTNAME", operation="a", value="GTI"
        )
        logger.info(f"Creating event file {outfile} from {files_to_join}")

        hsp.ftmerge(infile=",".join(files_to_join), outfile=outfile, copyall="NO")

        logger.info(f"Sorting event file {outfile}")

        hsp.ftsort(infile=outfile, outfile="!" + outfile, columns="TIME")

        logger.info(
            f"Adding GTIs from {outfile_gti}'s first extension to event file {outfile}"
        )

        hsp.fappend(infile=f"{outfile_gti}[1]", outfile=outfile)

        outfiles.append(outfile)

    return outfiles


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def barycenter_data(obsid, ra, dec, src=1):
    logger = get_run_logger()
    outdir = nu_base_output_path.fn(obsid)
    logger.info(f"Barycentering data in directory {outdir}")
    pipe_outdir = nu_pipeline_output_path.fn(obsid)
    for fpm in "A", "B":
        infiles = glob.glob(
            os.path.join(outdir, f"*{obsid}{fpm}01_cl_src{src}.evt*")
        ) + glob.glob(os.path.join(outdir, f"*{obsid}{fpm}_src{src}.evt*"))
        for infile in infiles:
            logger.info(f"Barycentering {infile}")

            outfile = infile.replace(".evt", "_bary.evt")
            logger.info(f"Output file: {outfile}")

            hsp.barycorr(
                infile=infile,
                outfile=outfile,
                ra=ra,
                dec=dec,
                ephem="JPLEPH.430",
                refframe="ICRS",
                clobber="yes",
                orbitfiles=os.path.join(pipe_outdir, f"nu{obsid}{fpm}.attorb"),
            )


@flow
def process_nustar_obsid(obsid, config, ra="NONE", dec="NONE"):
    os.makedirs(os.path.join(nu_base_output_path(obsid)), exist_ok=True)
    outdir = nu_run_l2_pipeline(obsid)
    splitdir = recover_spacecraft_science_data(obsid)
    separate_sources([outdir, splitdir])
    outfiles = join_source_data(obsid, [outdir, splitdir])
    barycenter_data(obsid, ra=48.962664, dec=+69.679298)
