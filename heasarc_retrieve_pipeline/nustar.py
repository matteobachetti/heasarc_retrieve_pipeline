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

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_local_raw_path_{obsid}",
)
def nu_local_raw_data_path(obsid, config, **kwargs):
    return os.path.join(config["input_data_path"], obsid)


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_remote_raw_path_{obsid}",
)
def nu_heasarc_raw_data_path(obsid, **kwargs):
    return os.path.normpath(f"/FTP/nustar/data/obs/{obsid[1:3]}/{obsid[0]}/{obsid}/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_base_output_{obsid}",
)
def nu_base_output_path(obsid, config):
    return os.path.join(config["out_data_path"], obsid)


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_pipeline_output_{obsid}",
)
def nu_pipeline_output_path(obsid, config):
    return os.path.join(config["out_data_path"], obsid + "/event_pipe/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="split_path_{obsid}",
)
def split_path(obsid, config):
    return os.path.join(config["out_data_path"], obsid + "/split/")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1000))
def separate_sources(directories, config):
    for d in directories:
        logger = get_run_logger()
        logger.info(f"Separating sources in {d}")
        for event_file in glob.glob(os.path.join(d, "nu*_cl.evt*")):
            if event_file.endswith(".gpg"):
                continue
            logger.info(f"Processing {event_file}")
            if os.path.exists(event_file.replace(".evt", "_src1.evt")):
                continue
            filter_sources_in_images(event_file)


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="l2_pipeline_obsid_{obsid}",
)
def nu_run_l2_pipeline(obsid, config):
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy not installed")
    logger = get_run_logger()
    nupipeline = hsp.HSPTask("nupipeline")
    logger.info("Running NuSTAR L2 pipeline")
    datadir = nu_local_raw_data_path.fn(obsid, config=config)
    ev_dir = nu_pipeline_output_path.fn(obsid, config=config)
    os.makedirs(ev_dir, exist_ok=True)
    stem = "nu" + obsid
    result = nupipeline(
        indir=datadir,
        outdir=ev_dir,
        clobber="yes",
        steminputs=stem,
        instrument="ALL",
        noprompt=True,
        verbose=True,
    )
    print("return code:", result.returncode)
    if result.returncode != 0:
        logger.error(f"nupipeline failed: {result.stderr}")
        raise RuntimeError("nupipeline failed")

    return ev_dir


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_recover_spacecraft_science_{obsid}",
)
def recover_spacecraft_science_data(obsid, config):
    logger = get_run_logger()
    logger.info(f"Squeezing every photon from spacecraft science data in {obsid}")
    datadir = nu_local_raw_data_path.fn(obsid, config)
    ev_dir = nu_pipeline_output_path.fn(obsid, config)
    splitdir = split_path.fn(obsid, config=config)

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


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_join_science_{obsid}_src{src_num}",
)
def join_source_data(obsid, directories, config, src_num=1):
    logger = get_run_logger()
    outdir = nu_base_output_path.fn(obsid, config=config)
    outfiles = []
    for fpm in "A", "B":
        outfile = os.path.join(outdir, f"nu{obsid}{fpm}_src{src_num}.evt")
        if os.path.exists(outfile):
            os.unlink(outfile)
        outfile_gti = os.path.join(outdir, f"nu{obsid}{fpm}.gti")
        if os.path.exists(outfile_gti):
            os.unlink(outfile_gti)
        logger.info(f"Joining source data for fpm {fpm} into {outfile}")
        files_to_join = []
        for d in directories:
            logger.info(f"Adding data from {d}")
            new_files = glob.glob(
                os.path.join(d, f"nu{obsid}{fpm}0[16]*_src{src_num}.evt*")
            )
            to_be_removed = []
            for nf in new_files:
                if f"{fpm}01" in nf:
                    logger.info(f"Copying {nf} to {outdir}")
                    os.system(f"cp {nf} {outdir}/")
                elif f"{fpm}06" in nf and "chu" not in nf:
                    logger.info(f"Discarding {nf}")
                    to_be_removed.append(nf)
            for nf in to_be_removed:
                new_files.remove(nf)
            files_to_join.extend(new_files)
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

        logger.info(f"Removing {outfile_gti}")

        os.unlink(outfile_gti)

    return outfiles


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=90),
    task_run_name="nu_barycenter_{infile}",
)
def barycenter_file(infile, attorb, ra=None, dec=None, src=1):
    logger = get_run_logger()
    logger.info(f"Barycentering {infile}")

    outfile = infile.replace(".evt", "_bary.evt")
    logger.info(f"Output file: {outfile}")
    print("bu")
    hsp.barycorr(
        infile=infile,
        outfile=outfile,
        ra=ra,
        dec=dec,
        ephem="JPLEPH.430",
        refframe="ICRS",
        clobber="yes",
        orbitfiles=attorb,
    )

    return outfile


@flow(flow_run_name="nu_barycenter_{obsid}")
def barycenter_data(obsid, ra, dec, config, src=1):
    logger = get_run_logger()
    outdir = nu_base_output_path.fn(obsid, config=config)
    logger.info(f"Barycentering data in directory {outdir}")
    pipe_outdir = nu_pipeline_output_path.fn(obsid, config=config)
    for fpm in "A", "B":
        infiles = glob.glob(
            os.path.join(outdir, f"nu{obsid}{fpm}01_cl_src{src}.evt*")
        ) + glob.glob(os.path.join(outdir, f"nu{obsid}{fpm}_src{src}.evt*"))
        for infile in infiles:
            barycenter_file(
                infile,
                os.path.join(pipe_outdir, f"nu{obsid}{fpm}.attorb"),
                ra=ra,
                dec=dec,
                src=src,
            )


@flow
def process_nustar_obsid(obsid, config=None, ra="NONE", dec="NONE"):
    config = DEFAULT_CONFIG if config is None else config
    logger = get_run_logger()
    logger.info(f"Processing NuSTAR observation {obsid}")
    os.makedirs(os.path.join(nu_base_output_path(obsid, config=config)), exist_ok=True)
    outdir = nu_run_l2_pipeline(obsid, config=config)
    splitdir = recover_spacecraft_science_data(
        obsid, config, wait_for=[nu_run_l2_pipeline]
    )
    separate_sources(
        [outdir, splitdir], config, wait_for=[recover_spacecraft_science_data]
    )
    outfiles = join_source_data(
        obsid, [outdir, splitdir], config, wait_for=[separate_sources]
    )
    barycenter_data(
        obsid, ra=48.962664, dec=+69.679298, config=config, wait_for=[join_source_data]
    )
