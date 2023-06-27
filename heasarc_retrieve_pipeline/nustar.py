import os
import glob
from prefect import flow, task


@task
def raw_data_path(obsid):
    return os.path.normpath(f"/FTP/nustar/data/obs/{obsid[1:3]}/{obsid[0]}/{obsid}/")


@task
def base_output_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid)


@task
def pipeline_output_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid + "/event_cl/")


@task
def split_path(obsid):
    return os.path.join(OUT_DATA_DIR, obsid + "/split/")


@task
def separate_sources(directories):
    for d in directories:
        for event_file in glob.glob(os.path.join(d, "nu*_cl.evt*")):
            if os.path.exists(event_file.replace(".evt", "_src1.evt")):
                continue
            filter_sources_in_images(event_file)


@task
def run_l2_pipeline(obsid):
    import heasoftpy as hsp

    pass


@task
def recover_spacecraft_science_data(obsid):
    print("Squeezing every photon from spacecraft science data")
    datadir = raw_data_path(obsid)
    ev_dir = pipeline_output_path(obsid)
    splitdir = split_path(obsid)

    hk_dir = os.path.join(datadir, "hk")

    evfiles_06 = glob.glob(os.path.join(ev_dir, "*[AB]06_cl.evt*"))

    if os.path.exists(splitdir):
        print("Output directory exists. Assuming processing done")
        return splitdir

    for evfile in evfiles_06:
        evfile_base = os.path.split(evfile)[1]
        chu123hkfile = [
            f for f in glob.glob(os.path.join(hk_dir, f"nu{obsid}_chu123.fits*")) if "gpg" not in f
        ][0]
        hkfile = [
            f
            for f in glob.glob(os.path.join(ev_dir, f"{evfile_base[:14]}_fpm.hk*"))
            if "gpg" not in f
        ][0]

        hsp.nusplitsc(
            infile=evfile, chu123hkfile=chu123hkfile, hkfile=hkfile, outdir=splitdir, clobber="yes"
        )

    return splitdir


@task
def join_source_data(obsid, directories, src_num=1):
    outdir = base_output_path(obsid)
    outfiles = []
    for fpm in "A", "B":
        outfile = os.path.join(outdir, f"{obsid}{fpm}_src{src_num}.evt")
        outfile_gti = os.path.join(outdir, f"{obsid}{fpm}.gti")
        print(outfile)
        files_to_join = []
        for d in directories:
            files_to_join.extend(
                glob.glob(os.path.join(d, f"nu{obsid}{fpm}0[16]*_src{src_num}.evt*"))
            )
        hsp.ftmgtime(
            ingtis=",".join([f + "[GTI]" for f in files_to_join]), outgti=outfile_gti, merge="OR"
        )
        hsp.ftsort(infile=outfile_gti, outfile="!" + outfile_gti, columns="START")

        hsp.ftmerge(infile=",".join(files_to_join), outfile=outfile, copyall="NO")
        hsp.ftsort(infile=outfile, outfile="!" + outfile, columns="TIME")
        hsp.fappend(infile=f"{outfile_gti}[GTI]", outfile=outfile)

        outfiles.append(outfile)
    return outfiles


@flow
def process_nustar_obsid(obsid, config, ra="NONE", dec="NONE"):
    os.makedirs(os.path.join(base_output_path(obsid)), exist_ok=True)
    outdir = run_l2_pipeline(obsid)
    splitdir = recover_spacecraft_science_data(obsid)
    separate_sources([outdir, splitdir])
    join_source_data(obsid, [outdir, splitdir])
