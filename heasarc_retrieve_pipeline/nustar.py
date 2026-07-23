import os
import re

import glob
from datetime import timedelta
import numpy as np
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from .image_utils import filter_sources_in_images
from .barycenter import barycenter_file
from .utils import splitext_improved

try:
    HAS_HEASOFT = True
    import heasoftpy as hsp
except ImportError:
    HAS_HEASOFT = False

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./", max_radius=80)

valid_re = re.compile(r"nu[0-9]{11}[AB]0[16].*")


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
    task_run_name="nu_product_output_{obsid}",
)
def nu_product_output_path(obsid, config):
    return os.path.join(config["out_data_path"], obsid + "/products/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_pipeline_output_{obsid}",
)
def nu_pipeline_done_file(obsid, config):
    return os.path.join(nu_pipeline_output_path.fn(obsid, config), "PIPELINE_DONE.TXT")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="split_path_{obsid}",
)
def split_path(obsid, config):
    return os.path.join(config["out_data_path"], obsid + "/split/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="splitext_{infile}",
)
def splitext(infile):
    return splitext_improved(infile)


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="rootname_{infile}",
)
def rootname(infile):
    return splitext(infile)[0]


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="barycentered_file_name_{infile}",
)
def barycentered_file_name(infile):
    root, ext = splitext(infile)
    return root + "_bary" + ext


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="goes_lc_file_name_{event_file}",
)
def goes_lc_file_name(event_file):
    root = rootname(event_file)
    return root + "_goes.fits"


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="goes_gti_file_name_{event_file}",
)
def goes_gti_file_name(event_file):
    root = rootname(event_file)
    return root + "_goes.gti"


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="flare_filtered_event_file_name_{event_file}",
)
def flare_filtered_event_file_name(event_file):
    root = rootname(event_file)
    return root + "_noflares.evt"


@task(
    task_run_name="separate_sources_in_event_file_{obsid}_{event_file}_region_{region_size}_back_{back_region_size}",
)
def separate_sources_in_event_file(event_file, region_size=30, back_region_size=55):
    logger = get_run_logger()
    if event_file.endswith(".gpg"):
        return None
    if not valid_re.search(event_file):
        return None
    logger.info(f"Processing {event_file}")
    # if os.path.exists(event_file.replace(".evt", "_back.evt")):
    #     logger.info("Older processing exists")
    #     return None
    return filter_sources_in_images(
        event_file, region_size=region_size, back_region_size=back_region_size
    )


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="separate_sources_{directories}_region_{region_size}_back_{back_region_size}",
)
def separate_sources(directories, config, region_size=30, back_region_size=55):

    for d in directories:
        separate_done_file = os.path.join(d, "SEPARATE_DONE.TXT")
        if os.path.exists(separate_done_file):
            logger = get_run_logger()
            logger.info(f"Source separation already done in {d}")
            continue
        logger = get_run_logger()
        logger.info(f"Separating sources in {d}")
        for event_file in glob.glob(os.path.join(d, "nu*_cl.evt*")):
            separate_sources_in_event_file.fn(
                event_file, region_size=region_size, back_region_size=back_region_size
            )
        with open(separate_done_file, "w") as f:
            f.write("")


@task(
    task_run_name="l2_pipeline_obsid_{obsid}",
)
def nu_run_l2_pipeline(obsid, config, flags=None):
    if not HAS_HEASOFT:
        raise ImportError("heasoftpy not installed")
    pipe_done_file = nu_pipeline_done_file.fn(obsid, config=config)
    if os.path.exists(pipe_done_file):
        logger = get_run_logger()
        logger.info(f"Data for {obsid} already preprocessed")
        return
    logger = get_run_logger()
    nupipeline = hsp.HSPTask("nupipeline")
    logger.info("Running NuSTAR L2 pipeline")
    datadir = nu_local_raw_data_path.fn(obsid, config=config)
    ev_dir = nu_pipeline_output_path.fn(obsid, config=config)
    os.makedirs(ev_dir, exist_ok=True)
    params = {
        "indir": datadir,
        "outdir": ev_dir,
        "steminputs": "nu" + obsid,
        "instrument": "ALL",
        "clobber": "yes",
        "noprompt": True,
        "verbose": True,
    }

    if flags:
        logger.info(f"Applying custom flags: {flags}")
        params.update(flags)

    result = nupipeline(**params)
    print("return code:", result.returncode)
    if result.returncode != 0:
        logger.error(f"nupipeline failed: {result.stderr}")
        raise RuntimeError("nupipeline failed")

    open(pipe_done_file, "a").close()

    return ev_dir


@task(
    task_run_name="nu_recover_spacecraft_science_{obsid}",
)
def recover_spacecraft_science_data(obsid, config):
    logger = get_run_logger()
    logger.info(f"Squeezing every photon from spacecraft science data in {obsid}")
    datadir = nu_local_raw_data_path.fn(obsid, config)
    ev_dir = nu_pipeline_output_path.fn(obsid, config)
    splitdir = split_path.fn(obsid, config=config)
    recover_done_file = os.path.join(splitdir, "RECOVER_DONE.TXT")
    hk_dir = os.path.join(datadir, "hk")

    evfiles_06 = glob.glob(os.path.join(ev_dir, "*[AB]06_cl.evt*"))

    if os.path.exists(recover_done_file):
        logger.info("Processing done")
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
            infile=evfile,
            chu123hkfile=chu123hkfile,
            hkfile=hkfile,
            outdir=splitdir,
            clobber="yes",
        )
    open(recover_done_file, "a").close()
    return splitdir


@task(task_run_name="nu_merge_gtis_{files_to_join}_into_{outfile_gti}_gti_{gti_operation}")
def merge_gtis(files_to_join, outfile_gti, gti_operation="OR"):
    if os.path.exists(outfile_gti):
        os.unlink(outfile_gti)
    logger = get_run_logger()

    logger.info(f"Creating GTI file {outfile_gti} from {files_to_join}")

    hsp.ftmgtime(
        ingtis=",".join([f + "[GTI]" for f in files_to_join]),
        outgti=outfile_gti,
        merge=gti_operation,
        chatter=5,
    )

    hsp.ftsort(infile=outfile_gti, outfile="!" + outfile_gti, columns="START")

    logger.info(f"Changing extension name to GTI in {outfile_gti}")

    hsp.fthedit(infile=outfile_gti + "+1", keyword="EXTNAME", operation="a", value="GTI")


@task(task_run_name="nu_merge_event_files_{files_to_join}_into_{outfile}_gti_{gti_operation}")
def merge_event_files(files_to_join, outfile, gti_operation="OR"):
    outdir, fname = os.path.split(outfile)
    root = splitext_improved(fname)[0]
    logger = get_run_logger()

    outfile_gti = os.path.join(outdir, f"{root}_{np.random.randint(1000000)}.gti")

    merge_gtis(files_to_join, outfile_gti, gti_operation=gti_operation)

    logger.info(f"Creating event file {outfile} from {files_to_join}")

    hsp.ftmerge(infile=",".join(files_to_join), outfile=outfile, copyall="NO")

    logger.info(f"Sorting event file {outfile}")

    hsp.ftsort(infile=outfile, outfile="!" + outfile, columns="TIME")

    logger.info(f"Adding GTIs from {outfile_gti}'s first extension to event file {outfile}")

    hsp.fappend(infile=f"{outfile_gti}[1]", outfile=outfile)

    logger.info(f"Removing {outfile_gti}")

    os.unlink(outfile_gti)


@task(
    task_run_name="nu_join_science_{obsid}_src{src_num}",
)
def join_source_data(obsid, directories, config, src_num=1):

    logger = get_run_logger()
    outdir = nu_base_output_path.fn(obsid, config=config)

    if src_num > 0:
        label = f"_src{src_num}"
    else:
        label = "_back"

    join_done_file = os.path.join(
        nu_base_output_path.fn(obsid, config=config), f"JOIN_DONE_SRC{src_num}.TXT"
    )
    if os.path.exists(join_done_file):
        logger = get_run_logger()
        logger.info(f"Source data for {obsid} already joined")
        return glob.glob(os.path.join(outdir, f"nu{obsid}*{label}.evt"))

    for fpm in "A", "B":
        outfile = os.path.join(outdir, f"nu{obsid}{fpm}{label}.evt")
        if os.path.exists(outfile):
            os.unlink(outfile)

        logger.info(f"Joining source data for fpm {fpm} into {outfile}")
        files_to_join = []
        for d in directories:
            logger.info(f"Adding data from {d}")
            new_files = glob.glob(os.path.join(d, f"nu{obsid}{fpm}0[16]*{label}.evt*"))
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
        merge_event_files(files_to_join, outfile)

    outfiles = []
    for a_file in glob.glob(os.path.join(outdir, f"nu{obsid}A{label}.evt")):
        b_file = a_file.replace("A", "B")
        outfile = os.path.join(outdir, f"nu{obsid}{label}.evt")
        merge_event_files([a_file, b_file], outfile, gti_operation="AND")
        outfiles.append(outfile)

    open(join_done_file, "a").close()
    return outfiles


@task(task_run_name="goes_lightcurve_{event_file}_mincat_{minimum_class}")
def get_goes_gtis(event_file, minimum_class="C5.0"):
    from sunpy import timeseries as ts
    from sunpy.net import Fido
    from sunpy.net import attrs as a
    from sunpy.time import parse_time
    from astropy.io.fits import getheader, getdata
    from nustar_gen import info, utils

    outfile_gti = goes_gti_file_name(event_file)

    if os.path.exists(outfile_gti):
        logger = get_run_logger()
        logger.info(f"GOES GTI file {outfile_gti} already exists, skipping")
        return outfile_gti

    # categories = ["A", "B", "C", "M", "X"]

    min_cat = minimum_class[0]
    min_num = float(minimum_class[1:])

    logger = get_run_logger()
    logger.info(f"Creating GOES light curve and GTIs for {event_file}")

    ns = info.NuSTAR()
    hdr = getheader(event_file, ext=1)
    tstart = hdr["TSTART"]
    tstop = hdr["TSTOP"]
    datestart = ns.met_to_time(tstart)
    dateend = ns.met_to_time(tstop)
    mjdref = hdr["MJDREFI"] + hdr["MJDREFF"]

    result = Fido.search(
        a.Time(datestart.fits, dateend.fits), a.Resolution("avg1m"), a.Instrument("XRS")
    )
    satellites = result["xrs"]["SatelliteNumber"].data
    sat_id = np.unique(satellites).max()
    result3 = Fido.search(
        a.Time(datestart.fits, dateend.fits),
        a.Instrument.xrs & a.goes.SatelliteNumber(sat_id) & a.Resolution("avg1m")
        | a.hek.FL & (a.hek.FRM.Name == "SWPC"),
    )
    files = Fido.fetch(result3, progress=False)
    goes_all = ts.TimeSeries(files, concatenate=True)
    goes = goes_all.truncate(datestart.iso, dateend.iso)

    hek_results = result3["hek"]
    flares_hek = hek_results

    # goes.to_table().write(root + "_goes.fits", overwrite=True)

    gtis = []
    previous_gti_start = tstart
    for flare_hek in flares_hek:
        flare_class = flare_hek["fl_goescls"]
        print(flare_class)
        category = flare_class[0]
        number = float(flare_class[1:])
        if category < min_cat:
            continue
        if category == min_cat and number < min_num:
            continue

        flare_start = (parse_time(flare_hek["event_starttime"]).mjd - mjdref) * 86400
        flare_end = (parse_time(flare_hek["event_endtime"]).mjd - mjdref) * 86400
        if flare_start >= tstop or flare_end <= tstart:
            continue

        gtis.append({"START": previous_gti_start, "STOP": flare_start})
        previous_gti_start = flare_end

    gtis.append({"START": previous_gti_start, "STOP": tstop})
    print(gtis)

    utils.make_usr_gti(gtis, overwrite=True, outfile=outfile_gti)
    logger.info(f"Changing extension name to GTI in {outfile_gti}")

    hsp.fthedit(infile=outfile_gti + "+1", keyword="EXTNAME", operation="a", value="GTI")

    if not os.path.exists(outfile_gti):
        raise RuntimeError(f"Failed to create GTI file {outfile_gti}")

    return outfile_gti


@flow(flow_run_name="nu_filter_solar_flares_{event_file}_mincat_{minimum_class}")
def filter_from_solar_flares(event_file, minimum_class="C5.0"):
    from astropy.io import fits
    from astropy.table import Table

    root = rootname(event_file)
    outfile_gti_temp = root + "_tmp.gti"
    outfile_filtered = flare_filtered_event_file_name(event_file)

    if os.path.exists(outfile_filtered):
        logger = get_run_logger()
        logger.info(f"Filtered event file {outfile_filtered} already exists, skipping")
        return outfile_filtered

    outfile_gti_goes = get_goes_gtis(event_file, minimum_class=minimum_class)

    merge_gtis([event_file, outfile_gti_goes], outfile_gti_temp, gti_operation="AND")

    with fits.open(event_file) as hdul, fits.open(outfile_gti_temp) as gti_hdul:
        hdul[2].data = gti_hdul[1].data

        hdul.writeto(outfile_filtered, overwrite=True)

    os.unlink(outfile_gti_temp)

    return outfile_filtered


@task(
    task_run_name="nu_barycenter_{infile}_ra{ra}_dec{dec}_src{src}",
)
def barycenter_file(infile, attorb, ra=None, dec=None, src=1):
    logger = get_run_logger()
    logger.info(f"Barycentering {infile}")

    outfile = infile.replace(".evt", "_bary.evt")
    logger.info(f"Output file: {outfile}")

    if os.path.exists(outfile):
        logger.info(f"Output file {outfile} already exists, skipping")
        return outfile

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


@flow(flow_run_name="nu_barycenter_{obsid}_src{src}_ra{ra}_dec{dec}")
def barycenter_data(obsid, ra, dec, config, src=1):
    logger = get_run_logger()
    outdir = nu_base_output_path.fn(obsid, config=config)
    logger.info(f"Barycentering data in directory {outdir}")
    pipe_outdir = nu_pipeline_output_path.fn(obsid, config=config)

    infiles = glob.glob(os.path.join(outdir, f"nu{obsid}*.evt*"))
    for infile in infiles:
        if "bary" in infile:
            continue

        barycenter_file(
            infile,
            os.path.join(pipe_outdir, f"nu{obsid}A.attorb"),
            ra=ra,
            dec=dec,
            src=src,
        )


@task(
    task_run_name="nu_best_source_reg_{infile}_pair_{pair}_elow_{elow}_ehigh_{ehigh}",
)
def get_best_source_region(infile, pair=None, elow=3, ehigh=80, out_rootname=None, config=None):
    from nustar_gen.radial_profile import find_source, make_radial_profile, optimize_radius_snr
    from nustar_gen.wrappers import make_image
    from astropy.io import fits
    from astropy.wcs import WCS
    from astropy.coordinates import SkyCoord

    if config is None:
        config = DEFAULT_CONFIG
    indir, fname = os.path.split(infile)
    if out_rootname is None:
        out_rootname = rootname(fname)

    src_out = os.path.join(indir, out_rootname + "_src.reg")
    bkg_out = os.path.join(indir, out_rootname + "_bkg.reg")
    if os.path.exists(src_out) and os.path.exists(bkg_out):
        from regions import Regions
        import astropy.units as u

        region_src = Regions.read(src_out, format="ds9")[0]
        logger = get_run_logger()
        logger.info(f"Source and background region files already exist for {infile}")
        return (
            region_src.center.ra.deg,
            region_src.center.dec.deg,
            region_src.radius.to(u.arcsec).value,
            src_out,
            bkg_out,
        )

    full_range = make_image(infile, elow=elow, ehigh=ehigh, clobber=True)
    if pair is None:
        pair = [elow, ehigh]
    coordinates = find_source(full_range, show_image=False, filt_range=3)
    # Get the WCS header and convert the pixel coordinates into an RA/Dec object
    hdu = fits.open(full_range, uint=True)[0]
    wcs = WCS(hdu.header)

    # The "flip" is necessary to go to [X, Y] ordering from native [Y, X] ordering, which wcs seems to require
    world = wcs.all_pix2world(np.flip(coordinates), 0)
    ra = world[0][0]
    dec = world[0][1]
    target = SkyCoord(ra, dec, unit="deg", frame="fk5")
    obj_j2000 = SkyCoord(hdu.header["RA_OBJ"], hdu.header["DEC_OBJ"], unit="deg", frame="fk5")

    # How far are we from the J2000 coordinates? If <15 arcsec, all is okay
    sep = target.separation(obj_j2000)
    # Now the radial image parts.

    # Make the radial image for the full energy range (or whatever is the best SNR)
    full_range = make_image(infile, elow=3, ehigh=80, clobber=True)
    rind, rad_profile, radial_err, psf_profile = make_radial_profile(
        full_range, show_image=False, coordinates=coordinates
    )
    coordinates = find_source(full_range, show_image=False)

    test_file = make_image(infile, elow=pair[0], ehigh=pair[1], clobber=True)
    rind, rad_profile, radial_err, psf_profile = make_radial_profile(
        test_file, show_image=False, coordinates=coordinates
    )
    rlimit = optimize_radius_snr(rind, rad_profile, radial_err, psf_profile, show=False)

    max_radius = config.get("max_radius", 80)
    print("Radius of peak SNR for {} to {} keV: {}".format(pair[0], pair[1], rlimit))
    if rlimit > max_radius:
        logger.warning(
            f"Calculated source region radius {rlimit} exceeds maximum allowed {max_radius}, using maximum"
        )
        rlimit = max_radius

    icrs = target.icrs

    src_reg = rf"""icrs
circle({icrs.ra.deg}, {icrs.dec.deg}, {rlimit}")
"""
    bkg_reg = rf"""icrs
-circle({icrs.ra.deg}, {icrs.dec.deg}, {max(rlimit, 100)}")
circle({icrs.ra.deg}, {icrs.dec.deg}, {max(rlimit * 2, 250)}")
"""

    with open(src_out, "w") as fobj:
        print(src_reg, file=fobj)
    with open(bkg_out, "w") as fobj:
        print(bkg_reg, file=fobj)

    return icrs.ra.deg, icrs.dec.deg, rlimit, src_out, bkg_out


@task(
    task_run_name="nu_best_source_regs_{obsid}",
)
def get_best_source_regions(obsid, config):
    indir = nu_pipeline_output_path.fn(obsid, config=config)
    outdir = nu_pipeline_output_path.fn(obsid, config=config)
    os.makedirs(outdir, exist_ok=True)
    mean_ra = 0
    mean_dec = 0
    mean_rlimit = 0
    count = 0
    for fpm in "A", "B":
        infiles = glob.glob(os.path.join(indir, f"nu{obsid}{fpm}01_cl.evt*"))
        for infile in infiles:
            if infile.endswith(".gpg"):
                continue
            root_name = rootname(infile)
            src_reg = os.path.join(outdir, root_name + "_src.reg")
            bkg_reg = os.path.join(outdir, root_name + "_bkg.reg")
            if not os.path.exists(src_reg) or not os.path.exists(bkg_reg):
                ra, dec, rlimit, src_out, bkg_out = get_best_source_region(infile)
                mean_ra += ra
                mean_dec += dec
                mean_rlimit += rlimit
                count += 1

    mean_ra /= count if count > 0 else 1
    mean_dec /= count if count > 0 else 1
    mean_rlimit /= count if count > 0 else 1

    return mean_ra, mean_dec, mean_rlimit


@task(
    task_run_name="nu_calc_spec_{obsid}_src-reg_{src_reg}_back-reg_{bkg_reg}",
)
def calculate_spectra(obsid, config, src_reg=None, bkg_reg=None):
    logger = get_run_logger()
    indir = nu_pipeline_output_path.fn(obsid, config=config)
    outdir = nu_product_output_path.fn(obsid, config=config)
    product_done_file = os.path.join(outdir, "PRODUCTS_DONE.TXT")
    if os.path.exists(product_done_file):
        logger.info(f"Spectra for {obsid} already calculated")
        return
    os.makedirs(outdir, exist_ok=True)
    logger.info(f"Calculating spectra in directory {outdir}")
    for fpm in "A", "B":
        infiles = glob.glob(os.path.join(indir, f"nu{obsid}{fpm}01_cl.evt*"))
        for infile in infiles:
            if infile.endswith(".gpg"):
                continue
            _, fname = os.path.split(infile)
            root_name = rootname(fname)
            if src_reg is None:
                src_reg = os.path.join(indir, root_name + "_src.reg")
            if bkg_reg is None:
                bkg_reg = os.path.join(indir, root_name + "_bkg.reg")

            outfile_gti_goes = get_goes_gtis(infile)
            outfile_gti_temp = os.path.join(indir, root_name + "_noflares.gti")

            merge_gtis([infile, outfile_gti_goes], outfile_gti_temp, gti_operation="AND")

            if not os.path.exists(src_reg) or not os.path.exists(bkg_reg):
                logger.warning(f"Source or background region file missing for {infile}")
                continue

            if not os.path.exists(outfile_gti_temp):
                logger.warning(f"Temporary GTI file missing for {infile}")

            break

        logger.info(f"Calculating spectrum for {infile}")

        params = dict(
            indir=indir,
            instrument=f"FPM{fpm}",
            steminputs="nu" + obsid,
            stemout="nu" + obsid + f"{fpm}01",
            srcregionfile=src_reg,
            bkgregionfile=bkg_reg,
            outdir=outdir,
            clobber="yes",
            runmkarf="yes",
            extended="no",
            runmkrmf="yes",
            rungrppha="yes",
            grpmincounts=20,
            grppibadlow=35,
            grppibadhigh=1909,
            usrgtifile=outfile_gti_temp,
            grpphafile=os.path.join(outdir, f"nu{obsid}{fpm}01_grp.pha"),
        )
        command = "nuproducts "
        for key, val in params.items():
            command += f"{key}={val} "
        print(command)
        hsp.nuproducts(params, noprompt=True, clobber=True, verbose=True)

    open(product_done_file, "w").close()


@flow
def process_nustar_obsid(obsid, config=None, ra="NONE", dec="NONE", flags=None):
    config = DEFAULT_CONFIG if config is None else config
    logger = get_run_logger()
    logger.info(f"Processing NuSTAR observation {obsid}")
    os.makedirs(os.path.join(nu_base_output_path(obsid, config=config)), exist_ok=True)
    basedir = nu_base_output_path.fn(obsid, config=config)
    # splitdir = split_path.fn(obsid, config=config)
    pipedir = nu_pipeline_output_path.fn(obsid, config=config)

    nu_run_l2_pipeline(obsid, config=config, flags=flags)

    splitdir = recover_spacecraft_science_data(obsid, config, wait_for=[nu_run_l2_pipeline])

    ra, dec, region_size = get_best_source_regions(obsid, config, wait_for=[nu_run_l2_pipeline])

    separate_sources(
        [pipedir, splitdir],
        config,
        wait_for=[recover_spacecraft_science_data],
        region_size=region_size,
        back_region_size=region_size + 25,
    )

    files = join_source_data(obsid, [pipedir, splitdir], config, wait_for=[separate_sources])
    for fname in files:
        filter_from_solar_flares(fname, wait_for=[join_source_data])

    join_source_data(obsid, [pipedir, splitdir], config, src_num=0, wait_for=[separate_sources])
    barycenter_data(obsid, ra=ra, dec=dec, config=config, wait_for=[join_source_data])

    calculate_spectra(obsid, config, wait_for=[get_best_source_regions])
