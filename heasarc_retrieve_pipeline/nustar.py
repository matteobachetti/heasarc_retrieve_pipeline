"""
NuSTAR reduction: Level-2 pipeline, source separation, merging, flare filtering, spectra.

The entry point is :func:`process_nustar_obsid`, which runs, in order:

1. :func:`nu_run_l2_pipeline` -- HEASOFT ``nupipeline``;
2. :func:`recover_spacecraft_science_data` -- ``nusplitsc`` on the mode-06 files, to
   recover the exposure taken while the optics-bench star tracker was unavailable;
3. :func:`get_best_source_regions` -- SNR-optimised extraction regions via ``nustar_gen``;
4. :func:`separate_sources` -- image-based splitting of the field into per-source and
   background event files;
5. :func:`join_source_data` -- merging of the event files and their GTIs;
6. :func:`filter_from_solar_flares` -- GOES/HEK solar-flare exclusion;
7. :func:`barycenter_data` -- HEASOFT ``barycorr``;
8. :func:`calculate_spectra` -- HEASOFT ``nuproducts``.

Only observing modes 01 (normal science) and 06 (spacecraft science) are considered;
``valid_re`` encodes that choice.

Output layout, relative to ``config["out_data_path"]``::

    <OBSID>/               merged event files, *_bary.evt, sentinel files
    <OBSID>/event_pipe/    nupipeline output and region files
    <OBSID>/split/         nusplitsc output (mode-06 sub-observations)
    <OBSID>/products/      nuproducts spectra, ARFs and RMFs

Everything past step 1 needs a working HEASOFT with ``heasoftpy``; steps 3 and 6 also
need ``nustar_gen``, ``sunpy`` and ``regions``, which are not currently declared as
dependencies.

See ``docs/technical_details.rst`` for the scientific rationale behind each step, and
``docs/known_issues.rst`` for known defects.
"""

import os
import re

import glob
from datetime import timedelta
import numpy as np
import astropy.units as u
from astropy.coordinates import SkyCoord
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from .image_utils import filter_sources_in_images
from .barycenter import barycenter_file
from .utils import apply_gti, get_logger, good_intervals, splitext_improved

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
    """
    Directory holding the raw (Level-1) data of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path``.

    Returns
    -------
    str
        ``<input_data_path>/<OBSID>``.
    """
    return os.path.join(config["input_data_path"], obsid)


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_remote_raw_path_{obsid}",
)
def nu_heasarc_raw_data_path(obsid, **kwargs):
    """
    Path of an observation in the HEASARC archive.

    NuSTAR observations are filed by the third and fourth digits of the OBSID, then by its
    first digit; for example ``90101005001`` lives under ``.../obs/01/9/90101005001/``.

    Parameters
    ----------
    obsid : str
        Observation identifier.

    Returns
    -------
    str
        ``/FTP/nustar/data/obs/<obsid[1:3]>/<obsid[0]>/<OBSID>``.
    """
    return os.path.normpath(f"/FTP/nustar/data/obs/{obsid[1:3]}/{obsid[0]}/{obsid}/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_base_output_{obsid}",
)
def nu_base_output_path(obsid, config):
    """
    Top-level output directory of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>``, where the merged and barycentred event files go.
    """
    return os.path.join(config["out_data_path"], obsid)


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_pipeline_output_{obsid}",
)
def nu_pipeline_output_path(obsid, config):
    """
    Directory for the ``nupipeline`` output of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_pipe/``.
    """
    return os.path.join(config["out_data_path"], obsid + "/event_pipe/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_product_output_{obsid}",
)
def nu_product_output_path(obsid, config):
    """
    Directory for the ``nuproducts`` output of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/products/``, where spectra, ARFs and RMFs go.
    """
    return os.path.join(config["out_data_path"], obsid + "/products/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_pipeline_output_{obsid}",
)
def nu_pipeline_done_file(obsid, config):
    """
    Path of the sentinel file marking a finished ``nupipeline`` run.

    The pipeline uses sentinel files rather than Prefect's cache for idempotency. Note that
    a sentinel records only *that* the step ran, not with which parameters, so changing
    ``flags`` will not trigger a re-run.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/event_pipe/PIPELINE_DONE.TXT``.
    """
    return os.path.join(nu_pipeline_output_path.fn(obsid, config), "PIPELINE_DONE.TXT")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="split_path_{obsid}",
)
def split_path(obsid, config):
    """
    Directory for the ``nusplitsc`` output of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/split/``.
    """
    return os.path.join(config["out_data_path"], obsid + "/split/")


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="splitext_{infile}",
)
def splitext(infile):
    """
    Split a path into root and extension, treating ``.evt.gz`` as one extension.

    Thin wrapper around :func:`heasarc_retrieve_pipeline.utils.splitext_improved`.

    Parameters
    ----------
    infile : str
        File path.

    Returns
    -------
    root : str
        The path without its extension.
    ext : str
        The extension, including any compression suffix.
    """
    return splitext_improved(infile)


def _cl_event_files(directory, pattern):
    """
    Cleaned event files matching a pattern, in a deterministic order.

    Encrypted (``.gpg``) files are excluded -- they cannot be read without the
    proprietary-data key. Sorting makes the choice reproducible when both a compressed and
    an uncompressed copy of the same file are present: ``.evt`` sorts before ``.evt.gz``.

    Parameters
    ----------
    directory : str
        Directory to look in.
    pattern : str
        Glob pattern, relative to ``directory``.

    Returns
    -------
    list of str
        Matching paths, sorted.
    """
    return sorted(
        f for f in glob.glob(os.path.join(directory, pattern)) if not f.endswith(".gpg")
    )


def spectral_input_files(obsid, config):
    """
    Event files to extract spectra from, with the module each one belongs to.

    Two kinds of file qualify. Mode 01, the normal science mode, comes from the
    ``nupipeline`` output directory. Mode 06, "spacecraft science", comes from the ``split``
    directory as the per-CHU files written by ``nusplitsc`` -- the unsplit mode-06 file is
    deliberately not included, because its aspect solution has not been reconstructed yet.

    Mode 01 is yielded first for each module, because it defines the reference position the
    mode-06 detections are checked against. Use :func:`mode_01_input_files` when only the
    mode-01 files are wanted.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Yields
    ------
    fpm : {"A", "B"}
        Focal-plane module the file belongs to.
    infile : str
        Path of the cleaned event file.
    """
    pipedir = nu_pipeline_output_path.fn(obsid, config=config)
    splitdir = split_path.fn(obsid, config=config)
    for fpm in "A", "B":
        for infile in _cl_event_files(pipedir, f"nu{obsid}{fpm}01_cl.evt*"):
            yield fpm, infile
        for infile in _cl_event_files(splitdir, f"nu{obsid}{fpm}06_chu*_cl.evt*"):
            yield fpm, infile


def mode_01_input_files(obsid, config):
    """
    Normal-science (mode 01) event files only, with the module each one belongs to.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Yields
    ------
    fpm : {"A", "B"}
        Focal-plane module the file belongs to.
    infile : str
        Path of the cleaned event file.

    Notes
    -----
    :func:`get_best_source_regions` averages over these files alone, and deliberately not
    over the mode-06 ones. Each CHU combination has its own aspect reconstruction, scattered
    by about 2 arcmin, so averaging them in moves the position the data are barycentred at:
    measured on 80002092008 the mean shifted by 63 arcsec, which is roughly 150 ms of
    barycentric delay -- ruinous for timing.

    Keeping mode-06 files out of this function also keeps their region files unwritten until
    :func:`calculate_spectra` asks for them, which is what lets that function apply the
    mode-01 reference position as a consistency check.
    """
    pipedir = nu_pipeline_output_path.fn(obsid, config=config)
    for fpm in "A", "B":
        for infile in _cl_event_files(pipedir, f"nu{obsid}{fpm}01_cl.evt*"):
            yield fpm, infile


def position_is_consistent(position, reference, max_offset):
    """
    Whether a detected source position is close enough to where it was expected.

    Each CHU1/CHU2/CHU3 combination recovered by ``nusplitsc`` carries its own aspect
    reconstruction, scattered by about 2 arcmin according to the ``nusplitsc``
    documentation. A detection further from the mode-01 position than that is not the same
    object, and extracting a spectrum there would silently produce the wrong source.

    Parameters
    ----------
    position : :class:`astropy.coordinates.SkyCoord`
        Detected source position.
    reference : :class:`astropy.coordinates.SkyCoord` or None
        Position the source is expected near. ``None`` means no constraint -- the case for
        mode-01 data, which is what defines the reference in the first place.
    max_offset : :class:`astropy.units.Quantity`
        Largest acceptable separation, as an angle.

    Returns
    -------
    bool
        ``True`` if the position is acceptable.

    Notes
    -----
    This takes a single reference position because the pipeline currently extracts one
    source, the brightest. Supporting several means passing the list of reference positions
    and matching each detected peak to its nearest one; the comparison itself is unchanged.
    """
    if reference is None:
        return True
    return position.separation(reference) <= max_offset


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="rootname_{infile}",
)
def rootname(infile):
    """
    Return a path with its extension removed.

    Parameters
    ----------
    infile : str
        File path.

    Returns
    -------
    str
        ``infile`` without its (possibly compound) extension.
    """
    return splitext(infile)[0]


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="barycentered_file_name_{infile}",
)
def barycentered_file_name(infile):
    """
    Name of the barycentred version of an event file.

    Inserts ``_bary`` before the extension, so that ``x.evt.gz`` becomes
    ``x_bary.evt.gz`` rather than being mangled by a naive string replacement.

    Parameters
    ----------
    infile : str
        Event file path.

    Returns
    -------
    str
        The barycentred file name.

    Notes
    -----
    Currently unused: :func:`barycenter_file` builds the output name with
    ``str.replace`` instead. See issue 13 in ``docs/known_issues.rst``.
    """
    root, ext = splitext(infile)
    return root + "_bary" + ext


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="goes_lc_file_name_{event_file}",
)
def goes_lc_file_name(event_file):
    """
    Name of the GOES light-curve file associated with an event file.

    Parameters
    ----------
    event_file : str
        Event file path.

    Returns
    -------
    str
        ``<root>_goes.fits``.

    Notes
    -----
    Unused: :func:`get_goes_gtis` downloads the GOES light curve but never writes it.
    """
    root = rootname(event_file)
    return root + "_goes.fits"


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="goes_gti_file_name_{event_file}",
)
def goes_gti_file_name(event_file):
    """
    Name of the solar-flare GTI file associated with an event file.

    Parameters
    ----------
    event_file : str
        Event file path.

    Returns
    -------
    str
        ``<root>_goes.gti``.
    """
    root = rootname(event_file)
    return root + "_goes.gti"


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="flare_filtered_event_file_name_{event_file}",
)
def flare_filtered_event_file_name(event_file):
    """
    Name of the flare-filtered version of an event file.

    Parameters
    ----------
    event_file : str
        Event file path.

    Returns
    -------
    str
        ``<root>_noflares.evt``.
    """
    root = rootname(event_file)
    return root + "_noflares.evt"


@task(
    task_run_name="separate_sources_in_event_file_{obsid}_{event_file}_region_{region_size}_back_{back_region_size}",
)
def separate_sources_in_event_file(event_file, region_size=30, back_region_size=55):
    """
    Split one cleaned event file into per-source and background event files.

    Skips encrypted (``.gpg``) files and anything that is not a mode 01 or mode 06 NuSTAR
    event file, then delegates to
    :func:`heasarc_retrieve_pipeline.image_utils.filter_sources_in_images`.

    Parameters
    ----------
    event_file : str
        Path of a cleaned event file.
    region_size : float, optional
        Radius of the source extraction circles, in sky pixels (1 pixel = 2.45 arcsec).
    back_region_size : float, optional
        Radius, in sky pixels, of the region excluded around every detected peak when
        building the background file.

    Returns
    -------
    bool or None
        ``True`` if files were written, ``None`` if the input was skipped or contained too
        few events.
    """
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

    """
    Run the image-based source separation over every cleaned event file in some directories.

    Writes a ``SEPARATE_DONE.TXT`` sentinel in each directory and skips directories that
    already have one.

    Parameters
    ----------
    directories : list of str
        Directories to scan for ``nu*_cl.evt*`` files -- normally the ``event_pipe`` and
        ``split`` directories of one observation.
    config : dict
        Pipeline configuration. Currently unused by this step.
    region_size : float, optional
        Source extraction radius in sky pixels.
    back_region_size : float, optional
        Background exclusion radius in sky pixels.
    """
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
    """
    Run the HEASOFT ``nupipeline`` Level-2 pipeline on one observation.

    ``nupipeline`` performs the standard NuSTAR screening: calibration, coordinate
    transformation, and the good-time selection on SAA passage, source occultation and
    attitude quality. It is run for both focal-plane modules (``instrument="ALL"``).

    Returns immediately if the ``PIPELINE_DONE.TXT`` sentinel already exists.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path`` and ``out_data_path``.
    flags : dict, optional
        Extra ``nupipeline`` parameters, merged over the defaults. This is how
        non-standard screening (for example a different SAA mode) is requested.

    Returns
    -------
    str or None
        The ``event_pipe`` directory, or ``None`` if the step was already done.

    Raises
    ------
    ImportError
        If ``heasoftpy`` is not available.
    RuntimeError
        If ``nupipeline`` exits with a non-zero return code.
    """
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
    """
    Recover the mode-06 "spacecraft science" exposure with ``nusplitsc``.

    NuSTAR's normal aspect solution comes from CHU4, the star tracker on the optics bench.
    When CHU4 is blinded -- typically by the Sun or the Moon -- the data are recorded in
    observing mode 06 and the aspect must be reconstructed from the spacecraft's own star
    trackers CHU1, CHU2 and CHU3. Standard Level-2 products drop these intervals.

    ``nusplitsc`` splits each mode-06 cleaned event file by which combination of star
    trackers was active, since each combination carries its own systematic astrometric
    offset and they must be treated as separate sub-observations. The result typically adds
    of order 10-20 per cent more exposure, at the cost of degraded pointing accuracy.

    Writes a ``RECOVER_DONE.TXT`` sentinel in the split directory and returns early if it
    already exists.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``input_data_path`` and ``out_data_path``.

    Returns
    -------
    str
        The ``split`` directory.
    """
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
    """
    Merge the GTI extensions of several event files into one GTI file.

    Runs HEASOFT ``ftmgtime`` to combine the intervals, ``ftsort`` to order them by
    ``START``, and ``fthedit`` to name the resulting extension ``GTI``.

    Parameters
    ----------
    files_to_join : list of str
        Files whose ``[GTI]`` extensions are to be combined.
    outfile_gti : str
        Output GTI file. Deleted first if it exists.
    gti_operation : {"OR", "AND"}, optional
        ``"OR"`` takes the union of the intervals -- correct when combining disjoint
        stretches of the same instrument. ``"AND"`` takes the intersection -- correct when
        combining simultaneous data from two instruments, so that the effective area is
        constant across the result.
    """
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
    """
    Merge several event files, and their GTIs, into a single event file.

    The GTIs are combined first with :func:`merge_gtis`, then ``ftmerge`` concatenates the
    event tables, ``ftsort`` orders them by ``TIME``, and ``fappend`` attaches the merged
    GTI extension. Doing it with HEASOFT rather than by hand is what keeps the GTI
    bookkeeping correct.

    Parameters
    ----------
    files_to_join : list of str
        Event files to merge.
    outfile : str
        Output event file.
    gti_operation : {"OR", "AND"}, optional
        How to combine the GTIs; see :func:`merge_gtis`.

    Notes
    -----
    The temporary GTI file is named with ``np.random.randint``, which makes the task's
    inputs non-deterministic. See issue 19 in ``docs/known_issues.rst``.
    """
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

    """
    Merge the per-source (or background) event files of one observation.

    Two stages:

    1. For each focal-plane module, merge the files produced by the source separation in
       all the given directories -- the ``nupipeline`` output and the ``nusplitsc``
       sub-observations -- with a logical **OR** of their GTIs, since these are disjoint
       stretches of the same telescope. Mode-01 files are also copied to the output
       directory as-is; the unsplit mode-06 files are discarded in favour of their
       CHU-resolved counterparts.
    2. Merge the FPMA and FPMB results into a single file, this time with a logical
       **AND** of the GTIs: an interval counts only if both telescopes were observing, so
       that the combined light curve has a constant effective area.

    The combined A+B file roughly doubles the counting statistics and is meant for timing
    analysis. It is not usable for spectroscopy, since two telescopes with different
    responses now share one event list.

    Writes a ``JOIN_DONE_SRC<n>.TXT`` sentinel and returns early if it exists.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    directories : list of str
        Directories to collect event files from.
    config : dict
        Must contain ``out_data_path``.
    src_num : int, optional
        Which product to merge: a positive number selects ``_src<n>`` files, 0 selects the
        ``_back`` files.

    Returns
    -------
    list of str
        The combined FPMA+FPMB event file, or an empty list if it is missing.

    Notes
    -----
    Both code paths return the same thing. They used to differ: the early return globbed
    ``nu<obsid>*<label>.evt``, which on a real observation also matches the per-module and
    per-mode intermediates that stage 1 leaves in the directory -- five files rather than
    one on 80002092008. Since ``process_nustar_obsid`` flare-filters and barycentres
    whatever this returns, a rerun did five times the work of a fresh run, on files that
    are not meant to be science products. See issue 6 in ``docs/known_issues.rst``.
    """
    logger = get_logger()
    outdir = nu_base_output_path.fn(obsid, config=config)

    if src_num > 0:
        label = f"_src{src_num}"
    else:
        label = "_back"

    combined_file = os.path.join(outdir, f"nu{obsid}{label}.evt")

    join_done_file = os.path.join(outdir, f"JOIN_DONE_SRC{src_num}.TXT")
    if os.path.exists(join_done_file):
        logger.info(f"Source data for {obsid} already joined")
        return [combined_file] if os.path.exists(combined_file) else []

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

    # Both module file names are known, so build them rather than globbing for FPMA and
    # deriving FPMB from it with str.replace: an output path containing a capital A --
    # /Users/.../ARCHIVE/, say -- would have had that A rewritten too.
    module_files = [os.path.join(outdir, f"nu{obsid}{fpm}{label}.evt") for fpm in "AB"]
    merge_event_files(module_files, combined_file, gti_operation="AND")

    open(join_done_file, "a").close()
    return [combined_file]


@task(task_run_name="goes_lightcurve_{event_file}_mincat_{minimum_class}")
def get_goes_gtis(event_file, minimum_class="C5.0"):
    """
    Build good time intervals that exclude solar flares.

    NuSTAR observes from low Earth orbit with an open detector aperture, and large solar
    flares raise its background substantially. This task looks up the flares that occurred
    during an observation and produces the complementary GTIs.

    The steps are: convert the observation's ``TSTART``/``TSTOP`` from NuSTAR
    mission-elapsed time to civil time; ask ``sunpy``'s ``Fido`` for the GOES XRS data of
    that interval, picking the highest-numbered (most recent) satellite that covers it;
    retrieve the HEK flare catalogue entries flagged by SWPC; and cut out every catalogued
    flare at or above ``minimum_class``. The surviving intervals are the complement of
    the flares inside ``[TSTART, TSTOP]``, computed by
    :func:`~heasarc_retrieve_pipeline.utils.good_intervals`.

    Flare classes are compared by letter and number separately. The GOES scale runs
    A, B, C, M, X, which is alphabetical, so comparing the letters as characters gives the
    correct ordering.

    Returns the existing file unchanged if it is already present.

    Parameters
    ----------
    event_file : str
        Event file whose time range defines the search interval.
    minimum_class : str, optional
        Smallest flare class to exclude, e.g. ``"C5.0"``.

    Returns
    -------
    str
        Path of the GTI file, ``<root>_goes.gti``.

    Notes
    -----
    The GOES X-ray light curve is downloaded but not used for the filtering itself, which
    runs entirely off the HEK flare catalogue.
    """
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

    flares = []
    for flare_hek in flares_hek:
        flare_class = flare_hek["fl_goescls"]
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

        logger.info(
            f"Excluding {flare_class} flare, MET {flare_start:.1f} -> {flare_end:.1f}"
        )
        flares.append((flare_start, flare_end))

    # good_intervals does the clipping, sorting, merging and empty-interval dropping, so
    # a flare overlapping TSTART or two overlapping flares cannot produce a broken GTI.
    good = good_intervals(flares, tstart, tstop)
    if len(good) == 0:
        raise RuntimeError(
            f"Flares of class {minimum_class} or above cover the whole of {event_file} "
            f"(MET {tstart} -- {tstop}); no good time is left."
        )
    logger.info(f"{len(flares)} flares excluded, leaving {len(good)} good intervals")

    gtis = [{"START": start, "STOP": stop} for start, stop in good]

    utils.make_usr_gti(gtis, overwrite=True, outfile=outfile_gti)
    logger.info(f"Changing extension name to GTI in {outfile_gti}")

    hsp.fthedit(infile=outfile_gti + "+1", keyword="EXTNAME", operation="a", value="GTI")

    if not os.path.exists(outfile_gti):
        raise RuntimeError(f"Failed to create GTI file {outfile_gti}")

    return outfile_gti


@flow(flow_run_name="nu_filter_solar_flares_{event_file}_mincat_{minimum_class}")
def filter_from_solar_flares(event_file, minimum_class="C5.0"):
    """
    Write a flare-free copy of an event file.

    Combines the event file's own GTIs with the flare-free intervals from
    :func:`get_goes_gtis` using a logical AND, and writes the result as
    ``<root>_noflares.evt``.

    The events recorded during the excluded intervals are removed from the event table and
    the exposure keywords are corrected, by
    :func:`~heasarc_retrieve_pipeline.utils.apply_gti`. Doing only half of this -- swapping
    the GTI extension and leaving the rest alone, as this function used to -- produces a
    file that still contains its flare counts and still advertises its original
    ``EXPOSURE``, so any tool that ignores GTIs, or takes a rate from the header, gets a
    wrong answer from a file whose name promises otherwise.

    Parameters
    ----------
    event_file : str
        Event file to filter.
    minimum_class : str, optional
        Smallest flare class to exclude.

    Returns
    -------
    str
        Path of the filtered file.
    """
    from astropy.io import fits

    root = rootname(event_file)
    outfile_gti_temp = root + "_tmp.gti"
    outfile_filtered = flare_filtered_event_file_name(event_file)

    logger = get_logger()

    if os.path.exists(outfile_filtered):
        logger.info(f"Filtered event file {outfile_filtered} already exists, skipping")
        return outfile_filtered

    outfile_gti_goes = get_goes_gtis(event_file, minimum_class=minimum_class)

    merge_gtis([event_file, outfile_gti_goes], outfile_gti_temp, gti_operation="AND")

    with fits.open(event_file) as hdul, fits.open(outfile_gti_temp) as gti_hdul:
        stats = apply_gti(hdul, gti_hdul[1].data)
        hdul.writeto(outfile_filtered, overwrite=True)

    logger.info(
        f"{outfile_filtered}: "
        f"{stats['nevents_before'] - stats['nevents_after']} of "
        f"{stats['nevents_before']} events removed, live time "
        f"{stats['livetime_before']:.1f} -> {stats['livetime_after']:.1f} s"
    )

    os.unlink(outfile_gti_temp)

    return outfile_filtered


@task(
    task_run_name="nu_barycenter_{infile}_ra{ra}_dec{dec}_src{src}",
)
def barycenter_file(infile, attorb, ra=None, dec=None, src=1):
    """
    Barycentre one event file with HEASOFT ``barycorr``.

    Converts photon arrival times from the spacecraft frame to the solar system
    barycentre, removing the up to ~500 s light-travel-time modulation caused by the
    Earth's and the satellite's motion. This is a prerequisite for any coherent timing
    analysis, and it is **position-dependent**: an error in the assumed RA/Dec translates
    directly into a timing error.

    Uses the JPL DE430 ephemeris in the ICRS frame.

    Parameters
    ----------
    infile : str
        Event file to barycentre.
    attorb : str
        Attitude/orbit file, as produced by ``nupipeline`` (``nu<OBSID><FPM>.attorb``).
    ra, dec : float, optional
        Source position in degrees. Accuracy here directly sets the timing accuracy.
    src : int, optional
        Source number; recorded in the task run name only.

    Returns
    -------
    str
        Path of the barycentred file.

    Notes
    -----
    This shadows the guarded implementation in
    :mod:`heasarc_retrieve_pipeline.barycenter`, which is imported at the top of this
    module and then never used. See issue 14 in ``docs/known_issues.rst``.
    """
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
    """
    Barycentre every event file of an observation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    ra, dec : float
        Source position in degrees.
    config : dict
        Must contain ``out_data_path``.
    src : int, optional
        Source number, passed through to :func:`barycenter_file`.

    Notes
    -----
    FPMA's attitude/orbit file is used for every file, including the FPMB and combined
    ones. See issue 13 in ``docs/known_issues.rst``.
    """
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
def get_best_source_region(
    infile,
    pair=None,
    elow=3,
    ehigh=80,
    out_rootname=None,
    config=None,
    reference=None,
    max_offset=None,
):
    """
    Find the source and choose the extraction radius that maximises its signal-to-noise.

    Delegates to ``nustar_gen``: make a sky image in the requested band, locate the
    brightest source, convert its pixel position to RA/Dec through the image WCS, build the
    radial profile together with the expected PSF profile, and pick the radius that
    maximises the SNR. The radius is capped at ``config["max_radius"]`` (default 80
    arcsec).

    Two DS9 region files are written next to the event file: a circle of the optimised
    radius at the source position, and a concentric background annulus of inner radius
    ``max(r, 100)`` arcsec and outer radius ``max(2r, 250)`` arcsec.

    If both region files already exist, they are read back and their parameters returned
    without recomputing.

    Parameters
    ----------
    infile : str
        Cleaned event file.
    pair : list of float, optional
        ``[elow, ehigh]`` band, in keV, in which the SNR is optimised. Defaults to
        ``[elow, ehigh]``.
    elow, ehigh : float, optional
        Band, in keV, used to build the image in which the source is located.
    out_rootname : str, optional
        Root name for the region files. Defaults to the event file's root.
    config : dict, optional
        Pipeline configuration; ``max_radius`` and ``max_source_offset_arcmin`` are read.
    reference : :class:`astropy.coordinates.SkyCoord`, optional
        Position the source is expected near. Used for mode-06 data, whose per-CHU aspect
        solutions each carry their own offset; ``None`` imposes no constraint.
    max_offset : :class:`astropy.units.Quantity`, optional
        Largest acceptable separation from ``reference``. Defaults to
        ``config["max_source_offset_arcmin"]`` arcmin, or 3 arcmin.

    Returns
    -------
    ra, dec : float
        Source position in ICRS degrees.
    rlimit : float
        Extraction radius in arcsec.
    src_out, bkg_out : str
        Paths of the source and background region files.

    Returns ``None``, writing no region files, if the detected source is further than
    ``max_offset`` from ``reference``.

    Notes
    -----
    A detection that locks onto the wrong object is caught only when ``reference`` is
    given; without one, the brightest peak in the field is taken on trust. A concentric
    annulus is also not the recommended NuSTAR background prescription, because the
    aperture stray-light background varies across the detector. See the science caveats in
    ``docs/known_issues.rst``.
    """
    logger = get_logger()
    if config is None:
        config = DEFAULT_CONFIG
    indir, fname = os.path.split(infile)
    if out_rootname is None:
        out_rootname = rootname.fn(fname)

    src_out = os.path.join(indir, out_rootname + "_src.reg")
    bkg_out = os.path.join(indir, out_rootname + "_bkg.reg")
    if os.path.exists(src_out) and os.path.exists(bkg_out):
        from regions import Regions

        region_src = Regions.read(src_out, format="ds9")[0]
        logger.info(f"Source and background region files already exist for {infile}")
        return (
            region_src.center.ra.deg,
            region_src.center.dec.deg,
            region_src.radius.to(u.arcsec).value,
            src_out,
            bkg_out,
        )

    # nustar_gen is only needed when a region has to be measured, not when one is read
    # back, so import it here rather than at the top of the function.
    from nustar_gen.radial_profile import find_source, make_radial_profile, optimize_radius_snr
    from nustar_gen.wrappers import make_image
    from astropy.io import fits
    from astropy.wcs import WCS

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

    if max_offset is None:
        max_offset = config.get("max_source_offset_arcmin", 3) * u.arcmin
    if not position_is_consistent(target, reference, max_offset):
        logger.warning(
            f"Source found in {infile} is "
            f"{target.separation(reference).to(u.arcmin):.2f} from the expected position, "
            f"more than {max_offset}. Writing no region file for it."
        )
        return None

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
    """
    Build extraction regions for both focal-plane modules and average their parameters.

    Runs :func:`get_best_source_region` on each mode-01 cleaned event file and returns the
    mean of the resulting positions and radii. The position feeds the barycentric
    correction, so mode-06 files are deliberately excluded -- see
    :func:`mode_01_input_files`.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    mean_ra, mean_dec : float
        Mean detected source position, in degrees.
    mean_rlimit : float
        Mean extraction radius, in arcsec.

    Notes
    -----
    Files whose region files already exist still contribute: :func:`get_best_source_region`
    reads the position and radius back out of them. ``(0.0, 0.0, 0.0)`` is returned only
    when there is no mode-01 cleaned event file at all -- which happens: 80002092003 has
    none.
    """
    logger = get_logger()
    outdir = nu_pipeline_output_path.fn(obsid, config=config)
    os.makedirs(outdir, exist_ok=True)

    mean_ra = mean_dec = mean_rlimit = 0.0
    count = 0
    for _, infile in mode_01_input_files(obsid, config):
        # get_best_source_region returns early when the region files already exist,
        # reading the position and radius back out of them, so every file counts.
        result = get_best_source_region.fn(infile, config=config)
        if result is None:
            continue
        ra, dec, rlimit, _, _ = result
        mean_ra += ra
        mean_dec += dec
        mean_rlimit += rlimit
        count += 1

    if count == 0:
        logger.warning(f"No cleaned event file to locate a source in for {obsid}")
        return 0.0, 0.0, 0.0

    return mean_ra / count, mean_dec / count, mean_rlimit / count


@task(
    task_run_name="nu_calc_spec_{obsid}_src-reg_{src_reg}_back-reg_{bkg_reg}",
)
def calculate_spectra(obsid, config, src_reg=None, bkg_reg=None, ra=None, dec=None):
    """
    Extract calibrated spectra with HEASOFT ``nuproducts``.

    Runs once per focal-plane module, using the region files from
    :func:`get_best_source_region` and the flare-free GTIs from :func:`get_goes_gtis`.
    Unlike the event files produced by the image-based separation, these products are
    properly calibrated and suitable for fitting in XSPEC:

    * ``runmkarf``/``runmkrmf`` generate the ancillary response -- effective area including
      the PSF correction for the chosen extraction radius and the vignetting for the
      source's off-axis angle -- and the redistribution matrix;
    * ``extended="no"`` treats the source as a point source, which is what makes the PSF
      correction valid;
    * ``grpmincounts=20`` groups the spectrum to at least 20 counts per bin, the usual
      minimum for chi-squared fitting to be approximately valid;
    * ``grppibadlow=35`` and ``grppibadhigh=1909`` mark channels outside 3.0-78.0 keV as
      bad, via ``E = 0.04 * PI + 1.6``.

    Writes a ``PRODUCTS_DONE.TXT`` sentinel and returns early if it exists.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    src_reg, bkg_reg : str, optional
        Region files to use. If ``None``, they are looked up next to each event file and
        measured if they are not there.
    ra, dec : float, optional
        Mode-01 source position, in degrees. Mode-06 detections are required to fall within
        ``config["max_source_offset_arcmin"]`` of it.

    Notes
    -----
    ``PRODUCTS_DONE.TXT`` is written only if nothing went wrong. An observation with no
    usable event files is a clean outcome -- nothing was produced and nothing failed -- and
    is marked done; a missing region or GTI file that should have been there is not, so the
    next run retries instead of the observation being marked done forever.
    """
    logger = get_run_logger()
    indir = nu_pipeline_output_path.fn(obsid, config=config)
    outdir = nu_product_output_path.fn(obsid, config=config)
    product_done_file = os.path.join(outdir, "PRODUCTS_DONE.TXT")
    if os.path.exists(product_done_file):
        logger.info(f"Spectra for {obsid} already calculated")
        return
    os.makedirs(outdir, exist_ok=True)
    logger.info(f"Calculating spectra in directory {outdir}")

    reference = None
    if ra not in (None, "NONE") and dec not in (None, "NONE"):
        reference = SkyCoord(float(ra), float(dec), unit="deg")
    max_offset = config.get("max_source_offset_arcmin", 3) * u.arcmin

    problems = 0
    for fpm, infile in spectral_input_files(obsid, config):
        root_name = rootname.fn(os.path.basename(infile))
        stem = root_name[: -len("_cl")] if root_name.endswith("_cl") else root_name
        filedir = os.path.dirname(infile)
        is_mode_06 = f"{fpm}06" in os.path.basename(infile)

        # Region files are per input file. Anything passed in by the caller wins, but it
        # must not be allowed to leak from one module to the next.
        this_src = src_reg or os.path.join(filedir, root_name + "_src.reg")
        this_bkg = bkg_reg or os.path.join(filedir, root_name + "_bkg.reg")
        if not os.path.exists(this_src) or not os.path.exists(this_bkg):
            # Every CHU combination has its own aspect solution, so it needs its own
            # region; the mode-01 position is the reference it has to agree with.
            get_best_source_region.fn(
                infile,
                config=config,
                reference=reference if is_mode_06 else None,
                max_offset=max_offset,
            )
        if not os.path.exists(this_src) or not os.path.exists(this_bkg):
            # Determinate: either no source was found or it was too far from the mode-01
            # position. Rerunning would decide the same, so this is a clean skip.
            logger.warning(f"No usable extraction region for {infile}, skipping")
            continue

        outfile_gti_goes = get_goes_gtis.fn(infile)
        outfile_gti_temp = os.path.join(filedir, root_name + "_noflares.gti")
        merge_gtis.fn([infile, outfile_gti_goes], outfile_gti_temp, gti_operation="AND")
        if not os.path.exists(outfile_gti_temp):
            logger.warning(f"Flare-free GTI file missing for {infile}, skipping")
            problems += 1
            continue

        logger.info(f"Calculating spectrum for {infile}")
        params = dict(
            indir=indir,
            infile=infile,
            instrument=f"FPM{fpm}",
            steminputs="nu" + obsid,
            stemout=stem,
            srcregionfile=this_src,
            bkgregionfile=this_bkg,
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
            grpphafile=os.path.join(outdir, stem + "_grp.pha"),
        )
        logger.debug("nuproducts " + " ".join(f"{k}={v}" for k, v in params.items()))
        hsp.nuproducts(params, noprompt=True, clobber=True, verbose=True)

    if problems > 0:
        logger.warning(
            f"{problems} file(s) could not be processed for {obsid}; "
            "not marking the observation as done"
        )
        return

    open(product_done_file, "w").close()


@flow
def process_nustar_obsid(obsid, config=None, ra="NONE", dec="NONE", flags=None):
    """
    Reduce one NuSTAR observation end to end.

    Runs the eight steps listed in the module docstring, from ``nupipeline`` to
    ``nuproducts``.

    Note that the ``ra``/``dec`` arguments are **overridden** by the position measured from
    the image by :func:`get_best_source_regions`. When the detection is correct this is
    better than the catalogue pointing; when the brightest object in the field is not the
    intended target, the data are barycentred to the wrong source.

    The SNR-optimised radius comes back in arcsec and is divided by 2.45, the NuSTAR sky
    pixel scale in arcsec per pixel, before being handed to the image-based separation.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict, optional
        Pipeline configuration. Defaults to ``DEFAULT_CONFIG``, which puts everything under
        the current working directory.
    ra, dec : float or str, optional
        Source position in degrees. Overridden as described above.
    flags : dict, optional
        Extra ``nupipeline`` parameters.
    """
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

    region_size = region_size / 2.45
    # TODO: ACCROCCHIO! Conversione da arcosecondo a pixel

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

    calculate_spectra(
        obsid,
        config,
        ra=ra,
        dec=dec,
        wait_for=[get_best_source_regions, filter_from_solar_flares],
    )
