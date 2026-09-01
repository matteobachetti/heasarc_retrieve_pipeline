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
from .barycenter import barycenter_file
from .image_utils import filter_sources_in_images
from .utils import (
    NO_SCIENCE_DATA,
    absolute_config,
    apply_gti,
    binned_lightcurve,
    get_logger,
    good_intervals,
    gti_to_array,
    intersect_intervals,
    intervals_above_threshold,
    intervals_removed,
    mask_from_gti,
    read_gti,
    rootname,
    splitext_improved,
)

from . import heasoft
from .heasoft import HAS_HEASOFT

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./", max_radius=80)

#: Energy band, in keV, in which :func:`get_best_source_region` images the field. These
#: live here rather than only in that function's signature because ``nustar_gen``'s
#: ``make_image`` names its output ``<stem>_<elow>to<ehigh>keV.fits``, which makes them
#: part of the longest file name the reduction builds -- see
#: :func:`nu_longest_output_name`. A test pins the two together.
IMAGE_ELOW, IMAGE_EHIGH = 3, 80

valid_re = re.compile(r"nu[0-9]{11}[AB]0[16].*")


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
        ``<out_data_path>/<OBSID>/event_pipe``. No trailing slash: measured in a
        ``nuproducts`` log, the tool given ``.../event_pipe/`` as ``indir`` went on to
        build ``.../event_pipe//nu<OBSID>A_fpm.hk``, wasting a character of a budget that
        is only 128 wide on some HEASOFT builds.
    """
    return os.path.join(config["out_data_path"], obsid, "event_pipe")


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
        ``<out_data_path>/<OBSID>/products``, where spectra, ARFs and RMFs go. No
        trailing slash, for the reason given in :func:`nu_pipeline_output_path`.
    """
    return os.path.join(config["out_data_path"], obsid, "products")


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
    return os.path.join(nu_pipeline_output_path(obsid, config), "PIPELINE_DONE.TXT")


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
        ``<out_data_path>/<OBSID>/split``. No trailing slash: ``nusplitsc`` adds one of
        its own, and the ``split//`` that resulted wasted a character of a budget that is
        only 128 wide on some HEASOFT builds.
    """
    return os.path.join(config["out_data_path"], obsid, "split")


def nu_longest_output_name(obsid, config):
    """
    The longest file name the reduction of one observation will build.

    Some HEASOFT builds truncate file names at 128 characters without saying so; see
    :func:`heasarc_retrieve_pipeline.utils.check_name_length`. The flow refuses to start
    when this name does not fit, which is cheap and happens before any download.

    The winner is the sky image ``nustar_gen``'s ``make_image`` writes when it measures an
    extraction region for a mode-06 (``SCIENCE_SC``) event file. Three things stack up in
    it: ``nusplitsc`` splits mode-06 data by star-tracker combination, and ``chu123`` is
    the longest of those; the split file keeps the ``_cl`` of the file it came from; and
    ``make_image`` appends the energy band. It is also the name most worth protecting,
    because ``make_image`` passes it to ``xselect`` as a ``save image`` argument -- the
    write side, which is the side that truncates.

    Found by walking two finished output trees rather than by reading the code, which is
    how the earlier answer -- one of ``nusplitsc``'s own temporaries, three characters
    shorter -- turned out to be wrong. ``tests/test_nustar.py`` now checks this against
    the full list of names those trees contained.

    One other name ties it exactly, which is worth knowing before shortening anything:
    :func:`merge_gtis` re-sorts its output in place with ``ftsort``, and CFITSIO's clobber
    prefix makes ``!<split>/nu<OBSID>A06_chu123_N_cl_noflares.gti`` 61 characters too.

    One name in a finished tree is longer still: the GOES solar-flare light curve
    ``sci_xrsf-l2-avg1m_g15_d<DATE>_v2-2-1.nc``, at 60 characters after the output root.
    It is downloaded by ``sunpy`` and never passed to a HEASOFT tool, so it is not what
    the limit applies to, and it is shorter than this one anyway.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        The longest name, which need not exist.

    Examples
    --------
    >>> name = nu_longest_output_name("80002092008", {"out_data_path": "/scratch/out"})
    >>> len(name) - len("/scratch/out")
    61
    >>> name.endswith("80002092008/split/nu80002092008A06_chu123_N_cl_3to80keV.fits")
    True
    """
    band = f"{IMAGE_ELOW}to{IMAGE_EHIGH}keV"
    return os.path.join(
        split_path(obsid, config), f"nu{obsid}A06_chu123_N_cl_{band}.fits"
    )


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
    return sorted(f for f in glob.glob(os.path.join(directory, pattern)) if not f.endswith(".gpg"))


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
    pipedir = nu_pipeline_output_path(obsid, config=config)
    splitdir = split_path(obsid, config=config)
    for fpm in "A", "B":
        for infile in _cl_event_files(pipedir, f"nu{obsid}{fpm}01_cl.evt*"):
            yield fpm, infile
        for infile in _cl_event_files(splitdir, f"nu{obsid}{fpm}06_chu*_cl.evt*"):
            yield fpm, infile


#: Observing modes that carry usable science. 01 is normal science, with the aspect
#: solution from CHU4 on the optics bench; 06 is "spacecraft science", recorded while CHU4
#: was blinded and reconstructed from CHU1-3 by :func:`recover_spacecraft_science_data`.
#: Everything else -- 02, 03, 04, 05 -- is slewing and settling, and holds no data this
#: pipeline can reduce.
SCIENCE_MODES = ("01", "06")

#: Matches a cleaned Level-2 event file and picks out its focal-plane module and mode.
CLEANED_EVENT_RE = re.compile(r"^nu(?P<obsid>\d+)(?P<fpm>[AB])(?P<mode>\d\d)_cl\.evt")


def observing_modes_present(obsid, config):
    """
    The observing modes Level 2 actually produced cleaned event files for.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    list of str
        Sorted two-character mode numbers, for example ``["01", "06"]``. Empty if the
        pipeline directory holds no cleaned event files at all.
    """
    pipedir = nu_pipeline_output_path(obsid, config=config)
    modes = set()
    for path in glob.glob(os.path.join(pipedir, f"nu{obsid}[AB]*_cl.evt*")):
        match = CLEANED_EVENT_RE.match(os.path.basename(path))
        if match is not None and match.group("obsid") == obsid:
            modes.add(match.group("mode"))
    return sorted(modes)


def has_science_data(obsid, config):
    """
    Whether an observation produced anything this pipeline can reduce.

    A NuSTAR observation that is really a slew -- the satellite moving between targets --
    carries only modes 02 and 03, sometimes 04. It is a real entry in ``numaster`` with a
    real OBSID and real downloaded files, and nothing in the FITS headers marks it as a
    slew, so the only way to tell is to look at which modes came out of Level 2.

    ``numaster`` does have an observation-mode column that says ``SLEW``, but it is set for
    only a handful of the observations that actually are slews. The SOC identifies the rest
    by their exposure being far shorter than the observation immediately following, which
    is a judgement, not a flag.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    bool
        True if mode 01 or mode 06 is present.
    """
    return bool(set(observing_modes_present(obsid, config)) & set(SCIENCE_MODES))


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
    pipedir = nu_pipeline_output_path(obsid, config=config)
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

    The file :func:`get_goes_gtis` writes there has a ``TIME`` column in the mission
    elapsed time of the event file -- not the GOES time scale -- so that the solar X-ray
    flux can be plotted directly against the event times. See
    :func:`plot_flare_filtering`.
    """
    root = rootname(event_file)
    return root + "_goes.fits"


def goes_download_path(event_file):
    """
    Where the raw GOES files of an observation are downloaded to.

    Sunpy downloads into one shared directory by default. Two observations taken on the
    same day want the same GOES file, and when they are reduced at the same time one can
    be handed a file the other is still writing -- the shared directory offers no way to
    tell a finished download from a running one. Each observation therefore keeps its own
    copy, next to the event file it was fetched for. They are a couple of megabytes each,
    and having them beside the data is worth more than the duplicate download costs.

    Parameters
    ----------
    event_file : str
        Event file path.

    Returns
    -------
    str
        A sunpy path template: the event file's directory, plus ``{file}``.
    """
    return os.path.join(os.path.dirname(os.path.abspath(event_file)), "{file}")


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
    task_run_name="nu_separate_sources_{event_file}_region_{region_size}",
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
    task_run_name="nu_separate_sources_in_{directories[0]}_region_{region_size}",
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
            separate_sources_in_event_file(
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
    pipe_done_file = nu_pipeline_done_file(obsid, config=config)
    if os.path.exists(pipe_done_file):
        logger = get_run_logger()
        logger.info(f"Data for {obsid} already preprocessed")
        return
    logger = get_run_logger()
    logger.info("Running NuSTAR L2 pipeline")
    datadir = nu_local_raw_data_path(obsid, config=config)
    ev_dir = nu_pipeline_output_path(obsid, config=config)
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

    # No return-code check here: heasoft.run_task raises on a non-zero code, with the
    # tool's own output in the message.
    heasoft.run_task("nupipeline", **params)

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

    Not every observation has mode-06 data: CHU4 only loses its solution when the Sun or
    the Moon blinds it, and an observation that never happened to point that way has
    nothing to recover. That is not a failure, and neither is a slew, which has no science
    data of any mode. Both leave ``nusplitsc`` with nothing to do, and the step still
    creates the split directory and writes its sentinel, so the observation carries on.

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
    logger = get_logger()
    logger.info(f"Squeezing every photon from spacecraft science data in {obsid}")
    datadir = nu_local_raw_data_path(obsid, config)
    ev_dir = nu_pipeline_output_path(obsid, config)
    splitdir = split_path(obsid, config=config)
    recover_done_file = os.path.join(splitdir, "RECOVER_DONE.TXT")
    hk_dir = os.path.join(datadir, "hk")

    evfiles_06 = glob.glob(os.path.join(ev_dir, "*[AB]06_cl.evt*"))

    if os.path.exists(recover_done_file):
        logger.info("Processing done")
        return splitdir

    if not evfiles_06:
        logger.info(f"No spacecraft science (mode 06) data in {obsid}; nothing to split")

    # nusplitsc makes this directory itself, but only if it has something to split. The
    # sentinel below has to land somewhere either way.
    os.makedirs(splitdir, exist_ok=True)

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

        heasoft.run(
            "nusplitsc",
            infile=evfile,
            chu123hkfile=chu123hkfile,
            hkfile=hkfile,
            outdir=splitdir,
            clobber="yes",
        )
    open(recover_done_file, "a").close()
    return splitdir


@task(task_run_name="nu_merge_gtis_into_{outfile_gti}_gti_{gti_operation}")
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
    logger = get_logger()

    logger.info(f"Creating GTI file {outfile_gti} from {files_to_join}")

    heasoft.run(
        "ftmgtime",
        ingtis=",".join([f + "[GTI]" for f in files_to_join]),
        outgti=outfile_gti,
        merge=gti_operation,
        chatter=5,
    )

    # "[1]" is not decoration: ftmgtime writes an empty primary header, and ftsort with a
    # bare file name lands on it and dies with CFITSIO ERROR NOT_TABLE (return code 235).
    heasoft.run(
        "ftsort", infile=outfile_gti + "[1]", outfile="!" + outfile_gti, columns="START"
    )

    logger.info(f"Changing extension name to GTI in {outfile_gti}")

    heasoft.run(
        "fthedit", infile=outfile_gti + "+1", keyword="EXTNAME", operation="a", value="GTI"
    )


@task(task_run_name="nu_merge_event_files_into_{outfile}_gti_{gti_operation}")
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
    The merged GTIs go to an intermediate file named after ``outfile``, so the name is the
    same on every run of the same merge -- it used to carry ``np.random.randint(1000000)``,
    which made the task's inputs different every time and left a stray file behind whenever
    a HEASOFT call raised. One output file means one intermediate, so the deterministic name
    cannot collide, and it is removed in a ``finally``.
    """
    outdir, fname = os.path.split(outfile)
    root = splitext_improved(fname)[0]
    logger = get_logger()

    outfile_gti = os.path.join(outdir, f"{root}_tmp.gti")

    try:
        merge_gtis(files_to_join, outfile_gti, gti_operation=gti_operation)

        logger.info(f"Creating event file {outfile} from {files_to_join}")

        heasoft.run(
            "ftmerge", infile=",".join(files_to_join), outfile=outfile, copyall="NO"
        )

        logger.info(f"Sorting event file {outfile}")

        heasoft.run("ftsort", infile=outfile, outfile="!" + outfile, columns="TIME")

        logger.info(
            f"Adding GTIs from {outfile_gti}'s first extension to event file {outfile}"
        )

        heasoft.run("fappend", infile=f"{outfile_gti}[1]", outfile=outfile)
    finally:
        if os.path.exists(outfile_gti):
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
    outdir = nu_base_output_path(obsid, config=config)

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


#: Lower flux bound of each GOES flare class, in W m^-2 of 1--8 A solar X-ray flux. The
#: scale is logarithmic and each class is ten times the previous one.
GOES_CLASS_FLUX = {"A": 1e-8, "B": 1e-7, "C": 1e-6, "M": 1e-5, "X": 1e-4}


def goes_class_to_flux(goes_class):
    """
    Convert a GOES flare class such as ``"C5.0"`` to a 1--8 A flux in W m^-2.

    Parameters
    ----------
    goes_class : str
        A letter from A, B, C, M, X followed by a multiplier.

    Returns
    -------
    float
        The flux in W m^-2.

    Examples
    --------
    >>> f"{goes_class_to_flux('C5.0'):.1e}"
    '5.0e-06'
    >>> f"{goes_class_to_flux('M1.0'):.1e}"
    '1.0e-05'
    """
    return GOES_CLASS_FLUX[goes_class[0].upper()] * float(goes_class[1:])


@task(task_run_name="goes_lightcurve_{event_file}_mincat_{minimum_class}")
def get_goes_gtis(event_file, minimum_class="C5.0", flux_class="C5.0"):
    """
    Build good time intervals that exclude solar flares.

    NuSTAR observes from low Earth orbit with an open detector aperture, and large solar
    flares raise its background substantially. This task looks up the flares that occurred
    during an observation and produces the complementary GTIs.

    The steps are: convert the observation's ``TSTART``/``TSTOP`` from NuSTAR
    mission-elapsed time to civil time; ask ``sunpy``'s ``Fido`` for the GOES XRS data of
    that interval, picking the highest-numbered (most recent) satellite that covers it;
    retrieve the HEK flare catalogue entries flagged by SWPC; and cut out every catalogued
    flare at or above ``minimum_class``, together with every minute in which the measured
    GOES 1--8 A flux reached ``flux_class``. The surviving intervals are the complement of
    all of those inside ``[TSTART, TSTOP]``, computed by
    :func:`~heasarc_retrieve_pipeline.utils.good_intervals`.

    **Why both.** The catalogue and the flux disagree in ways that matter, and each catches
    what the other misses. The catalogue's end time is when the *solar* flare ended, not
    when NuSTAR's background recovered from it: on 80002092008 the 3--10 keV background is
    still three times its baseline for several hundred seconds past the catalogued end. And
    a rise that was never catalogued at the requested class is invisible to the catalogue
    entirely; that same observation ends with the GOES flux back above C5.0 and the NuSTAR
    background climbing with it, with nothing excluded. Conversely the flux is sampled once
    a minute and has gaps, so a short flare between samples, or during a gap, is something
    only the catalogue knows about. The union of the two is what gets excluded.

    Flare classes are compared by letter and number separately. The GOES scale runs
    A, B, C, M, X, which is alphabetical, so comparing the letters as characters gives the
    correct ordering.

    The time range searched is the wider of the header's ``TSTART``/``TSTOP`` and the
    event file's own GTI extent. On a merged file those disagree, and the GTI is the
    honest one.

    Returns the existing file unchanged if it is already present.

    Parameters
    ----------
    event_file : str
        Event file whose time range defines the search interval.
    minimum_class : str, optional
        Smallest **catalogued** flare class to exclude, e.g. ``"C5.0"``.
    flux_class : str or None, optional
        Exclude every sample whose measured GOES 1--8 A flux reaches this class. ``None``
        turns the flux criterion off and leaves the filtering catalogue-driven.

        Choose this above the Sun's quiescent 1--8 A flux, which moves with the solar
        cycle and was around 1.5e-6 W m^-2 (mid C1) in February 2014. A threshold below
        quiescent excludes the entire observation except where the flux is missing: on
        80002092008, ``"C1.0"`` throws away 54013 s of 58889 s. This is why it is a
        parameter of its own rather than sharing ``minimum_class``: lowering the class at
        which catalogued flares are excluded is a reasonable thing to want, and it must
        not silently destroy the observation.

    The GOES X-ray light curve is also written to :func:`goes_lc_file_name`, on the event
    file's own time scale, so that :func:`plot_flare_filtering` can show what the Sun was
    doing without downloading anything a second time.

    Returns
    -------
    str
        Path of the GTI file, ``<root>_goes.gti``.
    """
    from sunpy import timeseries as ts
    from sunpy.net import Fido
    from sunpy.net import attrs as a
    from sunpy.time import parse_time
    from astropy.io import fits
    from astropy.io.fits import getheader
    from astropy.table import Table
    from nustar_gen import info, utils

    outfile_gti = goes_gti_file_name(event_file)

    if os.path.exists(outfile_gti):
        logger = get_run_logger()
        logger.info(f"GOES GTI file {outfile_gti} already exists, skipping")
        return outfile_gti

    min_cat = minimum_class[0]
    min_num = float(minimum_class[1:])

    logger = get_run_logger()
    logger.info(f"Creating GOES light curve and GTIs for {event_file}")

    ns = info.NuSTAR()
    hdr = getheader(event_file, ext=1)
    with fits.open(event_file) as hdul:
        file_gti = read_gti(hdul)

    # TSTART/TSTOP are not trustworthy on a merged file: ftmerge copies them from its
    # first input, so they can be narrower than the merged GTI (issue 35 in
    # known_issues.rst). Since this GTI is later ANDed with the event file's own, a
    # narrow range would silently delete good time at the edges of the observation --
    # 791 s of the 80002092008 background product. Take whichever bound is wider.
    tstart = min(hdr["TSTART"], file_gti[:, 0].min()) if len(file_gti) else hdr["TSTART"]
    tstop = max(hdr["TSTOP"], file_gti[:, 1].max()) if len(file_gti) else hdr["TSTOP"]
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
    files = Fido.fetch(result3, progress=False, path=goes_download_path(event_file))
    goes_all = ts.TimeSeries(files, concatenate=True)
    goes = goes_all.truncate(datestart.iso, dateend.iso)

    hek_results = result3["hek"]
    flares_hek = hek_results

    outfile_lc = goes_lc_file_name(event_file)
    goes_table = goes.to_table()
    # sunpy names the time column after the source file's own index -- "date" for XRS --
    # and gives it as datetime64, so take the times from the TimeSeries itself instead.
    lightcurve = {"TIME": (goes.time.mjd - mjdref) * 86400}
    for channel in "xrsa", "xrsb":
        if channel in goes_table.colnames:
            # The flux columns are masked where the satellite reported bad data. NaN is
            # what matplotlib skips, and what FITS can hold.
            lightcurve[channel.upper()] = np.ma.filled(
                np.ma.asarray(goes_table[channel], dtype=float), np.nan
            )
    logger.info(
        f"Writing the GOES X-ray light curve ({len(lightcurve['TIME'])} points) " f"to {outfile_lc}"
    )
    Table(lightcurve).write(outfile_lc, overwrite=True)

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

        logger.info(f"Excluding {flare_class} flare, MET {flare_start:.1f} -> {flare_end:.1f}")
        flares.append((flare_start, flare_end))

    bright = np.zeros((0, 2))
    if flux_class is not None and "XRSB" in lightcurve:
        threshold = goes_class_to_flux(flux_class)
        bright = intervals_above_threshold(lightcurve["TIME"], lightcurve["XRSB"], threshold)
        excluded = float(np.sum(bright[:, 1] - bright[:, 0]))
        logger.info(
            f"GOES 1-8 A flux reaches {flux_class} ({threshold:.1e} W/m2) over "
            f"{len(bright)} intervals totalling {excluded:.1f} s"
        )

    # good_intervals does the clipping, sorting, merging and empty-interval dropping, so
    # a flare overlapping TSTART, two overlapping flares, or a catalogue window that runs
    # into a bright-flux window cannot produce a broken GTI.
    bad = np.vstack([np.array(flares).reshape(-1, 2), bright])
    good = good_intervals(bad, tstart, tstop)
    if len(good) == 0:
        raise RuntimeError(
            f"Solar flares cover the whole of {event_file} (MET {tstart} -- {tstop}); no "
            f"good time is left. If flux_class={flux_class} is below the Sun's quiescent "
            f"1-8 A flux, raise it: that alone will exclude everything."
        )
    logger.info(
        f"{len(flares)} catalogued flares and {len(bright)} bright-flux intervals "
        f"excluded, leaving {len(good)} good intervals"
    )

    # A warning rather than an error: a genuinely flare-dominated observation really can
    # lose most of its good time, and only the person analysing it can tell that case from
    # a threshold set below the Sun's quiescent flux for the epoch.
    surviving = intersect_intervals(file_gti, good)
    before = float(np.sum(file_gti[:, 1] - file_gti[:, 0]))
    after = float(np.sum(surviving[:, 1] - surviving[:, 0])) if len(surviving) else 0.0
    if before > 0 and after < 0.5 * before:
        logger.warning(
            f"Flare filtering would remove {100 * (1 - after / before):.0f}% of the good "
            f"time in {event_file} ({before:.0f} -> {after:.0f} s). If that is not a "
            f"genuinely flare-dominated observation, flux_class={flux_class} is probably "
            f"below the Sun's quiescent 1-8 A flux for this epoch. Check the diagnostic "
            f"figure written next to the filtered file."
        )

    gtis = [{"START": start, "STOP": stop} for start, stop in good]

    utils.make_usr_gti(gtis, overwrite=True, outfile=outfile_gti)
    logger.info(f"Changing extension name to GTI in {outfile_gti}")

    heasoft.run(
        "fthedit", infile=outfile_gti + "+1", keyword="EXTNAME", operation="a", value="GTI"
    )

    if not os.path.exists(outfile_gti):
        raise RuntimeError(f"Failed to create GTI file {outfile_gti}")

    return outfile_gti


def chi2_dof_against_a_constant(lightcurve):
    """
    Reduced chi-squared of a light curve against the hypothesis that it is constant.

    A single number for "how variable is this?", used to say whether filtering made a
    light curve steadier. A perfectly Poissonian constant source gives 1; the flare-time
    excess in a NuSTAR background region gives several.

    The mean is the count-weighted one -- total counts over total exposure -- rather than
    the mean of the per-bin rates, so that bins with less exposure count for less.

    Parameters
    ----------
    lightcurve : dict
        As returned by :func:`~heasarc_retrieve_pipeline.utils.binned_lightcurve`.

    Returns
    -------
    float
        ``nan`` when there are fewer than two bins to compare.
    """
    counts, exposure = lightcurve["counts"], lightcurve["exposure"]
    if counts.size < 2:
        return np.nan

    mean_rate = counts.sum() / exposure.sum()
    # An empty bin has zero Poisson error, which would divide by zero. One count is the
    # smallest error that bin could have had.
    error = np.sqrt(np.maximum(counts, 1.0)) / exposure
    return float(np.sum(((lightcurve["rate"] - mean_rate) / error) ** 2) / (counts.size - 1))


@task(task_run_name="nu_flare_diagnostic_{event_file}")
def plot_flare_filtering(
    event_file,
    gti_before,
    gti_after,
    outfile=None,
    dt=100.0,
    minimum_class="C5.0",
    flux_class="C5.0",
):
    """
    Show what the solar-flare filtering removed, and what it left alone.

    Cleaning an event file is easy to get wrong in ways that leave no trace in the output:
    too little is removed, or too much, and either way the file looks fine. This draws the
    evidence instead, as three panels on one shared time axis:

    1. the GOES X-ray flux, with the flare-class thresholds marked, so the cut is visible
       where it acts;
    2. the event file's 3--10 keV light curve, the band in which solar stray light lands;
    3. the same in 10--79 keV, as a control. Solar flares do not produce hard X-rays at
       NuSTAR's aperture, so this panel should look the same before and after. If it
       does not, the cut is removing more than solar flares.

    In panels 2 and 3 the light curve before filtering is drawn in grey and the one after
    in colour, and the removed intervals are shaded, so what went away is the difference
    between the two.

    The figure is built through ``matplotlib.figure.Figure`` rather than ``pyplot``. That
    is headless by construction -- no backend to force, no window to open on a pipeline
    machine -- and it cannot leak a figure into pyplot's global registry, which is the
    defect issue 31 records elsewhere in this package.

    Parameters
    ----------
    event_file : str
        The **unfiltered** event file. Read, never written.
    gti_before, gti_after : array-like or table
        Good time intervals before and after the flare filtering.
    outfile : str, optional
        Where to write the figure. Defaults to ``<root>_flares.jpg``, next to the event
        file, following the convention of :mod:`heasarc_retrieve_pipeline.image_utils`.
    dt : float, optional
        Light-curve bin width in seconds.
    minimum_class : str, optional
        The catalogued-flare class cut used, named in the GOES panel's legend.
    flux_class : str or None, optional
        The flux cut used. This one acts directly on the curve in the top panel, so it is
        drawn there as a horizontal line.

    Returns
    -------
    str
        Path of the figure.
    """
    from astropy.io import fits
    from astropy.table import Table
    from matplotlib.figure import Figure

    logger = get_logger()

    gti_before = gti_to_array(gti_before)
    gti_after = gti_to_array(gti_after)
    if outfile is None:
        outfile = rootname(event_file) + "_flares.jpg"

    with fits.open(event_file) as hdul:
        events = hdul["EVENTS"]
        times = np.asarray(events.data["TIME"], dtype=float) + float(
            events.header.get("TIMEZERO", 0.0)
        )
        # NuSTAR's pulse-invariant channels are linear in energy: E = 0.04 * PI + 1.6 keV.
        energy = 0.04 * np.asarray(events.data["PI"], dtype=float) + 1.6
        livetime_before = float(events.header.get("LIVETIME", np.nan))

    ontime_before = float(np.sum(gti_before[:, 1] - gti_before[:, 0]))
    ontime_after = float(np.sum(gti_after[:, 1] - gti_after[:, 0]))
    livetime_after = livetime_before * ontime_after / ontime_before if ontime_before else 0.0

    kept = mask_from_gti(times, gti_after)
    removed = intervals_removed(gti_before, gti_after)

    fig = Figure(figsize=(11, 9))
    axes = fig.subplots(3, 1, sharex=True)

    goes_file = goes_lc_file_name(event_file)
    if os.path.exists(goes_file):
        goes = Table.read(goes_file)
        for column, label, colour in (
            ("XRSB", "GOES 1--8 $\\AA$", "tab:red"),
            ("XRSA", "GOES 0.5--4 $\\AA$", "tab:blue"),
        ):
            if column in goes.colnames:
                axes[0].plot(goes["TIME"], goes[column], color=colour, lw=1, label=label)
        axes[0].set_yscale("log")
        axes[0].set_ylim(1e-9, 1e-3)
        for letter, flux in GOES_CLASS_FLUX.items():
            axes[0].axhline(flux, color="k", ls=":", lw=0.5)
            axes[0].text(
                0.004,
                flux * 1.3,
                letter,
                transform=axes[0].get_yaxis_transform(),
                fontsize="small",
                color="0.4",
            )
        if flux_class is not None:
            axes[0].axhline(
                goes_class_to_flux(flux_class),
                color="tab:orange",
                lw=1.2,
                label=f"flux cut {flux_class}",
            )
        axes[0].plot([], [], " ", label=f"HEK catalogue $\\geq$ {minimum_class}")
        axes[0].legend(loc="upper right", fontsize="small", ncol=4)
    else:
        logger.warning(f"No GOES light curve at {goes_file}; leaving that panel empty")
        axes[0].text(
            0.5,
            0.5,
            f"no GOES light curve at {os.path.basename(goes_file)}",
            ha="center",
            transform=axes[0].transAxes,
            color="0.4",
        )
    axes[0].set_ylabel("Solar X-ray flux (W m$^{-2}$)")

    bands = [
        (3.0, 10.0, "3--10 keV: where solar stray light lands"),
        (10.0, 79.0, "10--79 keV: control, flares should not contribute here"),
    ]
    chi2 = {}
    for axis, (emin, emax, title) in zip(axes[1:], bands):
        in_band = (energy >= emin) & (energy < emax)
        before = binned_lightcurve(times[in_band], gti_before, dt)
        after = binned_lightcurve(times[in_band & kept], gti_after, dt)
        chi2[emin] = (
            chi2_dof_against_a_constant(before),
            chi2_dof_against_a_constant(after),
        )

        axis.errorbar(
            before["time"],
            before["rate"],
            before["rate_err"],
            fmt=".",
            color="0.65",
            ms=4,
            lw=0.8,
            label="before filtering",
            zorder=2,
        )
        axis.errorbar(
            after["time"],
            after["rate"],
            after["rate_err"],
            fmt=".",
            color="tab:blue",
            ms=4,
            lw=0.8,
            label="after filtering",
            zorder=3,
        )
        axis.set_ylabel(f"{emin:.0f}--{emax:.0f} keV rate (s$^{{-1}}$)")
        axis.set_title(
            f"{title}   ($\\chi^2$/dof {chi2[emin][0]:.2f} $\\rightarrow$ " f"{chi2[emin][1]:.2f})",
            fontsize="small",
            loc="left",
        )
        axis.legend(loc="upper right", fontsize="small", ncol=2)

    for axis in axes:
        for start, stop in removed:
            axis.axvspan(start, stop, color="tab:orange", alpha=0.18, lw=0, zorder=1)

    axes[-1].set_xlabel(f"NuSTAR mission elapsed time (s), {dt:.0f} s bins")
    fig.suptitle(
        f"{os.path.basename(event_file)}: solar-flare filtering\n"
        f"{times.size - int(kept.sum())} of {times.size} events removed"
        f"   |   live time {livetime_before:.0f} $\\rightarrow$ {livetime_after:.0f} s"
        f"   |   {len(removed)} interval(s) excluded",
        fontsize="medium",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(outfile, dpi=110)

    logger.info(f"Wrote the flare-filtering diagnostic to {outfile}")
    return outfile


@flow(flow_run_name="nu_filter_solar_flares_{event_file}_mincat_{minimum_class}")
def filter_from_solar_flares(event_file, minimum_class="C5.0", flux_class="C5.0"):
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
        Smallest catalogued flare class to exclude.
    flux_class : str or None, optional
        Also exclude every minute in which the measured GOES 1--8 A flux reaches this
        class. See :func:`get_goes_gtis` for why this is separate from ``minimum_class``.

    A diagnostic figure, ``<root>_flares.jpg``, is written alongside by
    :func:`plot_flare_filtering`. Failing to draw it is logged, not raised: the science
    product is already on disk by then.

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

    outfile_gti_goes = get_goes_gtis(event_file, minimum_class=minimum_class, flux_class=flux_class)

    merge_gtis([event_file, outfile_gti_goes], outfile_gti_temp, gti_operation="AND")

    with fits.open(event_file) as hdul, fits.open(outfile_gti_temp) as gti_hdul:
        gti_before = read_gti(hdul)
        gti_after = gti_to_array(gti_hdul[1].data)
        stats = apply_gti(hdul, gti_after)
        hdul.writeto(outfile_filtered, overwrite=True)

    logger.info(
        f"{outfile_filtered}: "
        f"{stats['nevents_before'] - stats['nevents_after']} of "
        f"{stats['nevents_before']} events removed, live time "
        f"{stats['livetime_before']:.1f} -> {stats['livetime_after']:.1f} s"
    )

    os.unlink(outfile_gti_temp)

    # The science product is already written. A diagnostic figure failing -- a missing
    # GOES file, a matplotlib problem on a headless machine -- must not take the
    # observation down with it, so it is logged rather than raised.
    try:
        plot_flare_filtering(
            event_file,
            gti_before,
            gti_after,
            minimum_class=minimum_class,
            flux_class=flux_class,
        )
    except Exception as error:
        logger.warning(f"Could not plot the flare filtering for {event_file}: {error}")

    return outfile_filtered


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
        Source number. Recorded in the flow run name only -- every event file in the
        directory is barycentred regardless.

    Notes
    -----
    FPMA's attitude/orbit file is used for every file, including the FPMB and combined
    ones. That is harmless: the two attitude/orbit files are identical in every column
    ``barycorr`` reads. See issue 13 in ``docs/known_issues.rst``.
    """
    logger = get_run_logger()
    outdir = nu_base_output_path(obsid, config=config)
    logger.info(f"Barycentering data in directory {outdir}")
    pipe_outdir = nu_pipeline_output_path(obsid, config=config)

    infiles = glob.glob(os.path.join(outdir, f"nu{obsid}*.evt*"))
    for infile in infiles:
        if "bary" in infile:
            continue

        barycenter_file(
            infile,
            os.path.join(pipe_outdir, f"nu{obsid}A.attorb"),
            ra=ra,
            dec=dec,
        )


@task(
    task_run_name="nu_best_source_reg_{infile}_pair_{pair}_elow_{elow}_ehigh_{ehigh}",
)
def get_best_source_region(
    infile,
    pair=None,
    elow=IMAGE_ELOW,
    ehigh=IMAGE_EHIGH,
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
        out_rootname = rootname(fname)

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
    full_range = make_image(infile, elow=IMAGE_ELOW, ehigh=IMAGE_EHIGH, clobber=True)
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
    outdir = nu_pipeline_output_path(obsid, config=config)
    os.makedirs(outdir, exist_ok=True)

    mean_ra = mean_dec = mean_rlimit = 0.0
    count = 0
    for _, infile in mode_01_input_files(obsid, config):
        # get_best_source_region returns early when the region files already exist,
        # reading the position and radius back out of them, so every file counts.
        result = get_best_source_region(infile, config=config)
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
    indir = nu_pipeline_output_path(obsid, config=config)
    outdir = nu_product_output_path(obsid, config=config)
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
        root_name = rootname(os.path.basename(infile))
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
            get_best_source_region(
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

        outfile_gti_goes = get_goes_gtis(infile)
        outfile_gti_temp = os.path.join(filedir, root_name + "_noflares.gti")
        merge_gtis([infile, outfile_gti_goes], outfile_gti_temp, gti_operation="AND")
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
        heasoft.run("nuproducts", params, noprompt=True, clobber=True, verbose=True)

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

    Returns
    -------
    str or None
        :data:`heasarc_retrieve_pipeline.utils.NO_SCIENCE_DATA` if Level 2 produced no
        science-mode data -- see :func:`has_science_data` -- and ``None`` otherwise.
    """
    # Pinned here, once: every path below hangs off these two entries, and a relative
    # path would mean "wherever this process is standing" at each separate use.
    config = absolute_config(config, DEFAULT_CONFIG)
    logger = get_run_logger()
    logger.info(f"Processing NuSTAR observation {obsid}")
    os.makedirs(os.path.join(nu_base_output_path(obsid, config=config)), exist_ok=True)
    basedir = nu_base_output_path(obsid, config=config)
    # splitdir = split_path(obsid, config=config)
    pipedir = nu_pipeline_output_path(obsid, config=config)

    # A future declares a dependency; resolving it is what makes the dependency bite.
    # Measured on Prefect 3.8.4: a task whose upstream future failed is skipped, returns
    # None, and leaves the flow run COMPLETED -- so the pipeline would carry None onwards
    # and report success. Calling .result() re-raises, and the flow run ends FAILED.
    pipeline = nu_run_l2_pipeline.submit(obsid, config=config, flags=flags)
    pipeline.result()

    # A slew is indistinguishable from a science observation until Level 2 has run: it has
    # an OBSID, a numaster row and downloaded files, and only the observing modes that come
    # out the far end give it away. Stopping here is not a failure, and must not be counted
    # as one. The data stay on disk -- the slew exposure next to a long observation may yet
    # be worth joining to it.
    if not has_science_data(obsid, config):
        modes = observing_modes_present(obsid, config)
        logger.warning(
            f"{obsid} has no science data: Level 2 produced cleaned events for observing "
            f"mode(s) {', '.join(modes) if modes else 'none at all'}, and none of "
            f"{', '.join(SCIENCE_MODES)}. This is what a slew looks like. Nothing to reduce."
        )
        return NO_SCIENCE_DATA

    splitdir = recover_spacecraft_science_data(obsid, config, wait_for=[pipeline])

    ra, dec, region_size = get_best_source_regions(obsid, config, wait_for=[pipeline])

    region_size = region_size / 2.45
    # TODO: ACCROCCHIO! Conversione da arcosecondo a pixel

    # separate_sources needs splitdir, which is recover_spacecraft_science_data's return
    # value: that argument is the dependency, and a stronger statement than wait_for.
    separated = separate_sources.submit(
        [pipedir, splitdir],
        config,
        region_size=region_size,
        back_region_size=region_size + 25,
    )
    separated.result()

    source_future = join_source_data.submit(
        obsid, [pipedir, splitdir], config, wait_for=[separated]
    )
    background_future = join_source_data.submit(
        obsid, [pipedir, splitdir], config, src_num=0, wait_for=[separated]
    )
    source_files = source_future.result()
    background_files = background_future.result()

    # Source and background go through the same flare filter, so that they share one GTI.
    # Subtracting an unfiltered background from a filtered source over-subtracts: flare
    # stray light is diffuse, so it lands mostly in the large background region. On
    # 80002092008 the unfiltered background is 3.7% too high in 3--10 keV.
    for fname in source_files + background_files:
        filter_from_solar_flares(fname)

    # barycenter_data globs the output directory rather than taking the file list, so the
    # join is a real dependency that no argument expresses.
    barycenter_data(
        obsid, ra=ra, dec=dec, config=config, wait_for=[source_future, background_future]
    )

    # ra and dec come from get_best_source_regions, and filter_from_solar_flares is a
    # subflow, which runs synchronously and raises: both dependencies already hold.
    calculate_spectra(obsid, config, ra=ra, dec=dec)
