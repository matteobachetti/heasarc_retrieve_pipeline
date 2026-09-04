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
from .coadd import apply_case_b_scaling, run_addspec
from .diagnostics import diagnostics_path, no_record, record_step
from .image_utils import filter_sources_in_images
from .utils import (
    NO_SCIENCE_DATA,
    NoGoesCoverage,
    NoSourceInScienceData,
    absolute_config,
    apply_gti,
    binned_lightcurve,
    drop_events_outside_gti,
    get_logger,
    good_intervals,
    gti_to_array,
    intersect_intervals,
    intervals_above_threshold,
    intervals_removed,
    mask_from_gti,
    merge_intervals,
    read_gti,
    record_skipped_input,
    rootname,
    splitext_improved,
    time_reference,
    tool_log_file,
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


def nu_goes_lc_file(obsid, config):
    """
    Path of an observation's GOES X-ray light curve.

    One per observation, not one per event file: the Sun does not care which module or
    which CHU subset the data came from, and fetching it once is the difference between
    52 downloads and 91.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/nu<OBSID>_goes.fits``.

    Notes
    -----
    The ``TIME`` column is in the mission elapsed time of the observation -- not the GOES
    time scale -- so the solar X-ray flux can be plotted straight against the event times.
    See :func:`record_flare_filtering`.
    """
    return os.path.join(nu_base_output_path(obsid, config=config), f"nu{obsid}_goes.fits")


def nu_goes_gti_file(obsid, config):
    """
    Path of an observation's flare-free GTI file.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        ``<out_data_path>/<OBSID>/nu<OBSID>_goes.gti``.
    """
    return os.path.join(nu_base_output_path(obsid, config=config), f"nu{obsid}_goes.gti")


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
    return os.path.join(split_path(obsid, config), f"nu{obsid}A06_chu123_N_cl_{band}.fits")


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


def spectral_input_key(obsid, fpm, path):
    """
    What an event file *is*, with the focal-plane module taken out.

    ``nu80002092006A06_chu12_N_cl.evt`` and ``nu80002092006B06_chu12_N_cl.evt`` are the same
    stretch of sky and time seen by the two modules, so they share a key -- here
    ``06_chu12_N``. A mode-01 file gives ``01``. This is what
    :func:`paired_spectral_inputs` matches FPMA against FPMB on.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    fpm : {"A", "B"}
        Focal-plane module the file belongs to.
    path : str
        Path of the cleaned event file.

    Returns
    -------
    str

    Examples
    --------
    >>> spectral_input_key("80002092006", "A", "nu80002092006A01_cl.evt")
    '01'
    >>> spectral_input_key("80002092006", "B", "x/nu80002092006B06_chu12_N_cl.evt.gz")
    '06_chu12_N'
    """
    root_name = rootname(os.path.basename(path))
    stem = root_name[: -len("_cl")] if root_name.endswith("_cl") else root_name
    prefix = f"nu{obsid}{fpm}"
    return stem[len(prefix) :] if stem.startswith(prefix) else stem


def paired_spectral_inputs(obsid, config):
    """
    The observation's event files, matched between the two focal-plane modules.

    FPMA and FPMB see the same source at the same time, so almost every file has a
    counterpart. A pair is what :func:`calculate_spectra` extracts over one shared good-time
    interval and what :mod:`heasarc_retrieve_pipeline.combine` may afterwards co-add; a file
    without a counterpart is still extracted, on its own, but can be no part of a combined
    product.

    Measured on ``90901333002``, the two modules' good times differ by at most three seconds
    out of three thousand, so intersecting them costs nothing. See ``technical_details.rst``.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    pairs : dict
        Key, as :func:`spectral_input_key` builds it, to ``{"A": path, "B": path}``. Mode 01
        comes first, as it does from :func:`spectral_input_files`.
    unpaired : list of tuple
        ``(fpm, path)`` for every file whose counterpart is not there.
    """
    found = {}
    for fpm, infile in spectral_input_files(obsid, config):
        found.setdefault(spectral_input_key(obsid, fpm, infile), {})[fpm] = infile
    pairs = {key: modules for key, modules in found.items() if len(modules) == 2}
    unpaired = [
        (fpm, path)
        for modules in found.values()
        if len(modules) < 2
        for fpm, path in modules.items()
    ]
    return pairs, unpaired


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


def goes_download_path(obsid, config):
    """
    Where the raw GOES files of an observation are downloaded to.

    Sunpy downloads into one shared directory by default. Two observations taken on the
    same day want the same GOES file, and when they are reduced at the same time one can
    be handed a file the other is still writing -- the shared directory offers no way to
    tell a finished download from a running one. Each observation therefore keeps its own
    copy, in its own output directory. They are a couple of megabytes each, and having
    them beside the data is worth more than the duplicate download costs.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        A sunpy path template: the observation's output directory, plus ``{file}``.
    """
    return os.path.join(nu_base_output_path(obsid, config=config), "{file}")


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
def separate_sources_in_event_file(
    event_file, region_size=30, back_region_size=55, diagnostics_dir=None, obsid=""
):
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
    diagnostics_dir : str, optional
        Where to record what the separation found, normally
        :func:`heasarc_retrieve_pipeline.diagnostics.diagnostics_path`. ``None`` records
        nothing.
    obsid : str, optional
        Observation the file belongs to. Only used in the record.

    Returns
    -------
    bool or None
        ``True`` if files were written, ``None`` if the input was skipped or contained too
        few events.
    """
    logger = get_logger()
    # Neither of these is a separation that went wrong, so neither leaves a record: an
    # encrypted file is one nobody can process, and a name that does not match is not an
    # event file at all.
    if event_file.endswith(".gpg"):
        return None
    if not valid_re.search(event_file):
        return None
    logger.info(f"Processing {event_file}")
    with record_step(
        diagnostics_dir,
        obsid,
        "separate_sources",
        key=rootname(os.path.basename(event_file)),
    ) as rec:
        return filter_sources_in_images(
            event_file,
            region_size=region_size,
            back_region_size=back_region_size,
            rec=rec,
        )


def separation_candidates(directory):
    """
    The cleaned event files in a directory that the separation would actually work on.

    Both loops in :func:`separate_sources` walk this, so the one that records a skip
    cannot disagree with the one that does the work about what counts as a candidate.
    The two rejected kinds are the same ones :func:`separate_sources_in_event_file`
    turns away: an encrypted file nobody can process, and a name that is not an event
    file at all.

    Parameters
    ----------
    directory : str
        Directory to scan.

    Returns
    -------
    list of str
        Full paths, sorted, so a run is reproducible.

    Examples
    --------
    >>> import os, tempfile
    >>> d = tempfile.mkdtemp()
    >>> for name in ("nu90101201002A01_cl.evt", "nu90101201002B01_cl.evt.gpg", "junk.evt"):
    ...     _ = open(os.path.join(d, name), "w").close()
    >>> [os.path.basename(f) for f in separation_candidates(d)]
    ['nu90101201002A01_cl.evt']
    """
    return sorted(
        f
        for f in glob.glob(os.path.join(directory, "nu*_cl.evt*"))
        if not f.endswith(".gpg") and valid_re.search(os.path.basename(f))
    )


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000),
    task_run_name="nu_separate_sources_in_{directories[0]}_region_{region_size}",
)
def separate_sources(directories, config, region_size=30, back_region_size=55, obsid=""):
    """
    Run the image-based source separation over every cleaned event file in some directories.

    Writes a ``SEPARATE_DONE.TXT`` sentinel in each directory and skips directories that
    already have one. A skipped directory still records one ``skipped`` record per
    candidate file, so the report can say the step was not run and can still draw it from
    what the run that did run measured.

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
    obsid : str, optional
        Observation these directories belong to. Without it nothing is recorded: this
        step is handed directories, not an observation, and the record has to go
        somewhere.
    """
    directory = diagnostics_path(obsid, config) if obsid else None
    logger = get_logger()
    for d in directories:
        separate_done_file = os.path.join(d, "SEPARATE_DONE.TXT")
        if os.path.exists(separate_done_file):
            logger.info(f"Source separation already done in {d}")
            # Recording the skip is what keeps the focal plane on the page. The record
            # this opens inherits the payload the run that did the work left beside it,
            # so the figure survives; skipping the directory in silence, as this used to,
            # left the step missing from the timeline and its images missing with it.
            for event_file in separation_candidates(d):
                with record_step(
                    directory,
                    obsid,
                    "separate_sources",
                    key=rootname(os.path.basename(event_file)),
                ) as rec:
                    rec.skip("SEPARATE_DONE.TXT already exists")
            continue
        logger.info(f"Separating sources in {d}")
        for event_file in separation_candidates(d):
            separate_sources_in_event_file(
                event_file,
                region_size=region_size,
                back_region_size=back_region_size,
                diagnostics_dir=directory,
                obsid=obsid,
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
    with record_step(diagnostics_path(obsid, config), obsid, "l2_pipeline") as rec:
        return _run_l2_pipeline(obsid, config, flags, rec)


def _run_l2_pipeline(obsid, config, flags, rec):
    """The body of :func:`nu_run_l2_pipeline`, with its diagnostics record open."""
    pipe_done_file = nu_pipeline_done_file(obsid, config=config)
    if os.path.exists(pipe_done_file):
        logger = get_logger()
        logger.info(f"Data for {obsid} already preprocessed")
        rec.skip("PIPELINE_DONE.TXT already exists")
        return
    logger = get_logger()
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
    }

    if flags:
        logger.info(f"Applying custom flags: {flags}")
        params.update(flags)

    # No return-code check here: heasoft.run_task raises on a non-zero code, with the
    # tool's own output in the message.
    heasoft.run_task(
        "nupipeline",
        produces=ev_dir,
        log_to=tool_log_file("nupipeline", obsid, config),
        **params,
    )

    open(pipe_done_file, "a").close()

    rec.value(
        flags=flags or {},
        cleaned_event_files=sorted(
            os.path.basename(f) for f in glob.glob(os.path.join(ev_dir, "*_cl.evt*"))
        ),
    )
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
    with record_step(diagnostics_path(obsid, config), obsid, "recover_spacecraft_science") as rec:
        return _recover_spacecraft_science_data(obsid, config, rec)


def _recover_spacecraft_science_data(obsid, config, rec):
    """The body of :func:`recover_spacecraft_science_data`, with its record open."""
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
        rec.skip("RECOVER_DONE.TXT already exists")
        return splitdir

    if not evfiles_06:
        logger.info(f"No spacecraft science (mode 06) data in {obsid}; nothing to split")
        rec.skip("no spacecraft science (mode 06) data to split")

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
            produces=splitdir,
            infile=evfile,
            chu123hkfile=chu123hkfile,
            hkfile=hkfile,
            outdir=splitdir,
            clobber="yes",
        )
    open(recover_done_file, "a").close()
    rec.value(
        mode_06_files=sorted(os.path.basename(f) for f in evfiles_06),
        split_files=sorted(
            os.path.basename(f) for f in glob.glob(os.path.join(splitdir, "*_cl.evt*"))
        ),
    )
    return splitdir


def join_input_files(obsid, directories, fpm, label):
    """
    The files the joining merges for one focal-plane module.

    Both the joining and :mod:`heasarc_retrieve_pipeline.recover` walk this, so a page
    drawn from a reduction it did not watch shows the same input rows the reduction
    itself would have recorded.

    Mode-01 and mode-06 cleaned files both count. The *unsplit* mode-06 file does not:
    ``nusplitsc`` has already replaced it with its CHU-resolved parts, and merging both
    would count those events twice.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    directories : list of str
        Directories to collect from -- the ``nupipeline`` output and the ``nusplitsc``
        sub-observations.
    fpm : str
        ``"A"`` or ``"B"``.
    label : str
        ``"_src<n>"`` or ``"_back"``.

    Returns
    -------
    list of str
        Full paths, in the order the directories were given.
    """
    files = []
    for d in directories:
        for path in glob.glob(os.path.join(d, f"nu{obsid}{fpm}0[16]*{label}.evt*")):
            if f"{fpm}06" in path and "chu" not in path:
                continue
            files.append(path)
    return files


def gti_of(path):
    """
    The good time intervals of an event file, as an ``(N, 2)`` array.

    An empty array when the file is missing or has no readable GTI extension: this is
    only ever called to draw a picture, and a picture must not take down a reduction.
    """
    from astropy.io import fits

    if not os.path.exists(path):
        return np.zeros((0, 2))
    try:
        with fits.open(path) as hdul:
            return np.asarray(read_gti(hdul), dtype=float)
    except Exception as error:
        get_logger().warning(f"Could not read the GTIs of {path}: {error}")
        return np.zeros((0, 2))


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
        produces=outfile_gti,
        ingtis=",".join([f + "[GTI]" for f in files_to_join]),
        outgti=outfile_gti,
        merge=gti_operation,
        chatter=5,
    )

    # "[1]" is not decoration: ftmgtime writes an empty primary header, and ftsort with a
    # bare file name lands on it and dies with CFITSIO ERROR NOT_TABLE (return code 235).
    heasoft.run(
        "ftsort",
        produces="!" + outfile_gti,
        infile=outfile_gti + "[1]",
        outfile="!" + outfile_gti,
        columns="START",
    )

    logger.info(f"Changing extension name to GTI in {outfile_gti}")

    heasoft.run(
        "fthedit",
        produces=heasoft.IN_PLACE(outfile_gti),
        infile=outfile_gti + "+1",
        keyword="EXTNAME",
        operation="a",
        value="GTI",
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

    Raises
    ------
    ValueError
        If ``outfile`` is one of ``files_to_join``. The output is deleted before the merge
        starts, so merging a file into itself would destroy it.

    Notes
    -----
    ``outfile`` is deleted first if it is there, as :func:`merge_gtis` does with its own
    output. ``ftmerge`` is called without CFITSIO's ``!`` clobber prefix and will not
    create a file that exists -- return code 105, ``failed to create new file (already
    exists?)`` -- which is how a rerun with the ``JOIN_DONE_SRC<n>.TXT`` markers removed
    used to lose a whole observation. The prefix is not used instead because it adds a
    character to a path that already has to fit in 128; see :func:`nu_longest_output_name`.

    The merged GTIs go to an intermediate file named after ``outfile``, so the name is the
    same on every run of the same merge -- it used to carry ``np.random.randint(1000000)``,
    which made the task's inputs different every time and left a stray file behind whenever
    a HEASOFT call raised. One output file means one intermediate, so the deterministic name
    cannot collide, and it is removed in a ``finally``.
    """
    if outfile in files_to_join:
        raise ValueError(
            f"{outfile} is both an input and the output of the same merge, and the "
            "output is deleted before the merge starts"
        )

    outdir, fname = os.path.split(outfile)
    root = splitext_improved(fname)[0]
    logger = get_logger()

    outfile_gti = os.path.join(outdir, f"{root}_tmp.gti")

    if os.path.exists(outfile):
        logger.info(f"Removing the {outfile} left by an earlier run")
        os.unlink(outfile)

    try:
        merge_gtis(files_to_join, outfile_gti, gti_operation=gti_operation)

        logger.info(f"Creating event file {outfile} from {files_to_join}")

        heasoft.run(
            "ftmerge",
            produces=outfile,
            infile=",".join(files_to_join),
            outfile=outfile,
            copyall="NO",
        )

        logger.info(f"Sorting event file {outfile}")

        heasoft.run(
            "ftsort",
            produces="!" + outfile,
            infile=outfile,
            outfile="!" + outfile,
            columns="TIME",
        )

        logger.info(f"Adding GTIs from {outfile_gti}'s first extension to event file {outfile}")

        heasoft.run(
            "fappend",
            produces=heasoft.IN_PLACE(outfile),
            infile=f"{outfile_gti}[1]",
            outfile=outfile,
        )

        if gti_operation == "AND":
            # ftmerge concatenated the event tables of both modules under the intersection
            # ftmgtime just wrote, and knows nothing about it. An event one module recorded
            # a fraction of a second after the other's good time ended is still in there.
            # See utils.drop_events_outside_gti for the measurement and the rationale.
            from astropy.io import fits

            with fits.open(outfile, mode="update") as hdul:
                stats = drop_events_outside_gti(hdul)
            dropped = stats["nevents_before"] - stats["nevents_after"]
            logger.info(
                f"Dropped {dropped} of {stats['nevents_before']} events from {outfile}, "
                "recorded while only one module was observing"
            )
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
        The combined FPMA+FPMB event file, or an empty list if it is missing or if neither
        module had anything to join.

    Raises
    ------
    heasarc_retrieve_pipeline.utils.NoSourceInScienceData
        If a module has mode-01 cleaned events and yet the source separation left nothing
        to merge for it. That is 30202022007 FPMA, whose mode-01 file got no region under
        the too-faint rule, and 90901332001 FPMB, which produced a ``_back`` file and no
        ``_src1``. Both used to reach ``ftmgtime`` with an empty input list, which exits 0
        and writes nothing, and surfaced one step later as ``ftsort failed with return
        code 33``. A module with no mode-01 data at all is skipped with a warning instead.

    Notes
    -----
    Both code paths return the same thing. They used to differ: the early return globbed
    ``nu<obsid>*<label>.evt``, which on a real observation also matches the per-module and
    per-mode intermediates that stage 1 leaves in the directory -- five files rather than
    one on 80002092008. Since ``process_nustar_obsid`` flare-filters and barycentres
    whatever this returns, a rerun did five times the work of a fresh run, on files that
    are not meant to be science products. See issue 6 in ``docs/known_issues.rst``.
    """
    label = f"_src{src_num}" if src_num > 0 else "_back"
    with record_step(
        diagnostics_path(obsid, config), obsid, "join_source_data", key=label.lstrip("_")
    ) as rec:
        return _join_source_data(obsid, directories, config, src_num, label, rec)


def _join_source_data(obsid, directories, config, src_num, label, rec):
    """The body of :func:`join_source_data`, with its diagnostics record open.

    The GTIs of every input and of every merge are recorded here rather than inside
    ``merge_event_files``, which knows neither the observation nor the configuration and
    so has nowhere to write. They are what the joining figure is drawn from: one row per
    input file, then the OR-merged row for each module, then the AND-merged row for the
    pair.
    """
    logger = get_logger()
    outdir = nu_base_output_path(obsid, config=config)

    combined_file = os.path.join(outdir, f"nu{obsid}{label}.evt")

    join_done_file = os.path.join(outdir, f"JOIN_DONE_SRC{src_num}.TXT")
    if os.path.exists(join_done_file):
        logger.info(f"Source data for {obsid} already joined")
        rec.skip(f"JOIN_DONE_SRC{src_num}.TXT already exists")
        return [combined_file] if os.path.exists(combined_file) else []

    # Both module file names are known, so they are built rather than globbed for FPMA and
    # derived from it with str.replace: an output path containing a capital A --
    # /Users/.../ARCHIVE/, say -- would have had that A rewritten too. Only the modules
    # that actually produced something end up in this list.
    module_files = []
    for fpm in "A", "B":
        outfile = os.path.join(outdir, f"nu{obsid}{fpm}{label}.evt")

        logger.info(f"Joining source data for fpm {fpm} into {outfile}")
        files_to_join = join_input_files(obsid, directories, fpm, label)
        for nf in files_to_join:
            if f"{fpm}01" in nf:
                logger.info(f"Copying {nf} to {outdir}")
                os.system(f"cp {nf} {outdir}/")

        if not files_to_join:
            # ftmgtime handed an empty list exits 0 and writes nothing, and ftsort then
            # fails with return code 33 on a file that was never created -- which is how
            # 30202022007 and 90901332001 were lost. Decide it here instead, where the
            # reason is still known.
            if any(module == fpm for module, _ in mode_01_input_files(obsid, config)):
                raise NoSourceInScienceData(
                    obsid, outfile, f"source separation produced nothing for FPM{fpm}"
                )
            logger.warning(
                f"Nothing to join for FPM{fpm} in {obsid}, and no mode-01 data either; "
                "skipping the module"
            )
            continue

        merge_event_files(files_to_join, outfile)
        module_files.append(outfile)

        for i, joined in enumerate(sorted(files_to_join)):
            rec.array(**{f"gti_{fpm}_in_{i}": gti_of(joined)})
        rec.array(**{f"gti_{fpm}_out": gti_of(outfile)})
        rec.value(**{f"inputs_{fpm}": sorted(os.path.basename(f) for f in files_to_join)})

    if not module_files:
        logger.warning(f"No module of {obsid} produced anything to join for {label}")
        rec.skip(f"no module produced anything to join for {label}")
        return []

    merge_event_files(module_files, combined_file, gti_operation="AND")

    rec.array(gti_combined=gti_of(combined_file))
    rec.value(
        modules=sorted(os.path.basename(f) for f in module_files),
        combined=os.path.basename(combined_file),
    )

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


def observation_time_span(obsid, config):
    """
    When an observation started and stopped, and the union of its good time intervals.

    Taken from the normal-science (mode 01) cleaned event files, which is what the whole
    observation looks like: a mode-06 CHU subset covers a few minutes of it, and asking
    GOES only about those minutes is how a short slice ends up with no solar data at all.
    When there is no mode-01 file, every cleaned event file in the ``nupipeline`` output
    directory is used instead.

    ``TSTART``/``TSTOP`` are not trustworthy on their own -- ``ftmerge`` copies them from
    its first input, so they can be narrower than the merged GTI, issue 35 in
    ``known_issues.rst``. Since the flare GTI is later ANDed with each file's own, a
    narrow range would silently delete good time at the edges of the observation: 791 s
    of the 80002092008 background product. The widest of the header bounds and the GTI
    extent is taken.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    tstart, tstop : float
        Mission elapsed time bounds of the observation.
    gti : numpy.ndarray
        Shape ``(N, 2)``: the union of the files' good time intervals.
    mjdref : float
        The files' reference MJD, for converting to and from civil time.

    Raises
    ------
    ValueError
        If the observation has no cleaned event file to read a time from.
    """
    from astropy.io import fits

    files = [infile for _, infile in mode_01_input_files(obsid, config)]
    if not files:
        files = _cl_event_files(
            nu_pipeline_output_path(obsid, config=config), f"nu{obsid}[AB]*_cl.evt*"
        )
    if not files:
        raise ValueError(f"No cleaned event file to take a time span from in {obsid}")

    starts = []
    stops = []
    gtis = []
    mjdref = None
    for path in files:
        with fits.open(path) as hdul:
            header = hdul[1].header
            gti = read_gti(hdul)
            # Read while the file is still open: the reference epoch may live on the
            # primary header rather than the events extension, and time_reference looks
            # in both.
            mjdrefi, mjdreff = time_reference(hdul)
        starts.append(float(header["TSTART"]))
        stops.append(float(header["TSTOP"]))
        mjdref = mjdrefi + mjdreff
        if len(gti):
            starts.append(float(gti[:, 0].min()))
            stops.append(float(gti[:, 1].max()))
            gtis.append(gti)

    gti = merge_intervals(np.vstack(gtis)) if gtis else np.zeros((0, 2))
    return min(starts), max(stops), gti, mjdref


def require_goes_coverage(npoints, obsid, tstart, tstop):
    """
    Raise unless GOES has at least one measurement inside an observation.

    Fatal on purpose. Keeping all the good time when there is no solar data would turn
    the flare filtering off without saying so, and whether an observation may be analysed
    that way is a scientific decision the pipeline must not make on its own.

    Takes a count rather than the time series itself, so that it can be tested without
    ``sunpy`` -- an optional dependency.

    Parameters
    ----------
    npoints : int
        Number of GOES samples left after truncating to the observation.
    obsid : str
        Observation identifier, for the message.
    tstart, tstop : float
        Mission elapsed time bounds that were asked for, for the message.

    Raises
    ------
    heasarc_retrieve_pipeline.utils.NoGoesCoverage
        If ``npoints`` is zero.

    Examples
    --------
    >>> require_goes_coverage(1, "90201037002", 0.0, 100.0)
    >>> require_goes_coverage(0, "90201037002", 0.0, 100.0)
    Traceback (most recent call last):
        ...
    heasarc_retrieve_pipeline.utils.NoGoesCoverage: No GOES ...
    """
    if npoints == 0:
        raise NoGoesCoverage(
            f"No GOES X-ray measurement covers {obsid} (MET {tstart} -- {tstop}). "
            "Solar flares cannot be filtered out without them."
        )


@task(task_run_name="goes_lightcurve_{obsid}_mincat_{minimum_class}")
def get_goes_gtis(obsid, config, minimum_class="C5.0", flux_class="C5.0"):
    """
    Build good time intervals that exclude solar flares.

    NuSTAR observes from low Earth orbit with an open detector aperture, and large solar
    flares raise its background substantially. This task looks up the flares that occurred
    during an observation and produces the complementary GTIs.

    **Once per observation, not once per event file.** The Sun does not care which module
    or which CHU subset the data came from, and a mode-06 CHU slice a few minutes long can
    fall entirely inside a gap in the GOES sampling -- which is how 90201037002 died with
    ``cannot guess format from input values with zero-size array``. Asking about the whole
    observation also cut the 2026 M82 run from 91 fetches to 52, and every fetch is a
    chance to meet a VSO mirror that is down.

    The steps are: convert the observation's time span from NuSTAR
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

    The time range searched comes from :func:`observation_time_span`: the wider of the
    mode-01 headers' ``TSTART``/``TSTOP`` and those files' own GTI extent. On a merged
    file those disagree, and the GTI is the honest one.

    Returns the existing file unchanged if it is already present.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
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

    Returns
    -------
    str
        Path of the GTI file, :func:`nu_goes_gti_file`.

    Notes
    -----
    The GOES X-ray light curve is also written to :func:`nu_goes_lc_file`, on the
    observation's own time scale, so that :func:`record_flare_filtering` can show what the
    Sun was doing without downloading anything a second time.

    Raises
    ------
    heasarc_retrieve_pipeline.utils.NoGoesCoverage
        If GOES has no measurement inside the observation.
    RuntimeError
        If solar flares cover the whole observation.
    """
    from sunpy import timeseries as ts
    from sunpy.net import Fido
    from sunpy.net import attrs as a
    from sunpy.time import parse_time
    from astropy.table import Table
    from nustar_gen import info, utils

    outfile_gti = nu_goes_gti_file(obsid, config)

    if os.path.exists(outfile_gti):
        logger = get_run_logger()
        logger.info(f"GOES GTI file {outfile_gti} already exists, skipping")
        return outfile_gti

    min_cat = minimum_class[0]
    min_num = float(minimum_class[1:])

    logger = get_run_logger()
    logger.info(f"Creating GOES light curve and GTIs for {obsid}")

    ns = info.NuSTAR()
    tstart, tstop, file_gti, mjdref = observation_time_span(obsid, config)
    os.makedirs(nu_base_output_path(obsid, config=config), exist_ok=True)
    datestart = ns.met_to_time(tstart)
    dateend = ns.met_to_time(tstop)

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
    files = Fido.fetch(result3, progress=False, path=goes_download_path(obsid, config))
    goes_all = ts.TimeSeries(files, concatenate=True)
    goes = goes_all.truncate(datestart.iso, dateend.iso)
    # Before anything asks for goes.time: building a Time from an empty index raises
    # "cannot guess format from input values with zero-size array", which says nothing
    # about what actually went wrong.
    require_goes_coverage(len(goes.to_dataframe()), obsid, tstart, tstop)

    hek_results = result3["hek"]
    flares_hek = hek_results

    outfile_lc = nu_goes_lc_file(obsid, config)
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
        f"Writing the GOES X-ray light curve ({len(lightcurve['TIME'])} points) to {outfile_lc}"
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
            f"Solar flares cover the whole of {obsid} (MET {tstart} -- {tstop}); no "
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
            f"time in {obsid} ({before:.0f} -> {after:.0f} s). If that is not a "
            f"genuinely flare-dominated observation, flux_class={flux_class} is probably "
            f"below the Sun's quiescent 1-8 A flux for this epoch. Check the diagnostic "
            f"figure written next to the filtered file."
        )

    gtis = [{"START": start, "STOP": stop} for start, stop in good]

    utils.make_usr_gti(gtis, overwrite=True, outfile=outfile_gti)
    logger.info(f"Changing extension name to GTI in {outfile_gti}")

    heasoft.run(
        "fthedit",
        produces=heasoft.IN_PLACE(outfile_gti),
        infile=outfile_gti + "+1",
        keyword="EXTNAME",
        operation="a",
        value="GTI",
    )

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
def record_flare_filtering(
    event_file,
    gti_before,
    gti_after,
    goes_lc_file=None,
    dt=100.0,
    minimum_class="C5.0",
    flux_class="C5.0",
    rec=None,
):
    """
    Measure what the solar-flare filtering removed, and what it left alone.

    Cleaning an event file is easy to get wrong in ways that leave no trace in the output:
    too little is removed, or too much, and either way the file looks fine. This records
    the evidence, as three curves on one time axis:

    1. the GOES X-ray flux, so the cut is visible where it acts;
    2. the event file's 3--10 keV light curve, the band in which solar stray light lands;
    3. the same in 10--79 keV, as a control. Solar flares do not produce hard X-rays at
       NuSTAR's aperture, so this one should look the same before and after. If it does
       not, the cut is removing more than solar flares -- which is why the chi-squared per
       degree of freedom against a constant is recorded for both bands, before and after.

    Each light curve is recorded before and after the filtering, with the removed
    intervals, so the report can draw the difference between the two.

    This used to write a JPEG next to the event file. The observation's page now draws the
    same three panels from these numbers, zoomable, with the rest of the reduction around
    them; see :mod:`heasarc_retrieve_pipeline.report`.

    Parameters
    ----------
    event_file : str
        The **unfiltered** event file. Read, never written.
    gti_before, gti_after : array-like or table
        Good time intervals before and after the flare filtering.
    goes_lc_file : str, optional
        The observation's GOES light curve, :func:`nu_goes_lc_file`. One per observation,
        so the caller passes it rather than deriving it from the event file. Omitting it,
        or naming a file that is not there, records no GOES curve -- a rerun skips the
        download, so its absence is not an error.
    dt : float, optional
        Light-curve bin width in seconds.
    minimum_class : str, optional
        The catalogued-flare class cut used. Recorded, not applied here.
    flux_class : str or None, optional
        The flux cut used. Recorded, not applied here.
    rec : :class:`heasarc_retrieve_pipeline.diagnostics.StepRecord`, optional
        Where the numbers go. ``None`` records nothing, which makes this function a
        somewhat expensive way to do nothing; the caller always passes one.

    Returns
    -------
    dict
        The light curves and the intervals, keyed as they were recorded. Returned as well
        as recorded so that this is testable without a diagnostics directory.
    """
    from astropy.io import fits
    from astropy.table import Table

    logger = get_logger()
    if rec is None:
        rec = no_record()

    gti_before = gti_to_array(gti_before)
    gti_after = gti_to_array(gti_after)

    with fits.open(event_file) as hdul:
        events = hdul["EVENTS"]
        times = np.asarray(events.data["TIME"], dtype=float) + float(
            events.header.get("TIMEZERO", 0.0)
        )
        # NuSTAR's pulse-invariant channels are linear in energy: E = 0.04 * PI + 1.6 keV.
        energy = 0.04 * np.asarray(events.data["PI"], dtype=float) + 1.6

    kept = mask_from_gti(times, gti_after)
    removed = intervals_removed(gti_before, gti_after)

    arrays = dict(
        gti_before=np.asarray(gti_before, dtype=float),
        gti_after=np.asarray(gti_after, dtype=float),
        removed=np.asarray(removed, dtype=float).reshape(-1, 2),
    )
    rec.value(
        n_intervals_removed=len(removed),
        bin_seconds=dt,
        minimum_class=minimum_class,
        flux_class=flux_class,
    )

    if goes_lc_file is not None and os.path.exists(goes_lc_file):
        goes = Table.read(goes_lc_file)
        arrays["goes_time"] = np.asarray(goes["TIME"], dtype=float)
        for column in ("XRSA", "XRSB"):
            if column in goes.colnames:
                arrays[f"goes_{column.lower()}"] = np.asarray(goes[column], dtype=float)
        rec.value(goes_light_curve=os.path.basename(goes_lc_file))
    else:
        logger.warning(f"No GOES light curve at {goes_lc_file}; recording none")
        rec.value(goes_light_curve=None)

    for emin, emax in ((3.0, 10.0), (10.0, 79.0)):
        in_band = (energy >= emin) & (energy < emax)
        before = binned_lightcurve(times[in_band], gti_before, dt)
        after = binned_lightcurve(times[in_band & kept], gti_after, dt)

        band = f"{emin:.0f}_{emax:.0f}"
        rec.value(
            **{
                f"chi2_dof_{band}": [
                    chi2_dof_against_a_constant(before),
                    chi2_dof_against_a_constant(after),
                ]
            }
        )
        for when, curve in (("before", before), ("after", after)):
            for column in ("time", "rate", "rate_err"):
                arrays[f"lc_{band}_{when}_{column}"] = np.asarray(curve[column], dtype=float)

    rec.array(**arrays)
    logger.info(
        f"{os.path.basename(event_file)}: recorded the flare filtering, "
        f"{times.size - int(kept.sum())} of {times.size} events removed in "
        f"{len(removed)} interval(s)"
    )
    return arrays


@flow(flow_run_name="nu_filter_solar_flares_{event_file}_mincat_{minimum_class}")
def filter_from_solar_flares(
    event_file,
    goes_gti_file,
    goes_lc_file=None,
    minimum_class="C5.0",
    flux_class="C5.0",
    diagnostics_dir=None,
    obsid="",
):
    """
    Write a flare-free copy of an event file.

    Combines the event file's own GTIs with the observation's flare-free intervals, which
    :func:`get_goes_gtis` worked out once for the whole observation, using a logical AND,
    and writes the result as ``<root>_noflares.evt``.

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
    goes_gti_file : str
        The observation's flare-free GTI file, from :func:`get_goes_gtis`. Passed in
        rather than looked up so that the fetch happens once per observation and the
        dependency is visible at the call site.
    goes_lc_file : str, optional
        The observation's GOES light curve, for the diagnostic.
    minimum_class : str, optional
        Smallest catalogued flare class that was excluded. Recorded, not applied here.
    flux_class : str or None, optional
        The flux cut that was applied, likewise only recorded. See :func:`get_goes_gtis`
        for why this is separate from ``minimum_class``.

    Returns
    -------
    str
        Path of the filtered file.

    Notes
    -----
    What the cut removed is measured by :func:`record_flare_filtering` and drawn on the
    observation's page. Failing to record it is logged, not raised: the science product is
    already on disk by then.
    """
    with record_step(
        diagnostics_dir, obsid, "flare_filtering", key=rootname(os.path.basename(event_file))
    ) as rec:
        return _filter_from_solar_flares(
            event_file, goes_gti_file, goes_lc_file, minimum_class, flux_class, rec
        )


def _filter_from_solar_flares(
    event_file, goes_gti_file, goes_lc_file, minimum_class, flux_class, rec
):
    """The body of :func:`filter_from_solar_flares`, with its record open."""
    from astropy.io import fits

    root = rootname(event_file)
    outfile_gti_temp = root + "_tmp.gti"
    outfile_filtered = flare_filtered_event_file_name(event_file)

    logger = get_logger()

    if os.path.exists(outfile_filtered):
        logger.info(f"Filtered event file {outfile_filtered} already exists, skipping")
        rec.skip("the flare-filtered file was already there")
        return outfile_filtered

    merge_gtis([event_file, goes_gti_file], outfile_gti_temp, gti_operation="AND")

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

    # What the filtering cost, in apply_gti's own words: it is the function that did it.
    rec.value(**stats)

    # The science product is already written. The diagnostic failing -- a missing GOES
    # file, an unreadable light curve -- must not take the observation down with it, so
    # it is logged rather than raised. Everything above is already on disk.
    try:
        record_flare_filtering(
            event_file,
            gti_before,
            gti_after,
            goes_lc_file=goes_lc_file,
            minimum_class=minimum_class,
            flux_class=flux_class,
            rec=rec,
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


def snr_optimised_radius(optimize, rind, rad_profile, radial_err, psf_profile):
    """
    The SNR-optimised extraction radius, or ``None`` when there is no source to optimise.

    ``nustar_gen.radial_profile.optimize_radius_snr`` steps outwards in radius keeping the
    radius at which the signal-to-noise was highest, and assigns that radius only inside
    ``if snr > old_snr``. ``old_snr`` starts at zero, so on a file with no source the
    condition never holds, ``best_radius`` is never bound, and the return statement raises
    ``UnboundLocalError``.

    Measured against nustar_gen 0.8.dev9: a flat radial profile, with or without counts,
    raises it every time. Three of the 56 M82 observations reprocessed in 2026 --
    30202022008, 30702012004 and 90101005002 -- lost the whole observation to it.

    A file too faint to place a region in is not a failure, and the caller already knows
    what to do with a file that has no region: :func:`get_best_source_regions` skips it and
    averages the others, and :func:`calculate_spectra` logs it and moves on.

    Parameters
    ----------
    optimize : callable
        ``nustar_gen``'s ``optimize_radius_snr``. Passed in rather than imported so this
        can be exercised without the optional dependency installed.
    rind, rad_profile, radial_err, psf_profile : array-like
        As returned by ``nustar_gen``'s ``make_radial_profile``.

    Returns
    -------
    float or None
        The radius in arcsec, or ``None`` if no radius maximises the signal-to-noise.
    """
    try:
        return optimize(rind, rad_profile, radial_err, psf_profile, show=False)
    except UnboundLocalError:
        return None


def first_source_position(coordinates, wcs):
    """
    Sky position of the brightest peak, or ``None`` if ``find_source`` found nothing.

    ``nustar_gen.radial_profile.find_source`` returns an array of pixel coordinates. On an
    image with too few counts to hold a peak it returns an empty one, and indexing it
    raises ``IndexError: index 0 is out of bounds for axis 0 with size 0``. That took down
    three observations of the 2026 M82 run -- 90202038002, 30502021004 and 30702012004 --
    and never on a mode-01 file: every one was a single-CHU subset of the mode-06 data,
    where a few minutes of exposure can genuinely hold no detectable source.

    A file with no source in it is an answer, not a failure. The callers already know what
    to do with ``None``: :func:`calculate_spectra` records the skip and moves on, and
    :func:`get_best_source_regions` treats it as fatal, because a mode-01 module with no
    source means something else is wrong.

    Parameters
    ----------
    coordinates : array-like
        Pixel coordinates as ``find_source`` returns them, in the native ``[Y, X]`` order.
    wcs : astropy.wcs.WCS
        World coordinate system of the image the peak was found in.

    Returns
    -------
    astropy.coordinates.SkyCoord or None
        The position in FK5, or ``None`` if there was no peak.
    """
    coordinates = np.asarray(coordinates)
    if coordinates.size == 0:
        return None
    # The flip goes from find_source's native [Y, X] to the [X, Y] that wcs wants.
    world = wcs.all_pix2world(np.flip(coordinates), 0)
    return SkyCoord(world[0][0], world[0][1], unit="deg", frame="fk5")


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
    rec=None,
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
    rec : :class:`heasarc_retrieve_pipeline.diagnostics.StepRecord`, optional
        Where to write the radial profile and the chosen radius. This function has no
        ``obsid``, so the caller -- which does -- opens the record and hands it in.
        ``None`` records nothing.

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
    if rec is None:
        rec = no_record()
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
        rec.value(
            ra=region_src.center.ra.deg,
            dec=region_src.center.dec.deg,
            rlimit=region_src.radius.to(u.arcsec).value,
            read_back=True,
        )
        rec.skip("the region files were already there")
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

    target = first_source_position(coordinates, wcs)
    if target is None:
        logger.warning(
            f"No source found in the {elow}-{ehigh} keV image of {infile}: there are too "
            "few counts in it to hold a peak. Writing no region file for it."
        )
        rec.skip(f"no source in the {elow}-{ehigh} keV image; too few counts to hold a peak")
        return None

    if max_offset is None:
        max_offset = config.get("max_source_offset_arcmin", 3) * u.arcmin
    if not position_is_consistent(target, reference, max_offset):
        separation = target.separation(reference).to(u.arcmin)
        logger.warning(
            f"Source found in {infile} is "
            f"{separation:.2f} from the expected position, "
            f"more than {max_offset}. Writing no region file for it."
        )
        rec.value(
            ra=target.icrs.ra.deg,
            dec=target.icrs.dec.deg,
            separation_arcmin=separation.value,
            max_offset_arcmin=max_offset.to(u.arcmin).value,
        )
        rec.skip(
            f"the source found is {separation:.2f} from the expected position, "
            f"more than {max_offset}"
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
    # The profile is recorded whatever happens next: a source too faint for a radius is
    # exactly the case somebody will want to look at.
    rec.array(
        radius=np.asarray(rind, dtype=float),
        profile=np.asarray(rad_profile, dtype=float),
        profile_error=np.asarray(radial_err, dtype=float),
        psf_profile=np.asarray(psf_profile, dtype=float),
    )
    rec.value(band_kev=[pair[0], pair[1]], image_band_kev=[elow, ehigh])

    rlimit = snr_optimised_radius(optimize_radius_snr, rind, rad_profile, radial_err, psf_profile)
    if rlimit is None:
        logger.warning(
            f"No radius maximises the signal-to-noise in {infile}: the source is too "
            "faint to place an extraction region on. Writing no region file for it."
        )
        rec.skip("no radius maximises the signal-to-noise; the source is too faint")
        return None

    max_radius = config.get("max_radius", 80)
    logger.info(f"Radius of peak SNR for {pair[0]} to {pair[1]} keV in {fname}: {rlimit} arcsec")
    rec.value(
        rlimit_snr=float(rlimit),
        max_radius=max_radius,
        capped_at_max_radius=rlimit > max_radius,
    )
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

    rec.value(ra=icrs.ra.deg, dec=icrs.dec.deg, rlimit=rlimit, read_back=False)
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

    Raises
    ------
    heasarc_retrieve_pipeline.utils.NoSourceInScienceData
        If a mode-01 file yields no region. Mode 01 is ordinary science with the full
        aspect solution, so a target the pipeline was pointed at has to be in it; half an
        observation delivered quietly is worse than a failure that says why.

    Notes
    -----
    Files whose region files already exist still contribute: :func:`get_best_source_region`
    reads the position and radius back out of them. ``(0.0, 0.0, 0.0)`` is returned only
    when there is no mode-01 cleaned event file at all -- which happens: 80002092003 has
    none. That is a different case from a mode-01 file with no source in it, and stays a
    clean outcome.
    """
    with record_step(diagnostics_path(obsid, config), obsid, "source_position") as rec:
        return _get_best_source_regions(obsid, config, rec)


def _get_best_source_regions(obsid, config, rec):
    """The body of :func:`get_best_source_regions`, with its diagnostics record open."""
    logger = get_logger()
    directory = diagnostics_path(obsid, config)
    outdir = nu_pipeline_output_path(obsid, config=config)
    os.makedirs(outdir, exist_ok=True)

    mean_ra = mean_dec = mean_rlimit = 0.0
    count = 0
    for fpm, infile in mode_01_input_files(obsid, config):
        # get_best_source_region returns early when the region files already exist,
        # reading the position and radius back out of them, so every file counts.
        # Each file gets its own record, keyed by its root name: this loop and
        # calculate_spectra between them measure a region for every event file there is.
        with record_step(
            directory, obsid, "source_region", key=rootname(os.path.basename(infile))
        ) as file_rec:
            result = get_best_source_region(infile, config=config, rec=file_rec)
        if result is None:
            # Mode 01 is ordinary science with the full aspect solution. A target the
            # pipeline was pointed at has to be in it, and half an observation is not an
            # outcome to deliver quietly. An unusable mode-06 CHU subset is different --
            # calculate_spectra records that one and carries on.
            raise NoSourceInScienceData(
                obsid, infile, f"FPM{fpm} produced no usable extraction region"
            )
        ra, dec, rlimit, _, _ = result
        mean_ra += ra
        mean_dec += dec
        mean_rlimit += rlimit
        count += 1

    if count == 0:
        logger.warning(f"No cleaned event file to locate a source in for {obsid}")
        rec.skip("no mode-01 cleaned event file to locate a source in")
        return 0.0, 0.0, 0.0

    rec.value(
        n_files=count,
        mean_ra=mean_ra / count,
        mean_dec=mean_dec / count,
        mean_rlimit=mean_rlimit / count,
    )
    return mean_ra / count, mean_dec / count, mean_rlimit / count


def read_spectrum(pha_file):
    """
    The counts spectrum of a PHA file, in energy rather than channel.

    ``nuproducts`` writes the source and background spectra of every extraction as
    ``<stem>_sr.pha`` and ``<stem>_bk.pha``. Those are the observation's last product and
    the one a reader most wants to look at, and until now nothing drew them.

    Channels are converted with the same relation the rest of this module uses,
    ``E [keV] = 0.04 * PI + 1.6``, rather than through the response matrix. That is exact
    for the channel *centres*, which is what a diagnostic plot needs; it is not a
    substitute for folding a model through the RMF, and nothing here should be used for
    fitting.

    Parameters
    ----------
    pha_file : str
        Path of a PHA spectrum.

    Returns
    -------
    dict or None
        ``energy`` (keV), ``rate`` (counts/s/keV) and ``rate_err``, or ``None`` if the
        file has no counts column this can read.

    Notes
    -----
    A PHA may carry ``COUNTS`` or ``RATE``; both are handled, and ``COUNTS`` is divided by
    the header ``EXPOSURE``. The uncertainty is Poisson on the counts, which is right for
    an unbinned spectrum and an underestimate for a grouped one -- so the ungrouped
    ``_sr.pha`` is what the reduction records, not the ``_grp.pha`` it also writes.
    """
    from astropy.io import fits

    with fits.open(pha_file) as hdul:
        data = hdul[1].data
        header = hdul[1].header
        columns = {name.upper() for name in data.columns.names}
        exposure = float(header.get("EXPOSURE") or header.get("ONTIME") or 1.0)
        if exposure <= 0:
            exposure = 1.0

        if "COUNTS" in columns:
            counts = np.asarray(data["COUNTS"], dtype=float)
        elif "RATE" in columns:
            counts = np.asarray(data["RATE"], dtype=float) * exposure
        else:
            return None

        channel = np.asarray(data["CHANNEL"], dtype=float)

    energy = 0.04 * channel + 1.6
    # Per keV, so that the shape does not depend on the channel width.
    width = 0.04
    return dict(
        energy=energy,
        rate=counts / exposure / width,
        rate_err=np.sqrt(np.maximum(counts, 0)) / exposure / width,
    )


#: How ``nuproducts`` names the source and background spectra of one extraction.
NUPRODUCTS_SPECTRA = (("src", "_sr.pha"), ("bkg", "_bk.pha"))

#: How ``addspec`` names them. A co-added product is a different family of files, which is
#: why the combined spectra do not end in ``_sr.pha`` -- see
#: :mod:`heasarc_retrieve_pipeline.coadd`.
ADDSPEC_SPECTRA = (("src", ".pha"), ("bkg", ".bak"))


def spectrum_arrays(outdir, stem, suffixes=NUPRODUCTS_SPECTRA):
    """
    The source and background spectra of one extraction, ready to record.

    Parameters
    ----------
    outdir : str
        The observation's products directory.
    stem : str
        ``stemout`` as handed to ``nuproducts``, or the root of an ``addspec`` product.
    suffixes : sequence of tuple, optional
        ``(which, suffix)`` pairs saying what the two spectra are called.
        :data:`NUPRODUCTS_SPECTRA` by default; :data:`ADDSPEC_SPECTRA` for a co-added one.

    Returns
    -------
    dict
        Empty if neither spectrum is readable -- a missing one is not an error, since
        ``nuproducts`` is allowed to have failed for a single file.
    """
    arrays = {}
    for which, suffix in suffixes:
        path = os.path.join(outdir, stem + suffix)
        if not os.path.exists(path):
            continue
        try:
            spectrum = read_spectrum(path)
        except Exception as error:
            get_logger().warning(f"Could not read the spectrum {path}: {error}")
            continue
        if spectrum is None:
            continue
        for key, values in spectrum.items():
            arrays[f"spec_{stem}_{which}_{key}"] = np.asarray(values, dtype=np.float32)
    return arrays


@task(
    task_run_name="nu_calc_spec_{obsid}_src-reg_{src_reg}_back-reg_{bkg_reg}",
)
def calculate_spectra(
    obsid, config, src_reg=None, bkg_reg=None, ra=None, dec=None, goes_gti_file=None
):
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
    goes_gti_file : str, optional
        The observation's flare-free GTI file. ``None`` asks :func:`get_goes_gtis` for it,
        which is what makes this function usable on its own; the flow passes the one it
        already has, so the GOES data are fetched once per observation.

    Notes
    -----
    ``PRODUCTS_DONE.TXT`` is written only if nothing went wrong. An observation with no
    usable event files is a clean outcome -- nothing was produced and nothing failed -- and
    is marked done; a missing region or GTI file that should have been there is not, so the
    next run retries instead of the observation being marked done forever.

    The good time interval is worked out **per pair of modules**, not per file: FPMA's file,
    FPMB's counterpart and the flare-free GTI are intersected, and the result is handed to
    both extractions. The two modules' spectra then cover exactly the same time, which is
    what makes them safe to co-add, and it is the same intersection
    :func:`join_source_data` has always applied to the combined event list. It is very
    nearly free -- on ``90901333002`` the two modules' good times differ by at most three
    seconds out of three thousand. A file whose counterpart is missing keeps its own good
    time and is recorded as ``unpaired``; it cannot become part of a combined product.
    """
    with record_step(diagnostics_path(obsid, config), obsid, "calculate_spectra") as rec:
        return _calculate_spectra(obsid, config, src_reg, bkg_reg, ra, dec, goes_gti_file, rec)


def _calculate_spectra(obsid, config, src_reg, bkg_reg, ra, dec, goes_gti_file, rec):
    """The body of :func:`calculate_spectra`, with its diagnostics record open."""
    logger = get_logger()
    indir = nu_pipeline_output_path(obsid, config=config)
    outdir = nu_product_output_path(obsid, config=config)
    product_done_file = os.path.join(outdir, "PRODUCTS_DONE.TXT")
    if os.path.exists(product_done_file):
        logger.info(f"Spectra for {obsid} already calculated")
        rec.skip("PRODUCTS_DONE.TXT already exists")
        return
    os.makedirs(outdir, exist_ok=True)

    if goes_gti_file is None:
        goes_gti_file = get_goes_gtis(obsid, config)

    logger.info(f"Calculating spectra in directory {outdir}")

    reference = None
    if ra not in (None, "NONE") and dec not in (None, "NONE"):
        reference = SkyCoord(float(ra), float(dec), unit="deg")
    max_offset = config.get("max_source_offset_arcmin", 3) * u.arcmin

    problems = 0
    inputs = []
    without_region = []
    spectra = []

    # First pass: the extraction regions, which decide which files can be used at all. The
    # pairing below has to be done over what survives this rather than over what is on
    # disk, because a module whose region could not be measured cannot be half of a pair.
    usable = []
    for fpm, infile in spectral_input_files(obsid, config):
        inputs.append(os.path.basename(infile))
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
            with record_step(
                diagnostics_path(obsid, config), obsid, "source_region", key=root_name
            ) as file_rec:
                get_best_source_region(
                    infile,
                    config=config,
                    reference=reference if is_mode_06 else None,
                    max_offset=max_offset,
                    rec=file_rec,
                )
        if not os.path.exists(this_src) or not os.path.exists(this_bkg):
            # Determinate: either no source was found, or it was too faint to place a
            # radius on, or it was too far from the mode-01 position. Rerunning would
            # decide the same, so this is a clean skip -- recorded so that a run can be
            # audited without reading a 40 MB log.
            logger.warning(f"No usable extraction region for {infile}, skipping")
            record_skipped_input(
                obsid, config, infile, "no usable extraction region could be measured"
            )
            without_region.append(os.path.basename(infile))
            continue

        usable.append(
            dict(
                key=spectral_input_key(obsid, fpm, infile),
                fpm=fpm,
                infile=infile,
                root_name=root_name,
                stem=stem,
                filedir=filedir,
                src=this_src,
                bkg=this_bkg,
            )
        )

    # Second pass: one flare-free GTI per key, shared by both modules where both are there.
    # FPMA and FPMB then see exactly the same good time, which is what lets their spectra be
    # co-added afterwards, and it is the same intersection the combined event list has always
    # used. It costs nothing: measured on 90901333002, the two modules' good times differ by
    # at most three seconds out of three thousand.
    by_key = {}
    for entry in usable:
        by_key.setdefault(entry["key"], []).append(entry)

    unpaired = []
    gti_files = {}
    for key, entries in by_key.items():
        if len(entries) == 2:
            gti_file = os.path.join(entries[0]["filedir"], f"nu{obsid}_{key}_noflares.gti")
        else:
            # No counterpart, so nothing to intersect with: this is the two-way AND the
            # pipeline has always done, and the file is barred from the combined products.
            unpaired.extend(os.path.basename(entry["infile"]) for entry in entries)
            gti_file = os.path.join(
                entries[0]["filedir"], entries[0]["root_name"] + "_noflares.gti"
            )
        merge_gtis(
            [entry["infile"] for entry in entries] + [goes_gti_file],
            gti_file,
            gti_operation="AND",
        )
        gti_files[key] = gti_file

    for entry in usable:
        infile = entry["infile"]
        stem = entry["stem"]
        gti_file = gti_files[entry["key"]]
        if not os.path.exists(gti_file):
            logger.warning(f"Flare-free GTI file missing for {infile}, skipping")
            problems += 1
            continue

        logger.info(f"Calculating spectrum for {infile}")
        params = dict(
            indir=indir,
            infile=infile,
            instrument=f"FPM{entry['fpm']}",
            steminputs="nu" + obsid,
            stemout=stem,
            srcregionfile=entry["src"],
            bkgregionfile=entry["bkg"],
            outdir=outdir,
            clobber="yes",
            runmkarf="yes",
            extended="no",
            runmkrmf="yes",
            rungrppha="yes",
            grpmincounts=20,
            grppibadlow=35,
            grppibadhigh=1909,
            usrgtifile=gti_file,
            grpphafile=os.path.join(outdir, stem + "_grp.pha"),
        )
        logger.debug("nuproducts " + " ".join(f"{k}={v}" for k, v in params.items()))
        heasoft.run(
            "nuproducts",
            params,
            produces=params["grpphafile"],
            noprompt=True,
            clobber=True,
            log_to=tool_log_file("nuproducts", obsid, config),
        )
        spectra.append(os.path.basename(params["grpphafile"]))
        # The spectrum itself, so the page can show what came out rather than only its
        # file name. Recorded here, where the stem is known.
        rec.array(**spectrum_arrays(outdir, stem))

    rec.value(
        inputs=inputs,
        without_region=without_region,
        spectra=spectra,
        missing_flare_free_gti=problems,
        unpaired=unpaired,
        # Which convention produced these spectra. Observations reduced before this was
        # introduced kept a good time per module, and PRODUCTS_DONE.TXT stops them being
        # redone, so an archive holds both and the difference has to be visible.
        gti_convention="shared_between_modules",
    )

    if problems > 0:
        logger.warning(
            f"{problems} file(s) could not be processed for {obsid}; "
            "not marking the observation as done"
        )
        # Not a failure -- the spectra that could be made were made -- but not a clean
        # finish either, and the report has to be able to tell the two apart.
        rec.skip(f"{problems} file(s) had no flare-free GTI; not marking the observation as done")
        return

    open(product_done_file, "w").close()


#: The combined products, and which observing modes go into each. ``comb01`` is the
#: conservative one -- normal science only, the best-understood aspect solution. ``comb06``
#: is the spacecraft-science data on its own. ``comb0106`` is everything, for when exposure
#: matters more than homogeneity; it is written only when there is mode-06 data to add,
#: since otherwise it would be a second copy of ``comb01``.
COMBINED_PRODUCTS = {
    "comb01": ("01",),
    "comb06": ("06",),
    "comb0106": ("01", "06"),
}


def _mode_of(key):
    """Which of :data:`SCIENCE_MODES` a :func:`spectral_input_key` belongs to."""
    return "01" if key.startswith("01") else "06"


def combined_spectrum_inputs(obsid, config, tag=None):
    """
    The source spectra each combined product is to be built from.

    Only spectra whose module counterpart is there as well, because a combined product is a
    sum over both modules and nothing else: see
    :func:`~heasarc_retrieve_pipeline.coadd.apply_case_b_scaling` for why letting an
    unpaired file in would silently break the exposure of the result.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    tag : str, optional
        Segment suffix, as :func:`~heasarc_retrieve_pipeline.segments.segment_tag` builds
        it. ``None``, the default, means the whole observation. With a tag the same pairing
        is applied to the segment spectra ``hrp-split-obsid`` wrote, whose names carry the
        tag last.

    Returns
    -------
    dict
        Key of :data:`COMBINED_PRODUCTS` to the list of spectra to co-add, FPMA before FPMB
        within each pair. A product with nothing to add, or one of whose modes produced
        nothing, is left out.
    """
    products = nu_product_output_path(obsid, config=config)
    pairs, _ = paired_spectral_inputs(obsid, config)
    ending = "_sr.pha" if tag is None else f"_sr_{tag}.pha"

    by_mode = {}
    for key in pairs:
        paths = [os.path.join(products, f"nu{obsid}{fpm}{key}{ending}") for fpm in ("A", "B")]
        if not all(os.path.exists(path) for path in paths):
            # The pair was extractable but one of the two extractions did not produce a
            # spectrum. Half a pair is no pair.
            continue
        by_mode.setdefault(_mode_of(key), []).extend(paths)

    found = {}
    for suffix, modes in COMBINED_PRODUCTS.items():
        if not all(mode in by_mode for mode in modes):
            continue
        found[suffix] = [path for mode in modes for path in by_mode[mode]]
    return found


def mode_06_exposure_fraction(spectra, obsid):
    """
    How much of a combined product's exposure came from spacecraft science.

    Mode-06 data has a reconstructed aspect solution, and ``addspec`` weights by exposure
    alone -- it has no notion of one input being less trusted than another. Recording the
    fraction is what lets a reader of the report see what they would be fitting.

    Parameters
    ----------
    spectra : list of str
        Paths of the co-added spectra.
    obsid : str
        Observation identifier, needed to find the mode in each file name.

    Returns
    -------
    float
        Between 0 and 1, or 0.0 if no exposure could be read at all.
    """
    from astropy.io import fits

    total = 0.0
    from_mode_06 = 0.0
    for path in spectra:
        try:
            exposure = float(fits.getheader(path, 1).get("EXPOSURE", 0.0) or 0.0)
        except Exception as error:  # pragma: no cover - unreadable input
            get_logger().warning(f"Could not read the exposure of {path}: {error}")
            continue
        total += exposure
        # nu<OBSID><FPM><key>_sr.pha, so the mode is what follows the module letter.
        key = os.path.basename(path)[len(f"nu{obsid}") + 1 :]
        if _mode_of(key) == "06":
            from_mode_06 += exposure
    return from_mode_06 / total if total else 0.0


def module_exposure_ratio(spectra, obsid):
    """
    How much longer FPMA was live than FPMB, over a combined product's inputs.

    Once the two modules share a good time interval this is pure deadtime, and deadtime
    tracks the count rate. So the ratio is a cheap witness that a time selection is not
    doing something odd: measured on ``90901333002`` it sat between 1.0084 and 1.0105
    across four independent selections, and
    :func:`~heasarc_retrieve_pipeline.segments.combine_segment_spectra` compares each
    segment's against the parent's for exactly that reason.

    Parameters
    ----------
    spectra : list of str
        Paths of the spectra that go into one combined product.
    obsid : str
        Observation identifier, needed to find the module letter in each file name.

    Returns
    -------
    float
        FPMA exposure over FPMB exposure, or ``nan`` if FPMB contributed nothing readable.
    """
    from astropy.io import fits

    totals = {"A": 0.0, "B": 0.0}
    for path in spectra:
        # nu<OBSID><FPM>..., so the module letter is the one after the identifier.
        fpm = os.path.basename(path)[len("nu") + len(obsid)]
        if fpm not in totals:
            continue
        try:
            totals[fpm] += float(fits.getheader(path, 1).get("EXPOSURE", 0.0) or 0.0)
        except Exception as error:  # pragma: no cover - unreadable input
            get_logger().warning(f"Could not read the exposure of {path}: {error}")
    return totals["A"] / totals["B"] if totals["B"] else float("nan")


@task(task_run_name="nu_combine_modules_{obsid}")
def combine_module_spectra(obsid, config):
    """
    Co-add FPMA and FPMB into one spectrum per observing mode.

    Three products, named for what went into them and written beside the per-module spectra
    they were built from::

        nu<OBSID>_comb01.pha    .bak  .rsp  _grp.pha    normal science only
        nu<OBSID>_comb06.pha    .bak  .rsp  _grp.pha    spacecraft science only
        nu<OBSID>_comb0106.pha  .bak  .rsp  _grp.pha    both, for maximum exposure

    The per-module spectra are untouched and remain the ones to use for a joint fit with a
    cross-normalisation constant between the modules, which is the more correct thing to do
    when the source is bright enough to allow it. These are for when it is not.

    Runs on the pairs :func:`paired_spectral_inputs` found, so both modules always
    contribute equally and :func:`~heasarc_retrieve_pipeline.coadd.apply_case_b_scaling` can
    correct the exposure by a factor it knows. Writes a ``COMBINE_DONE.TXT`` sentinel of its
    own -- not ``PRODUCTS_DONE.TXT``, which belongs to :func:`calculate_spectra` and would
    make this step unreachable on any observation reduced before it existed.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    list of str
        Base names of the files written.
    """
    with record_step(diagnostics_path(obsid, config), obsid, "combine_modules") as rec:
        return _combine_module_spectra(obsid, config, rec)


def _combine_module_spectra(obsid, config, rec):
    """The body of :func:`combine_module_spectra`, with its diagnostics record open."""
    logger = get_logger()
    outdir = nu_product_output_path(obsid, config=config)
    done_file = os.path.join(outdir, "COMBINE_DONE.TXT")
    if os.path.exists(done_file):
        logger.info(f"Modules for {obsid} already combined")
        rec.skip("COMBINE_DONE.TXT already exists")
        return []
    os.makedirs(outdir, exist_ok=True)

    found = combined_spectrum_inputs(obsid, config)
    if not found:
        # An observation with only one module, or with no pair whose extraction succeeded.
        # Nothing was produced and nothing failed.
        logger.warning(f"No pair of module spectra to combine for {obsid}")
        rec.value(spectra=[], inputs={})
        rec.skip("no pair of module spectra to combine")
        open(done_file, "w").close()
        return []

    written = []
    inputs = {}
    fractions = {}
    for suffix, spectra in found.items():
        root = f"nu{obsid}_{suffix}"
        inputs[suffix] = [os.path.basename(path) for path in spectra]
        fractions[suffix] = mode_06_exposure_fraction(spectra, obsid)
        logger.info(f"Combining {len(spectra)} spectra into {root}.pha")
        written.extend(run_addspec(spectra, outdir, root, f"_inputs_{suffix}"))

        # addspec added the two modules' exposures as though they had observed one after the
        # other. They did not. See coadd.apply_case_b_scaling.
        apply_case_b_scaling([os.path.join(outdir, root + end) for end in (".pha", "_grp.pha")], 2)
        rec.array(**spectrum_arrays(outdir, root, suffixes=ADDSPEC_SPECTRA))

    rec.value(spectra=written, inputs=inputs, mode06_exposure_fraction=fractions)
    open(done_file, "w").close()
    return written


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
        obsid=obsid,
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

    # One GOES fetch for the whole observation. Doing it per file made 91 downloads out of
    # 52 observations in the 2026 run, each one a chance to meet a VSO mirror that is down,
    # and asked GOES about CHU subsets too short to be covered at all.
    goes_gti_file = get_goes_gtis(obsid, config)

    # Source and background go through the same flare filter, so that they share one GTI.
    # Subtracting an unfiltered background from a filtered source over-subtracts: flare
    # stray light is diffuse, so it lands mostly in the large background region. On
    # 80002092008 the unfiltered background is 3.7% too high in 3--10 keV.
    for fname in source_files + background_files:
        filter_from_solar_flares(
            fname,
            goes_gti_file,
            goes_lc_file=nu_goes_lc_file(obsid, config),
            diagnostics_dir=diagnostics_path(obsid, config),
            obsid=obsid,
        )

    # barycenter_data globs the output directory rather than taking the file list, so the
    # join is a real dependency that no argument expresses.
    barycenter_data(
        obsid, ra=ra, dec=dec, config=config, wait_for=[source_future, background_future]
    )

    # ra and dec come from get_best_source_regions, and filter_from_solar_flares is a
    # subflow, which runs synchronously and raises: both dependencies already hold.
    calculate_spectra(obsid, config, ra=ra, dec=dec, goes_gti_file=goes_gti_file)

    # Its own step, with its own sentinel, so that an observation reduced before this
    # existed can be combined without redoing the extraction.
    combine_module_spectra(obsid, config)
