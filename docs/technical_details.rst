.. _technical_details:

Technical details
=================

This document describes what ``heasarc_retrieve_pipeline`` does, both scientifically
(which data are selected, how they are screened, what the numbers mean) and technically
(how the code is organised and what it writes to disk).

It is written to be read alongside the source. Every section names the functions it
describes, so that ``grep`` is enough to jump from here into the code.

.. note::

   The package README describes this as "an experiment of pipelines using Prefect. Not to
   be even marginally considered for production." That is an accurate self-assessment, and
   this document does not pretend otherwise: it documents the code as it is, including the
   places where it is fragile. A companion page, :ref:`known_issues`, lists the defects
   found during the review that produced this document.


Scope and intent
----------------

The package automates the boring half of a multi-mission X-ray timing project:

1. Ask HEASARC "which observations exist of this source (or this OBSID)?"
2. Fetch the raw data for each of them, from whichever of the three HEASARC mirrors is
   cheapest in the current environment.
3. Run the mission's standard Level-2 reduction on each observation.
4. Extract source and background events, merge what can be merged, remove intervals
   contaminated by solar flares, and barycentre the result.

Three missions are supported: **NuSTAR** (the most developed path, including spectral
products), **NICER** (download plus ``nicerl2`` plus barycentring), and **RXTE/PCA** (a
partial, pure-Python re-implementation of the standard screening).

The intended deployment targets are, in order of preference expressed by the code:

* **SciServer**, where the HEASARC archive is mounted as a local filesystem under ``/FTP``
  and "downloading" is a directory copy. Selected automatically when the environment
  variable ``SCISERVER_USER_ID`` is set.
* **AWS S3**, the ``nasa-heasarc`` public bucket, read anonymously. This is the default
  everywhere else.
* **HEASARC over HTTPS**, by scraping the Apache directory index. Used when
  ``force_heasarc=True``.

Steps 3 and 4 require a working HEASOFT installation with ``heasoftpy``. Without it, the
download half of the pipeline still works and the processing half raises.


Architecture
------------

Entry points
~~~~~~~~~~~~

Two Prefect flows in :mod:`heasarc_retrieve_pipeline.core` are the public interface::

    retrieve_heasarc_data_by_source_name(source, outdir, mission, radius_deg, test,
                                         force_heasarc, force_s3)
    retrieve_heasarc_data_by_obsid(obsid, outdir, mission, test, flags,
                                   force_heasarc, force_s3)

The first resolves a source name to coordinates via ``SkyCoord.from_name`` (which queries
SIMBAD/NED), cone-searches the mission's master catalogue, and processes every match. The
second looks up a single OBSID. Both converge on the same worker flow,
``retrieve_and_process_data``.

Data flow
~~~~~~~~~

::

    source name                        OBSID
         |                               |
         v                               |
    get_source_position                  |
    (SkyCoord.from_name)                 |
         |                               |
         v                               v
    retrieve_heasarc_table_      retrieve_info_for_obsid
    by_position  (ADQL cone       (ADQL exact match)
     search on master cat.)
         \                              /
          \                            /
           v                          v
            astropy Table with a __row column
                        |
                        v
              Heasarc.locate_data()          <- datalink service
                        |
              access_url / aws / sciserver
                        |
                        v
                 recursive_download          <- dispatches on URL scheme
                  /       |        \
            https://   s3://     everything else
                |         |            |
      scrape Apache   list_objects  shutil.copytree
      index + fetch   + download
                        |
                        v
              <outdir>/<OBSID>/  (raw data, mission layout)
                        |
                        v
        MISSION_CONFIG[mission]["obsid_processing"]
                        |
        +---------------+---------------+
        |               |               |
    process_nustar  process_nicer   process_rxte
      _obsid          _obsid          _obsid

The mission dispatch table
~~~~~~~~~~~~~~~~~~~~~~~~~~

``MISSION_CONFIG`` (``core.py:270``) is the single place where per-mission knowledge lives
in the download half of the code:

.. list-table::
   :header-rows: 1
   :widths: 12 18 18 20 32

   * - mission
     - catalogue
     - exposure column
     - name column
     - extra columns
   * - ``nustar``
     - ``numaster``
     - ``exposure_a``
     - ``name``
     - ``solar_activity``
   * - ``nicer``
     - ``nicermastr``
     - ``exposure``
     - ``name``
     - --
   * - ``rxte``
     - ``xtemaster``
     - ``exposure``
     - ``target_name``
     - ``cycle``, ``prnb``

Each entry also carries ``obsid_processing``, the flow that reduces one observation.

The differences are real archive quirks, not arbitrary: NuSTAR's master catalogue reports
per-telescope exposures (``exposure_a`` is FPMA), and RXTE's catalogue uses ``target_name``
rather than ``name`` and needs ``cycle`` and ``prnb`` selected alongside it.

Entries used to carry a ``path_func`` as well, building an archive path from the OBSID.
Nothing needs it: the URL of an observation's directory comes from the datalink service,
which is authoritative, while a hand-built path encodes a layout convention that can drift.
The builders and their only caller were deleted with the legacy fallback path they served.

Catalogue queries
~~~~~~~~~~~~~~~~~

Both query functions build ADQL by string formatting and send it through
``Heasarc.query_tap``.

The cone search (``retrieve_heasarc_table_by_position``, ``core.py:308``) is::

    SELECT <name> as source_name, obsid, time, <exposure>, ra, dec, __row
           [, public_date] [, <extra columns>]
    FROM public.<catalogue> as cat
    WHERE contains(point('ICRS', cat.ra, cat.dec),
                   circle('ICRS', <ra>, <dec>, <radius>)) = 1
      AND <exposure condition>
    ORDER BY cat.time

Notes on the astronomy encoded here:

* The cone is centred on the *source*, but ``cat.ra``/``cat.dec`` are the *pointing*
  coordinates of the observation, so the default ``radius_deg=0.1`` (6 arcmin) selects
  observations pointed within 6' of the target. For NuSTAR's 12'x12' field of view a
  source can be well inside the field while the pointing is further than 6' away, so the
  default radius is conservative and will miss serendipitous coverage. Widen
  ``radius_deg`` when that matters.
* The exposure condition comes from ``exposure_condition(mission)``. A null or negative
  exposure is a plan, not an observation, and is excluded for every mission. Zero is the
  interesting case, and the catalogues do not agree on what it means: ``numaster`` means
  it, so NuSTAR uses ``> 0`` and never downloads a zero-exposure observation; ``nicermastr``
  sometimes reports zero because NICER's own pipeline filtered the data wrongly, and the
  data are fine, so NICER keeps ``>= 0``. RXTE has not been checked and gets the cautious
  answer. The per-mission switch is ``MISSION_CONFIG[...]["zero_exposure_may_be_wrong"]``.
  The single-OBSID query keeps ``>= 0`` for every mission: when an OBSID has been named
  explicitly, returning nothing at all is more confusing than returning the row.
* ``public_date`` is selected for NuSTAR and NICER but not for RXTE, because ``xtemaster``
  has no such column -- all RXTE data are public.
* ``__row`` is astroquery's internal row identifier. It is what ``Heasarc.locate_data``
  needs in order to ask the datalink service where the files live, so it must survive any
  filtering the caller does on the table.

The single-OBSID query (``retrieve_info_for_obsid``, ``core.py:343``) is the same shape
with ``WHERE cat.obsid = '<obsid>'``, and additionally selects ``cycle`` for every mission
(all three master catalogues have it).

Locating the files
~~~~~~~~~~~~~~~~~~

``Heasarc.locate_data(table, catalog_name=...)`` turns the ``__row`` identifiers into a
table of access URLs, with three columns of interest:

``access_url``
    ``https://heasarc.gsfc.nasa.gov/FTP/...`` -- the public HTTPS archive.
``sciserver``
    ``/FTP/...`` -- a plain filesystem path, valid only inside SciServer.
``aws``
    ``s3://nasa-heasarc/...`` -- the public S3 mirror.

``retrieve_and_process_data`` (``core.py:432``) picks one of these by name and hands it to
``recursive_download``, which dispatches on the string: ``http`` prefix means scrape,
``s3://`` means boto3, anything else is assumed to be a local directory and is copied with
``shutil.copytree``. That last branch is how SciServer support works -- there is no special
SciServer code, just a path that happens to exist locally.

The download layer
~~~~~~~~~~~~~~~~~~

Three implementations sit behind ``recursive_download`` (``core.py:249``):

**HTTPS** (``recursive_download_https``, ``core.py:189``). ``get_remote_directory_listing``
walks the Apache directory index with BeautifulSoup, recursing into subdirectories, and
returns a flat list of URLs. Each URL is then filtered against ``re_include`` and
``re_exclude`` and passed to ``download_node``, which maps it to a local path and fetches
it with pySmartDL (a multi-threaded downloader).

The parsing itself lives in ``parse_directory_index``, which takes the page and returns the
entries, so it can be tested without touching the network. The rule is that only *relative*
links are entries: an ``href`` starting with ``?``, ``#`` or ``/``, containing ``://``, or
resolving to ``..`` is dropped. That is what separates the six real entries of a HEASARC
index page from the four column-sort links (``?C=N;O=D`` and friends) and the absolute
"Parent Directory" link that sit beside them. Directories keep their trailing slash, which
is how the caller knows to recurse into them.

**S3** (``recursive_download_s3``, ``core.py:122``). Creates an unsigned (anonymous) boto3
client, walks the bucket under the key prefix with ``get_paginator("list_objects_v2")``,
applies the same include/exclude regexes, and downloads each key. The paginator matters:
one ``list_objects_v2`` call returns at most 1000 keys, which is comfortably above a NuSTAR
observation and below an RXTE one.

Verifying what arrived
^^^^^^^^^^^^^^^^^^^^^^

Both remote transports check every file against the size the archive reports, and the check
runs on files already on disk as well as on new ones. The reason for the second half is
that a mirror is only useful if it is honest about being incomplete: a run killed
mid-transfer, or a disk that filled up, leaves a short file that every later run would
otherwise accept as finished, forever.

``file_needs_download(path, expected_size)`` holds the policy in one pure function and
returns a reason fit for a log line. Absent means fetch. Matching size means skip. A
differing size means fetch again, with a WARNING naming both numbers -- the local tree is a
mirror and the archive is authoritative, so a short file is a failed download rather than
data worth protecting. An unknown size means accept the file and say in the log that it
could not be verified.

Where the expected size comes from differs by transport, and neither costs much:

* HTTPS: ``remote_file_size`` makes a HEAD request for ``Content-Length``. Only files
  already on disk need it -- for a fresh download, pySmartDL has already fetched the header
  and ``_download_pysmartdl`` passes ``get_final_filesize()`` back with the destination.
* S3: ``Size`` arrives with every key in the listing, so the check is free.

A transfer that fails, or that lands the wrong size, raises ``RuntimeError`` and takes any
``<dest>.000``-style part files with it. ``download_node`` sets ``retries=3,
retry_delay_seconds=10``, so a transient network failure is retried and a persistent one
stops the observation instead of letting the pipeline reduce an incomplete dataset.
Aborting is safe to recover from: on the next run every verified file is skipped, and the
download picks up where it stopped.

**Local** (``copy_local_directory``, ``core.py:237``). ``shutil.copytree`` into
``<outdir>/<basename>``.

All three are arranged to produce the same local layout, ``<outdir>/<OBSID>/...``,
mirroring the archive's own directory structure below the OBSID.

Two filtering mechanisms exist and are easy to confuse:

``re_include`` / ``re_exclude``
    Regular expressions matched against the *remote* path. This is the useful one: for
    example ``re_include=r"[AB]0.*evt"`` with ``re_exclude=r"[AB]0[2-5]"`` fetches only
    NuSTAR event files from observing modes 01 and 06, skipping modes 02-05.

``test_str``
    A plain substring that must appear in the *local* destination path, defaulting to
    ``"."``. In practice this means "only fetch things with a dot in the name".

Passing ``test=True`` anywhere in the stack fakes every download: directories and log
messages are produced, but no bytes move. This is what the test suite uses.


NuSTAR
------

The NuSTAR path (:mod:`heasarc_retrieve_pipeline.nustar`) is the most complete, and the
only one that produces spectra. ``process_nustar_obsid`` (``nustar.py:729``) runs, in
order:

1. ``nu_run_l2_pipeline`` -- HEASOFT ``nupipeline``
2. ``recover_spacecraft_science_data`` -- HEASOFT ``nusplitsc``
3. ``get_best_source_regions`` -- SNR-optimised extraction regions
4. ``separate_sources`` -- image-based source/background separation
5. ``join_source_data`` -- merge event files
6. ``filter_from_solar_flares`` -- GOES/HEK flare exclusion
7. ``barycenter_data`` -- HEASOFT ``barycorr``
8. ``calculate_spectra`` -- HEASOFT ``nuproducts``

Output layout::

    <out_data_path>/<OBSID>/               joined event files, *_bary.evt, PIPELINE sentinels
    <out_data_path>/<OBSID>/event_pipe/    nupipeline output, region files
    <out_data_path>/<OBSID>/split/         nusplitsc output (mode 06 sub-observations)
    <out_data_path>/<OBSID>/products/      nuproducts spectra, ARFs, RMFs

and, one of each per observation, in ``<out_data_path>/<OBSID>/``::

    nu<OBSID>_goes.fits                    the observation's GOES X-ray light curve
    nu<OBSID>_goes.gti                     the flare-free intervals derived from it
    skipped_inputs.txt                     the inputs the reduction had to skip

Observing modes 01 and 06
~~~~~~~~~~~~~~~~~~~~~~~~~

NuSTAR event files are named ``nu<OBSID><FPM><mode>_cl.evt``, where ``<FPM>`` is ``A`` or
``B`` (the two focal-plane modules) and ``<mode>`` is a two-digit observing mode. The
module-level regular expression ::

    valid_re = re.compile(r"nu[0-9]{11}[AB]0[16].*")

accepts only modes **01** and **06**, which is the scientifically meaningful choice:

* **Mode 01** is normal science mode, where the star tracker on the optics bench (CHU4)
  provides the aspect solution. This is the bulk of the data.
* **Mode 06** is "spacecraft science": intervals where CHU4 was not available (typically
  when the Sun or Moon blinded it) and the aspect must be reconstructed from the
  spacecraft's own star trackers, CHU1/2/3. These intervals are excluded from the standard
  Level-2 products and are simply thrown away by most analyses.

Modes 02-05 are engineering/calibration modes with no usable science content.

**Slews.** Some catalogue entries are not observations at all but the satellite moving
between targets. They have an OBSID, a ``numaster`` row and real downloadable files, and
Level 2 produces only modes 02 and 03, sometimes 04. Nothing in the FITS headers marks
them. ``numaster`` does carry an observation-mode column that reads ``SLEW``, but it is
set for only a handful of the observations that really are slews; the SOC identifies the
rest by their exposure being far shorter than the observation immediately after, which is
a judgement rather than a flag -- and the pattern that a slew's OBSID ends in an odd digit
just before a much longer even-numbered one is neither necessary nor sufficient
(80002092007 ends odd and is a long, real observation).

So the pipeline decides from the data. ``observing_modes_present`` lists the modes Level 2
produced cleaned event files for, and ``has_science_data`` asks whether any of them is in
``SCIENCE_MODES = ("01", "06")``. ``process_nustar_obsid`` stops there when the answer is
no, logs the modes it did find, and returns ``utils.NO_SCIENCE_DATA``, which
``process_observations`` counts apart from the failures. It is deliberately a returned
value and not an exception: nothing went wrong, so the flow run must not end ``Failed``.

The data are left on disk. A slew's exposure sitting next to a long observation may yet be
worth joining to it for the extra hundreds of seconds, and skipping the download to save a
few minutes of computation would throw that away.

Not every observation without mode 06 is a slew, and the two must not be confused. Mode 06
exists only when CHU4 was blinded, so plenty of ordinary observations have good mode-01
science and no mode-06 at all. ``recover_spacecraft_science_data`` creates the ``split``
directory and writes its sentinel whether or not ``nusplitsc`` had anything to do, so those
observations carry on normally.

``recover_spacecraft_science_data`` (``nustar.py:231``) is where the "squeezing every
photon" comment in the code comes from: it runs ``nusplitsc`` on every mode-06 cleaned
event file, splitting it by which combination of star trackers was active (CHU1, CHU2,
CHU3, CHU1+2, ...). Each combination has its own systematic astrometric offset, so they
must be treated as separate sub-observations rather than merged blindly. The output lands
in ``split/``, and both ``split/`` and ``event_pipe/`` are fed into the later steps.

This can add of order 10-20% more exposure on a typical observation, which matters for
faint sources and for timing analyses where every photon counts. It costs astrometric
accuracy: the mode-06 sub-observations have degraded and CHU-dependent pointing.

Image-based source separation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:func:`~heasarc_retrieve_pipeline.image_utils.filter_sources_in_images` is the
scientifically most opinionated part of the package. Given one cleaned event file it
detects the sources in the field and writes one event file per source plus one background
event file. The steps:

**1. Energy filter.** Events are converted from pulse-invariant channel to energy with the
standard NuSTAR relation

.. math::

   E\,[\mathrm{keV}] = 0.04 \times \mathrm{PI} + 1.6

(NuSTAR PI channels are 40 eV wide with the first channel starting at 1.6 keV), and kept if
:math:`3 \le E < 79` keV. That is NuSTAR's nominal usable band: below 3 keV the optics
response collapses, above 78.4 keV is the iodine K-edge of the CZT detectors. Events with
``X <= 0`` or ``Y <= 0`` -- the null value for events without a valid sky position -- are
dropped. If fewer than 20 events survive, the function gives up and returns.

**2. Image.** A 100x100 two-dimensional histogram in sky ``X``/``Y``, smoothed with a
Gaussian of :math:`\sigma = 1` bin. The bin grid spans the range of the surviving events,
so its physical scale depends on the data: for a full NuSTAR field (roughly 1000 detector
pixels across) one bin is about 10 sky pixels, i.e. roughly 25 arcsec.

**3. Peak detection.** ``skimage.feature.peak_local_max`` with ``min_distance=20`` bins and
``threshold_abs = 0.5 * max(img)``. The absolute threshold means only peaks at least half
as bright as the brightest pixel in the smoothed image are considered candidates -- this is
a deliberately conservative choice that finds the target and any comparably bright
companion, and ignores everything fainter.

**4. Background level, and the detection statistic.** ``get_random_fluxes_in_img``
(``image_utils.py:58``) throws 300 circular apertures of radius 30 sky pixels at random
positions inside the field (keeping ``3 * region_size`` away from the edges) and counts the
events in each. From these 300 numbers it takes

* ``median`` -- a robust estimate of the typical counts in an aperture of that size, and
* ``std = statsmodels.robust.mad(fluxes)`` -- the median absolute deviation, rescaled by
  the usual factor :math:`1/0.6745` so that it estimates the Gaussian standard deviation.

A candidate peak is accepted as a source if its aperture count satisfies

.. math::

   \mathrm{flux} \ge \mathrm{median} + \mathrm{MAD}

This is, in effect, a **1-sigma cut**, and it is worth being explicit about what that does
and does not mean:

* It is a robust threshold in the sense that median and MAD are insensitive to the source
  apertures that inevitably land on real sources -- that is the reason for choosing them
  over mean and standard deviation.
* It is *not* a calibrated detection significance. There is no Poisson treatment, no
  correction for the number of trials (300 apertures, plus however many peaks were
  detected), and no accounting for the fact that the random apertures sample a field that
  contains the sources themselves, which biases both the median and the MAD upwards.
* In practice the real gatekeeper is step 3's ``threshold_abs = 0.5 * max``, which is far
  more restrictive than a 1-sigma flux cut. The flux cut mostly serves to reject peaks that
  are sharp in the smoothed image but contain few counts.

The consequence is that this routine should be read as "split the obviously bright things
in the field", not as a source detection algorithm with a quantified false-alarm rate. For
the intended use -- an operator who already knows there is a bright target in the field and
wants its events separated from a nearby contaminant -- that is adequate. It is not a
substitute for a proper detection tool if the question is "is there a source here?".

**5. Extraction.** Accepted peaks are sorted by aperture counts in decreasing order, so
``_src1`` is always the brightest. For each, the events within ``region_size`` (default 30
sky pixels, about 74 arcsec) of the peak are written to
``<eventfile>_src<N>.evt``. The background file ``<eventfile>_back.evt`` contains everything
*outside* ``back_region_size`` (default 55 sky pixels) of **every** detected peak, including
the ones that failed the flux cut -- so sub-threshold sources contaminate neither the source
files nor the background file.

What the separation found -- the image, the threshold, the peaks and which of them
were accepted -- is recorded rather than drawn, and the observation's page draws it; see
:ref:`diagnostics_and_reporting`.

**What these products are good for.** Timing. The source event files are plain circular
extractions with no PSF or aperture correction, no ARF, and no exposure map, and the
background file covers a large and inhomogeneous part of the detector. NuSTAR's background
is strongly position-dependent -- it is dominated below about 20 keV by "aperture stray
light", photons from sources outside the field of view that reach the detector without
passing through the optics, and that component varies across the detector plane. Using
these files for spectroscopy would be wrong; that is what the ``nuproducts`` path (below)
is for.

Region optimisation
~~~~~~~~~~~~~~~~~~~

``get_best_source_region`` (``nustar.py:537``) takes a different and more rigorous approach,
delegating to the ``nustar_gen`` package:

1. ``make_image`` produces a sky image in the requested band.
2. ``find_source`` locates the brightest source; its pixel position is converted to
   RA/Dec through the image WCS.
3. ``make_radial_profile`` builds the radial profile of the source together with the
   expected PSF profile.
4. ``optimize_radius_snr`` returns the extraction radius that maximises the
   signal-to-noise ratio for that profile, through the wrapper ``snr_optimised_radius``.

Step 4 goes through a wrapper because ``optimize_radius_snr`` steps outwards in radius and
binds its ``best_radius`` only inside ``if snr > old_snr``, with ``old_snr`` starting at
zero. On a file with no source the condition never holds and the return statement raises
``UnboundLocalError``. Reproduced directly against ``nustar_gen`` 0.8.dev9: a flat radial
profile raises it every time, with counts or without. ``snr_optimised_radius`` turns that
one exception into ``None`` -- "this file is too faint to place a region on" -- and lets
every other exception through. ``None`` is a case the callers already handle, and
they handle it differently by observing mode: ``get_best_source_regions``, which sees only
mode-01 files, raises ``NoSourceInScienceData``; ``calculate_spectra`` records the skip and
moves on. It is the same path the position-consistency check uses, and the same path
``first_source_position`` takes when ``find_source`` returns no peak at all.

The radius is capped at ``config["max_radius"]`` (default 80 arcsec). Two DS9 region files
are written next to the event file:

* ``*_src.reg`` -- a circle of the optimised radius at the detected position;
* ``*_bkg.reg`` -- an annulus, inner radius ``max(r, 100)`` arcsec, outer radius
  ``max(2r, 250)`` arcsec, concentric with the source.

A concentric annulus is a reasonable first approximation but, for the stray-light reason
above, not the recommended NuSTAR background prescription; on a 12'x12' field a 250 arcsec
outer radius also risks running off the detector or across the chip gaps.

The optimised radius, in arcsec, is converted to sky pixels in ``process_nustar_obsid`` by
dividing by **2.45**, the NuSTAR sky pixel scale in arcsec per pixel, and that pixel radius
is then used as ``region_size`` for the image-based separation described above. (The code
marks this conversion with a ``TODO``.)

Merging event files
~~~~~~~~~~~~~~~~~~~

``join_source_data`` (``nustar.py:320``) collects, for each FPM, all the per-source (or
background) event files produced from ``event_pipe/`` and ``split/``, and merges them into a
single file per FPM. Merging is done with HEASOFT rather than by hand, which matters,
because event files carry Good Time Intervals that have to be merged too:

* ``ftmgtime`` merges the GTI extensions of all input files with a logical **OR** -- the
  union, correct when combining disjoint time intervals from the same instrument;
* ``ftsort`` sorts the resulting GTIs by ``START``;
* ``fthedit`` sets ``EXTNAME = GTI`` on the result;
* ``ftmerge`` concatenates the event tables, ``ftsort`` orders them by ``TIME``;
* ``fappend`` attaches the merged GTI extension to the merged event file.

A second stage then merges the FPMA and FPMB files into a single ``nu<OBSID>_src<N>.evt``,
this time with a logical **AND** of the GTIs. That is the right choice for the combined
file: an interval is good for the combination only if both telescopes were observing, so
that a light curve built from it has a well-defined and constant effective area.

The combined A+B file roughly doubles the counting statistics, which is exactly what a
timing analysis wants. It is again unusable for spectroscopy: two telescopes with different
responses are now in one event list, and no single ARF describes it.

The intersection has to be applied to the *events* as well as to the GTI extension, and
that is a separate step. ``ftmgtime`` computes the intersection correctly, but the
``ftmerge`` on the line after it concatenates both event tables and knows nothing about it,
so a module whose good time ran a fraction of a second longer than the other's contributes
its events from that fraction of a second. Two such events out of 62 705 on observation
90901333002. ``merge_event_files`` therefore reopens the output of an ``AND`` merge and
drops the events its own GTI excludes, through ``utils.drop_events_outside_gti``. Without
that the constant effective area the intersection exists to guarantee does not actually
hold. The ``OR`` merge needs no such step: the union covers every input event already. See
issue 53 in :ref:`known_issues`, which also records why the exposure keywords of a merged
file are *not* corrected here and should not be trusted.

Both merges replace whatever is already at their output path: ``merge_gtis`` and
``merge_event_files`` each delete their own output before they start. That is what makes the
step re-runnable over a directory that still holds the previous run's products -- which is
how you force the join to run again, by removing ``JOIN_DONE_SRC<N>.TXT``. ``ftmerge`` is
called without CFITSIO's ``!`` clobber prefix and simply refuses to create a file that
exists (return code 105, ``failed to create new file (already exists?)``); the prefix is not
used because it adds a character to a path that already has to fit in 128 (see `How long a
file name may be`_). ``merge_event_files`` refuses outright to merge a file into itself, since it would
delete that input first. See issue 52 in :ref:`known_issues`.

Solar flare filtering
~~~~~~~~~~~~~~~~~~~~~

NuSTAR observes at low Earth orbit with an open detector aperture, and large solar flares
raise its background substantially. ``get_goes_gtis`` builds GTIs that exclude flare
intervals, **once per observation**:

1. The observation's time span (NuSTAR mission-elapsed time) is converted to civil time
   with ``nustar_gen.info.NuSTAR.met_to_time``.
2. ``sunpy``'s ``Fido`` queries the GOES XRS instrument for that interval, choosing the
   highest-numbered (i.e. most recent) GOES satellite that covers it.
3. The same query retrieves the **HEK flare catalogue** entries flagged by SWPC.
4. Every catalogued flare at or above ``minimum_class`` (default ``"C5.0"``) is collected,
   together with every stretch where the measured 1--8 A flux reaches ``flux_class``
   (default ``"C5.0"`` as well). The union of the two is handed to ``utils.good_intervals``,
   which returns its complement inside ``[TSTART, TSTOP]``.
5. ``nustar_gen.utils.make_usr_gti`` writes the intervals as a GTI file.

Flare classes are compared by splitting the class string into its letter and its number
(``"C5.0"`` -> ``"C"``, ``5.0``) and comparing the letters as characters. The GOES scale
runs A, B, C, M, X, which happens to be alphabetical, so a plain string comparison gives
the right ordering.

Putting the interval arithmetic in ``good_intervals`` rather than inline is what makes the
awkward cases correct: a flare that began before the observation started, two flares that
overlap, HEK rows that come back out of order. The function clips to the observation, sorts,
merges overlaps and drops empty intervals, so that its output always satisfies the three
properties a GTI list is expected to have -- positive length, sorted, disjoint. It is pure,
and its tests are offline; 80002092008's single flare falls well inside the observation, so
no real observation in hand would have exercised any of those cases.

**Why both criteria.** The HEK catalogue gives the times of *solar* flares, which are not
the times NuSTAR's background is contaminated. Its end time is when the flare ended on the
Sun; NuSTAR's background is still elevated after that, as the diagnostic figure showed on
the very first observation it was run against. Rises that never got catalogued, or that sit
just under the class cut, are not listed at all. The measured flux catches those. It has
its own blind spot -- the GOES series is sampled once a minute and carries NaN gaps, which
contribute nothing to a threshold -- and the catalogue covers those, so the two are used
together rather than one instead of the other. ``utils.intervals_above_threshold`` turns the
sampled flux into intervals, each hot sample covering half a cadence either side of itself;
``utils.merge_intervals`` merges the result with the catalogued flares.

Those sample times need one small allowance. GOES timestamps are not exactly one cadence
apart -- over 80002092008 the 1-minute series wanders by about 600 ns -- so taken literally,
one bright sample's interval can end a microsecond before the next one's begins, and a
single bright stretch comes back as several intervals divided by slivers of good time a few
tens of nanoseconds wide. ``merge_intervals`` therefore takes a ``tolerance``, and
``intervals_above_threshold`` passes a thousandth of the cadence: three orders of magnitude
above the jitter, three below any gap that could mean anything. Without it the flux cut on
that observation produced six intervals where there are physically two.

The light curve is written to ``<OBSID>/nu<OBSID>_goes.fits``, with its ``TIME`` column
converted to the observation's mission elapsed time, so the diagnostic below plots against
the same data the cut was made on, at no extra network cost.

**One fetch per observation, not one per file.** This step used to be keyed on an event
file, so every module and every CHU subset repeated the whole lookup: 91 ``goes_lightcurve``
task runs across the 52 observations of the 2026 M82 run, up to eleven for one observation,
each performing its own ``Fido.search`` and ``Fido.fetch`` and writing its own copy of the
downloaded files. Two of the eleven failures in that run were ``No online VSO mirrors could
be found``, and every extra attempt is another chance to meet a mirror that is down.

It was also wrong on its own terms. The Sun does not care which module or which CHU subset
the events came from, and a mode-06 CHU slice a few minutes long can fall entirely inside a
gap in the GOES sampling: asking about ``A06_chu3`` alone on 90201037002 returned a time
series with no rows, and astropy raised ``cannot guess format from input values with
zero-size array`` -- a message about nothing that matters.

The interval now comes from ``observation_time_span``, which reads the **mode-01** cleaned
event files and takes the widest of their ``TSTART``/``TSTOP`` and their own GTI extents,
for the reason in issue 35: ``ftmerge`` copies those keywords from its first input, so on a
merged file the header can be narrower than the data, and this GTI is later ANDed with each
file's own. An observation with no mode-01 file falls back to every cleaned event file in
the ``nupipeline`` output directory.

One guard remains for the case where GOES genuinely has nothing: ``require_goes_coverage``
raises ``NoGoesCoverage`` when the truncated series has no samples at all. That is fatal on
purpose. Keeping all the good time instead would turn the flare filtering off without
saying so, and whether an observation may be analysed that way is a scientific decision the
pipeline must not make on its own. It takes a count rather than a time series so that it is
testable without ``sunpy``, which is an optional dependency.

**The hazard of a flux threshold, and what the code does about it.** The Sun's quiescent
1--8 A flux is not a fixed number: it rises and falls with the eleven-year cycle. Near solar
maximum it sits well inside class C. In February 2014, when 80002092008 was taken, it was
around 1.5e-6 W/m2 -- mid-C1. A threshold set below that excludes the entire observation
except the sampling gaps: ``flux_class="C1.0"`` on that observation removes 54013 of 58889 s
of good time, and what survives is only where the flux is NaN. The default ``"C5.0"`` sits
above the quiescent level for any epoch, but a user lowering it has no warning from the
physics that they have crossed the line. So ``get_goes_gtis`` measures the damage before
writing anything: if the cut would remove more than half of the file's existing good time,
it logs a prominent warning naming the percentage. It still writes the GTI. A genuinely
flare-dominated observation really can lose most of its exposure, and nothing the code can
inspect distinguishes that from a threshold set too low -- only the person analysing the
observation can. Passing ``flux_class=None`` falls back to the catalogue alone. Both that
warning and the "Solar flares cover the whole of" error are now per observation, which is
what they always meant.

``filter_from_solar_flares`` then ANDs the flare GTIs with the event file's existing GTIs
and writes ``*_noflares.evt`` through ``utils.apply_gti``, which does the whole job: events
outside the surviving intervals are dropped from the event table, the new intervals replace
the GTI extension, ``ONTIME`` becomes their exact total, and ``LIVETIME`` and ``EXPOSURE``
are scaled by the ``ONTIME`` ratio.

Doing only half of that -- swapping the GTI extension and leaving the events and the header
alone, which is what this step used to do -- produces a file whose name promises one thing
and whose contents say another. On 80002092008 the "filtered" file had exactly the same
51870 events and exactly the same ``EXPOSURE`` of 33646.06 s as the unfiltered one, over a
GTI that had shrunk from 58888.6 s to 56850.9 s.

**On scaling LIVETIME.** The exact surviving live time is the integral of the instrument's
live fraction over the surviving intervals, which needs the housekeeping file. That
integral was measured on 80002092008 to check whether proportional scaling is good enough:
integrating the housekeeping live fraction over the full GTI gives 33675.99 s against a
header ``LIVETIME`` of 33646.06 s, a 0.089% difference, which is the accuracy of the
integration itself. Over the flare-free GTI, exact integration gives 32725.75 s against
32694.29 s for proportional scaling -- 0.096%, the same order. Scaling is therefore adequate,
and it keeps ``apply_gti`` independent of any mission's housekeeping file. Note the sign:
dead time is worse during a flare, so scaling proportionally very slightly *under*\ estimates
the surviving live time.

Both the source and the background go through this filter. They did not always: the
``src_num=0`` join used to run after the filtering step and be left alone, so a
background-subtracted rate mixed a filtered source with an unfiltered background. That
over-subtracts, because flare stray light is diffuse and therefore lands mostly in the large
background region rather than in the compact source aperture. Measured on 80002092008 over
the one catalogued flare window, 3--10 keV:

.. list-table::
   :header-rows: 1

   * - region
     - rate inside the flare
     - rate outside
     - ratio
   * - background
     - 0.726 c/s
     - 0.205 c/s
     - **3.54**
   * - source 1
     - 0.386 c/s
     - 0.361 c/s
     - 1.07

The flare-free GTI is also passed to ``nuproducts`` as ``usrgtifile``, so it affects the
spectral products properly.

The filtering diagnostic
~~~~~~~~~~~~~~~~~~~~~~~~

Cleaning an event file is easy to get wrong in ways that leave no trace in the output: too
little is removed, or too much, and the file looks fine either way. That is exactly how the
two problems above survived. So every filtered product is measured by
``record_flare_filtering`` and drawn on the observation's page (see
:ref:`diagnostics_and_reporting`). Three panels share one time axis:

1. the GOES X-ray flux in the 1--8 A and 0.5--4 A channels, on a log scale, with the
   A/B/C/M/X class thresholds drawn as horizontal lines and the ``flux_class`` cut marked on
   top of them, so both cuts are visible where they act -- the flux threshold as the line the
   1--8 A curve crosses, the catalogue cut as the shading it produces;
2. the event file's 3--10 keV light curve, the band solar stray light lands in, with the
   light curve before filtering in grey and the one after in colour;
3. the same in 10--79 keV. This is the control: solar flares do not produce hard X-rays at
   NuSTAR's aperture, so this panel should look the same before and after. If filtering
   visibly changes it, the cut is removing more than solar flares.

The intervals removed are shaded in all three panels -- and only the intervals this step
actually removed. ``utils.intervals_removed`` subtracts the two GTIs rather than taking the
complement of the surviving one, so Earth occultations and orbit gaps, which were never good
time to begin with, are not shaded as though the flare filter had caused them.

Alongside the panels the page states what the filtering cost and bought: events removed,
live time before and after, and reduced chi-squared against a constant before and after,
for each band. Light curves are built with ``utils.binned_lightcurve``, which gives every
bin its real exposure -- the overlap between the bin and the GTIs -- rather than assuming a
full bin width. Without that, every GTI edge produces a spurious dip that looks like source
variability.

A failure to record is logged rather than raised: the science product is already on disk by
that point, and a diagnostic must not take an observation down with it.

What the filtering measurably does
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Measured on 80002092008, which contains one catalogued M1.7 flare, excluded over a 1020 s
window (MET 129784853.8 -> 129785873.8). Light curves are 3--10 keV, 100 s bins, GTI-aware,
built with ``binned_lightcurve``; "before" is the joined product with its own GTI, "after"
is the ``*_noflares.evt`` the pipeline wrote from it.

.. list-table::
   :header-rows: 1

   * - quantity
     - background before
     - background after
     - source before
     - source after
   * - chi2/dof against a constant
     - 5.17
     - **3.62**
     - 1.23
     - 1.22
   * - fractional rms
     - 0.703
     - **0.429**
     - 0.050
     - 0.050
   * - max / median bin
     - 13.20
     - **7.29**
     - 1.45
     - 1.45
   * - mean rate (c/s)
     - 0.3892
     - 0.3755
     - 0.7087
     - 0.7086
   * - usable 100 s bins
     - 588
     - 578
     - 588
     - 578

Read the two halves of that table together. The background becomes markedly steadier -- a
third off its reduced chi-squared, nearly half off its fractional rms -- while the source is
untouched to three decimal places. That is the signature of removing background
contamination rather than discarding good source signal, and it is what makes the source
column the honest control rather than a disappointment: flare stray light is diffuse, so it
swamps the large background region and barely reaches the compact source aperture. The
filtering costs 1.7% of the live time (33646 -> 33063 s) and 1830 of 63936 background events.

**The claim must not overreach.** The background is still variable afterwards: chi2/dof 3.62
and a brightest bin seven times the median. Other backgrounds, sub-threshold flares and
Earth-limb effects are untouched by this work.

Two things the diagnostic showed that the numbers did not, both visible in the background
panels of 80002092008 as the catalogue-only cut left them:

* **The excluded window ended too early.** The 3--10 keV background peaks at 4.6 c/s, more
  than ten times its 0.35 c/s baseline, inside the shaded window -- but it was still at
  2.5, 2.0 and 1.5 c/s in the three bins *after* it. The HEK catalogue's flare end time is
  when the *solar* flare ended, not when NuSTAR's background recovered from it.
* **A second rise at the end of the observation was not excluded at all.** The GOES 1--8 A
  flux climbs back above the C5.0 level in the last few hundred seconds, and the NuSTAR
  background rises with it to about 1 c/s, with nothing shaded.

Both point the same way, and it is what the flux criterion above was added for. The
diagnostic exists to make exactly this kind of thing visible, and it did so on its first run
against real data. Adding the flux cut to the catalogue cut, same observation, same band and
binning, background region:

.. list-table::
   :header-rows: 1

   * - criterion
     - chi2/dof
     - fractional rms
     - max / median
     - good time removed
   * - none
     - 5.17
     - 0.703
     - 13.20
     - --
   * - HEK >= C5.0
     - 3.62
     - 0.429
     - 7.29
     - 1020 s (1.7%)
   * - flux >= C5.0 alone
     - 1.83
     - 0.179
     - 3.08
     - 4560 s (7.7%)
   * - HEK >= C5.0 **and** flux >= C5.0
     - **1.83**
     - **0.179**
     - **3.08**
     - 4650 s (7.9%)

Half again off the reduced chi-squared, for four and a half times the exposure. What the
flux threshold removes is contamination, and the exposure it costs was never usable
background time in the first place.

Note the third row honestly: on *this* observation the flux cut subsumes the catalogue
almost entirely, and the catalogue adds only 90 s to what the flux alone would remove. That
is not an argument for dropping it. The catalogue is what covers the flux series' own gaps
-- 95 of the 1056 samples over this observation are ``NaN``, and a threshold cannot see a
flare it has no measurement of -- and one observation is not evidence about the general
case. The two are cheap to combine and fail in different ways, which is the whole reason
for using both.

**A second observation, where the catalogue does nothing at all.** 90901333002 sits in a
period of frequent small flares: the GOES 1--8 A curve has about eight peaks across its
73.6 ks, and the HEK catalogue lists none of them at or above C5.0. Catalogue-only
filtering therefore removes one event out of 67743 -- rounding, not filtering. Two of those
peaks do cross C5.0 in the measured flux, and they are exactly where the NuSTAR background
spikes, to 1.0 and 0.6 c/s against a 0.3 c/s baseline.

.. list-table::
   :header-rows: 1

   * - criterion
     - background chi2/dof
     - background frms
     - source chi2/dof
     - good time removed
   * - HEK >= C5.0
     - 1.64
     - 0.178
     - 2.09
     - 0 s
   * - HEK >= C5.0 **and** flux >= C5.0
     - **1.22**
     - **0.070**
     - 2.10
     - 2280 s (3.1%)

The source column moves by 0.01 in reduced chi-squared, which is the control holding: 2054
source-region events go with the cut, and the source light curve does not notice. This is
the case the flux criterion exists for, and no threshold on the catalogue could have found
it.

The same figure supports the default threshold rather than a lower one. Six of the eight
GOES peaks in that observation stay below C5.0, and none of them raises the NuSTAR
background visibly. Cutting at C1.0 would have thrown away most of the observation to
remove nothing.

Barycentring
~~~~~~~~~~~~

``barycenter_data`` (``nustar.py:514``) runs HEASOFT ``barycorr`` on every event file in the
observation's output directory, writing ``*_bary.evt``. The ``barycorr`` call itself lives
in :mod:`heasarc_retrieve_pipeline.barycenter`, shared with NICER: it refuses to start with
a readable message when heasoftpy is missing, checks afterwards that ``barycorr`` actually
wrote the file rather than assuming it did, and skips a file whose output is already there
unless the caller passes ``overwrite=True``. The output name comes from
``barycentered_file_name``, which puts ``_bary`` before the extension whatever that
extension is and keeps a compression suffix last -- missions do not agree on whether an
event file is ``.evt``, ``.fits`` or ``.ds``. The parameters are

* ``ephem="JPLEPH.430"`` -- the JPL DE430 solar-system ephemeris;
* ``refframe="ICRS"``;
* ``orbitfiles`` -- the ``.attorb`` file produced by ``nupipeline``;
* ``ra``/``dec`` -- the source position.

Barycentring converts photon arrival times from the spacecraft frame to the solar system
barycentre, removing the up to ~500 s of light-travel-time modulation caused by the Earth's
and the satellite's motion. It is a prerequisite for any coherent timing analysis (pulsar
timing, orbital searches) and it is **position-dependent**: an error in the assumed RA/Dec
translates directly into a timing error, of order the source-position error in radians times
500 s.

That last point is why it matters that ``process_nustar_obsid`` overrides the RA/Dec it was
given with the position measured by ``get_best_source_regions``. When the detection is
correct this is an improvement over the catalogue pointing; when the brightest thing in the
field is not the intended target it silently barycentres to the wrong source.

Spectral products
~~~~~~~~~~~~~~~~~

``calculate_spectra`` calls HEASOFT ``nuproducts`` once per event file. Which files those
are is decided by ``spectral_input_files``: the mode-01 cleaned file from ``event_pipe`` for
each module, followed by every per-CHU mode-06 file from ``split``. Mode 01 comes first
because it defines the reference position (see below). The unsplit mode-06 file is not
included -- its aspect solution has not been reconstructed.

``infile``
    The event file, passed explicitly. Left at its default, ``nuproducts`` uses "the 01
    event file from the input directory", which is why mode-06 data was previously invisible
    to this step.
``indir``
    Always the ``event_pipe`` directory, even for a mode-06 file in ``split``: ``attfile``,
    ``hkfile``, ``mastaspectfile``, ``optaxisfile`` and ``det1reffile`` all default to "the
    file from the input directory", and those auxiliary files live there.
``stemout``
    The input file's own root, so each CHU combination gets its own output names instead of
    colliding on ``nu<OBSID><FPM>01``.
``srcregionfile`` / ``bkgregionfile``
    The DS9 regions written by ``get_best_source_region``, measured per input file and
    measured on demand if they are not already there.
``runmkarf="yes"``, ``runmkrmf="yes"``
    Generate the ancillary response (effective area, including the PSF correction for the
    chosen extraction radius and the vignetting for the source's off-axis angle) and the
    redistribution matrix.
``extended="no"``
    Treat the source as a point source, which is what makes the PSF correction valid.
``usrgtifile``
    The flare-free GTI.
``rungrppha="yes"``, ``grpmincounts=20``
    Group the spectrum to at least 20 counts per bin, the usual minimum for
    :math:`\chi^2` fitting to be approximately valid.
``grppibadlow=35``, ``grppibadhigh=1909``
    Mark channels outside PI 35-1909 as bad. Through :math:`E = 0.04\,\mathrm{PI} + 1.6`
    these are 3.0 keV and 78.0 keV -- the same band used for the image filtering, expressed
    in channels.

These products, unlike everything above, are calibrated and suitable for spectral fitting
in XSPEC.

Mode-06 spectra and the reference position
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each CHU1/CHU2/CHU3 combination produced by ``nusplitsc`` carries its own aspect
reconstruction, and the ``nusplitsc`` documentation puts the scatter between them at about
2 arcmin. Against an extraction radius capped at 80 arcsec, that is far more than the
aperture, so the mode-01 region cannot simply be reused: each CHU file needs its own
region, measured in its own sky frame. This is why ``calculate_spectra`` measures regions
per input file rather than once per module.

An independent detection per file is also a way to pick up the wrong object, so the mode-06
detections are constrained. ``position_is_consistent`` requires the source found in a CHU
file to lie within ``config["max_source_offset_arcmin"]`` -- 3 arcmin by default, chosen
against the documented 2 arcmin scatter -- of the mode-01 position that
``get_best_source_regions`` measured and ``process_nustar_obsid`` passes down. A file that
fails the check gets no region file and no spectrum, and says so in the log. Mode-01 files
themselves are unconstrained: they are what defines the reference.

The check takes a single reference position because the pipeline extracts one source, the
brightest. Extending it to several means passing the list of mode-01 positions and matching
each detected peak to its nearest one; the comparison itself does not change.

Two things follow from this ordering, and both are easy to get wrong.
``get_best_source_regions`` iterates ``mode_01_input_files``, **not**
``spectral_input_files``. First, its mean position is what ``process_nustar_obsid`` hands to
the barycentric correction, and the CHU scatter has no business in it: measured on
80002092008, averaging the eight CHU positions in with the two mode-01 ones moved the mean
by 63 arcsec. The barycentric delay is the Earth-Sun vector, about 499 light-seconds,
projected on the source direction, so 63 arcsec is worth roughly 150 ms of delay -- enough
to destroy any timing analysis downstream. Second, ``get_best_source_regions`` runs before
``calculate_spectra``, and ``get_best_source_region`` skips any file whose region files
already exist. Had it measured the mode-06 files too, their regions would already be on disk
by the time ``calculate_spectra`` looked, and the reference check above would never run at
all.

Measured offsets on 80002092008, mode 06 against mode 01 of the same module:

.. list-table::
   :header-rows: 1

   * - CHU combination
     - FPMA
     - FPMB
   * - ``chu2``
     - 0.85'
     - 0.83'
   * - ``chu12``
     - 1.21'
     - 1.25'
   * - ``chu23``
     - 1.44'
     - 1.48'
   * - ``chu3``
     - 1.77'
     - 1.77'

All well inside the 3 arcmin default, and consistent with the roughly 2 arcmin scatter the
``nusplitsc`` documentation quotes.

The resulting spectra are one set per CHU combination per module. They are **not** to be
combined by merging their event files -- the merged event files this pipeline produces are
timing-only precisely because they mix aspect solutions and exposures. Combine them at the
spectrum level -- which is what ``hrp-merge-obsids`` below does -- or load them as separate
datasets and fit them jointly.


Splitting and merging observations
----------------------------------

Two post-processing tools work on a tree the pipeline has already finished. Neither
re-runs ``nupipeline`` and neither regenerates the 68 MB responses, so neither costs
anything like the 50 minutes a reduction does::

    hrp-split-obsid  <out_data_path> <OBSID> <MJD> [<MJD> ...]
    hrp-merge-obsids <out_data_path> <OBSID> <OBSID> [...] [--name NAME]

The first cuts one observation into time segments -- for a source that changes state
part-way through. The second co-adds several observations that are each too faint to fit
on their own.

They are not equally quick, and it is worth knowing which is which before you start one.
The merge is seconds: ``addspec`` and ``grppha`` do arithmetic on files that already
exist. The split is minutes, because each ``nuproducts`` call runs ``nuexpomap`` and
``nubackscale`` -- ``runbackscale`` defaults to ``yes`` and there is no reason to turn it
off, since ``BACKSCAL`` genuinely has to be recomputed over each segment's own good time.
Measured on 90901333002, which has eight spectral input files (mode 01 and three CHU
combinations per module), one cut cost **16 minutes** for the sixteen resulting spectra:
about 90 s per mode-01 file per segment and about 35 s per mode-06 file per segment. It
scales with the number of cuts, so a three-way split of the same observation is about
three quarters of an hour. Still far short of a reduction, but not something to fire off
casually in a loop.

Time conventions
~~~~~~~~~~~~~~~~

Turning a user's MJD into the number a FITS file counts in is where a silent error would
be worst, so it lives in three small functions in ``utils.py`` -- ``time_reference``,
``met_from_mjd`` and ``mjd_from_met`` -- with their own tests. Four things they have to
get right:

**Two reference conventions.** A file states its reference epoch one of two ways, never
both:

.. list-table::
   :header-rows: 1

   * - Form
     - Who writes it
   * - ``MJDREFI`` (integer day) + ``MJDREFF`` (fraction)
     - NuSTAR, NICER, RXTE, Swift, most modern HEASARC missions
   * - ``MJDREF`` (a single float)
     - XMM, Chandra, older files

``time_reference`` accepts either and always returns the split pair; for a single
``MJDREF`` it returns ``(floor(MJDREF), MJDREF - floor(MJDREF))``. A header with neither
raises, naming what was looked for, rather than defaulting to zero and producing a time
55197 days wrong. NuSTAR repeats the keywords in every HDU including the primary while
other missions put them only in the events extension, so the lookup tries the extension
it was handed, then the primary, then any HDU that has them.

**Keep the integer and the fraction apart.** ``met_from_mjd`` computes

.. code-block:: python

    ((mjd - mjdrefi) - mjdreff) * 86400.0     # yes
    (mjd - (mjdrefi + mjdreff)) * 86400.0     # no

The second form collapses the reference into one float before subtracting, and
``55197.000766...`` in a float64 has about a nanosecond-of-a-day of resolution left over
after the integer part. Measured over 500 epochs against exact decimal arithmetic, the
split form stays within 2 units in the last place -- at worst 43 nanoseconds -- while the
collapsed form has a **floor** of 140 to 200 nanoseconds that does not shrink as the
answer gets smaller. It makes no difference to a split at MJD 56689 and it costs nothing,
but this is a package people do timing with. (For a file that carries only a single
``MJDREF``, the precision is gone before we see it and nothing can recover it.)

**Which scale the answer is on.** The FITS convention is
``absolute = MJDREF + (TIMEZERO + TIME)/86400``, so ``met_from_mjd`` returns a time on the
``TIME + TIMEZERO`` scale -- the scale ``read_gti`` and ``apply_gti`` already work on. The
helpers take no ``TIMEZERO`` term of their own and everything downstream goes through
those two functions. This is worth stating because it is the trap: NuSTAR has no
``TIMEZERO`` at all, so the distinction is invisible here, but RXTE does and some NICER
releases carry ``TIMEZERO = -1.0``. ``TIMEZERO`` is also per-HDU, and the events and GTI
extensions may legitimately disagree; ``apply_gti`` reads them separately and the split
code does not undo that.

**TT versus UTC.** NuSTAR's ``TIMESYS`` is ``TT``, so MJD 56689 in this arithmetic means
MJD 56689 *TT*, which is 67.184 s earlier than MJD 56689 UTC. A split time read off a
light curve labelled in civil time and fed in naively lands a minute out. The default is
to read the given MJD in the file's own ``TIMESYS``, since that is what round-trips
through ``mjd_from_met``; ``--utc`` reads it as UTC and converts. The tool logs the
resolved MET and the civil date it corresponds to, so a wrong choice is visible before
anything is written.

Splitting: ``segments.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~

``N`` split times give ``N + 1`` segments, ``seg1`` ... ``seg<N+1>`` in time order. The
numbering never shifts: a split time outside the observation leaves an *empty* segment
rather than renumbering the others, because ``seg2`` has to mean the same stretch of the
observation no matter what else was asked for.

Everything is written into the parent's own tree, with the tag last::

    <OBSID>/products/nu<OBSID>A01_sr_seg1.pha     source spectrum
    <OBSID>/products/nu<OBSID>A01_bk_seg1.pha     background spectrum
    <OBSID>/products/nu<OBSID>A01_grp_seg1.pha    grouped, the one you fit
    <OBSID>/nu<OBSID>A_src1_bary_seg1.evt         event list

Living in the same directory as the parent is not cosmetic. ``RESPFILE``, ``ANCRFILE`` and
``BACKFILE`` are bare file names that XSPEC resolves relative to the spectrum's own
directory, so a segment sitting next to its parent points at the parent's 68 MB ``.rmf``
with no copy, no symbolic link and no path rewriting.

The spectra come from ``nuproducts``, not from editing the parent's ``COUNTS`` column.
``calculate_spectra`` already passes ``usrgtifile`` on every call -- the flare-free GTI --
so handing it a *segment's* GTI instead is the whole of the spectral split, and the
segments are consistent with the unsplit products by construction rather than by this
module reimplementing the region and grade filters. ``runmkarf=no runmkrmf=no`` skips the
slow part; ``lcfile``, ``bkglcfile`` and ``imagefile`` are ``NONE``; ``phafile``,
``bkgphafile`` and ``grpphafile`` are named explicitly, which is where the layout above
comes from with no renaming pass. ``stemout`` carries the tag too, so that the plot
``nuproducts`` insists on writing (``plotdevice`` has no "off" setting) cannot land on the
parent's; it is deleted afterwards, in the spirit of commit 710c1d5.

Each segment's GTI file is a **copy of the parent's** ``<root>_noflares.gti`` with only
its table rows replaced, by ``intersect_intervals`` of that GTI with the segment bounds.
Copying rather than building one from scratch keeps ``MJDREFI``, ``TIMESYS``, ``TIMEUNIT``
and everything else exactly as HEASOFT wrote it. A segment whose intersection is empty is
skipped and recorded.

**The first and last segment are open-ended, and that matters.** ``segment_bounds`` is
called with ``-inf`` and ``+inf`` rather than with the observation's ``TSTART`` and
``TSTOP``. Only the cut times the caller asked for carry information; where the data begin
and end is a different answer for every file, and each file's bounds are intersected with
its own GTI anyway, so an open outer edge and that file's real edge give the same result.
Closing them on the observation span does *not*. ``observation_time_span`` comes from the
mode-01 event file, and the mode-06 per-CHU files that ``nusplitsc`` writes are not
confined to it: on 90901333002 one CHU combination starts 150 s before the first mode-01
event and two others run 800 s past the last. Clamping every file to the mode-01 span
quietly deleted that good time -- 720 s of exposure on the worst file, with the segments'
counts failing to add up to the parent's -- while the mode-01 spectra themselves
partitioned perfectly, so nothing looked wrong unless you checked mode 06. The regression
test in ``TestSegmentsSpanEachFile`` builds a CHU file that overruns both ends and asserts
its segments' good time adds back up.

One consequence: the recorded ``bounds`` in the diagnostics carry ``null`` for those open
edges. JSON has no infinity, and ``json.dumps`` would otherwise write a bare ``Infinity``
that Python reads back happily and nothing else does.

Regions are the parent's, looked up next to the event file. If they are missing the file
is skipped and the reason recorded -- deliberately, rather than re-measuring: a segment
must use the parent's region or the ARF it is about to reuse is wrong.

Event lists are split in pure astropy, no HEASOFT: ``apply_gti`` already drops the events,
rewrites the GTI extension and rescales ``ONTIME``/``LIVETIME``/``EXPOSURE``. It does not
touch ``TSTART``/``TSTOP``/``TELAPSE`` or the ``DATE-*``/``MJD-*`` keywords, which after a
time cut would be plainly wrong, so ``utils.update_time_bounds`` does that beside it. It
narrows only keywords that were already there, subtracts each HDU's own ``TIMEZERO``, and
writes the dates in the file's own ``TIMESYS`` -- for NuSTAR that is TT, so a ``DATE-OBS``
from this pipeline is a TT date, exactly as HEASOFT's own are.

**The caveat worth stating.** Reusing the parent's ARF for a time segment assumes the
effective area did not change across the split. The ARF folds in the vignetting for the
source's off-axis angle and the PSF correction for the extraction radius, both averaged
over the exposure. Within a single pointing these are effectively constant, so the
approximation is good -- but it is an approximation, and it would break down where the
aspect wanders, which is exactly what mode 06 is. If a segment's spectrum ever looks
suspicious, regenerating its response with ``runmkarf=yes`` is a one-flag change and the
honest cross-check.

Merging: ``combine.py``
~~~~~~~~~~~~~~~~~~~~~~~

Output goes to a **new sibling tree**, ``<out_data_path>/<NAME>/`` (default
``merged_<first>_<last>``). That choice is what gets the merged dataset a report page for
free: ``observation_directories`` treats any subdirectory with a ``diagnostics/`` in it as
an observation, so writing records through ``record_step`` there needs no change to
``report.py`` at all.

Spectra are grouped by focal-plane module -- A with A, B with B -- across all input
OBSIDs, and go through ``addspec`` with ``qaddrmf=yes`` (exposure-weights the ARFs and
RMFs into one response) and ``qsubback=yes`` (combines the backgrounds), then ``grppha``
with the pipeline's usual ``group min 20 & bad 0-34 & bad 1910-4095``.

Only files ending ``_sr.pha`` are inputs. The anchor on the end matters: it is what keeps
a ``_sr_seg1.pha`` left by ``hrp-split-obsid`` out of a merge, which would otherwise count
the same photons twice.

By default this includes mode 01 *and* the mode-06 CHU spectra, because ``addspec``
weights their different responses correctly and the mode-06 exposure is a large part of
why they are extracted at all. ``--mode01-only`` restricts to normal science.

``addspec`` is run from a **staging directory**, which looks like needless copying and is
not. It works around one specific ``addspec`` bug. To co-add the backgrounds, ``addspec``
builds a ``mathpha`` expression out of the ``BACKFILE`` values and spawns it -- but,
unlike the expression it builds for the source spectra, it does not quote the operands::

    mathpha "expr='/path/nu..._sr.pha'+'/path/nu..._sr.pha'"             quoted, fine
    mathpha "expr=(/path/nu..._bk.pha*31.5)+(/path/nu..._bk.pha*31.5)"   not quoted

``mathpha`` reads the second as arithmetic, so every ``/`` in the path becomes a division
operator and the run dies on ``fitsio 4.060 error message: could not open the named
file``. A ``BACKFILE`` must therefore carry no directory at all, and being in the right
directory is then the only way to say which file is meant.

The staging is no wider than that constraint, which was established by experiment rather
than assumed: with only ``BACKFILE`` made bare, ``addspec`` completes and builds its
``.rsp`` while the list file holds absolute paths and ``RESPFILE``/``ANCRFILE`` are
absolute too. So each spectrum is copied -- the originals are never touched -- with
``BACKFILE`` reduced to a bare name and ``RESPFILE``/``ANCRFILE`` made absolute, and only
the background spectra are linked in beside it. The 68 MB ``.rmf`` files are neither
copied nor linked.

Changing the working directory is otherwise forbidden in this package, and
``test_prefect_wiring`` enforces that by walking the AST for ``os.chdir``. The one
exception is listed there with this reason. It holds ``HEASOFT_LOCK`` for the duration, so
no other HEASOFT call in the process can see the directory move, and ``hrp-merge-obsids``
is post-processing on a finished tree rather than a step inside the reduction flow.

Afterwards the outputs are moved into ``products/`` and the staging directory is removed;
the list file is kept there as ``<NAME>_<FPM>_inputs.lis``, the record of what was
co-added.

Event lists are merged by ``merge_event_files``, which already handled arbitrary lists of
paths -- only its callers were OBSID-scoped -- with ``gti_operation="OR"``. Only the
**barycentred** files are merged: the days-long gap between two observations shows up as a
gap in the merged GTI, which is correct and is what any downstream timing code should see,
but only once the times are on a common inertial clock.

Checked against HEASOFT on two copies of a real reduced observation: for both modules the
merged ``COUNTS`` array equals the sum of the inputs channel for channel, and ``EXPOSURE``
equals their sum.

Both tools and the file-name limit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nu_longest_output_name`` still bounds the tree. The longest name either tool builds is
``<OBSID>/products/nu<OBSID>A06_chu123_N_grp_seg1.pha``, 60 characters after the output
root, against the reduction's own champion at 61 -- so the 128-character check the flow
makes before it starts still covers everything written afterwards. ``TestLongestOutputName``
asserts it, so it stays that way.


NICER
-----

:mod:`heasarc_retrieve_pipeline.nicer` is much shorter. ``process_nicer_obsid``
(``nicer.py:151``) runs ``nicerl2`` and then barycentres.

``ni_run_l2_pipeline`` (``nicer.py:69``) invokes ``nicerl2`` as an external command through
``subprocess.run`` rather than through ``heasoftpy``, capturing stdout and stderr to
``nicerl2_process_<OBSID>.log`` and ``.err`` in the output directory, and raising if the
return code is non-zero. (It still checks ``HAS_HEASOFT`` first, as a proxy for "is HEASOFT
set up at all".) The parameters are the defaults plus ``clobber=True`` and ``chatter=5``;
any user ``flags`` are merged in, which is how per-observation screening changes (for
example a relaxed undershoot or overshoot cut) get through.

``nicerl2`` performs the standard NICER screening: calibration, per-MPU event merging,
and the good-time selection on orbital day/night, undershoot and overshoot rates, pointing
offset and so on. Its cleaned output is the single merged file
``ni<OBSID>_0mpu7_cl.evt`` -- "0mpu7" means all seven Measurement/Power Units, i.e. all 52
active detectors combined.

``barycenter_data`` (``nicer.py:130``) barycentres that file against the orbit file
``auxil/ni<OBSID>.orb.gz``, using the shared implementation in
:mod:`heasarc_retrieve_pipeline.barycenter` (same DE430/ICRS parameters as NuSTAR).

NICER has no imaging capability -- it is a collimated instrument with a roughly 3 arcmin
field of view -- so there is no source separation step and no equivalent of the NuSTAR
image analysis. Everything within the field of view is in the event file.

The NICER path performs no spectral extraction and produces no background model. NICER
background is not measurable from the data themselves (there is no off-source region), and
requires one of the community background models (``nibackgen3C50``, the "space weather"
model, etc.); none is invoked here.


RXTE / PCA
----------

:mod:`heasarc_retrieve_pipeline.rxte` does not use HEASOFT at all. It re-implements a
subset of the standard PCA screening in astropy. ``process_rxte_obsid`` (``rxte.py:167``)
runs three steps.

**1. ``setup_workspace`` (``rxte.py:38``).** Finds an event-mode file by searching, in
order, for ``GX*.evt.gz``, ``SE*.evt.gz`` and ``FS*.evt.gz`` anywhere under the
observation directory, and gunzips the first match. If nothing matches, the observation is
skipped with a warning -- this is the guard against binned-mode-only observations, which
this code cannot process. The prefixes correspond to RXTE's data-mode file naming:
GoodXenon, Science Event, and the standard FITS science files respectively.

**2. ``create_gti_with_astropy`` (``rxte.py:68``).** Reads the standard filter file
``stdprod/*.xfl.gz``, which contains housekeeping quantities sampled every ``TIMEDEL``
seconds (16 s for RXTE), and builds good time intervals from three conditions:

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - condition
     - meaning
     - why
   * - ``ELV > 10``
     - Earth elevation angle above 10 degrees
     - Below this the target is too close to the Earth's limb and the atmosphere
       contributes X-ray absorption and albedo background.
   * - ``OFFSET < 0.02``
     - pointing offset below 0.02 degrees
     - The PCA collimator response falls off over about a degree; 1.2 arcmin keeps the
       effective area essentially constant and rejects slews and unsettled pointing.
   * - ``NUM_PCU_ON > 0``
     - at least one Proportional Counter Unit active
     - There are no counts at all when every PCU is off.

Contiguous runs of good samples are collapsed into intervals; each interval starts at the
``Time`` of its first good sample and stops at the ``Time`` of its last good sample plus one
``TIMEDEL``. The intervals are written as a FITS ``GTI`` extension with
``TELESCOP = 'XTE'``.

This is a **reduced** version of the standard PCA screening. What it omits, and why that
matters:

* **South Atlantic Anomaly.** Standard screening requires ``TIME_SINCE_SAA > 30`` minutes
  (or at least excludes the passage itself). Without it, intervals of strongly elevated and
  rapidly decaying particle background are kept.
* **Breakdown / electron-ratio cuts.** Standard screening cuts on ``ELECTRON2 < 0.1``
  (per-PCU electron ratios), which flags detector breakdown events.
* **Which PCUs.** ``NUM_PCU_ON > 0`` records *how many* PCUs were on, not *which*. Since
  the number of active PCUs changed during most observations and no per-PCU event
  selection is applied, the effective area varies within the resulting GTIs. A count rate
  from these products is therefore not proportional to flux, and no valid response matrix
  can be built for them.
* **Deadtime.** No deadtime correction information is propagated.

**3. ``apply_gti_with_astropy`` (``rxte.py:123``).** Reads the event file, forms absolute
event times as ``TIME + TIMEZERO``, marks every event that falls in any GTI, and writes the
surviving events to ``l2_files/<OBSID>_cl_evt.fits``.

Two things to know about the output: the GTI extension is **not** copied into the cleaned
file, and the exposure keywords in the events header are inherited unchanged from the
unfiltered file. So the file records which photons survived but not how long the instrument
was actually collecting, and any rate computed from its header will be too low.

RXTE data are also not barycentred by this pipeline, unlike NuSTAR and NICER.

**What these products are good for.** A first look at the event list of a
single event-mode file, and nothing more. They are not calibrated, not deadtime-corrected,
carry no response, and represent only one of the event-mode files that an observation may
contain (GoodXenon observations always have two, ``GX1`` and ``GX2``, which must be merged;
only the first is used here).


Orchestration with Prefect
--------------------------

In Prefect's model a *flow* is a unit of work that can call tasks and other flows; a *task*
is an individually tracked, retryable, cacheable step. Many tasks here also carry::

    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000)

which asks Prefect to hash the inputs and reuse a previous result if the same inputs are
seen again within a thousand days.

Four rules decide what is decorated, and they are worth stating because the package spent a
long time without them (issues 15--20 in :doc:`known_issues`):

**Flows are entry points.** ``retrieve_heasarc_data_by_obsid``,
``retrieve_heasarc_data_by_source_name`` and the per-mission ``process_*_obsid`` flows. A
subflow call is synchronous and raises on failure; there is no ``flow.submit()`` in Prefect
3.8.4.

**Tasks are steps that do work** -- run a HEASOFT tool, download a file, read or write data.
These are the steps a person watching a run wants to see, and the only ones worth a cache
lookup or a retry.

**Path and name builders are plain functions.** Inserting ``_bary`` before an extension is
not a step. When these were tasks they were 43% of the task runs of a real observation, and
the interesting work was lost among them.

**Never call a task through ``.fn`` in production code.** ``.fn`` is the undecorated
function: it bypasses the run, the run name, the cache and the retries. It is the right
thing to use *in tests*, where calling the undecorated function is the standard way to unit
test a task. Where a task must recurse, the recursion goes into a plain helper and the task
stays as its entry point -- ``walk_remote_directory`` under
``get_remote_directory_listing`` -- so that a whole tree is one task run.

One rule about dependencies. ``wait_for`` accepts futures, and passing it a bare function
object does nothing at all. When a real ordering constraint has to be declared -- the
dependent step does not consume the upstream result, so no argument states it -- the
upstream is ``.submit()``-ed and the future passed, **and the future is always resolved with
``.result()``**. This is not redundant: measured on Prefect 3.8.4, a downstream call whose
``wait_for`` future failed is skipped, returns ``None``, and leaves the flow run
**COMPLETED**. Only ``.result()`` re-raises and fails the run. The ``.result()`` gives
fail-fast; the ``wait_for`` gives the edge in the graph.

Two AST guards in ``tests/test_prefect_wiring.py`` keep both rules from drifting: every name
in a ``wait_for`` list must come from a ``.submit()``, and every ``task_run_name`` template
must name real parameters of the function it decorates.

Idempotency is achieved not by Prefect's cache but by **sentinel files** written
into the output tree:

.. list-table::
   :header-rows: 1
   :widths: 32 68

   * - file
     - written by
   * - ``event_pipe/PIPELINE_DONE.TXT``
     - ``nu_run_l2_pipeline`` / ``ni_run_l2_pipeline``
   * - ``split/RECOVER_DONE.TXT``
     - ``recover_spacecraft_science_data``
   * - ``<dir>/SEPARATE_DONE.TXT``
     - ``separate_sources``
   * - ``JOIN_DONE_SRC<N>.TXT``
     - ``join_source_data``
   * - ``products/PRODUCTS_DONE.TXT``
     - ``calculate_spectra``

Each step checks for its sentinel and returns early if it exists. Several steps also check
for the existence of their output file directly (``barycenter_file``, ``get_goes_gtis``,
``filter_from_solar_flares``, ``get_best_source_region``).

The sentinels record only *that* a step ran, not with which parameters. Re-running an
observation with different ``flags``, a different ``minimum_class`` or a different region
size will not invalidate them; the output directory has to be deleted by hand.

Removing a sentinel by hand does make its step run again, and the step must survive finding
its own outputs already in place. ``join_source_data`` is the one that had to be fixed for
this (issue 52); the others either write through ``clobber="yes"`` or delete first.


What a HEASOFT tool says it produces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A zero return code is not evidence that a file was written. ``ftmgtime`` handed an empty
list of input GTIs exits 0 and writes nothing at all; ``ftsort``, told to sort a file that
was never created, then fails with ``PIL ERROR PIL_BAD_FILE_ACCESS`` and return code 33.
The message names the wrong tool, and two observations of the 2026 run were lost with it.

So ``heasoft.run`` and ``heasoft.run_task`` take a **required keyword-only** ``produces``,
and check it inside the lock as soon as the return code has been accepted:

* a path, or a list of paths -- each must exist and be non-empty;
* a directory path -- must exist and hold at least one entry;
* ``heasoft.IN_PLACE(path)`` -- for a tool that edits a file that was already there
  (``fthedit``, ``fappend``): the file must still exist and still not be empty.

A leading ``!``, HEASOFT's clobber marker, is not part of the name and is stripped before
checking. Failure raises ``RuntimeError`` naming the tool and the path, in the same voice
as the return-code error next to it.

It is required rather than optional on purpose: every one of the twelve call sites has a
nameable output, and a caller who has to write it down cannot forget the lesson above. An
AST guard in ``tests/test_prefect_wiring.py`` keeps it that way, and a signature check in
``tests/test_heasoft.py`` keeps the argument mandatory.

The one place this can newly fail is ``nuproducts``. If a spectrum has too few counts for
``rungrppha`` to write the grouped file, an observation that used to pass in silence now
raises. That is the intent, and it is the thing to watch in the next cluster run.

Inputs the reduction had to skip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two outcomes are deliberately different:

* **A mode-01 module with no usable source fails the observation.** Mode 01 is ordinary
  science with the full aspect solution from CHU4. A target the pipeline was pointed at
  has to be in it, and half an observation delivered quietly is worse than a failure that
  says why. ``get_best_source_regions`` raises ``NoSourceInScienceData`` when a mode-01
  file yields no region, and ``join_source_data`` raises it when a module has mode-01
  cleaned events and yet nothing to merge. An observation with no mode-01 file *at all* is
  a different case -- 80002092003 is one -- and stays a clean zero.
* **An unusable mode-06 CHU subset is skipped, and the observation still counts as
  reduced.** Each CHU combination is a few minutes of exposure with its own reconstructed
  aspect, and some of them genuinely hold no detectable source.

Every skip of the second kind is recorded in ``<out_data_path>/<OBSID>/skipped_inputs.txt``
by ``utils.record_skipped_input``, so that a run can be audited without reading a 40 MB
cluster log::

    # Inputs skipped while reducing 90202038002
    nu90202038002A06_chu1_N_cl.evt  no usable extraction region could be measured

Only the **base name** is recorded, never a full path: worker processes see the output tree
through a ``short_workspace`` symbolic link under ``/tmp`` whose name is different on every
run, so an absolute path recorded today means nothing tomorrow. The file is read, added to
and replaced whole -- ``tempfile.mkstemp`` in the same directory, then ``os.replace``,
under a module-level lock -- so a reader never sees it half written and two tasks skipping
at the same time cannot lose one of the two records. Recording the same pair twice leaves
one line, which makes a resumed run idempotent. ``process_observations`` names the
observations whose report is not empty at the end of its tally.

Running several observations at once
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``retrieve_and_process_data`` takes ``n_workers``. Whatever its value, the observations are
handed to a ``ProcessPoolTaskRunner``: one observation is one **process**, which downloads
its data and reduces it from beginning to end. With the default ``n_workers=1`` that is a
pool of one, so the ordinary single-observation run travels exactly the same code path as a
parallel one and there is no second path to keep working.

**Why a process and not a thread.** ``heasoftpy`` runs each tool as a subprocess and reads
and rewrites ``$PFILES/<tool>.par`` around every call. ``PFILES`` is an environment
variable, which belongs to the process; threads of one process cannot each have their own.
Measured with 200 ``ftlist`` calls, eight at a time:

.. list-table::
   :header-rows: 1
   :widths: 30 28 20

   * - eight at a time
     - ``PFILES``
     - failures
   * - threads
     - shared ``~/pfiles``
     - 19 / 200
   * - processes
     - shared ``~/pfiles``
     - 9 / 200
   * - processes
     - one directory each
     - **0 / 200**

The failures are ``parameter file .../ftlist.par not found``: one call deletes and rewrites
the file another is in the middle of reading.

**What a worker sets up.** ``core.prepare_worker(pfiles_root, work_root)`` runs once per
worker process, at the top of ``download_and_process_observation``, and is idempotent (a
module-level ``_WORKER_DIRECTORY`` guard) because Prefect 3.7 has no pool ``initializer``.
It claims ``<pfiles_root>/worker_<pid>/pfiles`` through ``heasoft.use_private_pfiles``,
which sets ``PFILES`` to ``"<private>;$HEADAS/syspfiles"`` -- the semicolon separates the
writable copy from the read-only system one -- creates ``<work_root>/worker_<pid>/``, and
``chdir``-s into it. The two roots are on different filesystems by design; see `How long a file name may
be`_. The ``chdir`` is the **only** one in the package, and a test asserts it stays that
way: the working directory is a property of the whole process, so using it to steer where a step writes is what made concurrent
observations impossible before. Every path a step is given is absolute --
``utils.absolute_config`` resolves the configuration once, at the start of a reduction --
and the process working directory exists to catch the scratch files HEASOFT tools drop
around themselves. Most carry the tool subprocess's PID -- ``86758tmp_gti.fits``,
``87340_tmp_nuexpomap`` -- but not all: ``xselect`` writes ``xsel_timefile.asc``, and two
workers sharing a directory destroy each other's.

**And keeps hold of it.** Setting ``PFILES`` once turned out not to be enough. In the 2026
reprocessing of 56 NuSTAR observations of M82, of 1016 ``fthedit`` calls a handful resolved
their parameter file to the shared ``$HOME/pfiles`` instead of the worker's private one, and
lost ``heasoftpy``'s own check-then-open race in ``HSPTask.find_pfile`` -- the file existed
when it was looked for and was gone when it was opened. Seven observations died that way,
five on ``fthedit.par`` and two on ``nuproducts``' ``extractor.par``. The messages appear
under all four worker PIDs, so this is not one worker that started without an environment.

What puts ``$HOME/pfiles`` back has not been pinned down. ``$HEADAS/BUILD_DIR/headas-setup``
forces it to the front of ``PFILES`` whenever HEASOFT is initialised, and ``heasoftpy``
splits ``PFILES`` on ``:`` as well as ``;``, so anything re-initialising HEASOFT inside the
process would do it. Rather than guess, ``heasoft.run`` and ``heasoft.run_task`` call
``_hold_on_to_private_pfiles`` inside the lock, immediately before invoking the tool:
``heasoftpy`` re-reads ``os.environ["PFILES"]`` on every call, so restoring it there is
early enough. The cost is a string comparison against a subprocess that runs for seconds.
The first repair in each process is logged with the value that was found, so the next run
says what did it.

**Inside one observation, one HEASOFT tool at a time.** The steps of a single reduction
still run in threads of the worker process, and they share that process's ``PFILES``. So
every HEASOFT call in the package goes through :mod:`heasarc_retrieve_pipeline.heasoft`,
which holds a module-level lock for the duration::

    heasoft.run("ftmerge", infile=..., outfile=..., copyall="NO")
    heasoft.run_task("nupipeline", **params)

A test in ``tests/test_heasoft.py`` reads every module's AST and fails if anything outside
that module calls ``heasoftpy`` directly. The lock costs nothing measurable: a HEASOFT tool
is an external process doing seconds to minutes of work, and two of them within one
observation have nothing to gain from overlapping.

``heasoft.run`` also **raises on a non-zero return code**. ``heasoftpy`` defaults to
``allow_failure=True``, so a tool that fails comes back as an ordinary result and the
caller carries on with a file that was never written. That is how ``ftsort`` came to fail on
every single run of ``merge_gtis``, unnoticed, for as long as the code existed.

**What each observation gets to itself.** GOES solar X-ray files are fetched into the
observation's own directory (``goes_download_path``) rather than sunpy's shared download
directory, where two observations from the same day would ask for the same file and one
could be handed it while the other was still writing it. Diagnostics go to
``<OBSID>/diagnostics/``, one file per writer, so no two of them ever touch the same path;
the plotting library that used to be the other piece of shared state here -- ``pyplot``,
with its process-wide figure registry and global backend -- is gone from the package
altogether.

**What is still shared, and what to do about it.** By default each worker process starts a
temporary Prefect server of its own, and all of them write one SQLite file. Measured with
``n_workers=4``: **five** temporary servers, one per worker plus the parent, and
``sqlite3.OperationalError: database is locked`` from the server's own telemetry service.

Give the whole run one server instead::

    export PREFECT_HOME=/somewhere/local/prefect_home
    export PREFECT_SERVER_DATABASE_CONNECTION_URL="sqlite+aiosqlite:///$PREFECT_HOME/run.db"
    export PREFECT_SERVER_ANALYTICS_ENABLED=false
    export PREFECT_SERVER_DATABASE_TIMEOUT=60

    prefect server start --host 127.0.0.1 --port 4277 &
    until curl -sf http://127.0.0.1:4277/api/health >/dev/null; do sleep 2; done

    export PREFECT_API_URL=http://127.0.0.1:4277/api
    python run_the_pipeline.py

With ``PREFECT_API_URL`` set, the workers connect as clients: measured, zero temporary
servers and no lock errors. ``PREFECT_SERVER_ANALYTICS_ENABLED=false`` turns off the
telemetry heartbeat, which is a pure-noise writer to the same database. Both settings exist
under these names in Prefect 3.7 and 3.8.

``PREFECT_HOME`` should be on **local** disk. SQLite locking over NFS or Lustre is
unreliable, so a database under a network-mounted home or scratch directory can report
"database is locked" no matter how few writers there are.

**Measured, three at a time.** Three real reductions of NuSTAR observation 90901333002 from
the join step through spectra, in three worker processes: with a private ``PFILES`` and
working directory each, 3 of 3 completed with no failed task run, 1433 s each, and all three
trees ended with identical output (97 product files, 80 split files, the same event counts,
merged GTIs present and sorted). With a shared parameter directory and a shared working
directory, 2 of 3 completed and 3 task runs failed -- one worker's ``xselect`` session was
offered as the default for another's, the mission was guessed as ``SUZAKU``, and the run
died on ``Cannot open xsel_timefile.asc``. Running three at once costs nothing per
observation; getting it wrong costs a whole observation and says nothing about why.

**Two consequences for callers.** A process pool re-imports the entry point in each worker,
so a parallel run must be started from a real script guarded by
``if __name__ == "__main__":`` -- a script piped through stdin dies with
``BrokenProcessPool``. And when several workers download at once the S3 mirror serves them
better than the HEASARC web server; S3 is the default, and asking for ``force_heasarc``
together with ``n_workers > 1`` logs a warning.

How long a file name may be
~~~~~~~~~~~~~~~~~~~~~~~~~~~

HEASOFT has two separate file-name limits, and the reduction can run into either.

**160 characters, in the old Fortran ftools**, whose file-name parameters are
``character*160``. Measured with ``fappend``: an output path of 160 characters works, 161
fails with ``could not open the named file``, status 104.

**128 characters, in some builds of** ``xselect``. Measured on a cluster run of 56
observations: 2376 messages of the form ``Error determining file type for <path>``, every
one of them exactly 128 characters long against real paths of 130. The tool says nothing
about having truncated anything; it reports "The file was not found" about a file that
exists, and ``save events`` emits a shell command whose closing quote has been cut off.
The same test on macOS with the same ``XSELECT V2.5c`` succeeded at 140 characters, so
this is a property of the build rather than of the tool.

``xselect`` resolves the directory it *reads* from -- it prints the real path in
``Data Directory is:`` even when handed a symbolic link -- but takes output names exactly
as given. The read side was measured good to 247 characters, so the constraint is on the
write side alone.

**What the pipeline does about it.** ``utils.short_workspace`` makes a private temporary
directory (``tempfile.mkdtemp``: unpredictable name, owner-only, so nothing can be planted
at that path on a shared node) and puts a symbolic link to the output directory inside it.
The flow hands the workers the link, so the output root is about fifteen characters however
deep the real tree is. The bytes never move, and the link is removed at the end of the run;
cleanup unlinks symbolic links and nothing else, so it cannot delete an output tree.

Measured with the tool that was failing: a real output tree 80 characters deep, reached
through a 15-character link, ran ``nusplitsc`` to ``Exit with success``, and the files
appeared in the real tree. Then end to end, on NuSTAR observation 80202020006 in a real
output tree 73 characters deep: the reduction saw an 18-character root, the longest name
it built was 76 characters against 131 without the link, ``nusplitsc`` and the merging
step (``ftmerge``, ``ftmgtime``, ``ftsort``, ``fappend``) both succeeded, and the six
per-CHU event files came out with the same event counts and the same exposure, to the
microsecond, as a reduction of the same observation through the real path.

The workspace also has to survive being where the temporary directory is long.
``tempfile.gettempdir()`` honours ``TMPDIR``, which on macOS is 48 characters under
``/var/folders``; ``short_workspace`` therefore takes the shortest writable choice among
that and ``/tmp``, and accepts an explicit ``tmpdir`` when neither is suitable.

The budget, if the link cannot be made and the real path is used: the reduction adds **61
characters** after the output root, so against the 128-character limit 67 characters are
left for the root. ``utils.check_name_length`` raises before anything is downloaded when
it does not fit, naming the path and both lengths; ``nustar.nu_longest_output_name`` is
what it checks, and a test pins that as longer than every other name any step builds.

The longest name is the mode-06 one, and it was found by walking two finished output
trees rather than by reading the code:

.. code-block:: text

    61  <OBSID>/split/nu<OBSID>A06_chu123_N_cl_3to80keV.fits   xselect, via make_image
    60  <OBSID>/split/nu<OBSID>A06_chu123_N_cl_3to80keV.log    the same call's log
    60  <OBSID>/split/nu<OBSID>A06_chu123_N_cl_noflares.gti    mgtime
    60  <OBSID>/split/sci_xrsf-l2-avg1m_g15_d<DATE>_v2-2-1.nc  sunpy, not HEASOFT
    58  <OBSID>/split/nu<OBSID>_chu123_merge_<pid>.fits        nusplitsc

Three things stack up in the winner. ``nusplitsc`` splits ``SCIENCE_SC`` data by
star-tracker combination and ``chu123`` is the longest of those; the split file keeps the
``_cl`` of the file it came from; and ``nustar_gen``'s ``make_image`` appends
``_<elow>to<ehigh>keV``. It is also the name most worth protecting, because ``make_image``
hands it to ``xselect`` as a ``save image`` argument -- the write side, which is the side
that truncates. The energy band lives in ``nustar.IMAGE_ELOW`` and ``nustar.IMAGE_EHIGH``
so that the default and the length check cannot drift apart; a test asserts they are the
same numbers ``get_best_source_region`` defaults to.

The GOES light curve in that table is longer than the ``nusplitsc`` temporary but is a
``sunpy`` download that no HEASOFT tool ever sees, so the limit does not apply to it.

One thing the guard does **not** cover, because it is not a file name: ``merge_gtis``
hands ``ftmgtime`` an ``ingtis`` parameter that is a comma-separated list of two paths
with ``[GTI]`` filters on them. Measured in a real run through the link, that string is
**152 characters**. It worked, and the limits documented here are per file name rather
than per parameter, so there is no reason to think it is a problem -- but it is the first
place to look if a run through a longer root fails in ``merge_gtis`` rather than in
``xselect``.

**Two kinds of scratch state, in two places.** A worker keeps two things nobody wants
afterwards, and they want opposite filesystems, so ``short_workspace`` hands back a
directory for each and ``prepare_worker(pfiles_root, work_root)`` takes both.

*Parameter files* are the small, hot half. One ``nupipeline`` run spawns at least 44
sub-tools, and ``heasoftpy`` reads and rewrites ``<PFILES>/<tool>.par`` around every one of
them. On the shared filesystem each of those was a network round trip for a few hundred
bytes -- metadata-bound work, which is the case a parallel filesystem handles worst and
local disk handles best. (A whole reduced observation is 352 files, median 31 kB, 79% of
them under 1 MB, so the same argument applies to the outputs; they have to be kept, which
settles where they go.) ``pfiles`` therefore goes in the same temporary directory as the
link, on local disk, and costs kilobytes.

*Working directories* are the large, cold half. HEASOFT scripts drop bulky temporary trees
beside themselves, and on NuSTAR observation 80202020006 -- 32.6 ks, 202 MB of raw input --
one worker's working directory peaked at **182.5 MB**, the largest contributor being
``<pid>_tmp_nucoord``. That is about 90% of the raw data size, and it scales with it, so
``n_workers`` full-length observations want gigabytes. The cluster this pipeline runs on
has 7.9 GB free on a ``/tmp`` that is part of a root filesystem already 85% full and shared
with every other job on the node, which is the wrong place to spend a gigabyte per worker.
``work`` therefore defaults to ``<outdir>/.workers``, on the filesystem that has room, and
the ``scratch_dir`` argument to ``retrieve_and_process_data`` moves it to a faster disk
where one exists.

Neither placement can strand a result: nothing in the package writes an output to a bare
relative name, and the HEASOFT tools address files inside the working directory by relative
name, so the working directory needs neither a short path nor a durable one. Both
directories are removed at the end of the run, and cleanup removes only directories
``short_workspace`` created -- a ``scratch_dir`` shared with another run survives.

**Deciding where to point** ``scratch_dir``. ``tools/fs_benchmark.sh`` measures candidate
filesystems on the profile HEASOFT actually produces -- many small files and many metadata
operations -- rather than on bandwidth, which is not the binding constraint here::

    srun -n1 bash tools/fs_benchmark.sh /scratch/your/project /tmp

It times creating, looking up and deleting 2000 files and writing and reading 400 files of
32 kB, the measured median size in a reduced observation. The per-file microsecond figures
are the ones to compare. All the per-file loops use shell builtins, and the two steps with
no portable builtin fork once for the whole batch: a fork costs about a millisecond, which
is longer than most of the operations being timed and would otherwise make every
filesystem look identical. On the local APFS disk this was written on: 62 us to create,
9 us to look up, 59 us to delete. If a shared filesystem is within a factor of a few of
that, leaving the working directories where they are costs little.
Whether it is worth copying whole observations to local disk and back is a separate
question, and one to settle by measuring the two filesystems rather than by argument.


.. _diagnostics_and_reporting:

Diagnostics and reporting
-------------------------

Every reduction step records what it did, and the run turns those records into one HTML
page per observation plus an index over the whole run. The pages are self-contained files
you open from disk; there is no server, and nothing has to be running for them to work.

Before this there was no way to *see* an observation. One reduction left 32 loose JPEGs
across three directories, a ``skipped_inputs.txt``, several ``*_DONE.TXT`` markers and a
log; a run of 56 left about 1800 of those images, none of them next to the numbers that
produced them, and three ``logger.info`` lines to summarise everything.

What is on disk
~~~~~~~~~~~~~~~

::

    <out_data_path>/index.html                            the run
    <out_data_path>/plotly.min.js                         4.9 MB, written once
    <out_data_path>/<OBSID>/diagnostics.html              the observation
    <out_data_path>/<OBSID>/diagnostics/manifest.json     the catalogue row
    <out_data_path>/<OBSID>/diagnostics/observation.json  the overall outcome
    <out_data_path>/<OBSID>/diagnostics/<step>__<key>.json
    <out_data_path>/<OBSID>/diagnostics/<step>__<key>.npz

``heasarc_retrieve_pipeline.diagnostics`` writes the records; ``report`` reads them and
builds the pages. The record layer imports nothing from the rest of the package beyond
``utils.get_logger`` -- ``image_utils`` imports *it* -- and carries no Prefect decorators,
because it has to be callable from a ``finally`` block after a task has already failed.

A record is a JSON file and, when there are arrays, an ``.npz`` sibling::

    with record_step(directory, obsid, "separate_sources", key=root) as rec:
        rec.value(threshold=..., n_peaks=...)       # JSON scalars
        rec.array(image=img, xbins=..., ybins=...)  # numpy -> the .npz
        rec.skip("fewer than 20 events passed the energy and position filter")

with fields ``obsid``, ``step``, ``key``, ``status``, ``reason``, ``error``,
``traceback``, ``started``, ``started_iso``, ``duration_s``, ``values`` and ``arrays``.

There are four statuses. ``running`` is written on entry and replaced on exit, so a run
killed mid-step still names the step it died in. ``done`` and ``skipped`` -- with a
human-readable ``reason`` -- are the two ordinary outcomes; ``rec.skip`` does not raise.
``failed`` carries the error and its traceback, and the exception is **re-raised**:
``process_observations`` still has to count the failure.

Why files, and why one file per writer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Observations are reduced in separate processes, so no in-memory accumulator can span
them, and nothing a task returns may change type -- it has to pickle. Files on disk are
the only collection point that works.

Inside one observation the steps are threads, and two ``join_source_data`` tasks run at
once. The invariant that makes that safe is **one file name per writer**: every record is
``<step>__<key>``, unique to the thing being recorded, so there is never a read-modify-write
and never a lock. (``record_skipped_input`` needs a lock precisely because it rewrites one
shared file.) Both files are written to a temporary name and moved into place with
``os.replace``, so a reader never sees half a record.

There is no process-global "current observation". That would be another
``_WORKER_DIRECTORY``, and it would be wrong under the thread pool the tests use, where
several observations share a process. The directory is passed explicitly as
``diagnostics_dir``, and ``None`` means record nothing -- which is what every direct call
from a test does. A function that is handed one *file* rather than an observation --
``get_best_source_region``, ``filter_sources_in_images``, ``record_flare_filtering`` --
takes a ``rec`` instead, defaulting to ``no_record()``.

Three records, not one
~~~~~~~~~~~~~~~~~~~~~~

Three things on disk describe what a run did, and they do not overlap:

.. list-table::
   :header-rows: 1

   * - record
     - where
     - what it owns
   * - ``skipped_inputs.txt``
     - ``<OBSID>/``
     - which *inputs* were skipped, and why
   * - step stamps
     - ``<OBSID>/.steps/``
     - what a step *produced*, with its CALDB and version provenance
   * - diagnostics
     - ``<OBSID>/diagnostics/``
     - a step's *status*, timing and numbers

The step stamp cannot absorb the diagnostics record, and the reason is its own design.
It is written only on success -- that invariant is what lets a rerun walk backwards
through the stamps -- so it can never carry a failure or a traceback. And it is read on
the hot path of every rerun, so it must stay small rather than hold a 50 kB light curve.
The report reads all three and degrades gracefully: the stamps are optional, and an
observation with a manifest and nothing else is listed as never having started.

What the pages show
~~~~~~~~~~~~~~~~~~~

The observation page opens with the catalogue row -- target, position, exposure, dates,
observing cycle -- then a timeline of every step with its status, duration and skip
reason, then a section per step:

* the **sky image** the separation worked on, with the detected peaks and the acceptance
  and exclusion circles drawn on it, in sky ``X``/``Y`` pixels;
* the **radial profile** with the PSF profile beside it and the chosen ``rlimit`` marked;
* a **GTI chart** for the join: one row per input file, then the OR-merged row per
  telescope, the AND-merged A+B row, and the flare-filtered row, so it is visible which
  input cost which good time;
* the **flare panels** described under "Solar flare filtering" above.

The sky image and the region files are in different frames -- pixels with no WCS on one
side, ICRS degrees on the other -- and come from different steps, so they are two figures
rather than one overlay.

The index lists every observation with its outcome, target, exposure, wall-clock time,
how many steps it skipped or failed and how many inputs it had to skip, each row linking
to its page; above the table, the run as one timeline, showing which observations actually
overlapped and where the failures fell.

The two named failures are first-class: ``NoSourceInScienceData`` names the telescope and
file that had no source in it, and ``NoGoesCoverage`` is a distinct outcome rather than a
generic crash.

How it is hooked in
~~~~~~~~~~~~~~~~~~~

The manifest is written in ``process_observations`` before anything is submitted, so a run
killed after the first observation still has a manifest for every one.
``download_and_process_observation`` wraps its whole body in an ``observation`` record and
writes the page in a ``finally``, which is the only place that knows both the OBSID and
the output directory *and* still runs when the observation raises. That write has its own
``try/except`` that logs: a reporting failure must never turn a good observation into a
failed one, or replace the exception that was already on its way up. The index and the
plotly bundle are written at the head and tail of ``process_observations``, inside
``short_workspace``, so the bytes land in the real tree while the symlink is still alive.

And everything can be rebuilt from what is on disk::

    python -m heasarc_retrieve_pipeline.report <out_data_path>

That needs no list of what the run meant to do -- it finds the observations by looking --
which is the difference between a crashed run leaving forty unreachable pages and one you
can browse.

Building the pages
~~~~~~~~~~~~~~~~~~

Plotly, with a hand-written ``string.Template`` shell rather than jinja2, and imported
inside the figure builders so that ``import heasarc_retrieve_pipeline`` still works
without it. Four measurements shaped the rest:

* ``to_html(..., include_plotlyjs="directory")`` emits a **bare** ``src="plotly.min.js"``
  with no directory part, and ``to_html`` never copies the bundle -- only ``write_html``
  does. A page at ``<OBSID>/diagnostics.html`` would point at a file nothing creates. So
  each figure is rendered with ``include_plotlyjs=False`` and the shell carries one
  ``<script src>`` with the relative path we compute.
* The bundle is 4.9 MB. It is written once at the run root and shared, never inlined.
* ``template="none"`` takes an empty figure from 7224 characters to 644 -- about 100 kB of
  theme boilerplate saved per page.
* dtype drives page size linearly, so arrays are cast before they reach plotly: a
  100x100 image is 110.7 kB as float64, 55.9 as float32 and 27.1 as uint16.

The figure data is inline in the page, deliberately. Moving it to sidecar files the page
fetched would be smaller, but ``fetch()`` against ``file://`` is blocked, and these pages
are opened as files.


Configuration and environment
-----------------------------

There is no configuration file format. Each mission module defines its own
``DEFAULT_CONFIG`` dictionary:

.. code-block:: python

    # nustar.py
    DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./", max_radius=80)
    # nicer.py, rxte.py
    DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")

``out_data_path`` and ``input_data_path`` default to the current directory. They are not
left that way: ``utils.absolute_config`` resolves both against the process working directory
once, at the start of each reduction, so that every path handed to a step is absolute and
means the same thing wherever it is used. The pipeline used to ``os.chdir`` into ``outdir``
instead, which made every relative path depend on when it was read and made two concurrent
observations impossible.

``core.read_config`` exists but is not called by anything and there is no schema for what it
would read.

Relevant environment:

``SCISERVER_USER_ID``
    If set, and neither ``force_s3`` nor ``force_heasarc`` was requested, the pipeline uses
    the ``sciserver`` local-filesystem paths.
``HEADAS`` and the rest of the HEASOFT environment
    Required for all processing. ``heasoftpy`` is imported behind a ``try``/``except`` in
    :mod:`heasarc_retrieve_pipeline.heasoft`, the one module that invokes it; the flag
    ``HAS_HEASOFT`` records whether it worked.
``PFILES``
    Set per worker process by ``core.prepare_worker``, inside the local temporary
    directory of ``utils.short_workspace``. Do not set it by hand for a parallel
    run: a shared parameter directory is what the private one exists to avoid.
``CALDB``
    Required by ``nupipeline``, ``nicerl2``, ``nuproducts`` and ``barycorr``.

Runtime dependencies beyond the standard scientific stack: ``prefect``, ``astroquery``,
``boto3``, ``pySmartDL``, ``beautifulsoup4``, ``scikit-image``, ``statsmodels``, and -- for
the NuSTAR spectral path -- ``sunpy``, ``nustar_gen`` and ``regions``.


Testing
-------

The test suite is :mod:`heasarc_retrieve_pipeline.tests.test_pipeline`: four test
functions, all marked ``@pytest.mark.remote_data``, all parametrised over the ``heasarc``
and ``aws`` hosts.

Three of them call the top-level flows with ``test=True``, so they exercise the catalogue
query, the datalink lookup and the download dispatch, but fake the actual transfer and
never reach the processing code. The fourth, ``test_recursive_download``, does a real
download of two NuSTAR event files and checks the include/exclude filtering.

Nothing runs without network access, and nothing exercises the mission processing modules,
the image analysis or the RXTE screening. See :ref:`known_issues` for a proposed offline
test suite.
