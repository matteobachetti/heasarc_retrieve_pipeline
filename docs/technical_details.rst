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

Each entry also carries ``path_func`` (build the archive path from the OBSID) and
``obsid_processing`` (the flow that reduces one observation).

The differences are real archive quirks, not arbitrary: NuSTAR's master catalogue reports
per-telescope exposures (``exposure_a`` is FPMA), RXTE's catalogue uses ``target_name``
rather than ``name``, and RXTE's archive is laid out by proposal cycle and proposal number
(``AO<cycle>/P<prnb>/<obsid>``) rather than by OBSID digits.

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
      AND cat.<exposure> >= 0
    ORDER BY cat.time

Notes on the astronomy encoded here:

* The cone is centred on the *source*, but ``cat.ra``/``cat.dec`` are the *pointing*
  coordinates of the observation, so the default ``radius_deg=0.1`` (6 arcmin) selects
  observations pointed within 6' of the target. For NuSTAR's 12'x12' field of view a
  source can be well inside the field while the pointing is further than 6' away, so the
  default radius is conservative and will miss serendipitous coverage. Widen
  ``radius_deg`` when that matters.
* ``exposure >= 0`` excludes catalogue rows for observations that were planned but never
  executed (these carry a null or negative exposure).
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

A diagnostic JPEG is written next to each output.

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
   signal-to-noise ratio for that profile.

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

Solar flare filtering
~~~~~~~~~~~~~~~~~~~~~

NuSTAR observes at low Earth orbit with an open detector aperture, and large solar flares
raise its background substantially. ``get_goes_gtis`` (``nustar.py:373``) builds GTIs that
exclude flare intervals:

1. The observation's ``TSTART``/``TSTOP`` (NuSTAR mission-elapsed time) are converted to
   civil time with ``nustar_gen.info.NuSTAR.met_to_time``.
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

The light curve is written to ``<root>_goes.fits``, with its ``TIME`` column converted to
the event file's mission elapsed time, so the diagnostic below plots against the same data
the cut was made on, at no extra network cost.

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
observation can. Passing ``flux_class=None`` falls back to the catalogue alone.

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
two problems above survived. So every filtered product now comes with a figure,
``<root>_flares.jpg``, written by ``plot_flare_filtering`` next to the event file --  the
same convention ``image_utils`` already uses for its image cut-outs. Three panels share one
time axis:

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

The figure is annotated with what the filtering cost and bought: events removed, live time
before and after, and reduced chi-squared against a constant before and after, for each
band. Light curves are built with ``utils.binned_lightcurve``, which gives every bin its real
exposure -- the overlap between the bin and the GTIs -- rather than assuming a full bin
width. Without that, every GTI edge produces a spurious dip that looks like source
variability.

``plot_flare_filtering`` builds its figure with ``matplotlib.figure.Figure`` rather than
``pyplot``. That is headless by construction, so there is no backend to force on a pipeline
machine, and it cannot leak a figure into pyplot's global registry -- which is the defect
issue 31 in ``known_issues.rst`` records for ``image_utils``. A failure to draw is logged
rather than raised: the science product is already on disk by that point, and a diagnostic
must not take an observation down with it.

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

Two things the diagnostic figure showed that the numbers did not, both visible on
``nu80002092008_back_flares.jpg`` as the catalogue-only cut left it:

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
observation's output directory, writing ``*_bary.evt``. The parameters are

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
spectrum level with ``addascaspec``, or load them as separate datasets and fit them jointly.


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

Every function of consequence is decorated with ``@task`` or ``@flow``. In Prefect's model a
*flow* is a unit of work that can call tasks and other flows; a *task* is an individually
tracked, retryable, cacheable step. Many tasks here also carry::

    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1000)

which asks Prefect to hash the inputs and reuse a previous result if the same inputs are
seen again within a thousand days.

In practice the package gets much less from Prefect than the decorators suggest, because
the code very often calls ``some_task.fn(...)`` instead of ``some_task(...)``. ``.fn`` is
the *undecorated* function: calling it bypasses the Prefect run, and with it the caching,
the retry logic and the run tracking. Where tasks are called normally they run
synchronously one after another (nothing is ``.submit()``-ed), so there is no concurrency
either. What remains is the structured logging through ``get_run_logger()`` and the run
names in the Prefect UI.

Idempotency is therefore achieved not by Prefect's cache but by **sentinel files** written
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


Configuration and environment
-----------------------------

There is no configuration file format. Each mission module defines its own
``DEFAULT_CONFIG`` dictionary:

.. code-block:: python

    # nustar.py
    DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./", max_radius=80)
    # nicer.py, rxte.py
    DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")

``out_data_path`` and ``input_data_path`` default to the current directory, which works
because ``retrieve_and_process_data`` ``os.chdir``-s into ``outdir`` before calling the
processing flow (and back to the original directory before the next download). All paths in
the mission modules are therefore relative to ``outdir``.

``core.read_config`` exists but is not called by anything and there is no schema for what it
would read.

Relevant environment:

``SCISERVER_USER_ID``
    If set, and neither ``force_s3`` nor ``force_heasarc`` was requested, the pipeline uses
    the ``sciserver`` local-filesystem paths.
``HEADAS`` and the rest of the HEASOFT environment
    Required for all processing. ``heasoftpy`` is imported behind a ``try``/``except`` in
    each mission module; the module-level flag ``HAS_HEASOFT`` records whether it worked.
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
