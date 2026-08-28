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

**S3** (``recursive_download_s3``, ``core.py:122``). Creates an unsigned (anonymous) boto3
client, lists the bucket under the key prefix, applies the same include/exclude regexes,
and downloads each key.

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
4. Every catalogued flare at or above ``minimum_class`` (default ``"C5.0"``) is cut out:
   the good intervals are the gaps between flares, from ``TSTART`` to the first flare
   start, between each flare end and the next flare start, and from the last flare end to
   ``TSTOP``.
5. ``nustar_gen.utils.make_usr_gti`` writes the intervals as a GTI file.

Flare classes are compared by splitting the class string into its letter and its number
(``"C5.0"`` -> ``"C"``, ``5.0``) and comparing the letters as characters. The GOES scale
runs A, B, C, M, X, which happens to be alphabetical, so a plain string comparison gives
the right ordering.

Note that although the GOES X-ray light curve is downloaded, the filtering uses only the
HEK *catalogue* of flare start/end times, not the light curve itself. A threshold on the
measured GOES flux would be a more direct proxy for the background actually seen by NuSTAR.

``filter_from_solar_flares`` (``nustar.py:458``) then ANDs the flare GTIs with the event
file's existing GTIs and writes ``*_noflares.evt``, in which the GTI extension has been
replaced. The event table itself is copied unchanged, so downstream tools must honour the
GTIs; and the exposure keywords in the header are *not* recomputed.

The flare-free GTI is passed to ``nuproducts`` as ``usrgtifile``, so it does affect the
spectral products properly.

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

``calculate_spectra`` (``nustar.py:662``) calls HEASOFT ``nuproducts`` once per FPM, with:

``srcregionfile`` / ``bkgregionfile``
    The DS9 regions written by ``get_best_source_region``.
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
