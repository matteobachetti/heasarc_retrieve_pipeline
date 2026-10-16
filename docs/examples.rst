Getting data, mission by mission
================================

One small observation per mission, from the query to the reduced products. Each is one the
test suite already knows, so its quirks are documented somewhere in
:doc:`technical_details`.

.. note::

   This page was written from the code and the tests, not from a fresh run of every
   example. Download sizes and run times are given only where they were measured; the
   rest will be filled in after a full run.

What every mission has in common
--------------------------------

There are two entry points, both Prefect flows in :mod:`heasarc_retrieve_pipeline.core`:

.. code-block:: python

    from heasarc_retrieve_pipeline.core import (
        retrieve_heasarc_data_by_obsid,
        retrieve_heasarc_data_by_source_name,
    )

    # One observation, or a list of them, by OBSID
    retrieve_heasarc_data_by_obsid("80002092008", mission="nustar", outdir="data")

    # Every observation pointed within radius_deg of a named source
    retrieve_heasarc_data_by_source_name("M82 X-2", mission="nustar", outdir="data",
                                         radius_deg=0.1)

The arguments they share:

``mission``
    ``nustar``, ``nicer``, ``rxte``, ``xmm``, ``chandra`` or ``xrism``
    (``fermi_gbm`` is query-only for now; see below).
``outdir``
    Where the observation is downloaded *and* reduced: everything lands in
    ``<outdir>/<OBSID>/``. It is made absolute, so the working directory does not matter.
``test=True``
    Fake the download and stop after the first observation, without reducing. The quickest
    way to check that a query finds what you expect.
``force_s3`` / ``force_heasarc``
    Where to download from: the public Amazon S3 copy of the archive (the default outside
    SciServer) or HEASARC's own HTTPS site. S3 is the better choice with several workers.
``n_workers``
    How many observations to reduce at the same time, one worker process each.
``flags`` (by OBSID only)
    Extra parameters for the mission's Level-2 pipeline -- ``nupipeline`` or ``nicerl2``.

**Mission settings.** The two flows reduce with the mission's defaults. To change one --
XMM's route, Chandra's reprocessing, RXTE's detector units, XRISM's energy band -- look the
observation up and call the worker flow, which takes a ``config`` dictionary laid over the
defaults:

.. code-block:: python

    from heasarc_retrieve_pipeline.core import (
        retrieve_and_process_data,
        retrieve_info_for_obsid,
    )

    table = retrieve_info_for_obsid("0153950401", mission="xmm")
    retrieve_and_process_data(table, mission="xmm", outdir="data",
                              config={"cameras": ["pn"]})

Pass ``source_position`` (an astropy ``SkyCoord``) to barycentre at a position of your
choosing. Without it, each observation is barycentred at its pointing -- for NuSTAR, at the
position measured from the image. For a pulsar, always give the position you searched for.

Searching by source name
~~~~~~~~~~~~~~~~~~~~~~~~

``retrieve_heasarc_data_by_source_name`` finds and reduces every observation of a source:

.. code-block:: python

    # See what would be reduced, without downloading anything
    retrieve_heasarc_data_by_source_name("M82 X-2", mission="nustar", test=True)

    # Then reduce all of it, four observations at a time
    retrieve_heasarc_data_by_source_name("M82 X-2", mission="nustar", outdir="data",
                                         radius_deg=0.2, n_workers=4)

What it does, in order:

1. **Resolves the name** with ``SkyCoord.from_name``, which asks the CDS name resolver (Sesame: SIMBAD,
   NED, VizieR).
2. **Cone-searches the mission's master catalogue** (``numaster``, ``nicermastr``,
   ``xtemaster``, ``xmmmaster``, ``chanmaster``, ``xrismmastr``) for observations whose
   *pointing* lies within ``radius_deg`` of the source, oldest first. The default, 0.1
   degrees (6 arcmin), is cautious: a source well inside NuSTAR's or XMM's field can be
   further than that from the pointing. Widen it to catch off-axis coverage.
3. **Drops rows that are plans, not observations**: no exposure, or a negative one. A
   zero exposure is also dropped for NuSTAR, where the catalogue means it, and kept for the
   other missions, where it can be wrong.
4. **Skips observations with no public data**, normally because they are still
   proprietary, and says so in the log. Encrypted data can be reduced if you have the
   passphrase: put ``<mission> <obsid> <passphrase>`` lines in
   ``~/.heasarc_retrieve_pgp_keys``, or pass ``pgp_keys_file``.
5. **Downloads and reduces** every observation found, exactly as
   ``retrieve_heasarc_data_by_obsid`` would, with one difference: every observation is
   barycentred at the **resolved position of the source**, not at its own pointing. For a
   pulsation search, this is the call to use.

It returns the catalogue table of the observations it processed. Two things it cannot do:
pass ``flags`` to the Level-2 pipeline, or a mission ``config``. For those, run the same
search as two steps:

.. code-block:: python

    from heasarc_retrieve_pipeline.core import (
        get_source_position,
        retrieve_heasarc_table_by_position,
    )

    position = get_source_position("M82 X-2")
    table = retrieve_heasarc_table_by_position(position.ra.deg, position.dec.deg,
                                               mission="xmm", radius_deg=0.2)
    table = table[table["duration"] > 10000]    # any selection, but keep the __row column
    retrieve_and_process_data(table, mission="xmm", outdir="data",
                              source_position=position, config={"cameras": ["pn"]})

The table can be filtered in any way in between, provided the ``__row`` column survives:
it is what the archive uses to find the files.

Fermi GBM is the exception: its catalogue has no positions, so ``mjd_start`` and
``mjd_stop`` are required, and the resolved position is kept for barycentring.

Every reduction also writes a record of what it did, step by step, and ``hrp-report``
turns it into an HTML page per observation.

**HEASOFT** must be set up (``HEADAS`` and ``CALDB``) for every mission: even where it
does not reduce, it barycentres or provides reference data. Without it the package still
queries and downloads, and stops there.

NuSTAR
------

The most complete path: ``nupipeline``, regions, solar-flare filtering, barycentring,
spectra. Every step is described in :doc:`technical_details`.

=================  ==============================================================
Observation        ``80002092008``, also the round-trip and split tests' observation
Needs              HEASOFT with the NuSTAR CALDB. ``pip install '.[imaging,solar]'``
Downloaded         the whole observation directory
=================  ==============================================================

.. code-block:: python

    retrieve_heasarc_data_by_obsid("80002092008", mission="nustar", outdir="data")

    # nupipeline parameters go through flags, for example
    retrieve_heasarc_data_by_obsid("80002092008", mission="nustar", outdir="data",
                                   flags={"saamode": "STRICT", "tentacle": "yes"})

What the reduction does to the data:

1. **Level 2** with ``nupipeline``: calibration, screening, cleaned event files per
   module (FPMA, FPMB) and per observing mode. An observation with neither mode 01
   (normal science) nor mode 06 (aspect from the spacecraft star trackers, usually
   thrown away) is a slew or an engineering run, and stops here as "no science data".
2. **Recovers mode 06** with ``nusplitsc``, which splits it by star-tracker combination,
   each with its own aspect reconstruction.
3. **Finds the source and its region** in the 3--79 keV image, with the extraction radius
   that maximises signal-to-noise (at most 80 arcsec; needs the ``snr`` extra), and
   **separates** each detected source and the background into their own event files.
4. **Merges** the pieces, per module with the union of their good time intervals, then
   the two modules together with their intersection, so the combined file has a
   constant collecting area.
5. **Removes solar flares**: intervals of GOES 1--8 A flux at or above C5, or HEK flares
   of that class, are cut, once per observation.
6. **Barycentres** every event file, at the given position, on DE430.
7. **Spectra** with ``nuproducts``, one per module and per mode-06 piece, each with a
   region measured in its own frame; then the two modules are **co-added** into
   ``comb01`` (mode 01 only), ``comb06`` and ``comb0106`` (both, when there is mode 06).

What lands on disk::

    data/80002092008/                joined event files, *_bary.evt, GOES curve and GTI
    data/80002092008/event_pipe/     nupipeline output, region files
    data/80002092008/split/          nusplitsc output (mode-06 sub-observations)
    data/80002092008/products/       nuproducts spectra, ARFs, RMFs

Splitting the result in time and merging observations back are done afterwards, with
``hrp-split-obsid`` and ``hrp-merge-obsids``.

NICER
-----

``nicerl2`` and barycentring, nothing more: no spectra, no background model.

=================  ==============================================================
Observation        ``1104010106``, the end-to-end test's observation
Needs              HEASOFT with the NICER CALDB
Downloaded         the whole observation directory
=================  ==============================================================

.. code-block:: python

    retrieve_heasarc_data_by_obsid("1104010106", mission="nicer", outdir="data")

    # nicerl2 screening changes go through flags
    retrieve_heasarc_data_by_obsid("1104010106", mission="nicer", outdir="data",
                                   flags={"underonly_range": "0-400"})

What the reduction does to the data:

1. **Level 2** with ``nicerl2``: calibration, merging of all seven detector groups, and
   the standard screening (orbit day/night, undershoot and overshoot rates, pointing
   offset), into one cleaned event file. ``flags`` change the screening.
2. **Barycentres** it with the observation's orbit file, on DE430.

NICER has no imaging, so there is no source separation: everything in its 3-arcmin field
is in the file.

What lands on disk::

    data/1104010106/                         the raw observation, as downloaded
    data/1104010106/l2files/                 nicerl2 output
        ni1104010106_0mpu7_cl.evt            screened events, all detectors
    data/1104010106/nicerl2_process_1104010106.log  .err

RXTE / PCA
----------

Screened, merged, barycentred PCA GoodXenon events, for timing. HEASOFT is used for
barycentring only. Not spectroscopy products: no dead-time correction, response or
background.

=================  ==============================================================
Observation        ``10408-01-05-000``, ``test_rxte.py``'s observation
Needs              HEASOFT (reference data and ``barycorr``)
Downloaded         GoodXenon events, Standard2, the filter file and the orbit
                   (about an eighth of a pointing, measured on 870 M82 pointings)
=================  ==============================================================

.. code-block:: python

    retrieve_heasarc_data_by_obsid("10408-01-05-000", mission="rxte", outdir="data")

The detector units kept are ``config["pcus"]``, PCU1-4 by default. Which units are worth
keeping depends on the date: PCU0 lost its propane veto layer on 2000-05-12 and PCU1 on
2006-12-25, and ``rxte.pcus_with_propane_veto(mjd)`` returns the right set:

.. code-block:: python

    from heasarc_retrieve_pipeline.rxte import pcus_with_propane_veto

    table = retrieve_info_for_obsid("10408-01-05-000", mission="rxte")
    pcus = pcus_with_propane_veto(table["time"][0])
    retrieve_and_process_data(table, mission="rxte", outdir="data",
                              config={"pcus": pcus})

What the reduction does to the data -- all in Python except the last step:

1. **Builds good time intervals per detector unit** from the filter file: Earth elevation
   above 10 degrees, pointing offset below 0.02 degrees, more than 30 minutes since the
   South Atlantic Anomaly, the unit switched on, and its electron ratio below 0.1. Per
   unit, because units were switched on and off at different times.
2. **Screens and merges the events**: every GoodXenon file of the pointing is read, and
   each event is kept only inside its *own* unit's good time. Times are made absolute.
   The good time of the union of units goes in ``GTI``, each unit's in ``GTI_PCU<n>``, and
   ``EXPOSURE`` is the good time actually kept.
3. **Barycentres** the events and all the interval extensions, with the pointing's orbit
   file, on DE430.

No energy calibration is applied: ``PHA`` and the mission keywords are kept, so that
Stingray and HENDRICS can convert to keV themselves.

What lands on disk::

    data/10408-01-05-000/...                    the raw pointing, as downloaded
    data/10408-01-05-000/l2_files/
        10408-01-05-000_cl.evt                  screened events, GTI and GTI_PCU<n>
                                                extensions, then barycentred

XMM-Newton / EPIC
-----------------

pn and MOS, imaging and timing, through ESA's SAS: flare screening, regions, a pile-up
measurement, spectra with responses, barycentring.

=================  ==============================================================
Observation        ``0153950401``, ``test_xmm.py``'s observation
Needs              SAS initialised (``source setsas.sh``) with a CCF mirror, and
                   HEASOFT. ``pip install '.[xmm]'``
Downloaded         PPS route: event lists, flare curves, source list and a little
                   ODF housekeeping, about a fortieth of the observation.
                   ODF route: the raw data
=================  ==============================================================

.. code-block:: python

    # The archive's own reduction (PPS), all three cameras
    retrieve_heasarc_data_by_obsid("0153950401", mission="xmm", outdir="data")

    # pn only, reprocessed from raw telemetry (ODF) with the current calibration
    table = retrieve_info_for_obsid("0153950401", mission="xmm")
    retrieve_and_process_data(table, mission="xmm", outdir="data",
                              config={"cameras": ["pn"], "products": "odf"})

The ODF route is about ten times slower. Its reason to exist is calibration, since a PPS
event list carries whatever the calibration was when it was made. Other useful keys:
``src_radius_arcsec`` (30), ``flare_rate_limit`` (``None`` means PPS's own threshold), and
``timing_src_rawx`` for timing-mode extraction.

What the reduction does to the data, for every EPIC exposure (one camera, one exposure,
one read-out mode) of the cameras asked for:

1. **Front end**: on the PPS route, the archive's calibrated event lists; on the ODF route,
   new ones from ``epproc`` and ``emproc``.
2. **Calibration index** built with ``cifbuild`` for the observation date, against the
   local calibration mirror.
3. **Flare screening**: the background light curve is cut at PPS's own threshold for that
   exposure (PPS route) or at 0.4 (pn) and 0.35 (MOS) counts/s above 10 keV (ODF route).
   A timing exposure uses the imaging curve of the same exposure where there is one.
4. **Regions**: a 30-arcsec circle and a background annulus (1.5--3 times the radius) in
   imaging mode, checked to fit on the illuminated chip; detector columns 31--45 in pn
   timing mode. MOS timing exposures are cleaned and skipped, as there is no safe default.
5. **Pile-up** is measured with ``epatplot`` and reported, never corrected.
6. **Barycentres** the cleaned and source event lists, with the PPS orbit file, on DE430.
7. **Spectra** with ``especget``: source, background, ARF and RMF, grouped to 25 counts a
   bin without oversampling the resolution by more than 3. pn, MOS1 and MOS2 are kept
   apart, to be fitted jointly.

What lands on disk, one set per exposure, named
``xmm<OBSID>_<camera>_<expid>_<mode>``::

    data/0153950401/event_cl/xmm0153950401_pn_S003_imaging_cl.evt       screened events
    data/0153950401/event_cl/xmm0153950401_pn_S003_imaging_src_bary.evt source, barycentred
    data/0153950401/event_cl/xmm0153950401_pn_S003_imaging_pat.pdf      pile-up diagram
    data/0153950401/products/xmm0153950401_pn_S003_imaging_grp.pi       grouped spectrum
    data/0153950401/products/xmm0153950401_pn_S003_imaging.arf  .rmf    responses

Chandra / ACIS and HRC
----------------------

Archive level-2 products or ``chandra_repro``, then flare screening, regions, a pile-up
measurement, spectra and barycentring, through CIAO.

=================  ==============================================================
Observation        ``5644``, ACIS, the known-answer test: M82 X-2's pulsations are
                   recovered from it. ``6298`` is the HRC-I observation the tests use
Needs              CIAO on ``PATH``, with its CALDB (and NumPy below 2.5 in the CIAO
                   environment). HEASOFT is not needed for Chandra
Downloaded         archive route: what the reduction reads, 30--40% of an observation.
                   Reprocessing route: about 78%
=================  ==============================================================

.. code-block:: python

    from astropy.coordinates import SkyCoord

    # The archive's level-2 products, barycentred at M82 X-2
    table = retrieve_info_for_obsid("5644", mission="chandra")
    retrieve_and_process_data(table, mission="chandra", outdir="data",
                              source_position=SkyCoord.from_name("M82 X-2"))

    # Reprocessed with chandra_repro and the installed CALDB
    retrieve_and_process_data(table, mission="chandra", outdir="data",
                              config={"products": "repro"})

An observation with no level-2 event list in the archive is switched to ``"repro"`` on its
own. CIAO's calibration database is found at ``$ASCDS_INSTALL/CALDB``, overriding the
HEASOFT ``CALDB``; ``config["caldb"]`` overrides both.

What the reduction does to the data:

1. **Front end**: reads the archive's level-2 event list, or the one ``chandra_repro``
   makes. Records the detector, the mode, the time resolution the data really support
   (for HRC, from the dead-time file, not the header), and how old the calibration is.
2. **Regions**: the source circle from the PSF size at 1 keV (90% of the energy), and an
   annulus 1.5--3 times as large, checked for other sources.
3. **Flare screening**: a background light curve in 200-s bins with the source cut out
   (0.5--7 keV on ACIS); bins 3 sigma above the quiescent level are removed. A cut of
   more than 10% of the exposure gets a warning, and one of more than 30% is reported
   but not applied: on Chandra, a cut that eats a good observation is the worse mistake.
4. **Cleaned events**: the whole field, with those intervals applied.
5. **Pile-up** is measured for ACIS in timed-exposure mode and reported, never corrected.
6. **Barycentres** with the ``barycenter`` package (or CIAO ``axbary``), then cuts the
   source events out of the result.
7. **Spectra** with ``specextract``, PSF-corrected, grouped to 15 counts a bin; grating
   observations get their grating products instead.

Barycentring comes before the spectrum, so an observation whose spectrum fails still has
its timing products.

What lands on disk, named ``chandra<OBSID, five digits>_<detector>_<mode>``::

    data/5644/primary/ secondary/                       the download
    data/5644/repro/                                    chandra_repro output, if run
    data/5644/event_cl/chandra05644_acis<..>_cl.evt     screened events
    data/5644/event_cl/chandra05644_acis<..>_src.evt    the source region
    data/5644/products/chandra05644_acis<..>_src.pi     spectrum, with responses

An observation taken in parts gets ``_part<n>`` appended to the stem.

XRISM / Resolve
---------------

The archive's cleaned Resolve events, merged and barycentred, for timing. Xtend is not
reduced.

=================  ==============================================================
Observation        ``101002010``, the Crab, 2024-10-03, ``test_xrism.py``'s observation
Needs              HEASOFT for the official ``barycen`` route only; the default
                   barycentring uses the ``barycenter`` package
Downloaded         the orbit file and the cleaned Resolve event files: 62 MB
=================  ==============================================================

.. code-block:: python

    table = retrieve_info_for_obsid("101002010", mission="xrism")
    retrieve_and_process_data(table, mission="xrism", outdir="data",
                              source_position=SkyCoord.from_name("Crab pulsar"),
                              config={"resolve_energy_band": [2, 10]})

``resolve_energy_band`` is ``[emin, emax]`` in keV, all energies by default; 2--10 keV keeps
877596 of this observation's 941064 events. ``resolve_itypes`` chooses the event grades, all
five by default.

What the reduction does to the data:

1. **Merges the cleaned files**: the archive writes one per filter-wheel position. They
   are joined in time, overlapping intervals are refused, and ``EXPOSURE`` is the sum.
   Pixel 12, the calibration pixel, is dropped.
2. **Selects** event grades (``resolve_itypes``) and the energy band
   (``resolve_energy_band``), if asked to.
3. **Barycentres** the merged file at the given position, on DE430.

Resolve cannot separate sources: the mirror spreads each one over most of the array. An
energy band is the only cut that helps a faint pulsar.

What lands on disk::

    data/101002010/auxil/ resolve/event_cl/      the download
    data/101002010/l2_files/101002010_rsl_cl.evt merged events, then barycentred

Fermi GBM (query only)
----------------------

GBM sees the whole unocculted sky, so its catalogue has one row per day and is searched by
date rather than by position. The reduction is not in the package yet; the query is:

.. code-block:: python

    from heasarc_retrieve_pipeline.core import retrieve_heasarc_table_by_time

    days = retrieve_heasarc_table_by_time(60391.2, 60391.8, "fermi_gbm")
    days["obsid"]          # ['20240322']: that GBM day starts at 23:59 the day before

Only days with continuous time-tagged events are returned, which means 2012-11-26 onward.

Where the settings are documented
---------------------------------

Every mission's defaults are the ``DEFAULT_CONFIG`` of its module --
:mod:`~heasarc_retrieve_pipeline.nustar`, :mod:`~heasarc_retrieve_pipeline.nicer`,
:mod:`~heasarc_retrieve_pipeline.rxte`, :mod:`~heasarc_retrieve_pipeline.xmm`,
:mod:`~heasarc_retrieve_pipeline.chandra`, :mod:`~heasarc_retrieve_pipeline.xrism` -- and the
keys shared by all of them (``barycenter_tool``, ``barycenter_ephemeris``) are in
:mod:`~heasarc_retrieve_pipeline.barycenter`. What each reduction does, and why, is in
:doc:`technical_details`; what not to trust yet is in :doc:`known_issues`.
