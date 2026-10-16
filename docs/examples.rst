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
