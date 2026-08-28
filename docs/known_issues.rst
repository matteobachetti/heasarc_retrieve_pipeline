.. _known_issues:

Known issues and roadmap
========================

This page records the defects found in a full read of the package, ranked by severity, plus
a proposal for a test suite and CI setup proportionate to the size of the project. It is a
companion to :ref:`technical_details`, which describes how the code is *meant* to work.

Line numbers refer to the state of the code at the time of writing and will drift.

Nothing here is a criticism of the project's ambition. The README is explicit that this is
an experiment; the point of the list is to make the sharp edges visible before somebody
runs an analysis on the output.


Blocking
--------

.. _issue_locate_data:

1. ``Heasarc.locate_data`` currently returns nothing, and the code cannot survive it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Verified live against the HEASARC service with astroquery 0.4.11.*

HEASARC's datalink service used to label the row pointing at an observation's data
directory with ``content_type='directory'``. It now returns ``content_type='text/html'``.
``astroquery.heasarc.Heasarc.locate_data`` keeps only rows where
``content_type == 'directory'`` or ``error_message != ''``, so it returns an **empty
table** for every query.

``retrieve_and_process_data`` (``core.py:466``) then evaluates ``links[i][link_col_name]``
and raises ``IndexError`` on the first observation.

The ``try``/``except`` fallback in ``retrieve_heasarc_data_by_source_name``
(``core.py:500``) does not help, for two reasons: ``locate_data`` does not raise, it
returns an empty table; and the fallback itself is broken (issue 2).

Suggested fix: stop relying on the ``content_type`` label. Fetch the datalink table and
select the row whose ``access_url`` contains ``/FTP/``, or upgrade astroquery once
upstream has adapted. Either way, ``retrieve_and_process_data`` should check
``len(links) == len(result_table)`` and match rows by ``__row``/``ID`` rather than by
position -- the datalink service returns several rows per input row, in no guaranteed
order, so positional indexing was never safe.

2. The legacy fallback path cannot run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``retrieve_heasarc_data_by_source_name_old`` (``core.py:385``) is what the code falls back
to when astroquery fails, and it has at least four independent faults:

* ``remote_data_url`` (``core.py:32``) calls ``MISSION_CONFIG[mission]["path_func"]`` with
  four positional arguments ``(obsid, time, cycle, prnb)``. No path function accepts that
  signature: ``nu_heasarc_raw_data_path(obsid, **kwargs)`` raises ``TypeError``, and
  ``rxte_heasarc_raw_data_path(obsid, cycle=None, prnb=None)`` silently receives ``time``
  as its ``cycle``.
* Those path functions are Prefect ``@task`` objects, being called outside a flow.
* It iterates over ``results["cycle"]`` and ``results["prnb"]``, which
  ``retrieve_heasarc_table_by_position`` only selects for RXTE -- ``KeyError`` for NuSTAR
  and NICER.
* It is a ``@task`` that calls other tasks (``get_source_position``), which Prefect does
  not support.

Suggested fix: delete it. The archive-path builders it depends on duplicate what the
datalink service provides, and keeping a fallback that has never been exercised is worse
than having none.

3. ``calculate_spectra`` runs ``nuproducts`` outside its own loop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:662``. The structure is::

    for fpm in "A", "B":
        infiles = glob.glob(...)
        for infile in infiles:
            ...
            break                      # <- always breaks on the first iteration
        logger.info(f"... {infile}")    # <- outside the loop, uses the leaked variable
        params = dict(...)
        hsp.nuproducts(params, ...)

Consequences:

* If ``infiles`` is empty for FPMA, ``infile`` is undefined and the task raises
  ``NameError``.
* If ``infiles`` is empty for FPMB only, ``infile`` still holds FPMA's file, and FPMB's
  spectrum is extracted from FPMA's events with FPMB's ``stemout`` -- a wrong result that
  looks right.
* ``src_reg``, ``bkg_reg`` and ``outfile_gti_temp`` are set on the first iteration and
  reused for FPMB. For the region files this is harmless (they are in sky coordinates), for
  the GTI file it is not: FPMB gets FPMA's flare-free GTI.

Suggested fix: put the ``nuproducts`` call inside the loop, drop the ``break``, and reset
``src_reg``/``bkg_reg`` per file.

4. ``get_best_source_regions`` can return ``(0, 0, 0)``, barycentring to RA=0, Dec=0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:629``. The function accumulates ``mean_ra``/``mean_dec``/``mean_rlimit`` only
for files whose region files do **not** already exist. If every region file exists,
``count`` stays 0 and the function returns ``(0.0, 0.0, 0.0)``.

``process_nustar_obsid`` (``nustar.py:742``) assigns that straight into ``ra``, ``dec`` and
``region_size``, so a re-run of an already-processed observation would:

* barycentre every event file to RA = 0, Dec = 0 -- a timing error of up to 500 s, silently;
* set ``region_size = 0 / 2.45 = 0``, extracting empty source regions.

This is currently masked by a second bug in the same function:
``root_name = rootname(infile)`` is computed from the *full path*, then joined again onto
``outdir``, so the existence check tests a doubled path (``./OBSID/event_pipe/./OBSID/event_pipe/nu...``)
that never exists. Two bugs cancelling is not a fix -- correcting the path bug alone would
activate the RA=0 bug.

Suggested fix: read the existing region files back with ``regions.Regions.read`` (the
single-file function ``get_best_source_region`` already does exactly this) instead of
skipping them, and use the basename consistently when constructing region paths.


Correctness
-----------

5. ``filter_from_solar_flares`` does not filter events or exposure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:476`` replaces ``hdul[2].data`` with the flare-free GTI and writes
``*_noflares.evt``, but copies the event table unchanged and does not update ``EXPOSURE``,
``ONTIME`` or ``LIVETIME``. The name promises a filtered event file; the content is an
unfiltered event file with a narrower GTI. Any tool that computes a rate from the header,
or that ignores GTIs, gets the wrong answer.

It also assumes the GTI lives in extension 2. Look it up by ``EXTNAME`` instead.

6. ``join_source_data``: brittle FPM substitution and an inconsistent return value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:363``: ``b_file = a_file.replace("A", "B")`` replaces *every* ``A`` in the
path. Any output directory containing a capital A -- a source name, a user directory --
produces a nonexistent filename. Use the FPM loop variable to build both names.

``nustar.py:404`` vs ``nustar.py:437``: on the cached path the function returns
``glob.glob(f"nu{obsid}*{label}.evt")``, which matches the per-FPM files *and* the combined
file; on the fresh path it returns only the combined files. ``process_nustar_obsid`` feeds
that list to ``filter_from_solar_flares``, so a re-run silently processes three times as
many files as a first run.

7. RXTE cleaned event files carry no GTI and a stale exposure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``rxte.py:123``. The output ``<OBSID>_cl_evt.fits`` contains the surviving events, a
``GTI_FILE`` header keyword naming the GTI file, and the *original* header -- with the
original ``EXPOSURE``/``ONTIME``. The GTI extension itself is not appended.

Any rate derived from this file's header underestimates the flux by the ratio of screened
to unscreened exposure. Append the GTI extension and recompute the exposure keywords from
the GTI total.

8. RXTE uses only the first event file it finds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``rxte.py:51``. ``setup_workspace`` breaks out of the pattern loop at the first match and
then uses ``event_gz_files[0]``. GoodXenon observations always produce **two** files
(``GX1_*`` and ``GX2_*``) which have to be merged; observations often contain several
event files covering different time ranges. Silently keeping one of them discards data
without a warning.

9. ``recursive_download_s3`` never paginates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``core.py:151``. ``list_objects_v2`` returns at most 1000 keys per call and sets
``IsTruncated``; the code reads ``response["Contents"]`` once and stops. Any observation
with more than 1000 files is silently truncated. Use
``s3_client.get_paginator("list_objects_v2")``.

10. The HTTPS scraper reads link text instead of ``href``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``core.py:67``: ``file_name = i.extract().get_text()``. *Verified against a real HEASARC
index*: this yields five spurious entries per directory -- ``Name``, ``Last modified``,
``Size``, ``Description`` (the column-sort links) and ``Parent Directory``.

They cause no damage today only because the default ``test_str="."`` rejects names without
a dot. Pass ``test_str=None`` and the downloader will try to fetch
``.../Parent Directory``. ``Parent Directory`` is also a latent recursion hazard.

Use ``i.get("href")``, skip anything starting with ``?`` or ``/``, and skip ``../``.

11. ``download_node`` reports success after a failed download
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``core.py:110``. ``download_cmd`` swallows every exception and returns ``(None, message)``;
``download_node`` logs a warning and then returns ``local_ver`` regardless. The caller
cannot distinguish a downloaded file from a missing one, and the pipeline proceeds to
process an incomplete observation. Return ``None`` on failure, or raise and let Prefect's
``retries=`` handle it (no task in the package sets ``retries``).

There is also no checksum verification against the archive.

12. ``get_goes_gtis`` can emit negative-length GTIs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:373``. A flare that starts before ``TSTART`` but ends during the observation
passes the ``flare_start >= tstop or flare_end <= tstart`` guard, and then appends
``{"START": tstart, "STOP": flare_start}`` with ``flare_start < tstart``. The HEK rows are
also assumed to be sorted in time and non-overlapping; neither is guaranteed. Clip flare
intervals to ``[tstart, tstop]``, sort them, merge overlaps, and drop empty intervals.

Separately: the GOES X-ray light curve is downloaded (``Fido.search`` /
``Fido.fetch``, and a ``goes_lc_file_name`` task exists) but never used -- the filtering
runs entirely off the HEK flare catalogue. Either use the light curve with a flux
threshold, which is a more direct proxy for NuSTAR's background, or drop the download.

13. NuSTAR barycentring uses FPMA's orbit file for everything
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:514``. ``barycenter_data`` globs every ``nu<OBSID>*.evt*`` in the output
directory and passes ``nu<OBSID>A.attorb`` for all of them, including FPMB files and the
combined A+B file. Match the attitude/orbit file to the FPM.

The output name is built with ``infile.replace(".evt", "_bary.evt")``, which turns
``x.evt.gz`` into ``x_bary.evt.gz`` -- a gzip extension on a file ``barycorr`` will not
compress. The module already contains ``barycentered_file_name`` (``nustar.py:110``), which
handles this correctly via ``splitext_improved``, and it is never called.

14. ``barycenter.barycenter_file`` is shadowed and dead
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:10`` imports ``barycenter_file`` from :mod:`~heasarc_retrieve_pipeline.barycenter`,
and ``nustar.py:488`` immediately redefines it. The shared implementation -- which has the
``HAS_HEASOFT`` guard and verifies that the output file was actually created -- is
unreachable from the NuSTAR path. NICER uses the good one. Delete the NuSTAR copy.


Prefect usage
-------------

These are systematic rather than incidental, so they are grouped together. The net effect
is that the package pays Prefect's complexity cost and receives only logging in return.

15. ``wait_for`` is given functions, not futures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

About ten places (``core.py:414``, ``core.py:424``, ``core.py:479``, ``core.py:553``,
``nustar.py:740`` and following) pass ``wait_for=[some_function_object]``. Prefect expects
futures or states from previous calls. As written the declared dependencies are inert --
they neither enforce ordering nor fail if the upstream step failed. Ordering happens to be
correct only because everything runs synchronously.

Either capture and pass the actual return values, or drop ``wait_for`` entirely and rely on
the sequential control flow that is already there.

16. Tasks call tasks
~~~~~~~~~~~~~~~~~~~~

Prefect does not support calling a task from inside another task. It happens in at least:
``rootname`` -> ``splitext`` (``nustar.py:102``), ``goes_lc_file_name``/``goes_gti_file_name``
-> ``rootname`` (``nustar.py:121``, ``nustar.py:131``), ``barycentered_file_name`` ->
``splitext`` (``nustar.py:111``), ``get_goes_gtis`` -> ``goes_gti_file_name``
(``nustar.py:381``), ``get_best_source_region`` -> ``rootname`` (``nustar.py:551``),
``get_best_source_regions`` -> ``rootname`` and -> ``get_best_source_region``,
``calculate_spectra`` -> ``rootname``.

17. Trivial helpers are cached tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``splitext``, ``rootname``, ``barycentered_file_name``, ``goes_lc_file_name``,
``goes_gti_file_name``, ``flare_filtered_event_file_name`` (``nustar.py:87-142``) are
one-line string manipulations wrapped in ``@task`` with a 1000-day input-hash cache. Each
call becomes a tracked run with a cache lookup. They should be plain functions in
:mod:`~heasarc_retrieve_pipeline.utils`, next to ``splitext_improved``.

18. ``.fn`` everywhere defeats the point
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Path builders, ``separate_sources_in_event_file``, ``get_goes_gtis``, ``merge_gtis`` and
others are routinely invoked as ``task.fn(...)``, the undecorated function. That bypasses
caching, retries and run tracking. Nothing is ever ``.submit()``-ed, so there is also no
concurrency: downloads and reductions of independent observations run strictly one after
another.

A defensible resolution is to keep ``@flow`` on the four or five real entry points, make
everything else a plain function, and add ``.submit()`` only where parallelism actually
pays (per-observation downloads).

19. ``merge_event_files`` uses a random temp filename
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:296``: ``f"{root}_{np.random.randint(1000000)}.gti"``. Non-deterministic inputs
defeat ``task_input_hash`` caching for everything downstream, and 10\ :sup:`6` values is a
small space for a collision. Use ``tempfile.NamedTemporaryFile`` / ``mkstemp``.

20. A ``task_run_name`` template refers to a nonexistent parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:146``: ``separate_sources_in_event_file`` has
``task_run_name="separate_sources_in_event_file_{obsid}_..."`` but no ``obsid`` parameter.
Prefect raises when it formats the name -- currently masked because the function is only
ever called through ``.fn``.

Also, ``merge_gtis``, ``merge_event_files`` and ``separate_sources`` interpolate whole
*lists* of filenames into their run names, producing unusable labels in the UI.


Packaging and hygiene
---------------------

21. A ``TOKEN`` file sits untracked in the repository root
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A 94-byte ``TOKEN`` file is present in the working tree, is not tracked, and is **not in**
``.gitignore``. A single ``git add -A`` commits a credential. Add it to ``.gitignore`` now,
and rotate the token if there is any doubt about its history.

``.gitignore`` currently covers only ``__pycache__``, ``*.pyc``, ``*.jpg``, ``*.log`` and
``version.py``. It should also cover ``out/``, ``test_dload/``, ``*.egg-info/``,
``heasarc_retrieve_pipeline/_version.py`` (the setuptools-scm target; the current entry
says ``version.py``), ``.hypothesis/``, ``.pytest_cache/``, ``.tox/`` and ``.DS_Store``.

22. Undeclared dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Imported but not in ``pyproject.toml``: ``sunpy`` and ``nustar_gen``
(``nustar.get_goes_gtis``, ``get_best_source_region``), ``regions``
(``get_best_source_region``), ``pyyaml`` (``core.read_config``). A fresh
``pip install heasarc_retrieve_pipeline`` cannot run the NuSTAR spectral path.

``heasoftpy`` is not declared at all, not even as an optional extra -- defensible, since it
ships with HEASOFT rather than from PyPI, but it should at least be documented as a
requirement.

Conversely, ``pytest`` is listed as a *runtime* dependency and imported (unused) at
``core.py:7``.

23. The installed console script points at nothing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``pyproject.toml`` declares::

    [project.scripts]
    astropy_package_template_example = "heasarc_retrieve_pipeline.example_mod:main"

``example_mod`` does not exist. Every install puts a command on the user's ``PATH`` that
crashes with ``ModuleNotFoundError``. It is a leftover from the OpenAstronomy package
template; remove it, or replace it with a real command-line entry point -- the package is a
pipeline and currently has none, so every user has to write Python.

24. ``read_config`` cannot work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``core.py:303``: ``yaml.load(f)`` without a ``Loader`` argument raises ``TypeError`` on
PyYAML 6. The function is also dead code, and there is no documented schema for the config
dictionary it would produce. Either implement configuration properly -- a single shared
``DEFAULT_CONFIG`` rather than three copies in ``nustar.py``, ``nicer.py`` and ``rxte.py``,
with ``yaml.safe_load`` -- or delete the function.

25. Shell-out for a file copy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py:352``: ``os.system(f"cp {nf} {outdir}/")``. Unquoted (breaks on any path with a
space), no error checking, and a shell injection vector if a filename is ever attacker-
controlled. ``shutil.copy(nf, outdir)``.

26. ``os.chdir`` in the processing loop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``core.py:463`` and ``core.py:471`` change the process's working directory inside the loop
over observations. If any step raises, the process is left in the wrong directory; and it
makes concurrent processing impossible, which is a shame given that the whole package is
built on a concurrency framework. Pass absolute paths through ``config["out_data_path"]``
instead.

27. Mutable default arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``retrieve_and_process_data(flags={})`` (``core.py:437``),
``retrieve_heasarc_data_by_obsid(flags={})`` (``core.py:532``),
``process_nicer_obsid(config={})`` (``nicer.py:151``),
``process_rxte_obsid(config={})`` (``rxte.py:167``).

For NICER and RXTE this is an actual bug, not just a style issue: the body reads
``current_config = DEFAULT_CONFIG if config is None else config``, and the default ``{}``
is not ``None``, so ``process_nicer_obsid("1234567890")`` reaches
``config["out_data_path"]`` and raises ``KeyError``. Use ``config=None``.

28. Missing ``HAS_HEASOFT`` guards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nu_run_l2_pipeline`` and ``ni_run_l2_pipeline`` check the flag and raise a clear
``ImportError``. These do not, and fail with ``NameError: name 'hsp' is not defined``:
``recover_spacecraft_science_data``, ``merge_gtis``, ``merge_event_files``,
``join_source_data``, ``get_goes_gtis``, ``calculate_spectra``, and NuSTAR's local
``barycenter_file``.

29. ``retrieve_heasarc_data_by_source_name`` drops ``flags``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``core.py:486`` has no ``flags`` parameter and calls ``retrieve_and_process_data`` without
one, so per-observation pipeline flags can be passed by OBSID but not by source name.
``process_rxte_obsid`` accepts ``flags`` and ignores it entirely.

30. ADQL is built by string interpolation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``core.py:355``: ``cat.obsid='{obsid}'``. The OBSID comes from the caller. This is not a
high-risk injection surface -- it is a read-only public TAP service -- but a quote in an
OBSID produces a confusing service error rather than a clear validation message. Validate
the OBSID against a per-mission pattern before interpolating.

31. Matplotlib figures are never closed, and no backend is forced
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``image_utils.py:110`` creates a figure whose ``plt.close(fig)`` is commented out. Looping
over observations therefore leaks one figure per event file and triggers matplotlib's
"more than 20 figures have been opened" warning. The module also imports ``pyplot`` at
import time without selecting a non-interactive backend, so importing the package on a
machine with a display can try to open a window.

32. Dead code and unused imports
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Unused imports: ``sys``, ``glob``, ``traceback``, ``pytest``, ``warnings``, ``hstack``
  (``core.py``); ``re``, ``boto3``, ``UNSIGNED``, ``Config`` (``rxte.py``); ``glob``
  (``nicer.py``); ``Table``, and ``astropy.visualization.hist`` which is shadowed by a
  local variable (``image_utils.py``); ``getdata``, ``Table`` (``nustar.py``).
* ``nustar.py:584``: ``sep = target.separation(obj_j2000)`` is computed, the comment says
  "if <15 arcsec, all is okay", and nothing checks it. This is exactly the guard that would
  catch the wrong-source detection described in issue 4 -- it should be implemented, not
  removed.
* ``nustar.py:719``: a ``nuproducts`` command string is assembled and printed but never
  used; ``hsp.nuproducts`` is called separately.
* ``nustar.py:571`` and ``nustar.py:596``: ``make_image`` is called three times and
  ``make_radial_profile`` twice, with the first results discarded.
* ``image_utils.py:132`` prints the detection threshold and flux to stdout on every
  candidate; library code should log, not print.
* ``nustar.py:734``: unused local ``basedir``.
* ``image_from_table`` takes a ``correct_zeros`` argument that is never used.

33. ``tox.ini`` cannot run
~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``env_list`` includes ``py{38,39,...}`` while ``pyproject.toml`` sets
  ``requires-python = ">=3.10"``.
* ``env_list`` names ``build_docs``, ``codestyle`` and ``linkcheck``; the file defines
  ``build-docs`` and neither of the other two.
* ``check-style`` runs ``ruff .``, which modern ruff rejects -- it needs ``ruff check .``.
* Comments refer to ``setup.cfg``, which the project does not have.

Either fix it or delete it; a ``tox.ini`` that fails on the first invocation is worse than
none, and the CI workflow does not use it.


Science caveats
---------------

These are choices rather than bugs. They are listed so that they are visible to anyone
reading the outputs, and they are described in more detail in :ref:`technical_details`.

* **The source acceptance threshold is 1 sigma.** ``flux >= median + MAD``
  (``image_utils.py:135``) with no Poisson treatment and no trials correction over the 300
  random apertures and the detected peaks. The real gatekeeper is
  ``threshold_abs = 0.5 * max(img)`` in ``peak_local_max``, which restricts detection to
  peaks at least half as bright as the brightest. Treat this as "split the obviously bright
  things", not as source detection.
* **The background estimate ignores the caller's region size.**
  ``image_utils.py:101`` calls ``get_random_fluxes_in_img(table, region_size=30, ...)`` with
  a hardcoded 30 while ``filter_sources_in_images`` takes ``region_size`` as a parameter --
  and ``process_nustar_obsid`` now passes an SNR-optimised value. So the threshold is
  computed for a different aperture than the one used for extraction. Pass ``region_size``
  through.
* **The random apertures include the sources.** Median and MAD are computed over apertures
  drawn anywhere in the field, including on the sources themselves. Robust statistics limit
  the damage but the estimate is biased high.
* **The image-based source and background files are timing-only products.** No PSF or
  aperture correction, no ARF, no exposure map; the background region is large and NuSTAR's
  background (aperture stray light in particular) is strongly position-dependent.
* **The combined FPMA+FPMB event file is also timing-only** -- two responses in one event
  list.
* **The NuSTAR background annulus is concentric with the source**, inner ``max(r, 100)``
  arcsec and outer ``max(2r, 250)`` arcsec. On a 12'x12' field the outer radius can run off
  the detector or across chip gaps, and a concentric annulus is not the recommended NuSTAR
  background prescription.
* **RXTE screening omits SAA and electron-ratio cuts**, and applies no per-PCU selection --
  ``NUM_PCU_ON > 0`` allows the effective area to change within a GTI. No response can
  validly be attached to these products.
* **RXTE GTI boundaries are approximate.** They are built from 16-second filter-file
  samples, taking each interval's start as the first good sample's ``Time`` and its stop as
  the last good sample's ``Time`` plus one ``TIMEDEL``; if ``Time`` is the sample midpoint
  this is offset by half a sample at both ends.
* **RXTE data are not barycentred**, unlike NuSTAR and NICER.
* **``process_nustar_obsid`` overrides the requested target position** with the detected
  peak (issue 4). When the detection is right this is better than the catalogue pointing;
  when it is wrong the data are barycentred to the wrong source, silently. It should be
  opt-in, and guarded by the separation check that already exists but is unused.
* **The default cone-search radius is 0.1 degrees**, i.e. 6 arcmin on the *pointing*
  position. NuSTAR's field of view is 12x12 arcmin, so serendipitous coverage of a target
  by an observation pointed elsewhere in the field will be missed at the default radius.


Testing and infrastructure
--------------------------

Current state
~~~~~~~~~~~~~

Four test functions, all ``@pytest.mark.remote_data``. Three fake the download
(``test=True``) and never reach any processing code; one downloads two real files. So:

* with no network, the suite collects **zero** runnable tests;
* the CI workflow always passes ``--remote-data``, so a HEASARC or AWS outage turns the
  build red for reasons unrelated to the code;
* ``heasoftpy`` is never installed in CI, so no processing code is executed anywhere, ever;
* ``image_utils``, ``rxte``, ``barycenter``, ``utils``, the path builders and the
  include/exclude filtering have no tests at all.

Proposal
~~~~~~~~

The goal is a suite that runs offline, in seconds, without HEASOFT. Deliberately modest:
one fixture module and roughly a dozen tests, targeting the code that is pure computation
and therefore both the easiest to test and the most likely to be silently wrong.

**A. Synthetic fixtures** (``tests/conftest.py``, about 40 lines of astropy):

* ``nustar_event_file`` -- a FITS event file with two Gaussian blobs at known ``X``/``Y``
  with known and different total counts, a ``PI`` column spanning the 3-79 keV band, a
  ``GTI`` extension and the header keywords the code reads. This one fixture unlocks most
  of the image tests.
* ``rxte_filter_file`` -- a ``*.xfl``-like table with a hand-made ``ELV``/``OFFSET``/
  ``NUM_PCU_ON``/``Time`` pattern whose correct GTIs can be written down by hand.
* ``fake_archive_dir`` -- a directory tree mimicking an OBSID, for the local-copy download
  path.

**B. Offline unit tests**, in rough order of value:

1. ``filter_sources_in_images`` end-to-end on ``nustar_event_file``: both blobs are found,
   at the right coordinates, ``_src1`` is the brighter one, and ``_back.evt`` excludes
   both. This single test covers the whole image pipeline and would have caught the
   X/Y-axis conventions.
2. ``mask_around_region`` -- including the integer-overflow case its own comment warns
   about (coordinates far enough apart that ``int16`` squaring would wrap).
3. ``filter_table`` and ``filter_table_outside_regions`` -- counts in and out of known
   circles, and the scalar-vs-list ``region_size`` branch.
4. ``image_from_table`` -- assert the axis order explicitly, since the function histograms
   ``(Y, X)`` and returns the transpose.
5. ``create_gti_with_astropy`` on ``rxte_filter_file`` -- exact GTI boundaries, including
   the ``+TIMEDEL`` edge convention and the empty-GTI branch.
6. ``apply_gti_with_astropy`` -- the right events survive, including the ``TIMEZERO``
   offset and the "no events left" branch.
7. ``splitext_improved`` -- already has doctests; just make sure they actually run
   (``--doctest-modules`` or the existing ``doctest_plus``).
8. ``MISSION_CONFIG`` consistency -- every mission has all six keys, and each ``path_func``
   produces the documented archive layout for a known OBSID.
9. ``recursive_download`` against ``fake_archive_dir`` -- the local branch is already
   implemented and completely untested.
10. ``re_include``/``re_exclude`` filtering against a canned listing (a list of paths, no
    network), asserting the same two-file result the current remote test checks.

**C. CI**. Split ``.github/workflows/python-package.yml`` into:

* a fast job on every push: install the package, run ``pytest`` **without**
  ``--remote-data``, upload coverage. This is the job that must stay green;
* the existing micromamba job, with ``--remote-data``, allowed to fail or scheduled nightly
  rather than gating every commit;
* optionally, install ``heasoftpy`` from the HEASARC conda channel in one job so that the
  mission modules are at least imported and their ``HAS_HEASOFT`` guards exercised.

Also: bump ``actions/checkout`` from v3, and add a ``concurrency`` block so superseded runs
are cancelled.

**D. Not proposed.** Deliberately out of scope, because the cost outweighs the benefit at
this size: mocking HEASOFT tasks, a fake HEASARC/S3 server, property-based testing, a
coverage gate, or a typing pass. The synthetic-fixture tests above give most of the
protection for a fraction of the effort.
