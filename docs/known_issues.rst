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

1. ``Heasarc.locate_data`` returns nothing -- WORKED AROUND
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Reported upstream as* `astroquery #3652
<https://github.com/astropy/astroquery/issues/3652>`_ *and worked around locally; the
description below is kept because the workaround is temporary.*

*Verified live against the HEASARC service with astroquery 0.4.11.*

HEASARC's datalink service used to label the row pointing at an observation's data
directory with ``content_type='directory'``. It now returns ``content_type='text/html'``.
``astroquery.heasarc.Heasarc.locate_data`` keeps only rows where
``content_type == 'directory'`` or ``error_message != ''``, so it returns an **empty
table** for every query.

``retrieve_and_process_data`` (``core.py:466``) then evaluates ``links[i][link_col_name]``
and raises ``IndexError`` on the first observation.

This is not hypothetical. Running the existing test suite against the live services
today gives::

    8 failed, 3 passed

All eight failures are
``IndexError: index 0 out of range for table with length 0``. The three that pass are
``test_recursive_download`` (both hosts), which bypasses ``locate_data`` and builds the
archive URL by hand, and the ``splitext_improved`` doctest. In other words, every test
that exercises a top-level flow is currently red.

The ``try``/``except`` fallback in ``retrieve_heasarc_data_by_source_name``
(``core.py:500``) does not help, for two reasons: ``locate_data`` does not raise, it
returns an empty table; and the fallback itself is broken (issue 2).

**Workaround in place.** ``core.locate_data`` is a copy of the astroquery method with
the row selection replaced by a test on the access URL (``'/FTP/' in access_url``, which
is how the ``sciserver`` and ``aws`` columns are derived anyway). It returns the same
columns as the original, and the ``Heasarc.locate_data`` call it replaces is kept
commented out at the call site: when astroquery #3652 is fixed, uncomment that line and
delete the local function.

The same change fixed the row matching. Links are now keyed on the datalink ``ID``
instead of being indexed positionally against the catalogue table. Positional indexing
was never safe, and it fails in ordinary use: observations still inside their
proprietary period are returned by the catalogue query but have no downloadable
products, so every observation after the first such gap was being downloaded under the
wrong OBSID. Those observations are now logged and skipped.

With the workaround, the full test suite passes: ``11 passed``.

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

3. ``calculate_spectra`` runs ``nuproducts`` outside its own loop -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

**Fixed.** The glob and the inner loop were replaced by ``spectral_input_files``, which
yields ``(module, file)`` pairs, so the body runs once per file with nothing leaking between
iterations. ``src_reg``/``bkg_reg`` are no longer reassigned, the "file missing" branches
now really skip, and ``PRODUCTS_DONE.TXT`` is written only when nothing went wrong -- an
observation with no usable files is a clean outcome and is marked done, a missing region or
GTI file is not.

The same restructuring made it possible to extract spectra from mode-06 data as well; see
the NuSTAR section of ``technical_details.rst``.

4. ``get_best_source_regions`` returns ``(0, 0, 0)`` on a rerun -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

**Fixed.** ``get_best_source_region`` is now called for every file and its result always
counted; it already returns early, reading the position and radius back out of the existing
region files with ``regions.Regions.read``. The existence check that carried the path bug is
gone entirely, so the two bugs are removed rather than left cancelling. Its ``nustar_gen``
import moved below that early return, since reading a region back needs neither
``nustar_gen`` nor an image, and ``regions`` is now declared in ``pyproject.toml``.

Pinned by ``test_existing_regions_are_read_back_on_a_rerun``, which runs offline.


Correctness
-----------

5. ``filter_from_solar_flares`` does not filter events or exposure -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py`` replaced ``hdul[2].data`` with the flare-free GTI and wrote
``*_noflares.evt``, but copied the event table unchanged and did not update ``EXPOSURE``,
``ONTIME`` or ``LIVETIME``. The name promised a filtered event file; the content was an
unfiltered event file with a narrower GTI. Any tool that computes a rate from the header,
or that ignores GTIs, got the wrong answer. Measured on 80002092008::

    nu80002092008_src1.evt           events=51870  EXPOSURE=33646.06  GTI sum=58888.6s
    nu80002092008_src1_noflares.evt  events=51870  EXPOSURE=33646.06  GTI sum=56850.9s

It also assumed the GTI lives in extension 2.

**Fixed.** The write goes through ``utils.apply_gti``, which drops the events outside the
new intervals, sets ``ONTIME`` to their exact total, and scales ``LIVETIME`` and
``EXPOSURE`` by the ``ONTIME`` ratio. It finds both the events and the GTI extension by
``EXTNAME``, and honours ``TIMEZERO``. See "Solar flare filtering" in
``technical_details.rst`` for the measurement that justifies scaling ``LIVETIME`` rather
than integrating the housekeeping live fraction.

Related, and fixed at the same time: ``process_nustar_obsid`` filtered only the ``src_num=1``
join and left the background alone, so a background-subtracted rate mixed a filtered source
with an unfiltered background. Both joins now go through the same filter.

6. ``join_source_data``: brittle FPM substitution and an inconsistent return value -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``b_file = a_file.replace("A", "B")`` replaced *every* ``A`` in the path. Any output
directory containing a capital A -- a source name, a user directory -- produced a
nonexistent filename.

On the cached path the function returned ``glob.glob(f"nu{obsid}*{label}.evt")``, which
matches the per-FPM and per-mode intermediates *as well as* the combined file; on the fresh
path it returned only the combined file. On the real 80002092008 tree that is **five** files
against one. ``process_nustar_obsid`` feeds that list to ``filter_from_solar_flares`` and
then to ``barycenter_data``, so a rerun did five times the work of a first run, on files
that are not science products.

**Fixed.** Both module file names are now built from the FPM loop variable rather than by
string substitution, and both code paths return ``[combined_file]`` -- the cached one after
checking the file is actually there.

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

9. ``recursive_download_s3`` never paginates -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``list_objects_v2`` returns at most 1000 keys per call and sets ``IsTruncated``; the code
read ``response["Contents"]`` once and stopped. Any observation with more than 1000 files
was silently truncated. NuSTAR observations are well under that; RXTE observations are not.

**Fixed.** The listing walks ``s3_client.get_paginator("list_objects_v2")``. The
key-to-destination decision moved into the pure ``s3_key_destination``, and the client
construction into ``_s3_client``, so both can be tested against a stub bucket offline: two
pages of keys are both downloaded, and an empty second page ends the listing cleanly.

The paginated listing also carries a ``Size`` for every key, so the completeness check
described under issue 11 costs nothing at all on this transport -- no extra request.

10. The HTTPS scraper reads link text instead of ``href`` -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``file_name = i.extract().get_text()``. *Verified against a real HEASARC index*: this
yielded five spurious entries per directory -- ``Name``, ``Last modified``, ``Size``,
``Description`` (the column-sort links, whose ``href`` is ``?C=N;O=D`` and whose *text* is
the column name) and ``Parent Directory`` (whose ``href`` is absolute and points up the
tree). *Verified in the end-to-end logs*: **25 spurious download tasks per observation**,
five per directory, each reported ``Finished in state Completed()``.

They caused no damage only because the default ``test_str="."`` rejects names without a
dot. With ``test_str=None`` the downloader would have tried to fetch
``.../Parent Directory``, and ``Parent Directory`` was also a latent recursion hazard.

**Fixed.** ``parse_directory_index`` reads ``href``, and keeps only relative ones: an
``href`` that starts with ``?``, ``#`` or ``/``, contains ``://``, or resolves to ``..`` is
dropped. ``get_remote_directory_listing`` keeps the fetch and the recursion and delegates
the parsing, which makes the parsing testable without network -- the tests run against the
real index page of 80002092008, captured verbatim. Live afterwards, that directory lists
64 entries, four directories and sixty files, with none spurious.

11. ``download_node`` reports success after a failed or partial download -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``download_cmd`` swallowed every exception and returned ``(None, message)``;
``download_node`` logged a warning and then returned ``local_ver`` regardless. The caller
could not distinguish a downloaded file from a missing one, and the pipeline went on to
process an incomplete observation.

The sharper half of the same problem was **incompleteness**. ``pySmartDL`` downloads in
parallel chunks to ``<dest>.000``, ``<dest>.001``, ... and combines them only at the end,
and it does compare the combined size against the server's ``Content-Length`` -- but it
skips that check entirely when the server sends no ``Content-Length``, and it tolerates a
shortfall of 4 kB per thread. The real hole was elsewhere: ``download_node`` returned early
on ``os.path.exists(local_ver)`` and never looked at the file's size, so a file left short
by a killed run, a full disk, or a copy from elsewhere was accepted as complete on every
later run, forever.

**Fixed.** Every file is now checked against the size the archive reports.

* ``remote_file_size`` asks for ``Content-Length`` with a HEAD request. *Measured*: 60 of
  60 files of one NuSTAR observation reported one, and all 60 matched the local copy
  exactly, so exact verification is possible over HTTPS. On S3 the size comes free with the
  listing (issue 9).
* ``file_needs_download`` holds the policy and is pure, so the tests pin the policy down:
  absent means fetch, matching size means skip, differing size means fetch again, and an
  unknown size means accept with the log line saying it could not be verified.
* A newly downloaded file costs no extra request: ``_download_pysmartdl`` returns
  ``get_final_filesize()``, the ``Content-Length`` the library already fetched.
* A file on disk of the wrong size is **re-downloaded with a WARNING naming both sizes**,
  the policy chosen deliberately: the local tree is a mirror and the archive is
  authoritative, so a short file is a failed download, not precious data.
* A failed transfer, or one landing the wrong size, **raises** ``RuntimeError`` naming the
  URL, the local path and the reason, and any ``<dest>.000``-style part files are removed.
  ``download_node`` carries ``retries=3, retry_delay_seconds=10`` -- the first use of
  Prefect's retries in the package -- so a transient failure retries and a persistent one
  stops the observation. Re-running an aborted observation is cheap, since verified files
  are skipped.

*Verified against the real archive*: all 60 local files of 90901333002 verified and
skipped, nothing re-fetched; then one copy truncated to two thirds was reported as
``present but 19556794 bytes against 29335191 expected``, re-downloaded, and restored to
exactly its archive size.

Checksum verification remains out of reach: size is what the archive exposes cheaply and
exactly, and there is no per-file hash to check against.

12. ``get_goes_gtis`` can emit negative-length GTIs -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A flare that starts before ``TSTART`` but ends during the observation passed the
``flare_start >= tstop or flare_end <= tstart`` guard, and then appended
``{"START": tstart, "STOP": flare_start}`` with ``flare_start < tstart``. The HEK rows were
also assumed to be sorted in time and non-overlapping; neither is guaranteed.

**Fixed.** The flares that pass the class cut are collected and handed to
``utils.good_intervals``, which clips them to ``[TSTART, TSTOP]``, sorts them, merges
overlaps and drops empty intervals, and returns the complement. Its offline tests cover
each of those cases -- 80002092008's single flare falls well inside the observation, so it
would not have exercised any of them. An observation entirely covered by flares now raises
with a clear message instead of writing an unusable GTI file.

The measured GOES X-ray light curve is now used for the filtering as well, not just the
catalogue: ``get_goes_gtis`` excludes the union of the catalogued flares and the times when
the 1--8 A flux reaches ``flux_class``. It also writes the light curve to
``<root>_goes.fits`` on the event file's own time scale, and ``plot_flare_filtering`` plots
it above the NuSTAR light curves so the cut can be checked by eye. The two criteria catch
different things, and adding the flux cut takes the background chi2/dof on 80002092008 from
3.62 to 1.83. See "Solar flare filtering" in ``technical_details.rst``, which also records
the one hazard of the flux threshold: set below the Sun's quiescent flux for the epoch, it
excludes the whole observation.

13. NuSTAR barycentring uses FPMA's orbit file for everything -- CORRECTED, harmless
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``barycenter_data`` globs every ``nu<OBSID>*.evt*`` in the output directory and passes
``nu<OBSID>A.attorb`` for all of them, including FPMB files and the combined A+B file. This
entry claimed that introduces a timing error. **It does not.**

*Measured on 90901333002*: ``nu90901333002A.attorb`` and ``nu90901333002B.attorb`` are
identical in every column ``barycorr`` reads -- the same TIME grid, ``POSITION`` differing
by exactly 0 km, ``VELOCITY`` by 0 km/s, ``RA``/``DEC`` by 0 degrees. There is one
spacecraft carrying both focal-plane modules, so there is one ephemeris, written twice
under two names. Passing FPMA's file to an FPMB event file is passing the same numbers.

The naming half of this entry **is fixed**, and it was worse than the entry said. The
output name was built with ``infile.replace(".evt", "_bary.evt")``. That is harmless for
NuSTAR and NICER, whose files end in ``.evt``, but ``barycenter_file`` is shared, and for a
mission that calls its event files ``.fits``, ``.ds`` or ``evt2.fits`` there is no ``.evt``
to replace: the output name came back **equal to the input**, at which point the
already-exists check fires and hands the caller its own unbarycentred file. A directory
with ``.evt`` in its name would have been renamed instead of the file, for the same reason
-- ``str.replace`` substitutes the first occurrence anywhere in the path.

``barycentered_file_name`` now builds the name for every mission, using
``splitext_improved``: ``_bary`` goes before the extension whatever the extension is, and a
compression suffix stays last, so ``x.evt.gz`` gives ``x_bary.evt.gz``. It used to be a
second, unused copy in ``nustar.py``; that copy is gone.

14. ``barycenter.barycenter_file`` is shadowed and dead -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar.py`` imported ``barycenter_file`` from
:mod:`~heasarc_retrieve_pipeline.barycenter` and then immediately redefined it, so NICER
used the guarded implementation and NuSTAR silently used a weaker copy of its own.

**Fixed.** There is now one ``barycenter_file``, in
:mod:`~heasarc_retrieve_pipeline.barycenter`, and both missions call it. The merged
function keeps what each side had:

* the ``ImportError`` with a readable message when heasoftpy is missing, and the
  ``FileNotFoundError`` when ``barycorr`` returns without writing its output -- the two
  things the shared version had and the NuSTAR copy did not;
* the skip when the output is already there, which the NuSTAR copy had and the shared one
  did not. It is now under an ``overwrite`` flag, defaulting to skipping, so re-running an
  observation does not redo work that is already done;
* an ``outfile`` argument, so a caller that wants to name the output itself can.

``src`` is gone from it. It never affected the barycentring -- it only appeared in the
Prefect task run name -- and ``barycenter_data`` still takes it for its own flow run name.

35. Merged event files carry a stale ``ONTIME``, ``LIVETIME`` and ``EXPOSURE``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Found while verifying the fix to issue 5 on real data, and it is why ``apply_gti`` scales on
the GTI rather than on the header.

``ftmerge`` copies the exposure keywords from its first input instead of recomputing them,
and ``join_source_data`` does not correct them afterwards. On 80002092008::

    nu80002092008_src1.evt   ONTIME=36058.05  LIVETIME=33646.06  GTI total=58888.6

``ONTIME`` is by definition the total of the GTI, so those two numbers cannot both be right:
the GTI is the union of 303 intervals from all the merged mode-01 and per-CHU mode-06 files,
while the keyword belongs to whichever single file ``ftmerge`` saw first. ``LIVETIME`` and
``EXPOSURE`` are stale in the same way, and they are the numbers anything computing a rate
from the header will use.

``apply_gti`` sets ``ONTIME`` to the exact GTI total, so the ``*_noflares.evt`` products are
self-consistent afterwards; their ``LIVETIME`` is still the stale value times the correct
ratio, so it remains wrong in absolute terms. Fixing it properly means recomputing the
keywords in ``join_source_data``: the OR merge of one module's files can sum their live
times, but the AND merge of FPMA and FPMB needs a decision about what "live time" even means
for two telescopes in one event list.

**Scope: this does not touch the spectra.** Header exposure keywords barely matter for
timing, which works from event times and GTIs, but they are central to spectroscopy, where
the count rate per channel is counts divided by exposure. The spectral path never sees these
files. ``spectral_input_files`` yields the per-mode cleaned files from ``nupipeline`` and
``nusplitsc`` -- ten of them for 80002092008, none of them merged -- and those are
self-consistent, ``ONTIME`` matching their own GTI total to within a millisecond. Checked on
the real tree::

    nu80002092008A01_cl.evt           ONTIME=36058.05  GTI total=36058.05
    nu80002092008A06_chu3_N_cl.evt    ONTIME=11487.52  GTI total=11487.52

``nuproducts`` then computes the exposure of each spectrum itself, and it honours the
flare-free GTI passed as ``usrgtifile``: ``nu80002092008A01_sr.pha`` carries
``ONTIME=35038.05`` against its input file's ``36058.05``, exactly the 1020 s flare window
shorter. So the spectra get the right exposure, and the defect above is confined to the
merged timing products.


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

21. A ``TOKEN`` file sat untracked in the repository root -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A 94-byte ``TOKEN`` file was present in the working tree, untracked and **not in**
``.gitignore``, one ``git add -A`` away from committing a credential.

``.gitignore`` has been rewritten to cover it, along with the other artefacts that were
accumulating untracked: ``out/``, ``out_test/``, ``test_dload/``, ``*.egg-info/``,
``build/``, ``dist/``, ``docs/_build/``, ``heasarc_retrieve_pipeline/_version.py`` (the
real setuptools-scm target -- the previous entry said ``version.py``, which matches
nothing), ``.pytest_cache/``, ``.hypothesis/``, ``.tox/``, coverage output and
``.DS_Store``.

Ignoring the file does not undo any exposure it may already have had. Check
``git log --all -- TOKEN`` (currently empty, so it was never committed on any branch)
and rotate the token if there is any doubt about where else it has been.

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

31. Matplotlib figures are never closed, and no backend is forced -- PARTLY FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``image_utils.py`` created a figure whose ``plt.close(fig)`` was commented out. Looping
over observations therefore leaked one figure per event file and triggered matplotlib's
"more than 20 figures have been opened" warning.

**Half fixed.** That ``plt.close(fig)`` is uncommented, so nothing leaks any more. The new
diagnostic ``plot_flare_filtering`` sidesteps the problem entirely by building its figure
with ``matplotlib.figure.Figure`` instead of ``pyplot``: that is headless by construction
and never enters pyplot's global figure registry, so there is nothing to close and no
backend to force. Its test asserts ``len(plt.get_fignums()) == 0`` afterwards.

**Still open**: ``image_utils.py`` imports ``pyplot`` at import time without selecting a
non-interactive backend, so importing the package on a machine with a display can still try
to open a window. Converting those three plots to ``Figure`` as well would close this.

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

33. The documentation did not build with a current Sphinx -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``docs/conf.py`` used the pre-Sphinx-5 form of the intersphinx configuration::

    intersphinx_mapping = {'https://docs.python.org/': None}

Sphinx 5 and later require a named mapping, and reject this one outright::

    sphinx.errors.ConfigError: Invalid `intersphinx_mapping` configuration (1 error).

So ``sphinx-build -W docs docs/_build`` -- exactly what the ``build-docs`` tox
environment runs -- failed before reading a single page. It is now::

    intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

and the documentation builds clean under ``-W`` (warnings as errors).

34. ``tox.ini`` cannot run
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
* the ``remote_data`` marker only skips anything if ``pytest-remotedata`` (part of
  ``pytest-astropy``) is installed. Without it the marker is inert and a plain
  ``pytest`` run hits the network regardless, while emitting an unknown-marker warning;
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
