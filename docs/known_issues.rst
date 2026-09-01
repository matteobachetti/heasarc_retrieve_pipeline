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

2. The legacy fallback path cannot run -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deleted, which is what this entry recommended. ``retrieve_heasarc_data_by_source_name_old``
was the fallback for when astroquery failed, and it had four independent faults: it called
``MISSION_CONFIG[mission]["path_func"]`` with a signature no path function accepted; those
path functions were Prefect tasks being called outside a flow; it iterated over
``results["cycle"]`` and ``results["prnb"]``, which exist only for RXTE; and it was a task
calling tasks. It had never run.

Deleting it exposed a cascade with no other caller: ``remote_data_url``, the three
``path_func`` entries of ``MISSION_CONFIG``, and the archive-path builders they named --
``nu_heasarc_raw_data_path``, ``rxte_heasarc_raw_data_path`` and ``ni_raw_data_path``,
which reconstructed by hand the directory layout that the datalink service already reports.
227 lines went in total.

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
``<root>_goes.fits`` on the event file's own time scale, and the observation's page shows it
above the NuSTAR light curves so the cut can be checked by eye. The two criteria catch
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

15. ``wait_for`` is given functions, not futures -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Thirteen declared edges across twelve call sites passed ``wait_for=[some_function_object]``.
Measured on Prefect 3.8.4: with a function object the downstream body runs even though the
upstream task raised. The edges were inert.

Eight of them were deleted rather than repaired, because the dependency was already stated
more strongly by an argument -- ``splitdir`` *is* the return value of
``recover_spacecraft_science_data`` -- or because the upstream was a subflow, and a subflow
call is synchronous and raises. There is no ``flow.submit()`` on 3.8.4 to produce a future
from one.

The three remaining edges, in ``process_nustar_obsid``, are real: the dependent step does
not consume the upstream result. Those upstreams are now ``.submit()``-ed and the future is
passed. Each is paired with a ``.result()``, and that pairing is the point of the entry.
Measured, with the upstream failing:

=========================================  ==========================  ==============
Pattern                                    Downstream                  Flow run ends
=========================================  ==========================  ==============
``downstream(wait_for=[future])``          skipped, returns ``None``   **COMPLETED**
``future.result()``, then ``downstream``   exception re-raised         **FAILED**
=========================================  ==========================  ==============

So a bare future would have turned a failed Level-2 pipeline into a green flow run handing
``None`` to the next step. The ``.result()`` keeps today's fail-fast behaviour; the
``wait_for`` puts the edge in the run graph. Neither substitutes for the other.

An AST guard in ``tests/test_prefect_wiring.py`` now asserts that every name in a
``wait_for`` list came from a ``.submit()``. It catches 13 offenders in the tree before this
work and none after.

16. Tasks call tasks -- CORRECTED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The premise was wrong for the installed Prefect. "Prefect does not support calling a task
from inside another task" was true of Prefect 2; on 3.8.4 the call returns the right answer
and produces a nested task run, both inside a flow and outside one. Measured.

What was really wrong with the examples listed here was not the nesting but what was being
nested: one-line string helpers. That is issue 17, and fixing it removed most of these call
chains anyway.

17. Trivial helpers are cached tasks -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Eighteen path and name builders were ``@task`` with a 1000-day input-hash cache, for work
like inserting ``_bary`` before an extension. In the end-to-end log of 90901333002 they
accounted for **85 of 199 task runs (43%)**: ``splitext`` 34, ``rootname`` 28,
``goes_lc_file_name`` 10, ``goes_gti_file_name`` 10, ``flare_filtered_event_file_name`` 2,
``nu_base_output_path`` 1. The steps actually worth watching -- barycentring, the joins, the
flare diagnostics, the spectra, the source regions -- came to 18.

They are plain functions now. ``nustar.splitext`` is gone (a one-line wrapper around
``utils.splitext_improved``), ``rootname`` moved to :mod:`~heasarc_retrieve_pipeline.utils`
since it is generic, and the mission path builders stay in their own modules. The package
went from 45 tasks to 26, and the offline test suite from 9.4 s to 6.0 s.

This is a legibility argument, not a speed one, and the difference should not be oversold:
one cached trivial task call costs 3.89 ms against 0.33 us for the plain function, about
12,000 times more, but 85 of them is 0.3 s inside a 50-minute run.

Re-running the same observation after the change gives **89 task runs and no string helper
among them** -- 66 of the 89 are the per-file downloads, and the rest are the steps that do
work. (The run itself is short because every processing step skips on its sentinel; what it
measures is which task runs the code asks for, not how long they take.)

The characterization tests these builders never had came first, in ``tests/test_nustar.py``
and the new ``tests/test_nicer.py`` and ``tests/test_rxte.py``. One of them went red
immediately: ``ni_pipeline_done_file`` passed ``obsid`` as the ``config`` argument, so NICER
never found its own sentinel file.

18. ``.fn`` everywhere defeats the point -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Twenty-eight of the forty production ``.fn(`` calls went away with issue 17: they are calls
to plain functions now. The remaining ten were tasks that do real work, and they are called
as tasks. ``merge_gtis`` and ``merge_event_files`` appear in the run graph for the first
time -- they ran zero times as tasks in the 199-run log, though they did the work. Checked
directly, since the sentinels skip them on a re-run: called inside a flow they now log
``nu_merge_event_files_into_out.evt_gti_OR`` with ``nu_merge_gtis_into_out_tmp.gti_gti_OR``
nested inside it. So does ``get_best_source_region``, which the census run does show, twice.

``get_remote_directory_listing`` was the one place where ``.fn`` was load-bearing: it
recursed through ``self.fn``, and calling it as a task would have opened a task run per
subdirectory. The walk is now a plain recursive function, ``walk_remote_directory``, with
the task as its entry point, so one directory tree is one task run.

``.fn`` in the tests stays: calling the undecorated function is the standard way to unit
test a Prefect task.

The concurrency half of this entry is not addressed. ``.submit()`` here buys dependency
edges, not parallelism, because ``os.chdir`` in both observation loops (issue 26) makes
concurrent observations unsafe -- HEASOFT tools resolve relative paths against the process
working directory. Issue 26 is the prerequisite.

19. ``merge_event_files`` uses a random temp filename -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The intermediate GTI file is now ``{root}_tmp.gti``, named after the output file. One output
file means one intermediate, so a deterministic name cannot collide, and ``merge_gtis``
unlinks it before writing in any case.

The random name had a second fault this entry did not mention: ``os.unlink`` was the last
statement of the task, so any HEASOFT call that raised left the file behind under a name
nothing would ever look for again. Removal is in a ``finally`` now.

``tempfile.mkstemp``, which the entry suggested, would have fixed the leak but not the
non-determinism -- mkstemp names are random too.

20. A ``task_run_name`` template refers to a nonexistent parameter -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``separate_sources_in_event_file`` named ``{obsid}``, which is not one of its parameters.
Measured: Prefect raises ``KeyError: 'obsid'`` when it formats the name, before the body
runs. It was masked only because the function was always reached through ``.fn`` -- and
issue 18 was about to stop doing that.

The three templates that interpolated whole lists of file names now name the output file, or
the first input. A second AST guard in ``tests/test_prefect_wiring.py`` checks every
``task_run_name`` and ``flow_run_name`` template in the package against the parameters of
the function it decorates.


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

26. ``os.chdir`` in the processing loop -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The loop over observations changed the process working directory into ``outdir`` before
each reduction and back afterwards, which is why the mission ``DEFAULT_CONFIG``\ s said
``"./"``: every path the pipeline built meant "wherever this process is standing right
now". ``outdir`` is now made absolute once and passed down as ``input_data_path`` and
``out_data_path``; ``utils.absolute_config`` pins a configuration's paths at flow entry,
and an AST guard asserts that no module calls ``os.chdir`` at all, with one name exempted.

That was the smaller half of the problem. Reducing several observations at once needs
three more things, and one of them decides the whole design.

**HEASOFT parameter files are shared, and that forces processes.** ``heasoftpy`` runs each
tool as a subprocess with ``env=os.environ.copy()``, and the tool reads and rewrites
``<PFILES>/<tool>.par``. Concurrent calls delete the file under each other. Measured, 200
calls of ``ftlist``, eight at a time:

=====================================  ==================
Setup                                  Failures out of 200
=====================================  ==================
8 threads, shared ``~/pfiles``         19
8 processes, shared ``~/pfiles``       9
8 processes, private ``PFILES`` each   **0**
=====================================  ==================

The failures are ``parameter file .../ftlist.par not found`` and ``FileNotFoundError``.
``PFILES`` is an environment variable and the environment belongs to the process, so
threads cannot be isolated this way at all: **an observation has to be a process**.
``core.prepare_worker`` gives each worker a private ``PFILES`` directory and a private
working directory. Both are needed. HEASOFT scripts drop scratch files in the current
directory by the dozen, and while most carry the tool subprocess's PID
(``86758tmp_gti.fits``, ``87340_tmp_nuexpomap``), not all of them do -- ``xselect`` writes
``xsel_timefile.asc``, with no PID at all. ``prepare_worker`` is the one place in the
package that calls ``os.chdir``, and it is the opposite of the pattern above: once, before
any work, never during.

**The whole thing measured, end to end.** Three real NuSTAR reductions of 90901333002, in
three worker processes, from the join step through spectra:

===========================================  =============================  ==========
Setup                                        Result                         Task runs
                                                                            failed
===========================================  =============================  ==========
private ``PFILES`` + working directory       3 of 3 completed, 1433 s each  **0**
shared ``~/pfiles`` + shared directory       2 of 3 completed, 1412 s each  3
===========================================  =============================  ==========

The isolated run produced identical output in all three trees: 97 product files, 80 split
files, the same event counts, and merged GTIs present and sorted. Running three at once
costs nothing in wall-clock time per observation.

The shared run failed inside ``nuproducts``, and the log shows exactly how. ``xselect``
offered one worker the *other* worker's session as a default::

    !> Enter session name >[xsel37372] xsel37575
     Command not found; type ? for a command listing
    !xsel37575:SUZAKU > read eve lnk-nu90901333002A06_chu23_N_cl.evt
    !> Enter the Event file dir >[37372_tmp_nuproducts/] 37575_tmp_nuproducts/

-- note the mission guessed as ``SUZAKU`` -- and the run ended with ``Cannot open
xsel_timefile.asc`` and a failed ``numkrmf``. This is the failure the isolation prevents,
and it is worth knowing what it looks like, because nothing in it says "concurrency".

**The GOES data were fetched into a shared directory.** ``Fido.fetch`` was called without
a ``path``, so two observations from the same day wrote the same file, and a reduction
could be handed a file another was still writing. Each observation now keeps its own copy
beside its event file.

**The diagnostic images went through pyplot**, whose figure registry is a global. There
are no diagnostic images any more: what they showed is recorded per observation, one file
per writer, and drawn on the observation's page. See issue 51.

``n_workers`` on the three entry flows says how many observations to reduce at once, and
``retrieve_heasarc_data_by_obsid`` takes a list of OBSIDs so that there is something to
parallelise. The default is one worker -- but still a worker process, so there is one code
path and it is the isolated one.

**Inside one observation, the steps are still threads, and they still share ``PFILES``.**
The first three-observation run proved it: ``FileNotFoundError:
.../worker_60907/pfiles/fthedit.par``, raised from the two ``join_source_data`` tasks that
``process_nustar_obsid`` submits at once. A private directory per *process* does nothing
for two threads inside it. Every HEASOFT call therefore now goes through
:mod:`heasarc_retrieve_pipeline.heasoft`, which holds a process-wide lock for the duration
of the call, and an AST guard fails if any other module invokes ``heasoftpy`` directly. The
lock is free in practice: these are external subprocesses doing seconds to minutes of work.

Two consequences to know about. The entry point must be a real script under
``if __name__ == "__main__":``, since a process pool cannot re-import a ``__main__`` that
came from standard input. And ``prepare_worker`` is called from inside the task rather
than as the pool's ``initializer``, because Prefect grew that argument in 3.8 and the
HEASOFT environment here has 3.7.

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

28. Missing ``HAS_HEASOFT`` guards -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nu_run_l2_pipeline`` and ``ni_run_l2_pipeline`` checked the flag and raised a clear
``ImportError``. These did not, and failed with ``NameError: name 'hsp' is not defined``:
``recover_spacecraft_science_data``, ``merge_gtis``, ``merge_event_files``,
``join_source_data``, ``get_goes_gtis``, ``calculate_spectra``, and NuSTAR's local
``barycenter_file``.

There is now one place that invokes HEASOFT, ``heasoft.run`` / ``heasoft.run_task``, and it
raises ``ImportError("heasoftpy not installed")`` before doing anything else. Every one of
these functions is covered by that, and no new call site can miss it.

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

31. Matplotlib figures are never closed, and no backend is forced -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``image_utils.py`` created a figure whose ``plt.close(fig)`` was commented out. Looping
over observations therefore leaked one figure per event file and triggered matplotlib's
"more than 20 figures have been opened" warning. It also imported ``pyplot`` at import
time without selecting a non-interactive backend, so importing the package on a machine
with a display could try to open a window.

This was fixed in stages. The ``plt.close(fig)`` was uncommented, which stopped the leak;
the figures were then rebuilt with ``matplotlib.figure.Figure`` instead of ``pyplot``,
which is headless by construction and never enters the global registry, so there was
nothing left to close and no backend to force.

**Closed** by issue 51: the figures themselves are gone. Nothing under
``heasarc_retrieve_pipeline`` imports matplotlib any more, and it is no longer a declared
dependency. Dropping it does not uninstall it -- ``nustar_gen`` requires it outright and
``sunpy``'s timeseries extra pulls it in -- but a machine that only reads the reports no
longer needs it.

32. Dead code and unused imports
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Unused imports: ``sys``, ``glob``, ``traceback``, ``pytest``, ``warnings``, ``hstack``
  (``core.py``); ``re``, ``boto3``, ``UNSIGNED``, ``Config`` (``rxte.py``); ``glob``
  (``nicer.py``); ``Table``, and ``astropy.visualization.hist`` which is shadowed by a
  local variable (``image_utils.py``); ``getdata``, ``Table`` (``nustar.py``). The
  shadowed ``hist`` is gone -- it was not merely dead, it pulled matplotlib into the
  module by itself, so removing the figures would not have made ``image_utils``
  matplotlib-free without it.
* ``nustar.py:584``: ``sep = target.separation(obj_j2000)`` is computed, the comment says
  "if <15 arcsec, all is okay", and nothing checks it. This is exactly the guard that would
  catch the wrong-source detection described in issue 4 -- it should be implemented, not
  removed.
* ``nustar.py:719``: a ``nuproducts`` command string is assembled and printed but never
  used; ``hsp.nuproducts`` is called separately.
* ``nustar.py:571`` and ``nustar.py:596``: ``make_image`` is called three times and
  ``make_radial_profile`` twice, with the first results discarded.
* ``image_utils.py:132`` printed the detection threshold and flux to stdout on every
  candidate, and ``get_best_source_region`` printed the radius it chose; library code
  should log, not print. **Fixed**: both are recorded values now, shown on the
  observation's page. See issue 51.
* ``nustar.py:734``: unused local ``basedir``.
* ``image_from_table`` took a ``correct_zeros`` argument that was never used. Removed
  -- and it was not merely dead, it was the vestige of a fix that had never been
  finished; see issue 50.

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


35. HEASOFT failures were invisible -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``heasoftpy`` defaults to ``allow_failure=True`` -- it says so in a deprecation warning on
every call -- so a tool that exits non-zero comes back as an ordinary result object with a
non-zero ``returncode``. Only ``nu_run_l2_pipeline`` ever looked. Every other call in the
package, some fifteen of them, carried on with whatever the tool had or had not written.

Observed cost, on a real three-observation run: ``fappend`` failed while attaching the
merged GTI extension, the merged event file went downstream with no GTI at all, and the
first sign of trouble was ``IndexError: list index out of range`` several steps later, in
``read_gti`` inside ``get_goes_gtis``. Nothing in the log connected the two.

``heasoft.run`` and ``heasoft.run_task`` now raise ``RuntimeError`` on a non-zero return
code, with the tool's own output in the message.

36. ``ftsort`` never sorted the merged GTIs -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Found immediately after issue 35 made failures visible. ``merge_gtis`` ran::

    ftsort infile=<gti file> outfile=!<gti file> columns=START

``ftmgtime`` writes its output with an empty primary header, and ``ftsort`` given a bare
file name lands on that primary header rather than on the table: ``CFITSIO ERROR NOT_TABLE:
CHDU not a table extension``, return code 235. Measured single-threaded, on every run, for
as long as the code has existed -- the sort simply never happened, and the failure was
swallowed. ``fthedit`` then renamed the unsorted extension to ``GTI`` and the pipeline
carried on.

The fix is ``infile=<gti file>[1]``. Four tests in ``tests/test_nustar.py`` record what each
of the three tool calls must say.

Whether the ordering ever mattered scientifically is a separate question -- ``ftmgtime``
appears to emit its intervals in order already -- but a call that fails every time is not
something to leave in place.

37. HEASOFT truncates file names at 160 characters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Several of the tools used here are old Fortran ftools, whose file-name parameters are
``character*160``. Measured with ``fappend``, varying only the length of ``outfile``:

============================  ================
``outfile`` length            result
============================  ================
156, 158, 159, **160**        succeeds
161, 162, 164, 170            ``could not open the named file``, status 104
============================  ================

The error message helpfully prints the truncated path, which is how it was identified. The
merged event file of an observation is ``<outdir>/<obsid>/nu<obsid>A_src1.evt``, about 40
characters past ``outdir``, so ``outdir`` has a practical budget of roughly 120 characters.

Not fixed in code -- there is nothing sensible to do about a tool's own limit -- but the
failure is now loud (issue 35) rather than a file that silently lacks an extension. It is
recorded in :doc:`technical_details` so that a deep output directory is a known cause
rather than a mystery.


38. Every worker starts its own Prefect server on one SQLite file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reported from a real 56-observation run at ``n_workers=4``::

    sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) database is locked
    [SQL: INSERT INTO configuration ("key", value, id, created, updated) ...]
    [parameters: {'key': 'TELEMETRY_SESSION', ...}]

Measured here with ``n_workers=4``: **five** temporary servers are started, one per worker
plus the parent, all writing the same SQLite file. In that particular traceback the loser
of the race is the server's own telemetry heartbeat, which is harmless -- the run carries
on -- but it is the first symptom of a database with more writers than SQLite wants.

Not a code fix: it is how the run is launched. Start one server and set ``PREFECT_API_URL``
so the workers connect to it as clients (measured: zero temporary servers, no lock errors),
turn the telemetry writer off with ``PREFECT_SERVER_ANALYTICS_ENABLED=false``, and raise
``PREFECT_SERVER_DATABASE_TIMEOUT``. The recipe is in :doc:`technical_details`.

Keep ``PREFECT_HOME`` on local disk. SQLite locking over NFS or Lustre is unreliable, so a
database under a network-mounted home or scratch directory can report "database is locked"
with any number of writers.


39. ``xselect`` truncates file names at 128 characters -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A different limit from issue 37, in a different tool, and much lower. Every one of the 56
observations in the M82 run failed at ``nusplitsc``::

    filter time file /scratch/.../out/80002092008/split//nu80002092008_chu2_gti_31721.fits
    Error determining file type for /scratch/.../split//nu80002092008_chu2_gti_31721.fi
    The file was not found

The name in the error ends ``.fi``, two characters short of ``.fits``. ``maketime`` had
just written that file successfully; ``xselect`` chopped the name and then looked for
something that does not exist. A few lines later the same truncation cuts the closing
quote off the shell command ``save events`` builds::

    save events /scratch/.../out/80002092008/split//nu80002092008B06_chu2_N_cl.evt
    sh: -c: line 0: unexpected EOF while looking for matching `"'
    sh: -c: line 1: syntax error: unexpected end of file
    Output event list not found.

**Measured.** Every ``Error determining file type for <path>`` in the run log was
extracted: **2376 of them, and all 2376 are exactly 128 characters long**, against real
paths of 130. That is a hard 128-character buffer, and nothing reports it as one.

**Not reproducible here.** On macOS, same ``XSELECT V2.5c``, ``save events`` was run at
100, 120, 126, 127, 128, 129, 130, 132 and 140 characters, with both a short and a
105-character input directory: every one succeeded. So this is a property of that HEASOFT
build, not of ``xselect`` in general, and the fix has to be to avoid long names rather
than to shorten any particular one.

**Which side of the tool matters.** ``xselect`` resolves the directory it *reads* from --
it prints the real path in ``Data Directory is:`` even when given a symbolic link -- but
takes output names exactly as given. The read path is the one measured good to 247
characters in issue 26, so the constraint is entirely on the write side.

**Fixed** by :func:`heasarc_retrieve_pipeline.utils.short_workspace`, which gives the
output directory a name about fifteen characters long in a private temporary directory,
and by :func:`heasarc_retrieve_pipeline.utils.check_name_length`, which refuses an
impossible path before anything is downloaded. Measured with the tool that was failing: a
real output tree 80 characters deep, reached through a 15-character link, ran ``nusplitsc``
to ``Exit with success`` with the files appearing in the real tree.

The budget, for reference: the reduction adds **61 characters** after the output root, so
against a 128-character limit an output root has to be 67 characters or fewer if it is
used directly. The user's was 77. Through the link it is about 76 characters in total
whatever the root is called.

The longest name is ``<OBSID>/split/nu<OBSID>A06_chu123_N_cl_3to80keV.fits``, the sky
image ``nustar_gen``'s ``make_image`` writes -- through ``xselect``'s ``save image``, the
write side -- while measuring an extraction region for a mode-06 (``SCIENCE_SC``) event
file. It was found by walking two finished output trees; the first answer, one of
``nusplitsc``'s own temporaries at 58 characters, came from reading the code and was three
characters short. See :ref:`technical_details` for the full table.

**What went to local disk with it, and what did not.** ``short_workspace`` also moves the
workers' private state off the shared filesystem, but only half of it. The HEASOFT
parameter files go on local disk: ``heasoftpy`` rewrites ``<PFILES>/<tool>.par`` around
each of the 44-plus sub-tools a single ``nupipeline`` run spawns, and on a parallel
filesystem every one of those was a network round trip for a few hundred bytes. The
workers' *working* directories do not. Measured on NuSTAR observation 80202020006 (32.6 ks,
202 MB of raw input) with a watcher polling the directory every five seconds, one worker's
working directory peaked at **182.5 MB** during ``nupipeline``, the largest contributor
being ``<pid>_tmp_nucoord``. That is about 90% of the raw data size and scales with it, so
eight workers on full-length observations would want gigabytes -- against 7.9 GB free on
the cluster's ``/tmp``, which is part of a root filesystem already 85% full and shared with
every other job on the node. The working directories therefore default to
``<outdir>/.workers``, and ``retrieve_and_process_data(scratch_dir=...)`` moves them to a
local disk where one has the room.



40. The 2026 M82 reprocessing, and what it found
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Issues 41 to 44 all come out of one run, so the run is recorded here once. 56 NuSTAR
observations of M82 X-2 on an Italian SLURM cluster, four worker processes, with the fixes
for issue 39 in place.

Before those fixes, **all 56 failed**. After them, **32 completed**. Every path-length
symptom was gone:

============================================  ==============  =============
Symptom                                       Before          After
============================================  ==============  =============
``Error determining file type``               2376            0
``nusplitsc`` failures                        1050            0
``The file was not found``                    many            0
``unexpected EOF``                            present         0
Segmentation faults                           2               0
============================================  ==============  =============

The two observations that had segfaulted, 80002092002 and 80002092004, both ran
``nupipeline`` from scratch and completed -- 13 m 43 s and 22 m 12 s, so they were not
skipped on a sentinel. That is the controlled comparison for the buffer-overrun explanation
in issue 39: the same raised stack limit, the same data, only the names shortened.

The 24 remaining failures, none of them path length:

============================================================  ===  ===========
Cause                                                          n   Issue
============================================================  ===  ===========
``FileNotFoundError`` on ``split/RECOVER_DONE.TXT``            8   41
Parameter file under ``$HOME/pfiles``                          7   42
``UnboundLocalError: best_radius``                             3   43
``IndexError: index 0 is out of bounds ... size 0``            2   47
``ValueError: cannot guess format ... zero-size array``        2   48
``RuntimeError: nusplitsc failed with return code 1``          1   not triaged
Solar flares cover the whole observation                       1   arguably right
============================================================  ===  ===========

The last one is 30901038001, 491 s of data entirely inside a flare interval. The pipeline
raising there is defensible; whether it should be a failure rather than a skip has not been
decided.

41. A slew is not a failure, and neither is a missing mode 06 -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``recover_spacecraft_science_data`` wrote its ``RECOVER_DONE.TXT`` sentinel into the split
directory, but ``nusplitsc`` is what creates that directory and ``nusplitsc`` only runs when
there is mode-06 data. An observation without any failed on ``FileNotFoundError``. Eight of
the 56.

**Measured**, from the modes ``nupipeline`` produced cleaned event files for, they were two
different things:

===============  ==========================  ==================================
OBSID            Cleaned modes produced      What it is
===============  ==========================  ==================================
30202022001      03                          slew
30502020001      02, 03                      slew
30502020003      02, 03                      slew
30502022001      02, 03                      slew
30202022003      01, 02, 03                  real science, no mode 06
30202022007      01, 02, 03                  real science, no mode 06
90202038001      01, 02, 03, 04              real science, no mode 06
90901332001      01, 03                      real science, no mode 06
===============  ==========================  ==================================

All 32 observations that completed produced both 01 and 06.

**Fixed** in two places. ``recover_spacecraft_science_data`` creates the split directory
before writing the sentinel, which is all the four real observations ever needed. And
``process_nustar_obsid`` stops after Level 2 when ``has_science_data`` is false, returning
``utils.NO_SCIENCE_DATA``, which ``process_observations`` counts separately from the
failures. See :ref:`technical_details` for why a slew cannot be recognised before Level 2
has run, and why its data are downloaded and kept anyway.

42. A worker's private parameter directory did not stay private -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Seven observations failed with a HEASOFT parameter file resolving to the shared
``$HOME/pfiles`` -- five on ``fthedit.par``, two on ``nuproducts``' ``extractor.par`` --
in three wordings that are all the same race::

    FileNotFoundError: [Errno 2] No such file or directory: '/home/.../pfiles/fthedit.par'
    OSError: parameter file /home/.../pfiles/fthedit.par not found
    Can't stat user parameter file /home/.../pfiles/extractor.par

**Measured.** 1016 ``fthedit`` calls ran in that job and a handful failed, and the messages
appear under all four worker PIDs rather than one. So it is intermittent contention, not a
worker that started without an environment. ``heasoftpy``'s ``HSPTask.find_pfile`` checks
that the file exists and then opens it; between those two the other workers had deleted and
rewritten it.

Note that this also disproves the obvious remedy. Creating ``$HOME/pfiles`` would not have
helped and the directory must already exist: if it did not, ``find_pfile`` would fall back
to ``$HEADAS/syspfiles`` and report nothing at all.

**Fixed** by repairing ``PFILES`` where the damage shows -- see :ref:`technical_details`.
What restores ``$HOME/pfiles`` is still unidentified; the guard logs the value it found, so
the next run will say.

43. A faint file took down the whole observation -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar_gen``'s ``optimize_radius_snr`` binds ``best_radius`` only inside
``if snr > old_snr``, and ``old_snr`` starts at zero, so a file with no source never binds
it and the return statement raises ``UnboundLocalError``. Three observations lost:
30202022008, 30702012004, 90101005002.

**Reproduced** directly against ``nustar_gen`` 0.8.dev9 -- a flat radial profile raises it
every time, with counts or without. This is a bug in the dependency; surviving it is ours.
``snr_optimised_radius`` turns it into ``None``, which the callers already handle.

44. Zero-exposure NuSTAR observations were downloaded -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both catalogue queries said ``exposure >= 0``, which correctly drops
planned-but-not-executed observations but keeps the ones the catalogue reports as having no
exposure at all.

``numaster`` means it: a NuSTAR observation with ``exposure_a`` of zero has no data.
``nicermastr`` does not always -- NICER's own pipeline sometimes filters an observation away
and records zero exposure for data that are perfectly usable -- so the filter must be
per-mission, not global. **Fixed** by ``core.exposure_condition``, driven by
``MISSION_CONFIG[...]["zero_exposure_may_be_wrong"]``: ``> 0`` for NuSTAR, ``>= 0`` for
NICER and for RXTE, which has not been checked. The single-OBSID query keeps ``>= 0``
everywhere.


45. An empty intermediate took down a good observation -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The 2026 M82 run was repeated with the fixes for issues 41 to 44 in place: 52
observations, four workers, 41 reduced, 0 held as no science data, 11 failed. Classifying
those eleven showed that **nine are the same defect wearing different clothes**. An empty
intermediate product -- an image with no counts, a GOES time series with no rows, an empty
list of files to merge -- reaches a step that assumes it is not empty, the step raises
instead of skipping, and the exception travels out of the task, out of the observation
flow, and marks the whole observation failed.

======================================================  ===  ==============
Cause                                                    n   Issue
======================================================  ===  ==============
``IndexError`` after ``find_source``                     3   47
``ftsort failed with return code 33``                    2   46
``ftmgtime failed with return code 45``                  1   46
``ValueError`` on a zero-size GOES series                1   48
``ConnectionError``: no online VSO mirrors               2   48
``nusplitsc failed with return code 1``                  1   not triaged
Solar flares cover the whole observation                 1   arguably right
======================================================  ===  ==============

Issues 46 to 49 record the individual mechanisms. The decision that shapes all four is a
scientific one: **a mode-01 module with no usable source fails the observation, and an
unusable mode-06 CHU subset is skipped**. M82 X-2 should never be undetectable in normal
science mode, so if it is, the run must say so rather than quietly deliver half an
observation; a single-CHU slice a few minutes long genuinely can hold nothing. Every skip
of the second kind goes into a per-observation ``skipped_inputs.txt``, so a run can be
audited without reading a 40 MB log. See :ref:`technical_details`.

46. A HEASOFT tool returned 0 and wrote nothing -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ftmgtime`` handed an empty list of input GTIs exits with return code 0 and writes no
file at all. ``ftsort``, told to sort the file that was never created, then fails with
``PIL ERROR PIL_BAD_FILE_ACCESS`` and return code 33 -- naming the wrong tool, one step
away from where the trouble started. Two observations were lost that way, 30202022007 and
90901332001, and a third, 30202022004, to ``ftmgtime failed with return code 45`` choking
on its second input.

Issue 35 fixed the case of a tool that *reports* failure. This is the case of a tool that
does not. **Fixed** by making ``produces`` a required argument of ``heasoft.run`` and
``heasoft.run_task``: every call declares the file, directory or in-place edit it must
leave behind, and the wrapper checks it before returning. An AST guard keeps all twelve
call sites declaring one.

The one place this can newly fail is ``nuproducts``: a spectrum with too few counts for
``rungrppha`` to write the grouped file will now raise where it used to pass in silence.
That is the intent, and it is what to watch in the next run.

47. No source in the image raised ``IndexError`` -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``nustar_gen``'s ``find_source`` returns an empty array when an image holds too few counts
to have a peak, and ``get_best_source_region`` indexed it immediately: ``IndexError: index
0 is out of bounds for axis 0 with size 0``.

Three observations were lost -- 90202038002 ``A06_chu1``, 30502021004 ``A06_chu1`` and
30702012004 ``B06_chu2`` -- and **never on a mode-01 file**. Every one was a single-CHU
subset of the spacecraft-science data. Every mode-01 file in those observations completed,
and in 30702012004 five of the six CHU subsets did too. The first two failed identically in
the previous run with a different worker layout and a different temporary directory, so it
is deterministic: those images really do hold too few counts.

**Fixed** by ``first_source_position``, which returns ``None`` instead, in the same shape
as ``snr_optimised_radius`` next to it. For a mode-06 subset that is a recorded skip; for a
mode-01 file ``get_best_source_regions`` raises ``NoSourceInScienceData``.

48. The GOES light curve was fetched once per event file -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``get_goes_gtis`` was a task keyed on an event file, so every module and every CHU subset
repeated the whole lookup: 91 ``goes_lightcurve`` task runs across 52 observations, up to
eleven for 31001019002 alone, each with its own ``Fido.search``, its own ``Fido.fetch`` and
its own copy of the downloaded files. Two failures were ``ConnectionError: No online VSO
mirrors could be found``, and every extra attempt is another chance to meet a mirror that
is down.

It was also wrong on its own terms. A mode-06 CHU slice a few minutes long can fall
entirely inside a gap in the once-a-minute GOES sampling. On 90201037002 ``A06_chu3`` the
truncated series had no rows, and astropy raised ``ValueError: cannot guess format from
input values with zero-size array``.

**Fixed** by fetching once per observation, over a span taken from the mode-01 cleaned
event files. ``require_goes_coverage`` still raises ``NoGoesCoverage`` when GOES genuinely
has nothing for an observation -- deliberately fatal, since silently keeping all the good
time would disable the flare filtering without saying so.

49. Merging an empty list of files -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``join_source_data`` called ``merge_event_files`` unconditionally, so a module for which
the source separation produced nothing reached ``ftmgtime`` with an empty input list --
issue 46's mechanism, one layer up. On 30202022007 this was a consequence of issue 43's
fix: FPMA's mode-01 file was too faint for a region, so nothing was extracted for FPMA, so
there was nothing to join. On 90901332001 the ``_back`` file was produced for FPMB and the
``_src1`` file was not.

**Fixed** by deciding it where the reason is still known. A module with mode-01 cleaned
events and nothing to merge raises ``NoSourceInScienceData`` naming the module; a module
with no mode-01 data at all is skipped with a warning, and the FPMA+FPMB merge takes only
the modules that produced something.


50. Three definitions of a valid sky position -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

NuSTAR sets ``X`` and ``Y`` to zero for an event the aspect reconstruction could not place
on the sky, and many observations carry a large pile-up of them at the origin.
``image_utils`` decided what that meant in three different ways, and in a fourth place did
not ask at all:

.. list-table::
   :header-rows: 1

   * - function
     - predicate
     - an event at (0, 0) was
   * - ``valid_table``
     - ``(X > 0) | (Y > 0)``
     - dropped -- but an event with only *one* coordinate set was kept
   * - ``filter_table_outside_regions``
     - ``(X < 0) | (Y < 0)``
     - **kept**, so the whole pile-up went into the background events
   * - ``filter_sources_in_images``
     - ``(X > 0) & (Y > 0)``
     - dropped; the only one that was right
   * - ``get_random_fluxes_in_img``
     - none
     - included in the bounding box the random apertures are thrown into

``valid_table``'s OR contradicted its own docstring, which said that zero and negative are
both the null marker. ``get_random_fluxes_in_img``'s docstring likewise already promised
apertures "inside the bounding box of the valid events", which is not what it computed: a
pile-up at the origin stretched the box from the field down to (0, 0), so most apertures
landed on empty sky, the median aperture count collapsed towards zero, and the ``median +
MAD`` acceptance threshold with it.

No output was actually wrong, because ``filter_sources_in_images`` applies the correct
predicate to the whole table before anything else runs, and every other helper sees only
what it passes on. The defects were latent, one reordering away from being live, and none
of the four helpers had a single test.

**Fixed** by ``has_sky_position``, one predicate used in all four places: both coordinates
must be positive. ``valid_table``, ``filter_table_outside_regions`` and
``filter_sources_in_images`` call it; ``get_random_fluxes_in_img`` takes its bounding box
from ``valid_table`` and so now does what it says. The dead ``correct_zeros`` argument of
``image_from_table`` (issue 32), which was an unfinished attempt at exactly this, is gone.

The four helpers now have tests, driven by an event table seeded with a pile-up at the
origin plus events with only one coordinate set. Each of the three old predicates was put
back in turn to confirm the tests fail with it: the OR breaks two ``valid_table`` tests and
the aperture test, ``< 0`` breaks the background test, and the raw bounding box breaks both
aperture tests.


51. Diagnostics were 1800 loose JPEGs and three log lines -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One reduced observation left 32 JPEGs across three directories, a ``skipped_inputs.txt``,
several ``*_DONE.TXT`` markers and a log. A run of 56 left about 1800 images, named by a
convention you had to know to find them, none of them zoomable, and none of them next to
the numbers that produced them. The run itself was summarised by three ``logger.info``
lines. ``get_best_source_region`` computed a radial profile and an SNR-optimised radius,
drew nothing at all, and printed one number to stdout.

The observation's own parameters never even reached the reduction: ``observation_work_items``
kept ``obsid``, ``url``, ``ra`` and ``dec`` and threw away the target name, exposure, dates,
observing cycle and solar activity that the catalogue query had already fetched.

**Fixed.** Every step records its status, timing and numbers under
``<OBSID>/diagnostics/``, and the run builds one self-contained ``diagnostics.html`` per
observation plus an ``index.html`` over all of them. The JPEGs are gone, and with them the
last matplotlib import in the package -- which is what finally closed issue 31. The design,
the record schema, and how it relates to ``skipped_inputs.txt`` and the step stamps are in
:ref:`diagnostics_and_reporting`.

Three properties are worth stating here, because they are what the design was for. A run
killed mid-step leaves a ``running`` record naming the step it died in. A page is written in
a ``finally``, so an observation that *failed* still gets one, with the error and its
traceback -- and a page that cannot be written is logged, never raised, so reporting can
never turn a good observation into a failed one. And ``python -m
heasarc_retrieve_pipeline.report <outdir>`` rebuilds everything from what is on disk,
finding the observations by looking rather than from a list of what the run meant to do.


52. A forced re-run of the join could not overwrite its own output -- FIXED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The way to make ``join_source_data`` run again is to delete its ``JOIN_DONE_SRC<N>.TXT``
markers. Doing that on an observation whose joined files were still on disk failed the whole
observation::

    RuntimeError: ftmerge failed with return code 105:
    failed to create new file (already exists?): .../nu30702012004_src1.evt
    CFITSIO ERROR FILE_NOT_CREATED

``ftmerge`` is called without CFITSIO's ``!`` clobber prefix, so it will not write a file
that exists. The per-module files ``nu<OBSID>{A,B}_src1.evt`` were deleted by
``join_source_data`` before each merge and so were fine; the combined ``nu<OBSID>_src1.evt``
was not, and it is the second merge, so the step got most of the way through before dying.
Reproduced on 30702012004.

**Fixed** in ``merge_event_files``, not in its caller: it now deletes its own output before
merging, exactly as ``merge_gtis`` immediately above it has always done with its own. The
caller's ``os.unlink`` is gone, so the rule lives in one place and holds for any future
caller. Deleting rather than passing ``!`` + ``outfile`` keeps the path a character shorter,
which matters against the 128-character limit of issue 39. Merging a file into itself is now
a ``ValueError`` rather than a silent loss of that input.

Tested by joining the same directory twice with the marker removed in between, running the
real ``merge_event_files`` against a HEASOFT double whose ``ftmerge`` refuses an existing
output the way the real one does.


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
