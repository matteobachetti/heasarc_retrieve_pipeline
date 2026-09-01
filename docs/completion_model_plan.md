# Resolve the reduction backwards, from a step that says what it made

*Design document, agreed 2026-09-01, not yet implemented. Sphinx ignores this file --
`docs/conf.py` sets `source_suffix = '.rst'` -- so it does not need to be in a toctree.
When the work lands, the durable parts move into `technical_details.rst` and
`known_issues.rst` and this file goes away.*

## Context

Written against commit `e14e6da`, on top of the completed "Make Prefect earn its keep"
work (path builders are plain functions, `wait_for` carries real futures, the legacy
fallback is gone) and the five fixes that came out of the 2026 M82 reprocessing.

The pipeline decides what to skip with eight hand-written checks, one per step, each
consulting its own marker file. `process_nustar_obsid` calls all nine steps in order on
every run, and each one globs `/scratch` before deciding to do nothing. Luigi's model is
better: a step declares a *target*, the scheduler walks the graph **backwards**, and a step
whose target is satisfied is neither run nor visited — so a finished observation exits at
once, having touched nothing.

Two things are wrong with the markers today, both visible in the code:

* **They are empty files.** `open(f, "a").close()`. They record that the step was reached,
  not what it produced. `nu_pipeline_done_file`'s own docstring already says so: "a
  sentinel records only *that* the step ran, not with which parameters, so changing
  `flags` will not trigger a re-run."
* **At least one lies.** `join_source_data` (`nustar.py:936`) writes `JOIN_DONE_SRC{n}.TXT`
  and then returns `[combined_file] if os.path.exists(combined_file) else []` — the marker
  can claim success on a run that produced no file, and the next run believes it. This is
  the same class of defect as the `RECOVER_DONE.TXT` bug fixed in `d9f5728`.

The goal is therefore both: **stop walking a finished reduction**, and **make "done" mean
something checkable**.

### Decisions taken with the user

1. **A new CALDB does not invalidate anything automatically.** The CALDB version and the
   pipeline version are recorded; a mismatch on a later run raises a **warning** and the
   existing products are reused. Reprocessing is the user's explicit choice, via `force`.
2. **Legacy trees are not invalidated.** An old empty marker counts as complete, logged
   once as unverified. The 32 observations that completed in the 2026 run are not redone.
3. **The per-file checks inside a step stay** — `barycenter_file` skipping an existing
   output, `get_best_source_region` reading region files back. They are what makes a
   crashed step cheap to resume, and once backwards resolution is in they only execute when
   the step genuinely has to run. "No needless checks" means not walking steps 1..N-1 when
   step N is already done.
4. **The old marker files keep being written**, alongside the new stamp, so that anything
   outside this package that looks for `PRODUCTS_DONE.TXT` goes on working. The stamp is
   authoritative; the marker is a courtesy. Both are written in the same place in the code,
   so they cannot drift apart.

## Approach

### 1. A step stamp that says what the step made

New in `utils.py`, next to the existing `absolute_config` / `short_workspace` helpers:

    write_step_stamp(path, step, outputs, provenance)
    read_step_stamp(path)          -> dict or None
    step_is_complete(path)         -> (bool, reason)

The stamp is JSON:

```json
{"stamp_version": 1,
 "step": "spectra",
 "finished": "2026-09-01T12:34:56+00:00",
 "pipeline_version": "0.4.2.dev12+g0a7e4e5",
 "caldb_version": "20260630",
 "flags": {"...": "..."},
 "outputs": {"nu80002092008A01_sr.pha": 123456}}
```

Two properties are not negotiable:

* **Output paths are relative to the stamp's own directory.** Workers see the tree through
  `short_workspace`'s symlink as `/tmp/hrp<random>/d/<OBSID>/...`, and that name is
  different on every run. An absolute path recorded today is meaningless tomorrow.
* **The write is atomic** — `tempfile.mkstemp` in the same directory, write, `os.replace`.
  A job killed mid-write must not leave something that parses as success. Reuse the
  `tempfile` import already in `utils.py`.

`step_is_complete` returns True when the stamp parses **and** every output it lists still
exists at the recorded size. A missing or resized output is named in the returned reason and
the step re-runs. An unparseable or empty file — a legacy marker — counts as complete and
returns the reason `"legacy marker, contents unverified"`, logged once per observation.

Version and CALDB mismatches do **not** affect completeness (decision 1).

### 2. Provenance, and the warning

`utils.caldb_version(caldb_path)` resolves `$CALDB/data/<caldb_path>/caldb.indx`, which is a
symbolic link to `index/caldb.indx<YYYYMMDD>`, and returns the date. Verified locally:
`~/devel/CALDB/data/nustar/fpm/caldb.indx -> index/caldb.indx20260630`. Returns `None` when
`CALDB` is unset or the mission's tree is absent — only NuSTAR's is installed on this Mac,
so that path is exercised by the tests rather than mocked.

`MISSION_CONFIG` (`core.py:771`) gains `"caldb_path"`: `"nustar/fpm"`, `"nicer/xti"`,
`"rxte/pca"`.

The pipeline version is `heasarc_retrieve_pipeline.__version__`, from `_version.py`.

On resume, `ReductionPlan` compares each satisfied step's recorded provenance with the
current one and logs **one** warning per observation, not one per step:

    80002092008 was reduced with pipeline 0.4.1 and CALDB 20250101; this run has 0.4.2 and
    CALDB 20260630 (newer). Reusing the existing products. Pass force=True to reprocess,
    or force="level2" to reprocess from a particular step.

CALDB versions are dates, so "newer"/"older" is stated. Package versions are only reported
as different, since ordering PEP 440 strings needs `packaging` and adds a dependency for
nothing. `flags` differing is reported the same way — which closes the gap
`nu_pipeline_done_file`'s docstring admits to.

### 3. One stamp per step, and the backwards walk

Every step gets exactly one stamp, in one place: `<outdir>/<OBSID>/.steps/<name>.json`.
That replaces markers currently scattered per directory (`SEPARATE_DONE.TXT`, one per
directory) and supplies one for the three steps that have none at all (regions, flares,
barycentring). The step table in `nustar.py`:

| name | step | legacy marker it also accepts |
|---|---|---|
| `level2` | `nu_run_l2_pipeline` | `event_pipe/PIPELINE_DONE.TXT` |
| `recover` | `recover_spacecraft_science_data` | `split/RECOVER_DONE.TXT` |
| `regions` | `get_best_source_regions` | — |
| `separate` | `separate_sources` | every directory's `SEPARATE_DONE.TXT` |
| `join` | `join_source_data` ×2 | `JOIN_DONE_SRC0.TXT`, `JOIN_DONE_SRC1.TXT` |
| `flares` | `filter_from_solar_flares` | — |
| `bary` | `barycenter_data` | — |
| `spectra` | `calculate_spectra` | `products/PRODUCTS_DONE.TXT` |

Legacy trees still exit immediately when finished: `spectra`'s legacy marker
`PRODUCTS_DONE.TXT` satisfies the last rung, and the walk stops there without looking at any
other step. The three steps with no legacy marker only matter for a *partial* legacy tree,
where they re-run and their surviving per-file checks make that cheap.

The legacy markers in that column go on being **written** as well as read (decision 4), by
the same `ReductionPlan.completed(...)` call that writes the stamp, so the two cannot drift.
The three steps that never had one do not gain one.

`ReductionPlan` (in `utils.py`, mission-independent) walks the table from the end and stops
at the first satisfied step:

```python
plan = ReductionPlan(NUSTAR_STEPS, base_dir, provenance, force=force)
if plan.finished:
    logger.info(f"{obsid} already reduced through {plan.last_step}; nothing to do")
    return
```

`process_nustar_obsid` keeps its one-line-per-step shape:

```python
ra, dec, region_size = plan.run_or_load(
    "regions",
    lambda: get_best_source_regions(obsid, config, wait_for=[pipeline]),
    lambda: read_source_regions(obsid, config),
)
plan.run("separate", lambda: separate_sources.submit(...).result())
```

**`load` is only needed where a step's return value feeds a later one**, which is three
places: `regions` (RA, Dec, radius — read back from the `.reg` files, which
`get_best_source_region` at `nustar.py:1639` already knows how to do; that branch is
promoted to a named function and reused), `recover` (returns `split_path(...)`, already a
pure path function), and `join` (a file list, recoverable by glob).

**The invariant the walk rests on**, to be stated in the docs and asserted by a test: step
targets are monotone — a stamp for step N exists only if steps 1..N-1 were complete when it
was written, because steps run in order and a stamp is written only on success.

### 4. `force`

Threaded through `retrieve_heasarc_data_by_source_name` and the OBSID flow →
`retrieve_and_process_data` → `process_observations` →
`download_and_process_observation` → the mission's `obsid_processing`, alongside the
existing `flags` and `scratch_dir` parameters.

* `force=False` (default) — as described above.
* `force=True` — ignore every stamp; run all steps.
* `force="regions"` — resume from that step, whatever the stamps say. This is what makes the
  CALDB warning actionable without `rm -rf`, and it is nearly free once the table exists.

An unknown step name raises `ValueError` naming the valid ones.

### 5. NICER

`process_nicer_obsid` (`nicer.py:266`) is two steps, `ni_run_l2_pipeline` then
`barycenter_data`. Same machinery, same table, legacy marker `PIPELINE_DONE.TXT`. RXTE is
left alone — it has no completion markers today and is out of scope, per the earlier
decision to leave the RXTE path be.

## Commits (local only, never pushed)

0. `Measure what a finished observation costs to re-walk` — rebuild one complete
   `80202020006` tree in `~/mamba/envs/henv313_x86` (about 50 minutes, unattended), then
   time `process_nustar_obsid` on it and count the Prefect task runs. The tree and the
   baseline both get reused by every later verification step. Recorded in
   `docs/known_issues.rst`, not a code change.
1. `Give a finished step a stamp that says what it made` — `utils.py`: write/read/complete,
   atomic write, relative output paths, legacy-marker tolerance. Tests only, nothing wired.
2. `Record the pipeline and CALDB versions a step ran with` — `caldb_version`,
   `MISSION_CONFIG["caldb_path"]`, provenance fields, the mismatch warning.
3. `Resolve the reduction backwards from its last step` — `ReductionPlan`, `NUSTAR_STEPS`,
   the legacy mapping, `read_source_regions` extracted from `get_best_source_region`, and
   `process_nustar_obsid` rewired. The step-level marker *checks* move out of the steps and
   into the plan; the marker *writes* move there too, so they still happen. Per-file checks
   stay where they are.
4. `Let the user force reprocessing` — `force` through both flows, including `force="<step>"`.
5. `Give NICER the same completion ladder`.
6. `Record the completion model` — `docs/technical_details.rst` gains a "How the pipeline
   knows what is already done" section with the table, the monotonicity invariant, the
   relative-path reason, and the `force` semantics; `docs/known_issues.rst` gains an entry
   for the `join_source_data` marker that lied, marked FIXED.

## Tests — offline, no network, no HEASOFT

New `tests/test_completion.py`:

* A stamp round-trips; output paths come back relative and resolve against a *different*
  parent directory (the `short_workspace` case).
* A deleted output makes the step incomplete, and the reason names the file.
* A resized output makes the step incomplete.
* A truncated/garbage stamp is incomplete; an **empty** one is complete-but-unverified.
* The write is atomic: a stamp written over an existing one is never observed half-written
  (write to a path, assert no temporary files remain in the directory).
* `caldb_version` against a fake CALDB tree built with `os.symlink` in `tmp_path`; and
  `None` when `CALDB` is unset.

In `tests/test_nustar.py`:

* **The ladder stops at the top.** With only the last step's stamp present, no earlier
  step's target function is consulted — assert via a table whose target callables record
  being called. This is the acceptance test for the whole plan.
* A satisfied step in the middle resumes at the next one.
* `force=True` runs everything; `force="regions"` resumes there; `force="nonsense"` raises.
* A legacy `PRODUCTS_DONE.TXT` alone is enough to exit immediately.
* A provenance mismatch warns **once** and does not change what runs.
* `read_source_regions` returns what `get_best_source_region` returns for the same
  `.reg` files.

* A finished step writes **both** the stamp and its legacy marker, and the marker is still
  an empty file where the old code made one (decision 4).

In `tests/test_prefect_wiring.py`, extend the existing AST guard: no step function may open
a `*_DONE*.TXT` path itself any more — the plan is the only place that writes them.

## Verification

* `pytest heasarc_retrieve_pipeline --doctest-modules` — currently **371 passed, 10
  skipped**; must not regress.
* `ruff check heasarc_retrieve_pipeline --isolated --select E4,E7,E9,F` — **18** findings
  today, all pre-existing; must not grow.
* `sphinx-build -W -E docs` — clean.
* **The end-to-end measurement, which is the acceptance criterion.** Commit 0 rebuilds the
  tree and takes the baseline, in `~/mamba/envs/henv313_x86` with a short output root per
  the session memory file. Then:
  1. **Before**, from commit 0: the wall time of `process_nustar_obsid` on the finished tree
     and the Prefect task-run count. That is the number this plan removes; it must not be
     claimed until it exists.
  2. **After**: the same run must produce **one** log line and **zero** task runs, and the
     output tree must be byte-identical (compare `find`+`stat` before and after).
  3. Delete `products/` and re-run: only `spectra` runs.
  4. Delete `.steps/spectra.json` but keep the products: `spectra` re-runs and overwrites
     cleanly.
  5. Touch the stamp's `caldb_version` to an older date: one warning, nothing re-run.
     Then `force="spectra"`: `spectra` re-runs.
* Environment incantation and the short-output-root rule: the session memory file
  `heasoft-x86-env-for-this-pipeline.md`.

## Out of scope

* **RXTE.** No completion markers today; adding them is its own piece of work.
* **Making each output file its own task**, Luigi-style. The chain is linear and the
  fan-out lives inside steps; a per-file scheduler would be a much larger change for the
  same observable behaviour.
* **Timestamp-based invalidation.** Deliberately not done — it is the classic source of
  spurious reruns, and provenance in the stamp covers the cases that matter here.
* The 26 direct `get_run_logger()` calls that should be `utils.get_logger()`, and the six
  untriaged failures from the 2026 run (2 `IndexError`, 2 `ValueError`, 1 `nusplitsc`,
  1 all-flares).
