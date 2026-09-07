# Adding XMM-Newton (EPIC) to `heasarc_retrieve_pipeline`

> **Handoff document.** Written 2026-09-07 against `heasarc_retrieve_pipeline` on branch
> `various_fixes` (HEAD `bc12c41`). **Commits 1–9 of the sequence below have landed**
> (`5c4ec2c`, `683fba1`, `e9bc841`, `c46a044`, `b1c7df4`, `a122c21`, `2cc6037`, `1e0db70`,
> step 7, step 8 and step 9, 2026-09-07); the rest is still the agreed design, not a
> report on work done. It is
> written to be picked up cold, by a person or a session with no memory of the
> conversation that produced it. Every number in
> it was measured against the live HEASARC archive on that date; the snippets under
> *Reproducing the archive facts* re-derive them, so none of it has to be taken on trust.
>
> Decisions marked **decided** were made by Matteo and should not be relitigated without
> him. Items under *Open items* are genuinely unresolved and need a machine with SAS.
>
> Not part of the Sphinx build: like `completion_model_plan.md`, it is listed in
> `docs/conf.py`'s `exclude_patterns`. Keep it updated as the steps land — strike a step
> when its commit is in, and move anything learned about the open items into
> `docs/technical_details.rst`, which is where the permanent record belongs. When every
> step is done this file has served its purpose and should be deleted rather than left to
> rot.

## Context

The pipeline reduces HEASARC data automatically: query a master catalogue, download an
observation, run the mission's reduction, record what happened. NuSTAR is worked out in
full; NICER and RXTE are thinner. We now want XMM-Newton, whose reduction needs ESA's SAS
(Science Analysis System) rather than HEASOFT.

Three things make XMM much cheaper than it first looks.

* **The mission seam already exists.** `MISSION_CONFIG` in `src/heasarc_retrieve_pipeline/core.py:891`
  is the *whole* abstraction — a dict of catalogue name, column names and one callable.
  Everything above it (queries, datalink, three download transports, the process pool,
  diagnostics, the HTML report) is mission-neutral and reused unchanged.
* **XMM's astrometry is good enough that no source finding is needed.** The user's position
  goes straight into the extraction region, which removes the most complicated part of the
  NuSTAR path (`image_utils.py`, `nustar_gen`, the SNR-optimised radius).
* **The archive is already reduced.** ESA bulk-reprocessed XMM with SAS 21.51 in
  November 2024. Measured against the live catalogue: `pps_flag = "Y"` for 22 779 of
  25 087 observations (91%), and both a 2000 observation and a 2018 one carry
  `sas_version = 21.51`, `pps_version = 21.51_20241115_1113`, `process_date` ≈ MJD 60640.
  The PPS (Pipeline Processing System) event lists are what `epproc`/`emproc` would
  produce, minus hours of CPU per exposure.

**Outcome:** `retrieve_heasarc_data_by_obsid(obsid, mission="xmm", ...)` downloads an XMM
observation and produces, per EPIC camera, exposure and mode, a flare-screened cleaned event
list, a barycentred copy, and a source spectrum with its background, ARF, RMF and
grouping — with the same diagnostics records and HTML page every other mission gets.

### Decisions taken (**decided** — do not relitigate without Matteo)

| | |
|---|---|
| Instruments | EPIC pn + MOS1 + MOS2. No RGS, no OM. |
| Modes | **Imaging and Timing both**, from the start. See *Timing mode* below. |
| Ingest | **PPS by default**, full ODF reprocessing available by config. |
| Flare GTI | Threshold the PPS `FBKTSR` light curve when present; `evselect` on the ODF route. The threshold is PPS's own `FLCUTTHR`, **decided 2026-09-07** — see *What changed while implementing step 6*. |
| Source list | Use PPS `OBSMLI` as a cross-check on the given position, never to override it. |
| Background | Annulus around the given position, radii configurable, user can override. |
| SAS access | Tasks run via `subprocess.run` with an argv list. (The probe was `import pysas`; it is now `SAS_DIR` plus `evselect` on `PATH` — see *What changed while implementing step 5*.) |
| Refactor | `heasoft.py` untouched. `sas.py` standalone, duplicating ~80 lines of output checking. |

### Measurements this plan rests on

Taken from the live archive and S3 listing for `0123700101`:

| | |
|---|---|
| Whole observation | 1.24 GB — PPS 977 MB, ODF 229 MB, `om_mosaic` 22 MB, `4XMM` 13 MB |
| **PPS files a reduction needs** | **~290 MB** — 4 EPIC event lists (280 MB), `FBKTSR`, `CALIND`, `ATTTSR`, `ORBTSR`, `OBSMLI`, `REGION`, `SUMMAR` |
| **ODF housekeeping** | **3.8 MB here, 5.3 MB on `0153950401`** — `SCX00000` + `ATS.FIT`, `ROS.ASC`, `RAS.ASC`, `SUM.ASC`, `TCS.FIT`, `TCX.FIT`, all but `SUM.ASC` gzipped |
| PPS event lists present | `PNS003PIEVLI`, `M1S001MIEVLI`, `M1U002MIEVLI`, `M2S002MIEVLI` |

So the PPS route downloads ~294 MB against the ODF route's 229 MB — a wash — and skips
`cifbuild`, `odfingest`, `epproc` and `emproc` entirely. The ODF housekeeping is fetched
on **both** routes because it is 3.8 MB and it is what the barycentring fallbacks need.

### Two constraints found while planning

1. **SAS cannot be declared as a dependency.** The `pysas` on PyPI is an unrelated
   speech-analysis package; ESA's pySAS ships inside SAS (`$SAS_DIR/lib/python/pysas`,
   reaching Python through `setsas.sh`) and the XMMGOF GitHub mirror has no packaging
   file. The HEASARC conda channel offers `heasoft`, `xspec`, `fv` and no SAS. So SAS is
   an *environment* requirement exactly like `HEADAS`, the `xmm` extra declares nothing
   installable, and **there is no CI job that can run a real SAS task.** XMM CI coverage
   is offline and stubbed, as `nupipeline`'s already is.
2. **`pysas.sastask.MyTask.run()` cannot report failure.** It runs the task with
   `shell=True`, logs `critical` on a non-zero exit, returns `None`, and never exposes
   `process.returncode`. It also re-quotes every value through the shell, which would
   mangle `evselect` expressions like `'#XMMEA_EP && (PATTERN<=4)'`. `sas.run` therefore
   builds an argv list and calls `subprocess.run` itself — the same escape hatch
   `nicer.ni_run_l2_pipeline` (`src/heasarc_retrieve_pipeline/nicer.py:179`) already uses
   for `nicerl2`.

### Timing mode (**decided**: in scope from the start)

The original plan was imaging-only, with Timing data treated as `NO_SCIENCE_DATA`. Matteo
asked for SAX J1808.4−3658, and **pn is in Timing mode on every modern observation of that
source** — as it is for most bright X-ray binaries, which is exactly the population this
pipeline is aimed at. Imaging-only would have declined the science it was built for.

PPS covers Timing, so this costs a product code and a region builder, not a new route:

* **Product code `TIEVLI`** alongside `PIEVLI` (pn imaging) and `MIEVLI` (MOS imaging).
  Confirmed present in the archive; `P0153950401PNS003TIEVLI0000.FTZ` has
  `DATAMODE = TIMING`, `SUBMODE = FastTiming`.
* **Regions are `RAWX` strips, not sky circles.** Timing mode collapses one dimension, so
  `ecoordconv` and the annulus do not apply. Source `RAWX in [31:45]`, background
  `RAWX in [3:5]` for pn; the numbers go in config, like the annulus factors.
* **Screening differs** — though not the way this paragraph originally guessed. There is
  no timing macro to replace `#XMMEA_EP` with: SAS applies the same expression to pn in
  both modes, and it is MOS whose pattern cut changes, to `PATTERN==0`. See *What changed
  while implementing step 8*.
* **Pile-up is real** at these count rates. Run `epatplot` and record its output as a
  diagnostic; do not attempt automatic correction.

**The parser must key on mode, not just exposure.** MOS `FastUncompressed` puts the central
CCD in timing and the outer six in imaging, and PPS emits **both** under one exposure ID —
verified on `0153950401`, where `M1S004` yields an `MIEVLI` with `DATAMODE = IMAGING` and a
`TIEVLI` with `DATAMODE = TIMING`, both `SUBMODE = FastUncompressed`. So the `Exposure` key
is `(instrument, expid, mode)`; a naive `(instrument, expid)` mapping silently drops one of
the two. This is a cheap unit test and it is the first one to write.

---

## Architecture: two front ends, one back end

```
PPS route (default)                    ODF route (config: products="odf")
  PPS event lists  ─┐                    cifbuild → ccf.cif
  CALIND → SAS_CCF  │                    odfingest → *SUM.SAS
  FBKTSR light curve│                    epproc + emproc → event lists
  OBSMLI source list│                    evselect → high-energy light curve
                    ▼                                   │
              ┌─────────────────────────────────────────┘
              ▼
   list of Exposure(instrument, expid, mode, submode, event_list, flare_lightcurve)
              │
              ├─ flare GTI          threshold the light curve  (pure Python, existing utils)
              ├─ clean event lists  evselect + screening macros
              ├─ source position    ecoordconv, cross-checked against OBSMLI
              ├─ spectra            especget + specgroup
              └─ barycentre         see step 8
```

Only the front end differs. That is the anti-duplication shape: one mission module, one
downstream path, one set of diagnostics records.

The pure-Python reuse is real and worth naming. Thresholding `FBKTSR` uses
`utils.intervals_above_threshold`, `utils.merge_intervals` and `utils.good_intervals`
(`utils.py:503, 457, 588`) unchanged; applying a GTI uses `utils.apply_gti` and
`utils.update_time_bounds` (`utils.py:1090, 1246`); XMM's single-float `MJDREF` is already
handled by `utils.time_reference` (`utils.py:842`) and `barycentered_file_name` is already
tested against `.ds` (`tests/test_barycenter.py:25`). `rxte.py` is the precedent for a
mission module doing GTI screening in pure astropy.

**Event screening stays in `evselect`, deliberately.** The standard expressions use
`#XMMEA_EP` / `#XMMEA_EM`, macros expanding to FLAG bit tests defined by SAS and the CCF,
not by us. Reimplementing those bitmasks in astropy would silently corrupt the science the
day they change. Thresholding a light curve, by contrast, is our own choice of number, so
it belongs in Python.

---

## Step 1 — `cycle` out of the hardcoded OBSID query

`obsid_query` (`core.py:1108`) selects `cycle` unconditionally. Verified against the live
TAP service: `numaster`, `nicermastr` and `xtemaster` have that column, **`xmmmaster` does
not** — so `retrieve_heasarc_data_by_obsid(mission="xmm")` fails on the query alone.

Move `cycle` into each mission's existing `"additional"` string and drop it from the
f-string. `nustar` gains `cycle, solar_activity`; `nicer` gains `cycle`; `rxte` already
names `cycle, prnb`, so remove the duplication there.

*Test:* for every key of `MISSION_CONFIG`, `obsid_query` mentions only columns that
catalogue has — asserted against a hardcoded per-mission set, so it stays offline.

## Step 2 — a per-mission download filter

`recursive_download` (`core.py:836`) already takes `re_include` / `re_exclude` and nothing
passes them. Add an optional `"download"` key to `MISSION_CONFIG`, read it in
`download_and_process_observation` (`core.py:1518`) and forward it. Missions without the
key behave exactly as now.

XMM needs two filters, chosen by the config's `products` setting:

* **PPS** — the EPIC event lists (`PIEVLI`, `MIEVLI`, `TIEVLI`), `FBKTSR`, `CALIND`,
  `ATTTSR`, `ORBTSR`, `OBSMLI`, `REGION`, `SUMMAR`, plus the six ODF housekeeping files.
  One alternation regex; the `.PDF` and `.PNG` companions of `FBKTSR` are excluded by
  anchoring on `\.(FTZ|ASC|HTM)$`.
* **ODF** — `/ODF/`.

Two traps in that regex, both found by listing real observations rather than by reasoning:

* **`OBSMLI` is not unique to EPIC.** The Optical Monitor emits one too, so
  `0153950401` carries `EPX000OBSMLI` *and* `OMX000OBSMLI`, each in `.FTZ` and a companion
  (`.HTM` for EPIC, `.ASC` for OM). Anchor on `EPX000OBSMLI` or the filter quietly pulls
  the OM list and the cross-check reads the wrong table.
* **Measured cost of the finished filter**, re-measured against the regex that shipped in
  `a122c21`: `0153950401` **39.8 MB of 205.8** (19 files of 461); Crab `0611180201` 69.8 MB
  of 204.5; SAX J1808 `0804330201` **77.3 MB of 393.0**; Mkn 421 `0123700101` 290.2 MB of
  1241.2, which is large only because its pn event list alone is 212 MB. The filter is what
  makes a 35 ks observation an ordinary download — *short* and *small* are different axes,
  and the filter decouples them.
* **The six ODF housekeeping files are not named the way this document first said.** They
  are `<revolution>_<OBSID>_SCX00000<CODE>.<EXT>`, and most of them are gzipped:
  `SCX00000ATS.FIT.gz`, `RAS.ASC.gz`, `ROS.ASC.gz`, `SUM.ASC` (not gzipped), `TCS.FIT.gz`,
  `TCX.FIT.gz`. They come to 5.3 MB on `0153950401`, of which `RAS.ASC.gz` is 4.9 —
  the earlier figure of 3.1 MB was for a different observation.

Since the filter now depends on the run's config and not only on the mission, the value in
`MISSION_CONFIG` is a callable `download_filter(config) -> dict`, not a literal dict.

*Test:* the regexes select exactly the intended files out of a recorded listing of
`0123700101` (the file names are already captured above; follow the
`ARCHIVE_INDEX_HTML` precedent in `tests/test_core.py`), and nothing is passed when a
mission omits the key.

## Step 3 — `src/heasarc_retrieve_pipeline/sas.py`

Mirrors `heasoft.py`'s public shape; standalone by decision.

```python
HAS_PYSAS   # `import pysas` succeeded
HAS_SAS     # HAS_PYSAS and os.environ.get("SAS_DIR") and shutil.which("evselect")
SAS_LOCK    # threading.RLock(), same reasoning as heasoft.HEASOFT_LOCK
IN_PLACE    # marker class, copied from heasoft.py:259
run(name, *, produces, log_to=None, env=None, **params)
sas_environment(obsid, config)
```

* `run` builds `[name, "k=v", ...]`, calls `subprocess.run(argv, env=env, ...)` under the
  lock with stdout+stderr to `log_to` (path from the existing `utils.tool_log_file`,
  `utils.py:1427`), raises `RuntimeError` naming the task on a non-zero return code, then
  runs the copied `_check_outputs` over `produces`. `produces` is keyword-only and
  mandatory, for the reason `heasoft.py`'s docstring gives: a zero return code is not
  evidence that anything was written.
* `sas_environment` returns a **copy** of `os.environ` with `SAS_CCF`, `SAS_ODF`,
  `SAS_CCFPATH` and `SAS_VERBOSITY` set. These are per-observation, and a worker process
  reduces several observations in sequence (`core.prepare_worker`, `core.py:1266`);
  passing the environment explicitly per call means there is no process-global state to go
  stale — the `PFILES` lesson applied before it bites.
* Module docstring says why this duplicates `heasoft.py` rather than sharing with it, and
  each copied helper carries a one-line pointer to its twin.

*Tests* (`tests/test_sas.py`, offline, `subprocess.run` monkeypatched): argv order and
`k=v` formatting; an expression containing spaces, `&&` and `#` survives unchanged; a
non-zero return code raises naming the task; a zero return code with a missing or empty
output raises; `IN_PLACE` accepts a pre-existing file; `sas_environment` never mutates
`os.environ`. Extend the AST guard at `tests/test_heasoft.py:401` so `sas.run` also fails
CI without `produces=`.

## Step 4 — `xmm.py`: config, paths, and the PPS front end

`DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./", products="pps",
sas_ccfpath=None, src_radius_arcsec=30.0, bkg_inner_factor=1.5, bkg_outer_factor=3.0,
flare_rate_limit=dict(pn=0.4, mos=0.35))`, plus per-instrument filter expressions as module
constants.

Path builders take `(obsid, config)`, matching NuSTAR (the majority, and what
`check_name_length` is driven from): `xmm_base_output_path`, `xmm_pps_path`,
`xmm_odf_path`, `xmm_pipeline_output_path` → `<OBSID>/event_cl`, `xmm_product_output_path`
→ `<OBSID>/products`, plus the `CALIND` / summary / sentinel locators. `event_cl` and
`products` are deliberate: `report.OBSERVATION_SUBDIRECTORIES` (`report.py:56`) already
recognises both, so `hrp-report` finds XMM trees with no change.

**`xmm_exposures_from_pps(obsid, config)`** — pure parsing, no SAS. PPS names are
`P<OBSID><INST><EXPID><PRODUCT><NNNN>.FTZ`, so `P0123700101PNS003PIEVLI0000.FTZ` yields
`("pn", "S003", imaging)`. Returns the `Exposure` list, pairing each event list with its
`FBKTSR` light curve. `CALIND` becomes `SAS_CCF`. If there are no EPIC event lists **of any
mode** the flow returns `utils.NO_SCIENCE_DATA` — the treatment a NuSTAR slew gets
(`nustar.py:3019`): not a failure, counted separately, data left on disk. (Before Timing
came into scope this test was "no *imaging* event lists"; with Timing supported, a
Timing-only observation is science, not an empty one.)

*Fully unit-testable from file names alone.* This is where most of the offline test value
is, and it is written first.

## Step 5 — the ODF front end (config `products="odf"`)

Reached when the observation has no usable PPS directory, the PPS `sas_version` is too old,
or the user asks.

**`pps_flag` is a hint, not a guarantee — the route must be chosen by probing the
directory.** `0973390101` has `pps_flag = "Y"` in `xmmmaster` and yet no PPS directory is
mirrored at HEASARC: only `ODF/`. Deciding the route from the catalogue row alone would
download nothing and then fail with an empty product list. So: read `pps_flag` to *predict*
the route and say so in the log, then confirm against the actual listing before committing,
and fall back to ODF when the prediction is wrong. This is one extra listing request, and
`recursive_download` already walks the directory anyway.

Stage `<OBSID>/odf/`: HEASARC serves the ODF as a flat directory of 325 `*.FIT.gz` /
`*.ASC` files (verified), and SAS recognises `.FIT`, `.FTZ` and `.ASC`, not `.FIT.gz` — so
stage symlinks renamed `.FIT.gz` → `.FTZ`, copying `SUM.ASC`. Then `cifbuild` → `ccf.cif`,
`odfingest` → `*SUM.SAS`, `epproc` + `emproc` → event lists, and an `evselect` high-energy
light curve per exposure to stand in for `FBKTSR`. Sentinel `PIPELINE_DONE.TXT`.
Diagnostics steps `"odf_ingest"` and `"l2_pipeline"` (the latter title already exists).

*The `.FIT.gz` → `.FTZ` rule is the one thing here I cannot verify without SAS. It is
isolated in a pure function with its own test, so the fix, if any, is one line.*

## Step 6 — shared back end: flares, cleaning, position

**`xmm_flare_gti`** — read the light curve (PPS `FBKTSR`, or the `evselect` one), threshold
it with `utils.intervals_above_threshold`, invert with `utils.good_intervals`, tidy with
`utils.merge_intervals`. Record the curve, the threshold and the kept/removed exposure with
`rec.array` / `rec.value` so the report draws it the way it draws NuSTAR's solar-flare
filtering. Step `"flare_filtering"` — title already in `STEP_TITLES`. No SAS, so it is
fully testable offline.

**`xmm_clean_event_lists`** — `evselect` with the GTI and the standard screening:
pn `#XMMEA_EP && (PATTERN<=4) && (PI in [200:12000]) && FLAG==0`,
MOS `#XMMEA_EM && (PATTERN<=12) && (PI in [200:12000])`.

**`xmm_source_sky_position`** — `ecoordconv coordtype=eqpos` converts the user's RA/Dec to
sky X/Y. Stdout parsing is a pure function with its own test. Region strings are pure
functions too — `((X,Y) IN circle(x,y,r))` and `((X,Y) IN annulus(x,y,r_in,r_out))` — with
arcsec converted at XMM's 0.05 arcsec per sky pixel, written as a named constant rather
than the magic number NuSTAR's 2.45 currently is (`nustar.py:3032`). **Timing exposures skip
this entirely**: there is no sky image, so the region is the `RAWX` strip and
`ecoordconv` is not called.

**OBSMLI cross-check** — match the given position against the PPS source list, record the
nearest detection and its offset. Note the schema: `SRCLIST` has 249 columns and **no rate
column at all** — the brightness measures are `EP_TOT_FLUX` and the per-band `EP_n_FLUX`
(erg cm⁻² s⁻¹), so record a flux, not a count rate. `utils.position_is_consistent`
(`nustar.py:551`) is the existing shape for this. It never moves the region; it only makes
a mistyped position or an absent source visible on the page. Step `"source_position"` —
title already exists.

**It must warn, never fail** — the source list misses real targets. On the Crab
(`0611180201`) the nearest `OBSMLI` detection is **328.79″** from the pulsar: the nebula is
extended and piled up, so maximum-likelihood point-source detection does not find it at all.
A pipeline that aborted on a large offset would refuse the Crab. Contrast `0153950401`,
where the nearest detection is **1.42″** away with the next at 33.8″ — an unambiguous match.
Report the offset, do not act on it.

## Step 7 — spectra

`especget` with `srcexp` and `backexp` does source spectrum, background spectrum,
`BACKSCAL`, `arfgen` and `rmfgen` in one call; then `specgroup` for the grouped spectrum
you fit. Step `"calculate_spectra"` — title already exists.

*`especget`'s output names are version-dependent; pin them by running it once and record
them as a module constant, as `NUPRODUCTS_SPECTRA` (`nustar.py:2458`) does.* — **not what
was done.** `withfilestem=no` names all four outputs outright, which makes the version
irrelevant instead of pinned. See *What changed while implementing step 9*.

`SAS_CCF` comes from the PPS `CALIND`, which is the CIF the SOC used. If a constituent it
names is missing from the local `SAS_CCFPATH`, the fallback costs nothing and needs no ODF:
`cifbuild withobservationdate=yes observationdate=<date>` builds a fresh index from the
observation date alone.

**No cross-instrument coaddition.** pn and MOS are different detectors with different
responses, so `addspec`'s case B does not apply and `coadd.apply_case_b_scaling` would be
wrong. The three spectra are meant to be fitted jointly. `epicspeccombine` is the right
tool if we ever want one file; noted in `known_issues.rst`, not built now.

## Step 8 — barycentring, the one open dependency

> **Settled, 2026-09-08: candidate 1 wins and candidate 3 is impossible.** See
> *What changed while implementing step 11* below for the measurements. The text below is
> the plan as written, kept for the reasoning.

SAS `barycen` edits in place and locates the orbit through `SAS_ODF`, which the PPS route
does not produce. Three candidates, to be settled on the first real run — which is why the
3.8 MB of ODF housekeeping is downloaded on both routes, and why this is the last step in
the sequence rather than a blocker:

1. `barycen` with `SAS_ODF` pointed at a minimal ODF assembled from the housekeeping files.
2. `barycen` on the ODF route only, with the PPS route documented as "no barycentring
   unless you also fetch the ODF".
3. HEASOFT `barycorr` with the PPS `ORBTSR` file, reusing `barycenter.barycenter_file`
   unchanged. Cheapest if it works, but unverified: `barycorr` is a Perl wrapper over
   `axBary` whose only mission-specific handling is Swift and NuSTAR, and whether
   `scorbit` reads XMM's `ORBTSR` format is exactly the open question.

Whichever wins, the surrounding code is reused: copy to the name
`barycenter.barycentered_file_name` gives (already extension-agnostic, already tested
against `P0123_events.ds`), declare `produces=IN_PLACE(...)`. Step `"barycenter"` — a new
entry in `STEP_TITLES`.

## Step 9 — wire it in

`core.py`: import beside the other three (`core.py:33`) and add at `core.py:891`:

```python
"xmm": {
    "table": "xmmmaster",
    "expo_column": "duration",
    "zero_exposure_may_be_wrong": True,
    "additional": "pn_time, mos1_time, mos2_time, pn_mode, mos1_mode, mos2_mode, "
                  "pps_flag, sas_version",
    "obsid_processing": process_xmm_obsid,
    "default_config": XMM_DEFAULT_CONFIG,
    "name_column": "name",
    "longest_output_name": xmm_longest_output_name,
    "download_filter": xmm_download_filter,
},
```

`pps_flag` and `sas_version` are in `additional` so the reduction can say "this observation
has no PPS, falling back to the ODF route" from the catalogue row, before downloading
anything.

**`process_xmm_obsid(obsid, config=None, ra="NONE", dec="NONE", flags=None)`** — the `@flow`
`MISSION_CONFIG` dispatches to. `config=None`, not `{}`, so the fallback to `DEFAULT_CONFIG`
actually fires (known issue 27, which bites NICER and RXTE). Futures are always
`.result()`-ed, for the reason at `nustar.py:3007`.

`report.py`: add `"odf_ingest"` and `"barycenter"` to `STEP_TITLES` (`report.py:75`) and
`"PPS"` / `"ODF"` to `OBSERVATION_SUBDIRECTORIES` (`report.py:56`), so a downloaded-but-
unreduced observation is recognised. Check `diagnostics.CANONICAL_COLUMNS`
(`diagnostics.py:128`) covers `xmmmaster`'s `name` and `duration`.

`pyproject.toml`: an `xmm` extra documenting that SAS is an environment requirement.

`segments.py`, `combine.py`, `roundtrip.py` and `recover.py` all `from .nustar import`
directly and stay NuSTAR-only. Splitting and merging XMM observations needs a
mission-neutral product layer first and is **explicitly out of scope**; say so in the docs
rather than let the generic CLI names imply otherwise.

## Step 10 — tests, docs

New `tests/test_xmm.py` and `tests/test_sas.py` in house style: `class TestSomeSentence`,
sentence-length names, `tmp_path` + `monkeypatch`, no committed FITS. Written first, step by
step. Everything marked "pure function" above is tested without SAS — the PPS name parser
(including MOS `FastUncompressed` yielding two lists under one exposure ID), the download
regexes (including `OMX000OBSMLI` being rejected), the ODF rename rule, the flare threshold
logic, the `ecoordconv` parser, the sky-region and `RAWX`-strip builders, the OBSMLI match
(both the 1.42″ hit and the 328.79″ miss), and `NO_SCIENCE_DATA` when there are no EPIC
event lists at all. Reduction steps use a `StubSas` double in the mould of
`test_segments.py:98`'s
`StubNuproducts`, writing the files each task claims to produce because `sas.run` verifies
its outputs.

Add an `xmm` marker to `conftest.py:55` alongside `heasoft`, skipping unless `sas.HAS_SAS`,
and one `@pytest.mark.remote_data` case in `tests/test_pipeline.py`.

Docs: an "XMM-Newton" section in `docs/technical_details.rst` after NICER, in the same
voice — what SAS is and why it is an environment requirement, the PPS-versus-ODF choice and
the November 2024 reprocessing that justifies the default, the flare screening, why the
background is an annulus and what that costs on pn (out-of-time events and chip gaps), and
why pn and MOS are not co-added. `automodule` blocks for `xmm` and `sas` in `docs/api.rst`
— the docs build runs `sphinx-build -W`, so this is not optional. Mission list and the SAS
note in `README.rst`. Deferred items into `docs/known_issues.rst`.

---

## Commit sequence

One commit each, tests first in every case.

1. ~~`cycle` out of the hardcoded OBSID query.~~ **Done, `5c4ec2c`.**
2. ~~Per-mission download filter threaded to `recursive_download`.~~ **Done, `683fba1`.**
3. ~~`sas.py` — probe, lock, `run` with mandatory `produces=`, explicit environment.~~
   **Done, `e9bc841`.**
4. ~~`xmm.py` — config, path builders, PPS name parser keyed on
   `(instrument, expid, mode)`, `NO_SCIENCE_DATA`.~~ **Done, `c46a044`.**
5. ~~PPS download filter and front end, with the route probed rather than trusted.~~
   **Done, in three commits.** `b1c7df4` drops the `pysas` import from the SAS probe;
   `a122c21` is `xmm_download_filter`; `2cc6037` is `core.list_archive_directory`, the
   `resolve_config` hook and `xmm_resolve_config`.
6. ~~Flare GTI from the PPS light curve (pure Python).~~ **Done, `1e0db70`** — but not at
   the threshold this document proposed; see below.
7. ~~`evselect` cleaning and `ecoordconv` position, with the OBSMLI cross-check.~~
   **Done** — and the first step verified against a real SAS run; see below.
8. ~~Timing mode — `RAWX` regions, timing screening, `epatplot` pile-up diagnostic.~~
   **Done, in four commits** — `b0b1a79` the mode-aware screening, `73b719c` the `RAWX`
   strips, `59d8bf0` the pile-up check, `e31cd3e` the plot name the real run corrected.
   Verified end to end against Her X-1; see below.
9. ~~`especget` spectra and grouping.~~ **Done, in two commits** — `c589274` the
   extraction, `8347a31` the working directory the real run forced. Verified end to end,
   including a load in XSPEC; see below.
10. ODF front end — staging, `cifbuild`, `odfingest`, `epproc`, `emproc`.
11. Barycentring, once one of the three candidates is verified.
12. `MISSION_CONFIG` entry, report titles and subdirectories.
13. Docs.

**The remaining order is not 10, 11, 12, 13 — decided by Matteo, 2026-09-07.** After the
M82 reconnaissance below, the sequence is

> **12 → the pn imaging acceptance run → 11 → the M82 X-2 batch → 10 → 13**,

with two pieces of step 12's neighbourhood pulled forward ahead of it because the
acceptance run needs them: the `cifbuild` calibration index (*Building the calibration
index*) and `Exposure.submode` with its window-fit warning.

The reasoning, so it is not relitigated: nothing can be run end to end until the
`MISSION_CONFIG` entry exists, which makes 12 the gate on both acceptance targets; the
M82 science is the 1.37 s pulsation, which puts 11 before the batch; and **step 10 is the
one remaining chunk neither acceptance target needs**, since every M82 observation has
PPS. Deferring it puts the largest, least verifiable step — `epproc` and `emproc` are
hours of CPU with no CI that can run them — after the work that proves the science.

(The step numbers above are the *commit* order; the prose sections are numbered
independently and Timing mode is folded into steps 4, 6 and 7 there.)

## What changed while implementing steps 1–4

Four departures from the text above, all small, none reversing a **decided** item. They
are listed so that the next session can see what the code does that the plan does not say.

* **`sas_environment` takes paths, not `(obsid, config)`.** Building XMM's paths inside
  `sas.py` would make the SAS layer import the mission layer, which is the one dependency
  `heasoft.py` does not have on `nustar.py`. The signature is
  `sas_environment(ccf=None, odf=None, ccfpath=None, verbosity=None)` and `xmm.py` supplies
  the paths.
* **`xmm.py` has an `xmm_config(config)` that merges the caller's config over
  `DEFAULT_CONFIG`.** `utils.absolute_config` substitutes the default only for `None`, so
  the partial config `core.download_and_process_observation` passes — the two paths and
  nothing else — would have left `config["products"]` raising `KeyError` mid-reduction.
  NuSTAR works around this with `config.get("max_radius", 80)`, duplicating the default at
  the point of use. `absolute_config` itself is untouched: changing it would change three
  other missions, and that is Matteo's call, not a side effect of adding XMM.
* **`mission_download_filter` refuses a filter that names anything but `re_include` and
  `re_exclude`.** A misspelt key would be dropped in silence and the symptom — a gigabyte
  arriving where forty megabytes were meant to — reads as a slow network.
* **The step-1 test records all four catalogue schemas, not just the interesting columns.**
  `tests/test_core.py` holds a copy of `TAP_SCHEMA.columns` for `numaster`, `nicermastr`,
  `xtemaster` and `xmmmaster` as of 2026-09-07, including `xmmmaster` before XMM is a
  mission, so the guard is already in place on the day the `MISSION_CONFIG` entry is added.

Two archive facts were re-derived and one number in this document was wrong: `0153950401`'s
MOS2 exposure is **`M2S005`**, not `M2S002`. Everything else checked out, including the
`DATAMODE`/`SUBMODE` pairs and the `OMX000OBSMLI` trap. One fact worth adding: **PPS writes
no `FBKTSR` for the pn timing exposure of `0153950401`** — the flare light curves present
are `M1S004`, `M2S005` and `R1S001` (RGS, not ours). Step 6 therefore cannot assume every
exposure has one; `Exposure.flare_lightcurve` is `None` there.

## What changed while implementing step 5

Step 5 became three commits, and one of them was not in the plan at all.

* **`has_sas` no longer requires that `import pysas` succeed** (`b1c7df4`). The plan had
  pysas as the probe for "is a SAS installation present". Matteo's machine is the first
  real evidence that it is the wrong probe: a complete SAS 22.1.0 with every task on
  `PATH`, and `import pysas` failing because `beautifultable` — a table formatter this
  package never touches — was missing from `henv313`. The pipeline called that "no SAS".
  Since `sas.run` reaches the tasks through `subprocess.run`, pysas is not on the path
  between this package and a reduction and has no say in whether one can happen. The probe
  is now `SAS_DIR` plus `evselect` on `PATH`, and the guard in `tests/test_sas.py` was
  widened from "only `sas.py` imports pysas" to "no module does" — and rewritten to read
  the syntax tree, since `sas.py`'s prose now discusses the import it does not make.
* **`pps_flag` is not read at all.** The plan wanted it read to *predict* the route and
  logged, then confirmed against the listing. `download_and_process_observation` never
  sees the catalogue row, and threading it down three layers to produce one log line is
  not worth it when the listing is authoritative and costs one request.
* **The probe is a second `MISSION_CONFIG` hook, `resolve_config(config, url) -> config`**,
  read by `core.mission_resolve_config` and called just before the download. Keeping it
  separate from `download_filter` leaves that one pure and offline-testable, and it is
  honest about the two being different questions: one decides the configuration, the other
  decides what to fetch. It refuses a hook that returns something that is not a dictionary.
* **`core.list_archive_directory(url)`** is the shallow listing the probe needs — one
  directory, no recursion, all three transports, answering in one spelling. It returns
  `None` for "could not look" and `[]` for "nothing here", and every caller keeps the
  difference: downgrading the route on a network timeout would fetch a quarter of a
  gigabyte of telemetry for an observation whose PPS products are sitting in the archive.
* **The demotion only ever runs one way**, `"pps"` → `"odf"`. A run that asked for the ODF
  route keeps it and the archive is not listed at all, which is also how "the user asks"
  is honoured without having to track whether a config key was set explicitly.
* **The route is not stored anywhere else.** The reduction reads it off the disk — if
  `<OBSID>/PPS` holds EPIC event lists, that is the PPS route — so there is no second copy
  of the answer to keep in step with the first.

Two further archive facts, verified live on both transports: `0973390101` really does hold
only `ODF/` on the HTTPS mirror as well as in S3, and `0153950401` holds `4XMM/`, `ODF/`,
`PPS/` and `om_mosaic/`.

## What changed while implementing step 6

**The flare threshold is not a number this pipeline picks.** This is the one thing about
step 6 that had to be measured, and it overturns `DEFAULT_CONFIG`'s
`flare_rate_limit=dict(pn=0.4, mos=0.35)`.

Those are the SAS cookbook's numbers and they are correct — for a light curve you build
yourself with `evselect` above 10 keV over the whole field, which is what the ODF route
will do. A PPS `FBKTSR` is made by `epiclccorr` and is on quite another scale. Measured
from the archive:

| exposure | median | max | `FLCUTTHR` |
|---|---|---|---|
| `0153950401` MOS1 S004 | 36.0 | 54.2 | 54.2 |
| `0153950401` MOS2 S005 | 45.1 | 102.4 | 82.1 |
| `0123700101` pn S003 | 2.2 | 1351.9 | 3.4 |
| `0123700101` MOS1 S001 | 1.0 | 228.4 | 1.8 |
| `0123700101` MOS1 U002 | 20.5 | 67.4 | 43.4 |
| `0804330201` MOS1 S002 | 0.9 | 1.7 | 1.7 |

A fixed 0.35 would throw away every bin of the first row and nothing at all of the last.
Rows three and five are the same camera in the same observation, twenty times apart, which
is the proof that no single number can do this.

**PPS has already chosen one, per exposure**, and writes it into the `RATE` header as
`FLCUTTHR` — "Optimised flare cut threshold". Present on all 41 EPIC `FBKTSR` files across
a twelve-observation sample; the only files without it are RGS, which is out of scope. When
PPS finds no flare it sets the keyword fractionally above the largest rate in the curve, so
nothing is cut, and because the keyword is stored at full precision while the rates are
single precision there is no edge case at the comparison.

So, **decided by Matteo on 2026-09-07**: `flare_rate_limit` defaults to `None`, meaning
"use this exposure's own `FLCUTTHR`"; a dictionary keyed by camera (`pn`, `mos1`, `mos2`)
or family (`pn`, `mos`) overrides it. The cookbook numbers move to
`odf_flare_rate_limit`, kept for step 10 so they do not have to be rediscovered. A cut
removing more than `flare_warn_fraction` (0.25) of an exposure is applied and warned about,
never capped: Mkn 421's pn loses 39% of itself to PPS's own threshold, which is very likely
right for a famously flare-wrecked observation, and capping it would be inventing science
policy.

Four smaller things:

* **PPS writes dead bins as a *signalling* NaN**, bit pattern `0x7f800001` rather than the
  quiet `0x7fc00000`. Widening one to double raises the invalid-operation flag, which numpy
  2 reports as `invalid value encountered in cast` — once per read, and Mkn 421's pn curve
  has 322 dead bins. `read_flare_lightcurve` casts under `np.errstate(invalid="ignore")`;
  what comes out is an ordinary quiet NaN, so nothing downstream is affected. There is a
  test that would fail if the guard were removed.
* **`TIME` holds bin centres**, offset from `TSTART` by exactly `TIMEDEL/2`, with spacing
  exactly `TIMEDEL` — so `utils.intervals_above_threshold`'s `[t ± cadence/2]` model is
  already right and nothing has to be shifted.
* **No `merge_intervals` call.** The plan asked for one after `good_intervals`, but
  `good_intervals` already merges, clips and sorts what it is given, so it would be a no-op.
* **A timing exposure is screened with the imaging curve of the same exposure**, one of the
  *Open items* below, now settled. MOS `FastUncompressed` reads its central CCD in timing
  and its outer six in imaging, so `M1S004`'s background curve comes from a field the
  timing event list does not have. Soft-proton flares illuminate the whole detector, so a
  flare seen in the outer CCDs is happening during the timing readout too — the cut is
  applied, and the curve's provenance recorded so the choice is visible on the page.

Not done here, and deliberately: **nothing draws this yet.** `report.flare_figure` is
hardwired to NuSTAR's three panels (GOES, 3–10 keV, 10–79 keV), and XMM's picture is a
different one — a single background curve with a threshold line and the removed intervals
shaded. The arrays are recorded under the same `flare_filtering` step name and in the same
shape (`gti_before`, `gti_after`, `removed`, plus `lc_time`/`lc_rate`/`lc_rate_err`), so
commit 12 has everything it needs.

## What changed while implementing step 7

**The flare GTI reaches `evselect` as a file we write, not via `tabgtigen`.** This was the
step's one genuinely open design question and SAS's own documentation settles it:
`selectlib`'s file-based filter is `gti(blockspec,Tcolumn)`, and the block it names must be
an OGIP-standard GTI table. So `write_gti_file` writes the `(N, 2)` array step 6 already
produced, and the screening expression ends `&& gti(<file>,TIME)`. The alternative,
`tabgtigen`, would rebuild the same intervals inside SAS from a threshold we would have to
hand it anyway — leaving the GTI shown on the report and the GTI applied to the events as
two separate derivations of one answer, free to disagree. One writer, one truth.

**`ecoordconv`'s output format is documented, not reverse-engineered.** The task writes no
output file at all; it prints the answer. Its documentation page (SAS 22.1.0, *Output*)
gives the exact lines and states that they "may be searched for in a script and every
effort will be made to keep them constant between versions". The sky position is the line
beginning `X: Y:`, and `ECOORDCONV_SKY_RE` is anchored at the start of the line so that
`DETX:` and `IM_X:` cannot be mistaken for it. The same page's table 1 is where
`SKY_PIXEL_ARCSEC = 0.05` comes from.

**`sas.run` gained a `capture` argument.** It could send a task's output to a log or to the
screen, and neither gives a caller the text. `ecoordconv` is the first task here whose
result *is* its standard output. With `log_to` as well the output is still written to the
log, so a task whose answer is read keeps the same paper trail as one whose answer is a
file.

**A new configuration key, `position_warn_arcsec`** (default 10). Only ever a warning
threshold — see below.

**Timing exposures are skipped by the cleaning, as planned.** Their screening expression and
their `RAWX` regions are commit 8; `xmm_source_sky_position` is never called for them,
because a timing read-out has no sky image to convert into.

### Verified against a real SAS run

Her X-1 `0153950401` was downloaded through the pipeline's own S3 transport and route probe
(19 files, 39 MB — the filter's predicted mix), and cleaned with the code as committed. The
CCF mirror on this machine is still incomplete, so this exercised `evselect` but **not**
`ecoordconv`, which needs `XMM_BORESIGHT`:

| exposure | events in → out | `ONTIME` in → out |
|---|---|---|
| MOS1 `S004` imaging | 421 539 → 272 768 | — (this file carries `ONTIME` 0) |
| MOS2 `S005` imaging | 523 126 → 356 699 | 5221 s → 5156 s |

Every clause was checked to have bitten, rather than assumed from a zero return code: in the
cleaned MOS2 list `PI` spans exactly 200–12000, `PATTERN` reaches 12 and no further, and
**0 of 356 699 events fall outside the flare GTI**. The flare cut had removed 78 s of MOS2's
5538 s, and `ONTIME` fell by 65 s, which is the same cut seen through SAS's own bookkeeping.
That `evselect` accepted the file at all is the point that mattered: the OGIP table
`write_gti_file` produces is valid input to the `gti()` selector.

Note that `evselect` needed no calibration access for this — `#XMMEA_EM` expanded from the
event file itself — so the cleaning half of this step runs on an incomplete CCF. The
`ecoordconv` half remains unverified on real data until the mirror finishes.

## What changed while implementing step 8

**There is no timing screening macro, so the mode changes one number instead.** The plan
said `#XMMEA_EP` would be "replaced by the timing-appropriate expression". No such
expression exists: `#XMMEA_EP`, `#XMMEA_EM` and `#XMMEA_SM` are the only EPIC macros in
the whole of SAS 22.1.0. The real difference is documented by SAS's own automatic
reduction — `xmmextractor` (`doc/xmmextractor/node3.html`) applies `PATTERN<=4`, `FLAG==0`
and `#XMMEA_EP` to pn *whatever mode it is in*, and `PATTERN<=12` (imaging) or
`PATTERN==0` (timing) with `#XMMEA_EM` to MOS. `SCREENING_EXPRESSIONS` is therefore keyed
on `(family, mode)` and `xmm_screening_expression` takes the mode as a required argument:
a timing exposure screened as an image is wrong in a way nothing downstream would notice.

**MOS Timing has no default `RAWX` strip, by decision (Matteo, 2026-09-07).** pn's
`[31:45]` and `[3:5]` are the cookbook's, and they are trustworthy because a pn Timing
read-out puts the source at a column fixed by the boresight. MOS has no equivalent
constant — SAS's own driver (`lib/perl5/run_epatplot.pl`) builds the MOS strip around a
source position it is *given* — so `xmm_timing_regions` returns `None` for MOS, warns
loudly, and everything downstream skips rather than extracting at an invented column. A
strip put in `timing_src_rawx`/`timing_bkg_rawx` is honoured for any camera.

*And the way to lift that restriction turned up in the same run: `ecoordconv` prints
`RAWX: RAWY:` alongside the sky position — 311.08 for Her X-1 on MOS1. So the MOS strip
can be centred on SAS's own conversion of the target position, with no histogram and no
guess. Left undone deliberately; it is a decision, not an oversight.*

**`epatplot`'s answer is a header keyword, not its standard output.** The task documents
appending the observed-to-model singles and doubles pattern fractions to the input event
set as `SNGL_OTM` and `DBLE_OTM`, with one-sigma errors `ESGL_OTM` and `EDBL_OTM`. Reading
keywords beats scraping a screen, and `ecoordconv` remains the only task here whose output
is parsed — because it writes no file at all. Confirmed on real data: they land on the
`EVENTS` extension.

**`epatplot` writes PDF and says PostScript.** Its `device` parameter still offers PGPLOT
devices and still defaults to `/VCPS`; SAS 22.1.0 draws the plot from Python instead and
warns "Only format supported now is pdf". Asked for `..._pat.ps` it writes `..._pat.pdf`
**and returns 0**. `sas.run`'s output check caught it — the task "returned success but did
not create" the file — which is the second time that check has named a failure that a
return code hid. `PILEUP_PLOT_EXTENSION` pins the measurement, as `especget`'s names are
to be pinned in step 9.

**The pile-up check measures and never corrects.** Correcting pile-up means excluding the
core of the point spread function, which changes which photons the science is done with.
That is a decision for whoever reads the plot.

### Verified against a real SAS run

Her X-1 `0153950401` again, with the CCF mirror now complete, on SAS 22.1.0. All four
exposures, both modes, both routes through the region builder:

| exposure | cleaned | source region | events in region | pile-up |
|---|---|---|---|---|
| pn `S003` timing | 299 677 | `(RAWX in [31:45])` | 131 568 | singles 1.012 ± 0.008, doubles 0.985 ± 0.019 |
| MOS1 `S004` imaging | 272 768 | circle at 24332.8, 24621.2 | **0** | skipped |
| MOS1 `S004` timing | 37 402 | — | — | skipped, no strip |
| MOS2 `S005` imaging | 356 699 | circle at 24332.8, 24621.2 | 14 893 | singles 1.053 ± 0.024, doubles 0.838 ± 0.035 |

Her X-1 is **not piled up** in this pn Timing exposure: both ratios sit within their errors
of 1, which is the answer a 6 ks snapshot of a 9.9e-11 erg cm⁻² s⁻¹ source should give.

The MOS1 imaging row is the interesting one, and it is right. `FastUncompressed` reads the
central CCD in timing, so the *imaging* event list of that exposure holds no events at the
target at all — 0 of 272 768 fall in the source circle. `epatplot` ran, fitted nothing,
wrote no keywords, and `xmm_pileup_check` recorded a skip. An exposure that has nothing to
say now says so instead of failing or, worse, reporting a ratio derived from nothing.

**`ecoordconv` is settled, and its parser is right.** The open item from step 7 is closed:
on the real MOS1 event list it printed

```
 X: Y: 24332.842 24621.217
 IM_X: IM_Y: 24332.842 24621.217
```

— the same numbers on two lines, which is exactly the trap `ECOORDCONV_SKY_RE`'s
start-of-line anchor was written for. MOS1 and MOS2 agree to the digit, as absolute sky
coordinates must. The `OBSMLI` cross-check reports **1.35″**, against the 1.42″ this
document predicted from the archive; the difference is that the pipeline compares against
the position asked for rather than the catalogue's own.

**A calibration lesson that is not about this code.** `ecoordconv` first failed with
`Could not open file 'XMM_BORESIGHT_0029.CCF'`. The mirror at `SAS_CCFPATH` holds
`XMM_BORESIGHT_0036.CCF` — the current issue — while the observation's `CALIND`, being the
index the SOC used for the November 2024 reprocessing, names issue 0029. **A mirror of
current issues only cannot satisfy an archival `CALIND`.** The fallback the plan predicted
works exactly as predicted, and takes 26 seconds with no ODF:

```bash
cifbuild withobservationdate=yes observationdate=2002-03-27 fullpath=yes
```

Everything above was run with the index that produced. **This fallback is not implemented
yet** — it belongs with the `SAS_CCF` handling in step 9 or 12, and it needs Matteo's
verdict: rebuild the index whenever a constituent is missing, or tell the user to complete
their mirror.

**`epatplot` needs ESA's pysas**, unlike everything else here: it shells out to
`$SAS_DIR/bin/epatplot_graph.py`, which imports `pysas.pyutils`. `sas.run` still does not,
and `has_sas` is still right not to probe for it — but a machine whose `PYTHONPATH` loses
`$SAS_DIR/lib/python` will run every other task and fail this one.

## What changed while implementing step 9

**The output names are dictated, not pinned.** The plan said to run `especget` once and
record its `filestem` convention as a constant. There is a better answer in its own
parameters: `withfilestem=no` with `srcspecset`, `bckspecset`, `srcarfset` and `srcrmfset`
names every output outright, so no convention has to be trusted at all. After `epatplot`'s
`.ps` that is really a `.pdf`, taking a name from a task rather than giving it one is a
habit worth avoiding.

**The tasks run in the products directory, and this is a bug fix rather than a tidy-up.**
`especget` writes the names it is *given* into the spectrum's `BACKFILE`, `RESPFILE` and
`ANCRFILE`, and a FITS header card holds 80 characters. The first real run wrote 140-character
absolute paths into all three — the same trap that truncated file names in an `addspec`
merge. `sas.run` gained a `cwd` argument; the two tasks are handed bare file names and run
where those names belong. The keywords now read `mos2S005_imaging_bkg.pi`, which is also
what a fitting program looks for beside the spectrum, and what survives the tree being
moved.

**The position asked for is handed to `arfgen` explicitly**, with `withsourcepos=yes
sourcecoords=eqpos`. Left alone `arfgen` takes the source position from the centre of the
extraction region — right for a circle on the sky, meaningless for a strip of columns,
where in timing mode it would otherwise fall back on the `SRCPOS` keyword or on
`RAWY=190`. The vignetting and encircled-energy corrections depend on that position.

**The recorded spectrum takes its energy scale from its own response.** NuSTAR's
`read_spectrum` converts channel to energy with `E = 0.04 * PI + 1.6`, that mission's own
linear relation. XMM needs no such constant: `read_xmm_spectrum` reads the `EBOUNDS`
extension of the RMF `especget` has just made, which is exact and survives a change of
spectral binning. Verified on real data — 4096 channels over 0–20.48 keV on pn, 2400 over
0–12 keV on MOS.

### Verified against a real SAS run

Her X-1 `0153950401` again, through `xmm_calculate_spectra`:

| exposure | wall clock | source spectrum | energies |
|---|---|---|---|
| pn `S003` timing | **524 s** | 131 568 counts in 3814 s, `BACKSCAL` ratio 5.17 | 0.003–20.477 keV |
| MOS2 `S005` imaging | **77 s** | 14 893 counts in 5105 s | 0.002–11.997 keV |
| MOS1 `S004` timing | 0 s | skipped, no strip | — |

The `BACKSCAL` ratio is worth a second look: 20 274 248 to 3 923 520 is 5.17, against the
5.00 the strips imply (15 columns of source to 3 of background). The difference is the bad
columns `arfgen` corrects for, which is the whole reason `BACKSCAL` is computed rather
than assumed.

**A pn Timing extraction is seven times slower than a MOS imaging one** — 524 s against
77 s, nearly all of it `arfgen`, since `rmfgen` finished in about a minute. Worth knowing
before a run over many observations: the slow camera is the one bright-source science
wants.

**The acceptance test in *Verification* below passes.** The grouped spectrum loads in
XSPEC and brings the other three with it:

```
Spectrum 1  Spectral Data File: mos2S005_imaging_grp.pi
Net count rate (cts/s) for Spectrum:1  2.730e+00 +/- 2.382e-02 (95.0 % total)
  Telescope: XMM Instrument: EMOS2  Channel Type: PI
 Using Background File                mos2S005_imaging_bkg.pi
 Using Response (RMF) File            mos2S005_imaging.rmf for Source 1
 Using Auxiliary Response (ARF) File  mos2S005_imaging.arf
```

594 groups from 2400 channels at `mincounts=25`, `oversample=3`; 1374 from 4096 on pn.

## Building the calibration index

**Matteo's ruling, 2026-09-07.** The CCF mirror on this machine — 550 files, 2.8 GB — is
ESA's **Valid CCF Set**, not a partial copy of anything. ESA assembles it daily and states
that it holds every constituent needed to process any XMM-Newton ODF *at the current date*.
The complete history (~1550 files, ~4.3 GB) exists only for reproducing the calibration
knowledge of a past moment, and many of its files have been superseded by better
calibration since. **New analyses want the current set.**

That reframes what the Her X-1 failure was. `0153950401`'s `CALIND` names
`XMM_BORESIGHT_0029.CCF`, which the mirror does not hold. That is not a missing file: it is
the SOC's record of what it used in November 2024, and issue 0036 supersedes it. Fetching
0029 would be *asking for worse calibration*.

**Decided by Matteo, 2026-09-07 — build it always, as the normal path.** `SAS_CCF`
points at an index this pipeline builds, rather than at the downloaded `CALIND`:

```bash
cifbuild withobservationdate=yes observationdate=<DATE-OBS> fullpath=yes
```

* `DATE-OBS` comes from the event list header, so this needs no ODF and no download. It
  selects the constituents *valid for the observation's epoch*, at their current issue —
  which is exactly the distinction ESA draws above.
* Measured at **26 s** on Her X-1, against 524 s for one pn `arfgen`. Negligible per
  observation, and it happens once, not once per exposure.
* It removes the failure mode entirely rather than catching it, so there is no fallback
  branch to test, and no observation old enough to break.
* Keep downloading `CALIND` anyway — it is about 90 kB and it records what ESA used, which
  is worth having on the report page next to the index we built.

One consequence to state plainly rather than discover: PPS event lists were *generated*
with the November 2024 calibration, and responses built against a newer CIF are then
marginally inconsistent with the `PI` values in those events. This is the ordinary
situation for anyone reanalysing archival data with current SAS, and the alternative is the
superseded calibration Matteo has just ruled out. Both index files should be named in the
report so the choice is visible.

## What changed while implementing step 12

**`process_xmm_obsid` builds the calibration index once per observation, not once per
exposure.** The index depends only on `DATE-OBS`, which every exposure of an observation
shares, and `cifbuild` costs 26 s. It is built after the exposures are listed and before
the loop, and the SAS environment made from it is passed down to every task.

**The submode is read from the event list, not from the master table.** `xmmmaster` carries
`pn_mode`/`mos1_mode`/`mos2_mode`, but those name the *mode* (imaging, timing), not the
*submode* (`PrimeFullWindow`, `PrimePartialW3`), and the submode is what says how much sky
the camera actually covers. `xmm_with_submodes` reads the `SUBMODE` keyword from each event
list and returns replaced `Exposure` records; an unreadable file leaves the submode `None`
rather than failing the observation.

**The window check measures the window rather than looking it up.** The plan proposed a
table of submode to window size in arcseconds. A table would have to be maintained against
SAS, and it cannot answer the question actually being asked, which is not "how big is a
`PrimePartialW3` window" but "does the background annulus for *this* source at *this*
position fall off the chip". `xmm_window_reach_arcsec` reads the events, picks the chip the
source lands on, and measures how far the events reach from the source in each direction.
This is a deviation from the plan Matteo endorsed and is flagged as one.

**The window edge is a percentile, not a minimum and maximum.** Measured on `0870940101`:
taking the extreme events put MOS1's `PrimePartialW3` chip at 8.80′ × 11.43′ when the
window is 300 × 300 pixels ≈ 5.5′, and put pn at 102.5″ — comfortably past the 90″ the
annulus needs — where the honest figure is 87.6″. A handful of stray events, some of them
far outside the window, were setting the answer. Clipping 0.1% from each end fixes it, with
a floor of two events so that a small test file is clipped at all. `BACKSCAL` confirms the
robust figure independently: the pn background annulus measures 6.01 against a geometric
6.75, an 11% deficit, which is a clipped annulus and not a full one.

**The window check reads the cleaned events, and getting this wrong is easy.** The first
implementation measured `exposure.event_list`, the archive's unscreened list. That list
carries flagged events out to the chip edges: on `0870940101`'s pn it reads 96.06″ from
576 146 events, against 87.61″ from the 401 788 that survive cleaning — on either side of
the 90″ threshold, so the raw list silently passed an annulus that is in fact clipped. The
extraction runs on the cleaned events, so the cleaned events define the window. Fixed in
`ed85047`; the test now passes a non-existent path as the exposure's own `event_list`, so a
regression fails loudly instead of measuring the wrong file.

**Recorded spectra are keyed by exposure.** `calculate_spectra` originally recorded flat
`energy`/`rate` arrays, so three cameras in one observation overwrote each other and the
report drew nothing. The keys are now `spec_<stem>_<src|bkg>_<energy|rate|rate_err>`, and
`report.spectrum_figure` reads the band from a recorded `energy_band` rather than assuming
NuSTAR's 3–79 keV.

**Two report bugs surfaced only because a real XMM run was rendered.** The page was drawing
NuSTAR's three-panel solar-flare figure empty for XMM's single light curve, and omitting
the Spectra section entirely — both silently, with nothing failing. `flare_figure` now
dispatches to a single-band builder when the record holds `lc_time`. The page for
`0870940101` went from 13 kB to 480 kB.

### Acceptance target 1: a pn imaging run, met

`0870940101` — M82, pn `PrimeLargeWindow`, MOS `PrimePartialW3` — end to end, PPS route:

| exposure | submode | net rate (c/s) | source fraction | pile-up |
|---|---|---|---|---|
| pn `U002` imaging | `PrimeLargeWindow` | 4.316 ± 0.014 | 92.2% | none |
| MOS1 `S017` imaging | `PrimePartialW3` | 1.444 ± 0.007 | 93.6% | none |
| MOS2 `S018` imaging | `PrimePartialW3` | 1.389 ± 0.007 | 93.8% | none |

All three grouped spectra load in XSPEC with background, ARF and RMF attached. The pn/MOS
ratio of 3.0 is the effective-area ratio these cameras should show, which is the cheapest
available check that the responses are not nonsense.

The window check reports pn's annulus as **clipped** at 87.6″ against the 90″ needed, and
warns. That is correct and is the `BACKSCAL` deficit above, not a false alarm: at
`PrimeLargeWindow` with the default 90″ annulus the background region really does run off
the chip. It costs background counts, which `BACKSCAL` accounts for; it is not silently
wrong, and the warning is the point.

## What changed while implementing step 11

**Candidate 3 is impossible, and this is documented rather than merely observed.**
`barycorr`'s own help states it "is designed to apply to data from RXTE, Swift,
Chandra/AXAF, NuSTAR and NICER". Run on XMM's PPS `ORBTSR` it fails with

```
barycorr: Invalid Observatory/Spacecraft position vector
hdaxbary: Error -11 correcting TSTART/TSTOP in HDU 1
```

before reading a single event. This was not taken at face value: `hdaxbary` carries three
orbit readers (`xtescorbit`, `swiftscorbit`, `nicerscorbit`), and the RXTE one wants
`X,Y,Z` and `VX,VY,VZ` — which XMM half satisfies already, having `GEI_X/Y/Z` in km and
`VX,VY,VZ` in km/s. Renaming the position columns, rescaling km to metres, rescaling to a
low-Earth-orbit magnitude, and forging `TELESCOP=XTE` all produce the identical error. The
orbit file is not the problem: it covers the exposure fully at 1 s cadence, 59 605 to
99 159 km. `barycorr` simply does not dispatch a reader for XMM. **The cheapest candidate
is out, and no amount of file surgery recovers it.**

**Candidate 1 wins, and the reason it first appeared not to is a silent format trap.**
`barycen` needs `SAS_ODF` pointing at a `SUM.SAS`, which only `odfingest` writes. Staging
the housekeeping as SAS's own `.FTZ` — the natural choice, since SAS reads `.FTZ`
everywhere else — makes `odfingest` behave as though the housekeeping were *absent*: it
lists only the `.ASC` files, fails to find a start/stop interval, and writes a truncated
summary that `barycen` then rejects with `UnexpectedEOF`. Staged as plain `.FIT` and
`.ASC` the same files ingest and `barycen` runs to completion. `odfingest` finds `.FTZ`
files when they are named to it, but does not *discover* them when scanning a directory.
`ODF_STAGED_SUFFIXES` pins this; it cost an hour to find and would cost it again.

**`odfingest` warns `NoScienceFiles` and that warning is correct and expected.** Only the
3.8 MB of housekeeping is downloaded on the PPS route, and the observation's duration is
recoverable from it alone. The code therefore checks for the `SUM.SAS` rather than
trusting a return code — the same habit as `sas.run`'s `produces`.

**The correction is applied to a copy.** `barycen` edits in place, and an event list whose
times are silently no longer spacecraft times is a trap for everything downstream, so
`barycenter.barycentered_file_name` names a copy and `produces=IN_PLACE(...)` checks it.
Spectra continue to be extracted from the uncorrected list; the barycentred file is an
additional product for timing.

**An observation with no ODF still reduces.** Barycentring is the one thing that cannot be
done without it, and it is not worth failing an otherwise complete reduction over: the
step records `barycentered: false` with a reason and the run continues.

### Verified against a real SAS run, and against an independent calculation

`0870940101` pn, 401 788 cleaned events. `barycen` corrected the `EVENTS` table, the nine
`EXPOSU` tables and the GTIs, and rewrote the headers to `TIMESYS=TDB`,
`TIMEREF=SOLARSYSTEM`.

That it *ran* proves nothing about whether it ran *correctly*, so the shift was recomputed
independently in astropy from the same `ORBTSR` positions and the source direction:

| offset into the exposure | SAS shift | astropy geometric | difference |
|---|---|---|---|
| 0 s | 65.5894 s | 65.5862 s | 3.2 ms |
| 6 869 s | 65.1352 s | 65.1319 s | 3.3 ms |
| 13 737 s | 64.6856 s | 64.6823 s | 3.3 ms |
| 20 606 s | 64.2406 s | 64.2373 s | 3.3 ms |
| 27 474 s | 63.8003 s | 63.7968 s | 3.5 ms |

A second check, internal and free: the three cameras are three independent detectors
writing three independent event lists, and the correction is a property of the spacecraft
and the source, not of the camera. Interpolated to a common instant, all three agree:

| camera | events | shift range | at t = 734150000 |
|---|---|---|---|
| pn `U002` | 401 788 | 63.800–65.590 s | **65.0096 s** |
| MOS1 `S017` | 131 752 | 63.780–65.843 s | **65.0096 s** |
| MOS2 `S018` | 146 858 | 63.780–65.843 s | **65.0096 s** |

To 0.1 ms. The MOS range is wider than pn's because the MOS exposures are longer, which is
what `xmmmaster` says of this observation. All three files read `TIMESYS=TDB`,
`TIMEREF=SOLARSYSTEM`.

The correction sweeps 1.79 s across the exposure, which is the quantity that matters: a
1.37 s pulsation would smear completely without it. The residual against astropy is
**3.3 ms and constant to 0.3 ms over 27 ks** — the constant part is the Einstein and
Shapiro terms astropy's geometric light-travel time does not include, and a constant
offset shifts no pulse profile. The agreement in the *time-dependent* part, which is the
only part a period search can see, is 0.3 ms in 27 ks.

## Verification

Offline suite (`-o addopts=` because `--doctest-rst` needs pytest-doctestplus):

```bash
/Users/meo/mamba/envs/py313-x64/bin/python -m pytest --pyargs heasarc_retrieve_pipeline -o addopts=
```

Codestyle, before every commit:

```bash
/Users/meo/mamba/envs/py313-x64/bin/ruff check src/heasarc_retrieve_pipeline docs && /Users/meo/mamba/envs/py313-x64/bin/ruff format --check src/heasarc_retrieve_pipeline docs
```

Catalogue and download path, no SAS needed — this already works today, the datalink
resolves `0123700101` to `https://heasarc.gsfc.nasa.gov/FTP/xmm/data/rev0//0123700101/`:

```bash
/Users/meo/mamba/envs/py313-x64/bin/python -m pytest --pyargs heasarc_retrieve_pipeline.tests.test_pipeline --remote-data -o addopts=
```

End to end, on a machine with SAS initialised and `SAS_CCFPATH` set — **the only step that
proves the science, and the one I cannot run here.**

**The test target is Her X-1, OBSID `0153950401`** — chosen by measurement over 152 public
short pn-Timing candidates, and pinned here so it does not have to be re-derived:

| | |
|---|---|
| Duration | 6271 s — pn 3886 s, MOS1 5266 s, MOS2 5649 s |
| Download under the step-2 filter | **39.8 MB** of a 206 MB observation |
| Public since | MJD 53112 (2004) |
| PPS | `pps_flag = Y`, `sas_version = 21.51` — the Nov 2024 reprocessing |
| Modes | pn `TI-ME`, MOS1 `FU-ME`, MOS2 `FF-ME` |

It was picked because one 40 MB download exercises nearly everything at once:

* **Both modes in one observation** — four event lists: pn FastTiming, MOS1
  FastUncompressed imaging *and* timing, MOS2 PrimeFullWindow imaging. It is also the
  observation that proves the `(instrument, expid, mode)` key, since `M1S004` appears twice.
* **A 1.24 s pulsar**, so step 8's barycentring is testable rather than assumed.
* **A point source**, so the annulus and the `RAWX` strip are both meaningful — unlike the
  Crab, whose nebula is extended.
* **Bright and in a high state**: `EP_TOT_FLUX = 9.9e-11` erg cm⁻² s⁻¹, so this snapshot
  caught neither the eclipse nor the 35-day off phase.
* **A clean `OBSMLI` match** at 1.42″, which is a positive control for the cross-check.

Once SAS is working, SAX J1808.4−3658 `0804330201` is the real science target: 35 ks, and
only **78 MB** under the filter.

```python
from heasarc_retrieve_pipeline.core import retrieve_heasarc_data_by_obsid

if __name__ == "__main__":  # required by the process pool
    retrieve_heasarc_data_by_obsid("0153950401", mission="xmm", outdir="out_xmm", n_workers=1)
```

Then open `out_xmm/index.html` and check: three cameras present, both an imaging and a
timing product for MOS1, the flare light curve and its threshold drawn, the OBSMLI
cross-check reporting ~1.4″, and a grouped spectrum that loads in XSPEC with its ARF, RMF
and background attached.

### Two acceptance targets Matteo set, 2026-09-07

Her X-1 proves a great deal, but it does **not** prove the most common EPIC configuration
of all. `0153950401`'s pn exposure is FastTiming, so **no pn imaging exposure has ever
been through this pipeline.** `SCREENING_EXPRESSIONS[("pn", IMAGING)]`, a sky circle and
annulus on a pn event list, and `especget` with a pn imaging response are all written and
unit-tested, and none of them has met real data.

1. **A pn imaging run — Full Frame or Small Window — before this work is called done.**
   Its own end-to-end run on its own observation, with the checks Her X-1 got, not folded
   into another step. Small Window matters separately from Full Frame because its window
   is about 4.4′ × 4.4′: the default 30″ source radius and `bkg_outer_factor=3.0` ask for
   an annulus reaching 90″, which fits, but a caller who widens the source radius will
   quietly ask for background from off the edge of the window. Nothing in the code reads
   `SUBMODE` today — `Exposure.submode` is declared and never populated — so nothing can
   warn about it. Frame times differ too, and only the submode tells them apart: Full
   Frame 73.4 ms, Extended Full Frame 199.1 ms, Large Window 47.7 ms, Small Window 5.7 ms.

2. **The final acceptance is a reanalysis of every XMM observation of M82 X-2.** That is a
   batch, not a single reduction, so it exercises the front end, the report and the
   parallel run at once. Three things to settle before promising it:

   * **M82 X-1 lies about 5″ from X-2**, against a point spread function roughly 6″ full
     width at half maximum and 15″ half-energy width, so a 30″ circle holds both plus the
     starburst's diffuse emission. **Matteo knows, and accepts it** -- it is worse in
     NuSTAR, and his main interest is timing, where the blend is unavoidable and the
     pulsation is what identifies X-2. Spectra are blended too. So this is a caveat to
     *state* in the report and in `docs/known_issues.rst`, not a problem for the
     extraction code to solve, and **the extraction is not to be redesigned around it**.
   * **A separate exercise he plans for later, not part of these thirteen commits**:
     recovering at least a flux *ratio* between X-1 and X-2 from the partially blended
     image, MOS especially, where the finer pixels give the better chance. Worth knowing
     before that starts: the PPS `OBSMLI` source list, which this pipeline already
     downloads for the position cross-check, is the output of `emldetect` -- SAS's
     maximum-likelihood fit of the point spread function, which fits neighbouring sources
     together and is the standard tool for exactly this. Whether it in fact splits the pair
     at 5″ on any given M82 pointing has **not** been checked; doing so costs one listing
     of a file already on the download path, and would say whether the ratio is a new
     analysis or a column read.
   * **The science is the 1.37 s pulsation**, which puts commit 11, barycentring, on the
     critical path for this target rather than making it a nicety.
   * **These are pn imaging observations**, the same untested path as target 1. Doing
     target 1 first is what keeps the batch from being the first real run of that code.
     The submodes actually used across the M82 pointings have not been checked yet; that
     listing is a cheap first move, and it decides whether Small Window is on the path or
     only Full Frame.

### M82: what the archive actually holds, measured 2026-09-07

The "cheap first move" the acceptance target asked for, done. Twenty observations lie
within 12′ of M82; the submodes were read from the PPS event list headers, not from the
catalogue's mode strings.

| Era | pn | MOS1 / MOS2 |
|---|---|---|
| Four **M82 X-2** pointings with EPIC data, 2021–22 — `0870940101`, `0870940401`, `0891060101`, `0891060401` | **`PrimeLargeWindow`** | **`PrimePartialW3`** |
| Eleven **M82** / **M82 X-1** pointings, 2001–2011 — `0112290201`, `0206080101`, `0560181301`, `0560590101/201/301`, `0657800101`, `0657801701`, `0657801901`, `0657802101`, `0657802301` | `PrimeFullWindow` | `PrimeFullWindow` |
| `0932391001` (GRB 231115A, 2023) | `PrimeFullWindow`, thin filter | same |

Four of the twenty — `0112290401`, `0870940501`, `0891060501`, `0891060601` — carry no
EPIC exposure time at all in `xmmmaster`, leaving **sixteen to reduce**. (An earlier
version of this table listed `0891060501` and `0891060601` in the X-2 row as well; they
have no EPIC data and belong only here. Re-queried 2026-09-08.) They are a free live test of the
`NO_SCIENCE_DATA` path, which until now has only ever been exercised offline.

Three things follow, and they simplify the acceptance targets rather than complicating
them.

* **Small Window is on the path, and the annulus worry does not bite at the defaults.**
  Measured on `0870940101` MOS1: the `PrimePartialW3` window is `RAWX` 150–452, `RAWY`
  152–449 on CCD 1 — 300 × 300 pixels, about **5.5′ × 5.5′** — while the five outer CCDs
  read out unwindowed, giving the event list a 9′ × 11′ sky extent overall. The default
  30″ source radius and `bkg_outer_factor=3.0` ask for 90″, which fits inside the central
  window with room to spare. It begins to fall off the window above roughly a **55″**
  source radius, which is exactly the case the `submode` warning is for.
* **No Full Frame pn among the X-2 pointings.** They are Large Window (27′ × 13.6′,
  comfortably larger than any annulus this pipeline will ask for). Full Frame pn appears
  only in the older M82 and M82 X-1 pointings — which still contain X-2 in the field, so
  they belong in the batch.
* **The two acceptance targets overlap.** An M82 pn imaging observation *is* the pn
  imaging acceptance run, on the batch's own target, so target 1 costs one observation
  rather than a separate campaign. `0870940101` is the one to use: it exercises pn
  `PrimeLargeWindow` and MOS `PrimePartialW3` together, and its MOS1 event list is 4.9 MB.
  Its pn is 48 MB, so the whole observation is an ordinary download.

## Prerequisites for whoever picks this up

**To do steps 1–9 and 11–12** (everything except the real reduction) you need nothing
beyond the repository and a Python environment. On Matteo's machine that is
`/Users/meo/mamba/envs/py313-x64`. No SAS, no HEASOFT, no network except for the
`--remote-data` tests. This is deliberate: the plan is arranged so that almost all of it
is written and tested offline.

**To finish step 10 and to close the open items** you need a machine with:

* SAS installed and initialised — `. $SAS_DIR/setsas.sh`, which sets `SAS_DIR`, `SAS_PATH`,
  `PATH` and puts ESA's `pysas` on `PYTHONPATH`. There is no pip or conda route (see
  *Two constraints*), so this is a manual SAS installation from ESA.
* `SAS_CCFPATH` pointing at a CCF (Current Calibration File) repository. ESA distributes it
  by rsync/FTP; a full mirror is tens of GB, and the pipeline only needs the constituents
  the observation's `CALIND` names.
* Enough disk for ~300 MB per observation downloaded plus the reduction's own output.

Matteo's environments, for reference: `henv313` / `henv313_x86` carry HEASOFT;
`py313-x64` is the test environment. **Neither has SAS as of 2026-09-07** — `cifbuild`
and `evselect` are not on `PATH` and no `SAS_*` variable is set — so the end-to-end run
has to happen elsewhere, or after installing SAS locally.

Ask Matteo before committing; per his standing instruction, print the commit message for
review instead if permission has not been given for the session.

## Reproducing the archive facts

Every measurement this plan rests on, re-derivable in under a minute. Run from anywhere.

```python
# 1. Catalogue schema: xmmmaster has no `cycle`, which is why step 1 exists.
from astroquery.heasarc import Heasarc

for t in ["numaster", "nicermastr", "xtemaster", "xmmmaster"]:
    cols = Heasarc.query_tap(
        f"SELECT column_name FROM TAP_SCHEMA.columns WHERE table_name='{t}'"
    ).to_table()
    names = {str(c) for c in cols["column_name"]}
    print(t, "cycle" in names, "public_date" in names)
```

```python
# 2. The November 2024 bulk reprocessing, and how much of the archive has PPS at all.
Heasarc.query_tap(
    "SELECT sas_version, count(*) as n FROM public.xmmmaster GROUP BY sas_version ORDER BY n DESC"
).to_table().pprint_all()
Heasarc.query_tap(
    "SELECT pps_flag, count(*) as n FROM public.xmmmaster GROUP BY pps_flag ORDER BY n DESC"
).to_table().pprint_all()
# Expect: 21.51 -> 18747, blank -> 6167, 21.80 -> 163, 9.0 -> 9, 14.21 -> 1
#         pps_flag Y -> 22779, N -> 2201, blank -> 107
```

```python
# 3. Sizes per product family, from the S3 listing (which carries Size for every key,
#    so this costs no HEAD requests).
import boto3, botocore, collections

s3 = boto3.resource(
    "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
).meta.client
prefix = "xmm/data/rev0/0123700101/"
objs = [
    (o["Key"], o["Size"])
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="nasa-heasarc", Prefix=prefix)
    for o in page.get("Contents", [])
]
top = collections.Counter()
for k, s in objs:
    top[k[len(prefix) :].split("/")[0]] += s
print(sum(s for _, s in objs) / 1e9, "GB total", top.most_common())
# Expect: 1.24 GB; PPS 976.7 MB, ODF 229.3 MB, om_mosaic 21.8 MB, 4XMM 13.4 MB
```

The four PPS event lists of `0123700101` are `PNS003PIEVLI` (212.2 MB), `M1S001MIEVLI`
(32.4), `M2S002MIEVLI` (25.6) and `M1U002MIEVLI` (9.4); the ODF housekeeping subtotal is
3.836 MB across `ATS.FIT`, `RAS.ASC` (3.1 MB of it), `ROS.ASC`, `SUM.ASC`, `TCS.FIT`,
`TCX.FIT`.

```python
# 4. The mode keywords behind the (instrument, expid, mode) key. Reads only the first
#    300 kB of each gzipped event list, so it costs nothing.
import boto3, botocore, zlib

s3 = boto3.resource(
    "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
).meta.client
for f in ["PNS003TIEVLI", "M1S004MIEVLI", "M1S004TIEVLI", "M2S005MIEVLI"]:
    key = f"xmm/data/rev0/0153950401/PPS/P0153950401{f}0000.FTZ"
    raw = s3.get_object(Bucket="nasa-heasarc", Key=key, Range="bytes=0-300000")["Body"].read()
    txt = zlib.decompressobj(zlib.MAX_WBITS | 16).decompress(raw).decode("latin-1", "replace")
    got = {}
    for kw in ("INSTRUME", "DATAMODE", "SUBMODE", "EXPIDSTR"):
        j = txt.find(kw.ljust(8) + "=")
        got[kw] = txt[j + 9 : j + 40].split("/")[0].strip().strip("'").strip()
    print(f, got)
# Expect M1S004MIEVLI -> IMAGING/FastUncompressed and M1S004TIEVLI -> TIMING/
# FastUncompressed: the same EXPIDSTR S004 in two modes.
```

```python
# 5. The OBSMLI cross-check, positive and negative controls.
import boto3, botocore, gzip, io, numpy as np
from astropy.table import Table

s3 = boto3.resource(
    "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
).meta.client
for obsid, (ra, dec) in [("0153950401", (254.4575, 35.3423)), ("0611180201", (83.6331, 22.0145))]:
    key = f"xmm/data/rev0/{obsid}/PPS/P{obsid}EPX000OBSMLI0000.FTZ"
    raw = gzip.decompress(s3.get_object(Bucket="nasa-heasarc", Key=key)["Body"].read())
    t = Table.read(io.BytesIO(raw), hdu="SRCLIST")
    sep = np.hypot((t["RA"] - ra) * np.cos(np.radians(t["DEC"])), t["DEC"] - dec) * 3600
    print(
        obsid,
        len(t),
        "rows,",
        len(t.colnames),
        "cols; nearest",
        round(float(sep.min()), 2),
        "arcsec",
    )
# Expect: 0153950401 -> 1.42 arcsec (Her X-1 found); 0611180201 -> 328.79 (Crab missed).
# Note there is no RATE column in either; brightness is EP_TOT_FLUX.
```

The two constraints on SAS packaging are checked the same way: `pypi.org/pypi/pysas/json`
returns a speech-analysis package, `github.com/XMMGOF/pysas` has no `setup.py` or
`pyproject.toml` at its root, and the HEASARC conda channel's `repodata.json` for
`noarch`, `osx-64` and `linux-64` lists only `fviewer`, `xspec-data`, `xstar-data`, `fv`,
`heasoft`, `heasoft-tests`, `xspec`, `xspec-compilers`. `pysas.sastask.MyTask.run()` is
readable at `raw.githubusercontent.com/XMMGOF/pysas/main/sastask.py`, lines 362–456.

## Open items, to settle on the first run with SAS

* Barycentring — which of the three candidates in step 8 works.
* ~~`especget`'s output file names, pinned as a constant.~~ **Settled** — dictated with
  `withfilestem=no` rather than pinned, so there is nothing left to pin.
* ~~**`ecoordconv` against a real observation.**~~ **Settled** — run on Her X-1's MOS1 and
  MOS2 event lists, 24332.842, 24621.217 on both; see *What changed while implementing
  step 8*. The mirror never did reach `XMM_BORESIGHT_0029.CCF`, because ESA's current
  issue is 0036 and an archival `CALIND` names the issue of its day; `cifbuild
  withobservationdate=yes` is what closed it.
* ~~**Whether a missing `CALIND` constituent should trigger `cifbuild` automatically.**~~
  **Settled by Matteo, 2026-09-07, and settled the other way round from how it was asked.**
  The ~548-file, 2.9 GB set on this machine is ESA's *Valid CCF Set*, which ESA assembles
  daily and describes as everything needed to process any XMM-Newton ODF at the current
  date. The ~1550-file, 4.3 GB full history exists only to reproduce the calibration
  knowledge of a given past moment, and many of its constituents have been superseded.
  Matteo does not want past calibration in a new analysis. So an archival `CALIND` naming
  `XMM_BORESIGHT_0029.CCF` is not a gap to fill by fetching issue 0029 -- it is a record of
  what the SOC used in November 2024, and the current issue supersedes it. The consequence
  is that `cifbuild` stops being a fallback and becomes the normal path; see *Building the
  calibration index* below.
* ~~Whether `arfgen`/`rmfgen` need `SAS_ODF`, or the PPS `CALIND` alone suffices.~~
  **Settled** — neither was given `SAS_ODF` and both produced a valid ARF and RMF on Her
  X-1, in imaging and in timing. `useodfatt=yes` exists for the rare case where the
  attitude in the spectrum header is not enough; it was not needed here.
* The `.FIT.gz` → `.FTZ` staging rule on the ODF route.
* ~~Default flare-rate thresholds.~~ **Settled** — PPS's own `FLCUTTHR`, see above.
* The pn Timing `RAWX` defaults — `[31:45]` source, `[3:5]` background are the cookbook's
  numbers, and the right background strip depends on how far the source wings spread. They
  gave 131 568 of 299 677 pn events on Her X-1 and a pile-up measurement consistent with
  no pile-up, so they are at least not obviously wrong.
* ~~**A MOS Timing strip from `ecoordconv`.**~~ **Asked and declined, 2026-09-07.** MOS
  timing stays skipped with its loud warning; it is not extracted at a guessed column and
  it is not centred on `ecoordconv`'s `RAWX:` either. The conversion would work — 311.08
  on Her X-1's MOS1 — and the option stays on the table, but nothing on the critical path
  needs it: every M82 observation is imaging, and pn, the camera bright-source timing
  actually uses, already has the cookbook's strip. Revisit when a science target needs MOS
  timing.
* ~~Whether flare screening on a Timing exposure should use `FBKTSR` at all.~~ **Settled**
  — yes, with the provenance recorded; see above.
* ~~How the flare GTI reaches `evselect`.~~ **Settled** — a file we write, filtered with
  `gti(file,TIME)`; verified on real data, see *What changed while implementing step 7*.
* ~~**Whether `Exposure.submode` should be populated and acted on.**~~ **Settled by
  Matteo, 2026-09-07: populate it, and warn.** `SUBMODE` is read from the event list
  header, carried on the `Exposure`, recorded on the report page, and checked against the
  window geometry — a configured outer annulus radius larger than the window can give is
  **warned about, never failed on**, in the same spirit as the flare cut that removes 39%
  of Mkn 421. This lands before the pn imaging acceptance run, since that run is what
  raised it.
