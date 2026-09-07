# Adding XMM-Newton (EPIC) to `heasarc_retrieve_pipeline`

> **Handoff document.** Written 2026-09-07 against `heasarc_retrieve_pipeline` on branch
> `various_fixes` (HEAD `bc12c41`). **Commits 1–7 of the sequence below have landed**
> (`5c4ec2c`, `683fba1`, `e9bc841`, `c46a044`, `b1c7df4`, `a122c21`, `2cc6037`, `1e0db70`,
> and step 7, 2026-09-07); the rest is still the agreed design, not a report on work done. It is
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
* **Screening differs**: `PATTERN<=4` and `FLAG==0` still hold for pn, but the imaging
  `#XMMEA_EP` macro is replaced by the timing-appropriate expression.
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
them as a module constant, as `NUPRODUCTS_SPECTRA` (`nustar.py:2458`) does.*

`SAS_CCF` comes from the PPS `CALIND`, which is the CIF the SOC used. If a constituent it
names is missing from the local `SAS_CCFPATH`, the fallback costs nothing and needs no ODF:
`cifbuild withobservationdate=yes observationdate=<date>` builds a fresh index from the
observation date alone.

**No cross-instrument coaddition.** pn and MOS are different detectors with different
responses, so `addspec`'s case B does not apply and `coadd.apply_case_b_scaling` would be
wrong. The three spectra are meant to be fitted jointly. `epicspeccombine` is the right
tool if we ever want one file; noted in `known_issues.rst`, not built now.

## Step 8 — barycentring, the one open dependency

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
8. Timing mode — `RAWX` regions, timing screening, `epatplot` pile-up diagnostic.
9. `especget` spectra and grouping.
10. ODF front end — staging, `cifbuild`, `odfingest`, `epproc`, `emproc`.
11. Barycentring, once one of the three candidates is verified.
12. `MISSION_CONFIG` entry, report titles and subdirectories.
13. Docs.

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
* `especget`'s output file names, pinned as a constant.
* **`ecoordconv` against a real observation** — the parser matches the format the task's
  own documentation publishes, and `xmm_source_sky_position` has not yet been run for real,
  because the CCF mirror on this machine had not reached `XMM_BORESIGHT_0029.CCF`. Confirm
  the sky position of Her X-1 once it has.
* Whether `arfgen`/`rmfgen` need `SAS_ODF`, or the PPS `CALIND` alone suffices.
* The `.FIT.gz` → `.FTZ` staging rule on the ODF route.
* ~~Default flare-rate thresholds.~~ **Settled** — PPS's own `FLCUTTHR`, see above.
* The pn Timing `RAWX` defaults — `[31:45]` source, `[3:5]` background are the cookbook's
  numbers, and the right background strip depends on how far the source wings spread.
* ~~Whether flare screening on a Timing exposure should use `FBKTSR` at all.~~ **Settled**
  — yes, with the provenance recorded; see above.
* ~~How the flare GTI reaches `evselect`.~~ **Settled** — a file we write, filtered with
  `gti(file,TIME)`; verified on real data, see *What changed while implementing step 7*.
