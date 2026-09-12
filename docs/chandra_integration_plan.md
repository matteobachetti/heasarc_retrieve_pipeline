# Adding Chandra (ACIS + HRC) to `heasarc_retrieve_pipeline`

> **Handoff document.** Written 2026-09-12 against `heasarc_retrieve_pipeline` on branch
> `various_fixes` (HEAD `1153f58`). **Nothing has been implemented yet**: this is the
> agreed design, not a report on work done. It is written to be picked up cold, by a
> person or a session with no memory of the conversation that produced it. Every number in
> it was measured against the live HEASARC archive, the live CXC conda channel, or real
> Chandra data files on 2026-09-12; the snippets under *Reproducing the archive facts*
> re-derive them, so none of it has to be taken on trust.
>
> Decisions marked **decided** were made by Matteo and should not be relitigated without
> him. Items under *Open items* are genuinely unresolved and need a machine with CIAO.
>
> Not part of the Sphinx build: like `xmm_integration_plan.md` and
> `completion_model_plan.md`, it must be listed in `docs/conf.py`'s `exclude_patterns`.
> Keep it updated as the steps land — strike a step when its commit is in, and move
> anything learned about the open items into `docs/technical_details.rst`, which is where
> the permanent record belongs. When every step is done this file has served its purpose
> and should be deleted rather than left to rot.
>
> Read `docs/xmm_integration_plan.md` first. This plan is deliberately the same shape, and
> most of what it does not explain is explained there.

## Context

The pipeline reduces HEASARC data automatically: query a master catalogue, download an
observation, run the mission's reduction, record what happened. NuSTAR and XMM-Newton are
worked out in full; NICER and RXTE are thinner. We now want Chandra, whose reduction needs
the CXC's CIAO (Chandra Interactive Analysis of Observations) rather than HEASOFT or SAS.

Chandra is cheaper than XMM was, for four reasons.

* **The mission seam is now proven twice over.** `MISSION_CONFIG` in
  `src/heasarc_retrieve_pipeline/core.py:980` is the whole abstraction, and XMM already
  added the two hooks Chandra needs: `download_filter` (a callable, so the filter can
  depend on the run's config) and `resolve_config` (probe the archive directory, choose a
  route). Nothing in `core.py` has to change.
* **The fan-out collapses.** XMM needed an `(instrument, expid, mode)` key because one
  observation yields several exposures across three cameras. **A Chandra observation is
  one detector in one mode.** The per-exposure machinery that dominates `xmm.py`'s 3 740
  lines is simply not needed, and `chandra.py` should come out substantially smaller.
* **Chandra's astrometry is sub-arcsecond**, so as for XMM the user's position goes
  straight into the extraction region and no source finding is needed.
* **CIAO installs from conda**, unlike SAS. See *Prerequisites*.

**Outcome:** `retrieve_heasarc_data_by_obsid(obsid, mission="chandra", ...)` downloads a
Chandra observation and produces a flare-screened cleaned event list, a barycentred copy,
an honest statement of the time resolution the data can actually support, a pile-up
measurement, and — where the detector allows it — a source spectrum with its background,
ARF and RMF, with the same diagnostics records and HTML page every other mission gets.

### Decisions taken (**decided** — do not relitigate without Matteo)

| | |
|---|---|
| Instruments | ACIS-I, ACIS-S, HRC-I, HRC-S. |
| Modes | Timed Exposure, Continuous Clocking and all HRC modes, from the start. |
| Gratings | **In scope, as collection only.** Pick up the archive's ready-made `pha2` + ARF/RMF set. We never run `tgextract` ourselves. **Decided 2026-09-12.** |
| Ingest | **Archive level-2 by default** (`primary/*_evt2.fits.gz`), `chandra_repro` available by config. **Decided 2026-09-12**, deliberately mirroring the XMM PPS/ODF split. |
| CIAO access | Tasks run via `subprocess.run` with an argv list, exactly as `sas.run` does. |
| Refactor | **None.** `ciao.py` is standalone and duplicates `sas.py`'s ~80 lines a third time. `heasoft.py` and `sas.py` are untouched. **Decided 2026-09-12.** |
| CI | **Offline and stubbed**, as XMM's is. Real-CIAO tests run locally behind a `ciao` marker, as `tests/test_heasoft_tools.py` does for HEASOFT. **Decided 2026-09-12.** |
| Pile-up | Measure and report. **Never correct.** Same ruling Matteo gave for XMM on 2026-09-08. |
| Barycentring | CIAO's `axbary`, `refframe=ICRS` (DE405). **Not** HEASOFT `barycorr`, which cannot read Chandra's orbit ephemeris — measured 2026-09-12, see *Step 10*. Chandra is therefore the one mission not on DE430, and the cost of that is **0.377 µs**, also measured. |
| Spectra from HRC | None. HRC has no usable energy resolution; the report says so rather than silently omitting it. |

### Decisions still open

Listed under *Open items*, at the end. None of them block step 1.

---

## Measurements this plan rests on

### The catalogue

`chanmaster`, on the live HEASARC TAP service, 2026-09-12:

| | |
|---|---|
| Rows | 29 162 — **27 122 `archived`**, 1 161 `unobserved`, 631 `observed`, 230 `untriggered`, 18 `scheduled` |
| Detector (archived) | ACIS-S 14 848, ACIS-I 8 600, HRC-I 2 005, HRC-S 1 669 |
| Grating (archived) | NONE 23 931, HETG 2 082, LETG 1 109 |
| Data mode (archived) | `TE_*` 22 953 (85%), `CC_*` 458 (**1.7%**), HRC `DEFAULT`/`S_*`/`SCENTER`/… the rest |

Two things follow. **The fast-timing subset of Chandra is bigger than the mode counts
suggest, and the catalogue cannot tell you how big.** It is tempting to read the table
above as "CC mode plus HRC, roughly 4 100 of 27 122, and the other 85% is stuck at 3.2 s".
That is wrong, and an earlier draft of this plan said it. **ACIS Timed Exposure on a
subarray reaches a few tenths of a second**, which is fast enough for a large class of
pulsars — and `data_mode` does not reveal it. Counter-example, measured 2026-09-12:
obsid `5644` is `TE_006AC` in the catalogue and `EXPTIME = 0.4 s`, `TIMEDEL = 0.44104 s`
in the event header, on a single chip (`DETNAM = ACIS-7`). See *The known-answer test*.

This is the same lesson as the HRC one, in a second place: **the catalogue's mode string
never tells you the true time resolution; the event header does.** The design already
does the right thing — step 5 reads `TIMEDEL` for ACIS rather than assuming 3.2 s — but
do not let anyone "optimise" the module by pre-filtering candidate observations on
`data_mode`. That filter would have discarded the one M82 observation with a published
pulsation detection.

And **gratings are 12% of the archive but the
large majority of the bright-source ACIS data**: of the twelve archived ACIS observations
of Her X-1 and SAX J1808.4−3658, nine carry HETG or LETG, because gratings are how you
observe a bright source with ACIS at all. Ruling gratings out of scope would have
discarded most of the data this pipeline is aimed at.

`chanmaster` has `cycle`, so **step 1 of the XMM plan has no analogue here** — the
hardcoded OBSID query already works.

**`chanmaster.obsid` is an `int`, not a `char`.** `obsid_query` builds
`cat.obsid IN ('2749')`, quoting every identifier. Verified against the live service: the
quoted form, the bare integer and the zero-padded `'02749'` all return the same single
row, so the TAP service coerces and **no change to `obsid_query` is needed**. Do not
"fix" this. What it does mean is that the row comes back with `obsid` as a NumPy `int32`,
so anything building a file name from it must pad — see *Output names*.

### The archive layout

HEASARC mirrors the whole Chandra archive. Datalink resolves `chanmaster` rows to
`https://heasarc.gsfc.nasa.gov/FTP/chandra/data/byobsid/<last digit of obsid>/<obsid>/`,
and equivalently `s3://nasa-heasarc/chandra/data/byobsid/<last digit>/<obsid>/`. The
layout is `00README`, `oif.fits`, `primary/`, `secondary/`, with `primary/responses/` for
grating observations.

Three real observations, chosen to span the cases, listed from S3:

| OBSID | Configuration | Whole directory | **After the filter** |
|---|---|---|---|
| `6298` | HRC-I, no grating | 51.0 MB, 28 files | **15.4 MB, 9 files** (30%) |
| `17661` | HRC-S `S_TIMING` | 264.5 MB, 35 files | **88.7 MB, 9 files** (34%) |
| `2749` | ACIS-S + HETG | 458.5 MB, 63 files | **180.2 MB, 33 files** (39%) |

The single biggest saving is one file. `secondary/axaff*_VV001_vvref2.pdf.gz` is a
verification-and-validation report for humans: **60 MB of `17661`'s 265, and 102 MB of
`2749`'s 459**. Excluding it alone is worth more than every other exclusion combined.

On `2749`, 155 MB of the 180 MB kept is `primary/responses/` — 24 files, the HEG and MEG
ARF/RMF pairs for orders ±1, ±2, ±3. That is the price of the grating decision, and it is
the right price: those files are the spectra, already made.

**One trap, found by listing real observations rather than by reasoning.** The bad-pixel
file is in a **different directory for the two instruments**: `primary/` for ACIS
(`acisf02749_000N004_bpix1.fits.gz`) and `secondary/` for HRC
(`hrcf06298_000N006_bpix1.fits.gz`). A filter anchored on `primary/` alone silently drops
it for every HRC observation, and the symptom appears much later, in `specextract`.

### The event file headers

Read from S3 with a 400 kB range request and a partial gzip inflate — the headers cost
nothing to fetch, and the snippet is under *Reproducing the archive facts*.

| | `6298` | `17661` | `2749` |
|---|---|---|---|
| `INSTRUME` | HRC | HRC | ACIS |
| `DETNAM` | `HRC-I` | `HRC-S` | `ACIS-456789` |
| `GRATING` | NONE | NONE | HETG |
| `DATAMODE` | `OBSERVING` | `OBSERVING` | `FAINT` |
| `READMODE` | *absent* | *absent* | `TIMED` |
| `TIMEDEL` | 1.5625e−05 | 1.5625e−05 | 2.54104 |
| `EXPTIME` | *absent* | *absent* | 2.5 |
| `TIMESYS` / `MJDREF` | TT / 50814.0 | TT / 50814.0 | TT / 50814.0 |
| `ASCDSVER` / `CALDBVER` | 10.10 / 4.9.5 | 10.9.2 / 4.9.3 | 10.9.4 / 4.9.4 |

Three things to take from that table.

* `MJDREF` is a **single float**, so `utils.time_reference` (`utils.py:842`) already
  handles it, exactly as it does for XMM.
* `DETNAM` for ACIS names the chips that were on (`ACIS-456789` = chips 4–9). Which chip
  the source lands on is read from here, not assumed.
* `CALDBVER` records the calibration the archive product was made with. `2749` says
  4.9.4; the current CALDB is 4.12.4. With archive level-2 as the default route, **this
  becomes a diagnostic to report**, not a route decision: the reduction says how stale its
  calibration is and lets the user ask for `chandra_repro` if they care.

---

## The timing problem

This is the part of Chandra that a naive reduction gets wrong, and it is the single most
valuable thing this module can do.

### What the data can actually support

| Configuration | Achievable time resolution | Share of archive |
|---|---|---|
| ACIS Timed Exposure, full frame | **3.2 s** — the frame time, in `TIMEDEL` | 85%, with subarrays |
| ACIS Timed Exposure, subarray | **down to ~0.4 s**; `5644` measures `TIMEDEL = 0.44104` | (part of the above, share unmeasured) |
| ACIS Continuous Clocking | **2.85 ms**, one spatial dimension destroyed | 1.7% |
| HRC-I, and HRC-S in ordinary imaging | **~4 ms**, *not* 16 µs — the wiring error | ~13% |
| HRC-S in `S_TIMING` | **15.625 µs**, fully recovered | a small part of the above |

Sources: the CXC's [Timing Analysis with Lightcurves](https://cxc.cfa.harvard.edu/ciao/why/lightcurve.html),
the [HRC Timing Anomalies caveat](https://cxc.cfa.harvard.edu/ciao/caveats/hrc_timing.html),
and the calibration team's [HRC Timing Issues](https://cxc.harvard.edu/cal/Hrc/timing_200304.html).

### The HRC wiring error, and why the header cannot be trusted

A backplane wiring error sends an inverted logic signal to the latch that captures an
event time, so the timing latch is set on every HRC **front-end trigger**, not on every
valid **telemetered event**. Each event therefore carries the time of the *following*
trigger. Where on-board vetoing discarded the intervening triggers, the shift cannot be
undone on the ground, and the residual uncertainty is about one over the total trigger
rate — the documented "about 4 milliseconds".

`HRC-S` in `S_TIMING` mode disables the outer segments **and all on-board vetoing**, so
every trigger reaches the ground, the time-tags can be shifted back, and the full
15.625 µs is recovered.

**The event file header does not distinguish the two cases.** Measured above: `6298`
(HRC-I, genuinely ~4 ms) and `17661` (HRC-S `S_TIMING`, genuinely 16 µs) have *identical*
timing keywords — `DATAMODE = 'OBSERVING'`, `TIMEDEL = 1.5625e-05`, no `READMODE`. Both
claim 15.625 µs. A reduction that reads `TIMEDEL` and believes it is wrong by a factor of
280 on one of them, silently.

`chanmaster.data_mode` *does* distinguish them (`S_TIMING` against `DEFAULT`), but the
catalogue row is **not** passed to the mission's reduction — `download_and_process_observation`
(`core.py:1764`) calls it with `obsid, config, ra, dec, flags` and nothing else. Plumbing
the row through would be a change to shared, mission-neutral code.

### The discriminator, and it is already on disk

It is not necessary. The `primary/*_dtf1.fits.gz` dead-time-factor file — 71 kB and 109 kB
on these two observations, and already in the download filter because HRC rates need it —
carries `TOTAL_EVT_COUNT` and `VALID_EVT_COUNT` sampled every 2.05 s. Their ratio is the
veto fraction, which is exactly what decides whether the wiring error is recoverable.

Measured, on rows with `STATUS` all-zero, using medians:

| | `6298` (HRC-I) | `17661` (HRC-S `S_TIMING`) |
|---|---|---|
| good rows | 2 392 of 2 769 | 14 586 of 14 588 |
| sample interval | 2.050 s | 2.050 s |
| median `TOTAL_EVT_COUNT` | 469 | 123 |
| median `VALID_EVT_COUNT` | 139 | 123 |
| **`VALID`/`TOTAL`** | **0.296** | **1.000** |
| trigger rate | 228.8 /s | 60.0 /s |
| 1 / trigger rate | **4.37 ms** | 16.67 ms |

`6298`'s 4.37 ms reproduces the CXC's documented "about 4 milliseconds" to the precision
they state it in. And `17661` shows **no vetoing whatsoever** — `VALID` equals `TOTAL`
exactly — which is the `S_TIMING` signature.

**Note the trap in the last row.** Applying `1 / rate` blindly to `17661` gives 16.67 ms,
which is a thousand times *worse* than the truth. The veto ratio must be tested **first**,
and the rate formula applied only when it is below one. Both branches of that algorithm
are now verified against real data.

### What to build

A pure-Python, offline, CIAO-free function:

```python
chandra_time_resolution(events, dtf=None) -> TimeResolution
```

returning the resolution in seconds, the basis it was derived from, and **a plain-English
reason**, recorded in the diagnostics for every observation. The branches:

* **ACIS**, `READMODE = TIMED` → `TIMEDEL` (the frame time). Honest and simple.
* **ACIS**, `READMODE = CONTINUOUS` → 2.85 ms, with the warning that one spatial
  dimension is gone and source and background overlap in it.
* **HRC**, no `dtf` available → the documented ~4 ms, flagged as an assumption.
* **HRC**, `VALID/TOTAL` ≥ a configurable threshold (start at 0.99) → `TIMEDEL`,
  15.625 µs, "all triggers telemetered; wiring error recoverable".
* **HRC**, otherwise → `1 / (median TOTAL_EVT_COUNT / sample interval)`, "wiring error,
  *f* per cent of triggers vetoed on board".

This is the honest analogue of XMM's extraction-window check: the pipeline states what
timing the data can support instead of letting a user read 16 µs off a spec sheet. It is
cheap to write, it needs no CIAO, and it is fully testable offline from small recorded
arrays. **It should be one of the first commits**, not one of the last.

---

## The pile-up problem

`pileup_map` reports counts per ACIS frame, and the CXC's conversion
([ahelp: pileup_map](https://cxc.harvard.edu/ciao/ahelp/pileup_map.html)) is:

| Pile-up fraction | Counts per frame | Counts/s at a 3.2 s frame |
|---|---|---|
| 1% | 0.02 | 0.007 |
| 5% | 0.1 | 0.03 |
| **10%** | **0.2** | **0.07** |

A source at 0.07 counts per second is already 10% piled. For the bright X-ray binaries
this pipeline is aimed at, **essentially every ACIS imaging observation is severely
piled**. That is not a corner case to flag; it is the normal condition, and it is exactly
why nine of those twelve bright-source ACIS observations carry a grating.

The plan, mirroring XMM's `epatplot` step and Matteo's 2026-09-08 ruling:

* Run `pileup_map` on the source chip, binned at single-pixel resolution, **with no energy
  filter** — pile-up skews energies upward, so filtering biases the measurement.
* Record the peak and the 90th-percentile counts-per-frame inside the source region, and
  the pile-up fraction they imply by interpolating the table above.
* Report it. **Never correct it.** No `jdpileup`, no annulus surgery, no readout streak.
* Skip it entirely for HRC (no frames) and for CC mode (no frames in the same sense).

---

## Architecture: two front ends, one back end

```
Archive route (default)                  Reprocessing route (config: products="repro")
  primary/*_evt2.fits.gz ─┐                chandra_repro indir=. outdir=repro
  primary/*_asol1        │                   → repro/*_repro_evt2.fits
  {primary,secondary}/*_bpix1              → repro/*_repro_bpix1.fits
  secondary/*_{msk1,flt1}│                   → repro/tg/*  (grating products)
  primary/*_dtf1  (HRC)  │                                  │
  primary/orbitf*_eph1   │                                  │
  primary/*_pha2 + responses/  (grating)                    │
                         ▼                                  │
              ┌─────────────────────────────────────────────┘
              ▼
   Observation(detector, grating, mode, events, asol, bpix, msk, flt, dtf,
               orbit_eph, grating_products)
              │
              ├─ time resolution    pure Python, dtf1 + header      ← no CIAO
              ├─ flare GTI          dmextract light curve, then existing utils
              ├─ clean event list   dmcopy with the GTI
              ├─ source position    dmcoords, given RA/Dec, never overridden
              ├─ extraction regions psfsize_srcs radius, annulus background
              ├─ pile-up            pileup_map                      ← ACIS imaging only
              ├─ spectra            specextract                     ← ACIS only
              │                     or collect the archive's pha2 + responses (grating)
              └─ barycentre         barycorr, DE430, orbitf*_eph1
```

Only the front end differs, which is the anti-duplication shape XMM established: one
mission module, one downstream path, one set of diagnostics records.

The pure-Python reuse is real and worth naming, exactly as it was for XMM. Thresholding a
light curve uses `utils.intervals_above_threshold`, `utils.merge_intervals` and
`utils.good_intervals` (`utils.py:503, 457, 588`) unchanged; applying a GTI uses
`utils.apply_gti` and `utils.update_time_bounds` (`utils.py:1090, 1246`); `MJDREF` is
handled by `utils.time_reference` (`utils.py:842`); and `barycentered_file_name` needs no
change.

---

## The `ardlib.par` hazard

**This is the one new failure mode Chandra brings, and it must be handled from the first
CIAO commit rather than discovered later.**

CIAO tools are parameter-file driven, like HEASOFT. `chandra_repro` and `acis_set_ardlib`
write the observation's **bad-pixel file path into `ardlib.par`**, and `specextract`,
`mkarf` and friends read it back. That is process-global state keyed to *one*
observation.

The pipeline reduces several observations in sequence inside one pool worker
(`core.prepare_worker`, `core.py:1266`). Two observations in the same worker will clobber
each other's `ardlib.par`, and the result is a spectrum built with the **wrong
observation's bad pixels** — wrong numbers, no error, no warning. This is the `PFILES`
lesson, with worse consequences, and the pipeline already knows the shape of the fix:

`ciao.ciao_environment(obsid, config)` returns a **copy** of `os.environ` with a
**per-observation** `PFILES` pointing at a private directory seeded from CIAO's defaults,
plus `ASCDS_INSTALL`, `CALDB` and `ASCDS_WORK_PATH`. Every `ciao.run` call takes that
environment explicitly. There is then no process-global state to go stale.

The test is the same one `test_sas.py` uses: `ciao_environment` never mutates
`os.environ`, and two calls for different OBSIDs return different `PFILES`.

---

## Steps

Ordered so that everything testable without CIAO comes first. Steps 1–4 need no CIAO
installed at all, which means real progress is possible before the environment is sorted.

### Step 1 — `MISSION_CONFIG` entry and the download filter

Add `"chandra"` to `MISSION_CONFIG`, with `table="chanmaster"`, `expo_column="exposure"`,
`name_column="name"`, `zero_exposure_may_be_wrong=True`, and
`additional="cycle, status, detector, grating, data_mode, type"`. Write
`chandra_download_filter(config)` returning `re_include` for the chosen route.

The archive-route regex, **as measured** (30%/34%/39% of the three test directories):

```
/primary/[^/]*_evt2\.fits\.gz$              the event list
/primary/[^/]*_asol1\.fits\.gz$             aspect solution, needed by specextract
/primary/[^/]*_fov1\.fits\.gz$              field of view
/primary/[^/]*_dtf1\.fits\.gz$              HRC dead time — and the timing discriminator
/primary/orbitf[^/]*_eph1\.fits\.gz$        orbit ephemeris, for barycorr
/primary/[^/]*_pha2\.fits\.gz$              grating spectra
/primary/responses/[^/]*_(arf|rmf)2\.fits\.gz$   grating responses
/(primary|secondary)/[^/]*_bpix1\.fits\.gz$ bad pixels — BOTH directories, see the trap
/secondary/[^/]*_(msk1|flt1)\.fits\.gz$     mask and GTI
/oif\.fits$                                 observation index
```

The reprocessing route takes everything except `\.pdf(\.gz)?$`, `\.jpg$` and
`_img2\.fits\.gz$` — `chandra_repro` needs the full `secondary/` tree, but never the V&V
report, which is the largest single file in it.

*Test:* the regexes select exactly the intended files out of recorded listings of `6298`,
`17661` and `2749` (all three are captured above), following the `ARCHIVE_INDEX_HTML`
precedent in `tests/test_core.py`. Assert explicitly that `bpix1` is selected for **both**
the ACIS and the HRC listing — that is the trap, and a test is how it stays fixed.

*Commit:* `Add Chandra to MISSION_CONFIG, with a measured download filter`

### Step 2 — `chandra.py`: config, paths, detector and mode parsing

`DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./", products="archive",
caldb=None, psf_ecf=0.9, psf_energy_kev=1.0, src_radius_arcsec=None,
bkg_inner_factor=1.5, bkg_outer_factor=3.0, flare_sigma=3.0,
hrc_veto_ratio_threshold=0.99, pileup_percentile=90.0)`.

`src_radius_arcsec=None` means "ask `psfsize_srcs`"; a number overrides it.

Path helpers mirroring `xmm.py`'s: `chandra_base_output_path`, `chandra_product_output_path`,
`chandra_pipeline_output_path`, and finders for each product family.

*Test:* offline, against a fabricated directory tree.

*Commit:* `Add chandra.py: configuration, output paths and product discovery`

### Step 3 — the time-resolution record

The `chandra_time_resolution` function described above, its `TimeResolution` dataclass, and
the `dtf1` reader. Pure Python, no CIAO, no network.

*Test:* every branch, from small recorded arrays. The two real cases are the fixtures:
`VALID/TOTAL = 0.296` at 228.8 triggers/s must give 4.37 ms, and `VALID/TOTAL = 1.000`
must give 15.625 µs **and not** 16.67 ms. That second assertion is the whole point of the
function and it is the first test to write.

*Commit:* `Say what time resolution a Chandra observation can actually support`

### Step 4 — the archive front end

`chandra_archive_front_end(obsid, config)` → one `Observation`, or `None` when the
directory holds no level-2 event list. Reads `DETNAM`, `GRATING`, `DATAMODE`, `READMODE`,
`TIMEDEL`, `CALDBVER`; locates the companion files; records the calibration-staleness
diagnostic.

*Test:* offline, against fabricated headers for the three measured configurations.

*Commit:* `Read a Chandra observation off the archive's level-2 products`

### Step 5 — `src/heasarc_retrieve_pipeline/ciao.py`

Mirrors `sas.py`'s public shape; standalone **by decision**, duplicating it a third time.

```python
HAS_CIAO      # ASCDS_INSTALL set and `dmlist` on PATH
CIAO_LOCK     # threading.RLock(), same reasoning as heasoft.HEASOFT_LOCK
IN_PLACE      # marker class, copied from heasoft.py:259
run(name, *, produces, log_to=None, env=None, **params)
ciao_environment(obsid, config)   # per-observation PFILES — see the ardlib hazard
```

Module docstring says why this duplicates `sas.py` rather than sharing with it, and each
copied helper carries a one-line pointer to its twin. Extend the AST guard at
`tests/test_heasoft.py:401` so `ciao.run` also fails CI without `produces=`.

*Tests* (`tests/test_ciao.py`, offline, `subprocess.run` monkeypatched): argv order and
`k=v` formatting; a `dmcopy` filter expression containing `[`, `]`, `#` and spaces
survives unchanged; a non-zero return code raises naming the task; a zero return code with
a missing output raises; `ciao_environment` never mutates `os.environ` and gives two
OBSIDs two different `PFILES`.

*Commit:* `Add ciao.py: run CIAO tasks with checked outputs and per-observation PFILES`

### Step 6 — position and extraction regions

`dmcoords` converts the given RA/Dec to sky and chip coordinates; `psfsize_srcs` gives the
radius enclosing `psf_ecf` of the counts at `psf_energy_kev`. Background is an annulus
with the configured factors, as for XMM. For CC mode the regions are 1-D strips in the
surviving spatial coordinate, the direct analogue of XMM's `RAWX` strips.

Chandra's PSF grows from about one arcsecond on-axis to over ten at eight arcminutes
off-axis, so a fixed radius — XMM's approach — would be wrong at both ends. That is why
the default is `None`.

*Commit:* `Size Chandra extraction regions from the PSF at the source's off-axis angle`

### Step 7 — flare screening and the cleaned event list

A background light curve with `dmextract`, thresholded with the **existing** pure-Python
interval utilities, written as a GTI, applied with `dmcopy`. Chandra background flares
matter far less than XMM's for bright sources, so the default must be gentle and must
never throw away a good observation.

*Commit:* `Screen Chandra background flares and write a cleaned event list`

### Step 8 — pile-up

`pileup_map` on the source chip, unfiltered in energy, single-pixel binning; peak and
percentile counts-per-frame inside the source region; the implied fraction; a diagnostics
record. ACIS Timed Exposure only.

*Commit:* `Measure ACIS pile-up with pileup_map and report it without correcting`

### Step 9 — spectra

`specextract` for ACIS imaging, producing source and background spectra with ARF and RMF —
the direct analogue of XMM's `especget`. For grating observations, **collect** the
archive's `pha2` and `responses/` set and record them; run nothing. For HRC, produce
nothing and say so.

*Commit:* `Extract ACIS spectra, collect grating products, and say why HRC has neither`

### Step 10 — barycentring

**This step was the plan's one open dependency, and it was settled by measurement on
2026-09-12. The answer is not the one the plan first assumed.**

#### HEASOFT `barycorr` does not work on Chandra

Run on the real `6298` HRC-I event list with its own `orbitf235397100N001_eph1.fits`, in
`henv313`. `barycorr` *does* recognise the mission — it starts, loads DE-430, and applies
a (zero) clock correction — and then dies inside `hdaxbary`:

```
ERROR: no bracketing sample found for time   235695882.04482999
ERROR: failed to find valid orbit ephem data for time   235695882.04482999
barycorr: Invalid Observatory/Spacecraft position vector
hdaxbary: Error 104 correcting TSTART/TSTOP in HDU 0
```

**The orbit file is not at fault.** Checked directly: 5 616 rows, strictly monotonic,
300.0 s median step, no gaps, spanning 235 397 100 – 237 081 600, and the event list's
`TSTART` of 235 695 882 is bracketed by the samples at 235 695 600 and 235 695 900. Both
files carry `TIMESYS = 'TT'` and `MJDREF = 50814`. The position magnitude at that sample
is 125 336 km, consistent with Chandra's apogee, so the units are metres.

Four things were tried and none of them helped:

* naming the extension explicitly — `+1`, and `[ORBITEPHEM]`;
* rewriting `TELESCOP` from `CHANDRA` to `AXAF`. `AXAF` *does* appear in `hdaxbary`'s
  string table, which is what suggested it, but the string belongs to the ephemeris code
  and not to the orbit reader;
* renaming the columns to the RXTE convention `hdaxbary` advertises — Chandra writes
  `Time` and `Vx, Vy, Vz`, and the tool's own error text names `{X,Y,Z} {VX,VY,VZ}`;
* converting the positions from metres to kilometres.

`barycorr` itself contains **no** Chandra branch — `grep -i 'chandra\|axaf'` over the
script returns nothing. It hands the orbit file straight to `hdaxbary`, whose only orbit
readers are `xtescorbit`, `nicerscorbit` and `swiftscorbit`. There is no Chandra reader,
and the fact that `hdaxbary` identifies itself as "axBary" — the tool's Chandra ancestor —
does not mean the port kept the Chandra path.

#### So: `axbary`, and what that costs

CIAO's `axbary` is the native tool and is what CXC's own thread uses. Its `refframe`
parameter admits exactly two values: `FK5` (DE200) and `ICRS` (**DE405**). There is no
DE430. That breaks the pipeline's one-ephemeris rule, so the question is what the break
is worth — and it is worth almost nothing.

Computed with astropy over this observation's own time span and position, geocentric so
that only the ephemerides differ:

| | |
|---|---|
| Barycentric delay | 492.263555 – 492.290393 s, identical to six decimals in both |
| **DE430 − DE405** | **+0.377 µs mean** |
| Variation across the 2-hour observation | **0.0016 µs peak-to-peak** |
| As a fraction of the HRC spec bin (15.625 µs) | 0.024 |
| As a fraction of an HRC-I bin (4 370 µs) | 0.000086 |

The difference is a **constant offset, not a drift** — one and a half nanoseconds of
variation across the whole observation. It therefore cannot distort a pulse profile, a
period, or a periodogram *within* an observation, at any Chandra time resolution. It
survives only as an absolute phase offset when Chandra times are combined with DE430
times from another mission, and 0.377 µs against M82 X-2's 1.37 s spin is 2.7e−7 in
phase.

**Decision:** use `axbary` with `refframe=ICRS`, record `PLEPHEM`/`refframe` in the
diagnostics for every observation so the choice is never invisible, and state the 0.377 µs
in `docs/technical_details.rst` next to the DE430 rule it breaks. Do **not** call
`barycenter.barycenter_file`; leave it untouched for the missions it serves.

`axbary` does not modify in place, so the existing `barycentered_file_name` still names
the output. The aspect solution must be barycentred alongside the events and `ASOLFILE`
updated, per the CXC thread — otherwise a later `specextract` mixes corrected and
uncorrected times.

*Commit:* `Barycentre Chandra with axbary at the searched position, and record DE405`

### Step 11 — the reprocessing route

`chandra_repro_front_end(obsid, config)`, behind `products="repro"`. `chandra_repro
indir=<obsid dir> outdir=<pipeline dir> set_ardlib=no`, then read the `repro/` products
with the same reader step 4 wrote. `set_ardlib=no` matters: we manage `ardlib.par`
ourselves, per observation.

*Commit:* `Add the chandra_repro route behind products="repro"`

### Step 12 — wire it in

`chandra_resolve_config` probes the archive directory and falls back to the reprocessing
route when no level-2 product exists, the way `xmm_resolve_config` does. `process_chandra_obsid`
returns `NO_SCIENCE_DATA` when there is no event list of any kind. Diagnostics records and
the HTML report.

*Commit:* `Wire Chandra into the pipeline, with diagnostics and a report page`

### Step 13 — tests, docs, output names

Round out the offline suite, add the `ciao` marker for real-tool tests, document the
module in `docs/technical_details.rst`, and add `chandra_integration_plan.md` to
`docs/conf.py`'s `exclude_patterns`.

*Commit:* `Document Chandra support and mark its real-tool tests`

---

## Output names

Following the rule established for XMM on 2026-09-08 — every file names its own
observation, because these files leave the tree that gives them context.

Because a Chandra observation is one detector in one mode, the stem is simpler than XMM's:

```
chandra<OBSID padded to 5>_<detector>_<mode>
chandra06298_hrci_imaging_src.evt
chandra17661_hrcs_timing_bary.evt
chandra02749_aciss_hetg_src.pi
```

The padding matters: `chanmaster` returns `obsid` as an integer, and the archive's own
file names are zero-padded to five digits (`acisf02749`, `hrcf06298`). An unpadded stem
would sort wrongly and would not match the archive.

As for XMM, renaming products on disk must also rewrite `BACKFILE`, `RESPFILE` and
`ANCRFILE` in each `.pi`, and the corresponding diagnostics fields. Keep the 80-character
FITS card limit in mind — the longest name above is 32 characters, so there is room.

---

## Prerequisites for whoever picks this up

**CIAO installs from conda**, which is the one place Chandra is *easier* than XMM. The CXC
channel `https://cxc.cfa.harvard.edu/conda/ciao` was live on 2026-09-12 and carries, for
`osx-arm64` natively:

| Package | Version | Size |
|---|---|---|
| `ciao` | 4.18.0 (Python 3.12) | 93 MB |
| `ciao-contrib` | 4.18.2 (`noarch`) | 2.4 MB |
| `caldb_main` | 4.12.4 (`noarch`) | **2 271 MB** |
| `sherpa` | 4.18.0 | 6 MB |

The `caldb` meta-package also pulls `acis_bkg_evt` (1 262 MB) and `hrc_bkg_evt`
(2 238 MB), which are blank-sky background event files. **Install `caldb_main` alone** —
5.8 GB against 2.3 GB, and nothing in this plan uses blank-sky backgrounds.

Note `ciao` 4.18.0 is built against **Python 3.12**, and there is no separate `pyciao`
package at that version — it was folded in. So CIAO lives in its **own** environment, not
in `py313-x64` or `henv313`, and `chandra.py` reaches it through `subprocess` exactly as
`sas.py` reaches SAS. Do not try to `import ciao_contrib` from the pipeline's interpreter.

This also means, unlike SAS, a real-CIAO CI job would be *possible*. Matteo ruled on
2026-09-12 that CI stays offline and stubbed anyway; revisit only if the module's coverage
turns out to need it.

`HAS_CIAO` should probe `ASCDS_INSTALL` in the environment and `dmlist` on `PATH`, which
is the shape `HAS_SAS` settled on after `import pysas` proved to be the wrong probe.

---

## Acceptance target: every HRC observation of M82, judged on timing

**Set by Matteo on 2026-09-12**, as the direct counterpart of the XMM acceptance target —
"every XMM observation of M82 X-2, as a batch" — but **concentrated on the timing side**,
because that is where Chandra is hard and where this module claims to add something.

The same field, so the two runs are comparable; the other instrument, so the failure modes
are different.

### What the archive holds

Measured 2026-09-12, a 12-arcminute cone on M82 (148.9685, +69.6797) against
`chanmaster`: **68 observations, 59 archived** — ACIS-I 27, ACIS-S 25, **HRC-I 14,
HRC-S 2**. All 16 HRC observations are archived, none proprietary:

| OBSID | Detector | Grating | `data_mode` | Exposure | Target |
|---|---|---|---|---|---|
| `1411` | HRC-I | NONE | `DEFAULT` | 54.0 ks | M82 |
| `8189` | **HRC-S** | NONE | **`S_TIMING`** | 61.6 ks | M82 |
| `8505` | **HRC-S** | NONE | **`S_TIMING`** | 83.6 ks | M82 |
| `23460`–`23471` | HRC-I | NONE (`23469` LETG) | `OBS20743` | ~5.2 ks each | M82 X-2 |
| `26111` | HRC-I | NONE | `OBS20743` | 5.2 ks | M82 X-2 |

Sixteen observations, **266 ks** in total. The split is exactly what the timing function
has to get right: **two observations that genuinely deliver 15.625 µs** and **fourteen
that do not**, with the same `TIMEDEL` in all sixteen headers.

Three properties make this a better test than it looks:

* **Thirteen of them point at M82 X-2 by name** — the same source as the XMM batch, and a
  ULX pulsar with a 1.37 s spin period. So the timing claim is checkable against a real
  signal rather than only against metadata.
* **`data_mode` is `OBS20743` for thirteen of them** — a custom mode string, neither
  `DEFAULT` nor `S_TIMING`. Any implementation that inferred the HRC mode by matching the
  catalogue's `data_mode` against a list of known strings would fail on thirteen of
  sixteen. **This is the argument for the `dtf1` veto-ratio discriminator**, which reads a
  number rather than parsing a name, and it is why that design should not be simplified
  away later.
* **`23469` carries LETG on HRC-I**, so the grating-collection path gets exercised on the
  detector where we produce no spectrum of our own. The right behaviour there is not
  obvious and the run will settle it.

### What counts as passing

1. **All 16 reduce**, or any that do not are reported with a reason, as the XMM batch's
   four `NO_SCIENCE_DATA` results were.
2. **`8189` and `8505` are reported at 15.625 µs; the other fourteen are not.** No
   observation is reported at the spec resolution on the strength of `TIMEDEL` alone.
   This is the single assertion the whole timing design exists to support.
3. **The fourteen carry a measured resolution and a stated reason**, each with its own
   veto fraction and trigger rate — not a shared constant, and not the plan's nominal
   "~4 ms".
4. **All 16 are barycentred**, at the position asked for, with `refframe=ICRS` and DE405
   recorded in the diagnostics.
5. **A spot check that the numbers are physical**: the veto fraction should be near 1.0
   for the two `S_TIMING` observations and well below it for the rest, and the trigger
   rates should be consistent across the thirteen `OBS20743` pointings of the same field
   at similar exposure.

**Not** a pass criterion: detecting the 1.37 s pulsation. M82 X-2 is faint, crowded, and
blended with other sources in the field, and a 5 ks HRC-I pointing may or may not show it.
If a coherent search on `8189` or `8505` does recover it, that is strong independent
evidence the barycentring and the time stamps are right, and it should be recorded — but
a non-detection is not evidence of a pipeline fault and must not be read as one.

### The known-answer test: obsid `5644`

**Matteo, 2026-09-12: Liu 2024 detects pulsations in obsid `5644` at 7.7 σ.**

This is the most valuable single item in the whole verification, and it is worth more than
the sixteen HRC observations put together, because it is the only place where the pipeline
can be checked against **a published answer** rather than against its own self-consistency.
Everything else here asks "did the module do what it said?"; this asks "did the module
recover a real astrophysical signal that someone else already found in the same data?"

`5644` is **not** one of the sixteen. From `chanmaster` and its event header, measured
2026-09-12:

| | |
|---|---|
| Detector | **ACIS-S**, `DETNAM = ACIS-7` (S3 alone) |
| Read mode | `READMODE = TIMED` — **Timed Exposure, not CC, not HRC** |
| Catalogue `data_mode` | `TE_006AC` |
| **Frame time** | **`EXPTIME = 0.4 s`, `TIMEDEL = 0.44104 s`** |
| `DATAMODE` | **`GRADED`** |
| Exposure | 75.1 ks on, 68.1 ks live |
| Target | `CXOM82 J095550.2+694047`, PI Strohmayer, cycle 6 |

At `TIMEDEL = 0.44104 s` the Nyquist period is 0.88 s, so a 1.37 s ULX-pulsar spin is
sampled about 3.1 times per cycle — comfortably detectable. **This is why the subarray
correction above matters:** any design that assumed ACIS Timed Exposure means 3.2 s would
report `5644` as far too slow to time, and would have thrown away the one M82 observation
with a published detection. `5644` is therefore the regression test for the ACIS branch of
step 5, and it should be in the offline suite as a recorded header the moment that branch
is written.

Two cautions, both to be resolved by reading the paper before this is used as a
*quantitative* benchmark:

* **The source is not yet pinned down here.** The target name is M82 X-1; M82 X-2, the
  1.37 s pulsar, is a couple of arcseconds away and in the same field. Which of them
  Liu 2024 reports, at what period, and with what search, has **not** been checked in this
  session — only the observation's timing capability has. Get the period and the source
  position from the paper and put them in this document before treating 7.7 σ as a target
  to reproduce.
* **`DATAMODE = GRADED`.** ACIS graded mode telemeters grade and total pulse height only.
  It is fine for timing, which is what is wanted here, but it constrains spectroscopy and
  CTI correction. Step 9 should detect `GRADED` and say so in the report rather than
  producing a spectrum that looks ordinary and is not.

**What passing looks like:** `5644` reduces; its reported time resolution is 0.44104 s and
not 3.2 s; it is barycentred at the position asked for; and a coherent search of the
barycentred event list recovers the published signal at a comparable significance. A
shortfall in significance is worth investigating — barycentring, GTI handling, or the
extraction region — before it is attributed to the search.

### Cost

The three long observations are the bulk. At the HRC ratio measured on `17661` — 88.7 MB
kept of 264.5 MB, for 29.8 ks — the 266 ks should come to roughly 700–800 MB after the
filter, against something over 2 GB unfiltered. That is an ordinary overnight batch, not a
special arrangement.

---

## Open items, to settle on the first run with CIAO

1. ~~**Does `barycorr` handle Chandra correctly?**~~ **Settled 2026-09-12: it does not.**
   HEASOFT has no Chandra orbit reader, `axbary` is the route, and the DE405 it forces
   costs a constant 0.377 µs. Full workings in *Step 10*. Nothing here is open any more,
   but Matteo has not yet seen the 0.377 µs figure — if he wants DE430 regardless, the
   only remaining route is computing the correction in astropy ourselves, which is real
   work and needs its own validation.
2. **The `S_TIMING` threshold.** `hrc_veto_ratio_threshold = 0.99` is chosen from two
   observations, one at 1.000 and one at 0.296. The gap is enormous, so almost any
   threshold works, but the distribution across the ~1 669 HRC-S observations has not been
   measured. Worth a survey once the module runs.
3. **Whether `chanmaster.data_mode` should be plumbed through** as a cross-check on the
   inferred HRC mode, the way XMM uses `OBSMLI` to cross-check position — reported, never
   authoritative. It needs a mission-neutral change to `core.py` to pass the catalogue row
   to the reduction. Deliberately **not** in this plan, because the `dtf1` discriminator
   makes it unnecessary; raise it only if the inference proves unreliable.
4. **How long `chandra_repro` actually takes**, which decides whether the reprocessing
   route is usable in a batch. Unmeasured.
5. **ACIS subarray frame times — now known to matter, and still unsurveyed.** `2749`
   measures `TIMEDEL = 2.54104` (`EXPTIME = 2.5`) and `5644` measures **`0.44104`**
   (`EXPTIME = 0.4`), against a nominal 3.2 s. The plan reads `TIMEDEL` and so is correct
   regardless, but the *distribution* across the 22 953 `TE_*` observations is unmeasured
   and **cannot be got from the catalogue** — `data_mode` does not carry the frame time,
   so it needs an event-header read per observation. Worth doing: it is the only way to
   say how much fast-timing ACIS data the archive actually holds, and `5644` shows the
   answer is not "none". Until then, do not quote a share for subarrays.
   **Liu 2024's detection in `5644` is the standing argument that this is science and not
   bookkeeping.**
6. **CC-mode background.** Continuous Clocking collapses one spatial dimension, so source
   and background overlap in it. The 1-D strip in step 6 is the XMM Timing analogue, but
   Chandra's geometry differs and the strip positions are a guess until tested.

---

## Reproducing the archive facts

Every number above comes from one of these. They need `pyvo`, `astropy` and `boto3`, all
already in `py313-x64`, and no credentials.

```python
# 1. Catalogue composition: 27 122 archived, and the detector/grating/mode split.
import warnings, collections, pyvo

warnings.filterwarnings("ignore")
tap = pyvo.dal.TAPService("https://heasarc.gsfc.nasa.gov/xamin/vo/tap")
t = tap.search("SELECT detector, grating, data_mode, status FROM chanmaster").to_table()
print(collections.Counter(t["status"]).most_common())
arch = t[t["status"] == "archived"]
print(collections.Counter(arch["detector"]).most_common())
print(collections.Counter(arch["grating"]).most_common())
print(collections.Counter(str(m).split("_")[0] for m in arch["data_mode"]).most_common(6))
# Expect: archived 27122; ACIS-S 14848, ACIS-I 8600, HRC-I 2005, HRC-S 1669;
#         NONE 23931, HETG 2082, LETG 1109; TE 22953, DEFAULT 1884, ..., CC 458
```

```python
# 2. The integer-obsid coercion, which is why obsid_query needs no change.
for wanted in ("'2749'", "2749", "'02749'"):
    q = f"SELECT obsid FROM public.chanmaster as cat WHERE cat.obsid IN ({wanted})"
    print(wanted, len(tap.search(q).to_table()))
# Expect: 1, 1, 1.
```

```python
# 3. The directory layout and the measured filter cost. Costs no HEAD requests: the S3
#    listing carries Size for every key.
import re, boto3
from botocore import UNSIGNED
from botocore.client import Config

c = boto3.client("s3", config=Config(signature_version=UNSIGNED))
INC = re.compile(
    r"(?:/primary/[^/]*_evt2\.fits\.gz$|/primary/[^/]*_asol1\.fits\.gz$"
    r"|/primary/[^/]*_fov1\.fits\.gz$|/primary/[^/]*_dtf1\.fits\.gz$"
    r"|/primary/orbitf[^/]*_eph1\.fits\.gz$|/primary/[^/]*_pha2\.fits\.gz$"
    r"|/primary/responses/[^/]*_(?:arf|rmf)2\.fits\.gz$"
    r"|/(?:primary|secondary)/[^/]*_bpix1\.fits\.gz$"
    r"|/secondary/[^/]*_(?:msk1|flt1)\.fits\.gz$|/oif\.fits$)"
)
for obsid, d in (("6298", "8"), ("17661", "1"), ("2749", "9")):
    pre = f"chandra/data/byobsid/{d}/{obsid}/"
    tot = keep = n = k = 0
    for page in c.get_paginator("list_objects_v2").paginate(Bucket="nasa-heasarc", Prefix=pre):
        for o in page.get("Contents", []):
            rel, size = "/" + o["Key"][len(pre) :], o["Size"]
            tot += size
            n += 1
            if INC.search(rel):
                keep += size
                k += 1
    print(f"{obsid}: {keep / 1e6:.1f} MB of {tot / 1e6:.1f} ({k} of {n} files)")
# Expect: 6298 15.4 of 51.0 (9 of 28); 17661 88.7 of 264.5 (9 of 35);
#         2749 180.2 of 458.5 (33 of 63).
```

```python
# 4. The event headers, including the two HRC files that claim the same time resolution
#    and do not have it. Reads only the first 400 kB of each gzipped list.
import zlib, io
from astropy.io import fits

for obsid, d, key in (
    ("6298", "8", "primary/hrcf06298N006_evt2.fits.gz"),
    ("17661", "1", "primary/hrcf17661N002_evt2.fits.gz"),
    ("2749", "9", "primary/acisf02749N004_evt2.fits.gz"),
):
    r = c.get_object(
        Bucket="nasa-heasarc", Key=f"chandra/data/byobsid/{d}/{obsid}/{key}", Range="bytes=0-400000"
    )
    raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(r["Body"].read())
    h = fits.open(io.BytesIO(raw), ignore_missing_end=True)[1].header
    print(obsid, h["DETNAM"], h.get("DATAMODE"), h.get("READMODE", "-"), h["TIMEDEL"])
# Expect: 6298 HRC-I OBSERVING - 1.5625e-05
#         17661 HRC-S OBSERVING - 1.5625e-05     <- identical, and 280x apart in truth
#         2749 ACIS-456789 FAINT TIMED 2.54104
```

```python
# 5. The discriminator: the veto fraction in the dead-time-factor file.
import gzip, numpy as np

for obsid, d, key in (
    ("6298", "8", "primary/hrcf06298_000N006_dtf1.fits.gz"),
    ("17661", "1", "primary/hrcf17661_001N002_dtf1.fits.gz"),
):
    r = c.get_object(Bucket="nasa-heasarc", Key=f"chandra/data/byobsid/{d}/{obsid}/{key}")
    dat = fits.open(io.BytesIO(gzip.decompress(r["Body"].read())))[1].data
    st = dat["STATUS"]
    g = dat[(st.sum(axis=1) == 0) if st.ndim > 1 else (st == 0)]
    dt = np.median(np.diff(np.sort(g["TIME"])))
    tot, val = np.median(g["TOTAL_EVT_COUNT"]), np.median(g["VALID_EVT_COUNT"])
    print(f"{obsid}: VALID/TOTAL={val / tot:.3f} rate={tot / dt:.1f}/s -> {dt * 1000 / tot:.2f} ms")
# Expect: 6298 VALID/TOTAL=0.296 rate=228.8/s -> 4.37 ms   (the documented ~4 ms)
#         17661 VALID/TOTAL=1.000 rate=60.0/s -> 16.67 ms  (and 16.67 ms is WRONG:
#         the ratio of 1.000 means no vetoing, so the answer is TIMEDEL, 15.625 us)
```

```bash
# 6. The CIAO conda channel, live, with no conda installed.
curl -s https://cxc.cfa.harvard.edu/conda/ciao/osx-arm64/repodata.json |
  python -c "import json,sys; d=json.load(sys.stdin); \
  print(sorted({v['name']+' '+v['version'] for v in d.get('packages.conda',{}).values()}))"
# Expect ciao 4.18.0 among them. The noarch subdir carries caldb_main 4.12.4 and
# ciao-contrib 4.18.2.
```

```python
# 7. The M82 acceptance target: 68 observations in the cone, 16 of them HRC.
q = """SELECT obsid, detector, grating, data_mode, exposure, status FROM chanmaster
       WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 148.9685, 69.6797, 0.2))=1
       ORDER BY obsid"""
t = tap.search(q).to_table()
hrc = t[[str(d).startswith("HRC") for d in t["detector"]]]
print(len(t), "in cone;", len(hrc), "HRC")
for r in hrc:
    print(r["obsid"], r["detector"], r["grating"], r["data_mode"], int(r["exposure"]), r["status"])
# Expect: 68 in cone; 16 HRC. 8189 and 8505 are HRC-S S_TIMING; 23460-23471 and 26111
# are HRC-I with data_mode OBS20743 -- a custom string, which is why the mode must be
# inferred from the dtf1 veto ratio and not by matching data_mode against known names.
```

```bash
# 8. HEASOFT barycorr does NOT work on Chandra. Needs the 6298 evt2 and its orbit file,
#    ungzipped side by side. Run under henv313, per the SAS/HEASOFT recipe.
export PATH=/Users/meo/mamba/envs/henv313/bin:$PATH
export CONDA_PREFIX=/Users/meo/mamba/envs/henv313
. $CONDA_PREFIX/etc/conda/activate.d/heainit.sh
mkdir -p pfiles && export PFILES="$PWD/pfiles;$HEADAS/syspfiles"
hdaxbary -i orbitf235397100N001_eph1.fits -f hrcf06298N006_evt2.fits -o out.fits \
         -ra 272.11834 -dec -36.98337 -ref ICRS
# Expect: "ERROR: no bracketing sample found for time 235695882.04482999", no output file.
# The orbit file is fine -- 5616 monotonic rows, 300 s step, no gaps, bracketing the
# event TSTART. barycorr has no Chandra branch (grep -i 'chandra\|axaf' on it is empty)
# and hdaxbary's only orbit readers are xtescorbit, nicerscorbit and swiftscorbit.
```

```python
# 9. What using axbary's DE405 instead of DE430 costs: 0.377 us, constant.
from astropy.time import Time
from astropy.coordinates import SkyCoord, solar_system_ephemeris, EarthLocation
import astropy.units as u, numpy as np

P = "https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/"
t = Time(
    50814.0 + np.linspace(235695882.04483, 235703017.07016, 25) / 86400.0, format="mjd", scale="tt"
)
src = SkyCoord(272.11834, -36.98337, unit="deg", frame="icrs")
geo = EarthLocation.from_geocentric(0, 0, 0, unit="m")
out = {}
for label, eph in (("DE405", P + "a_old_versions/de405.bsp"), ("DE430", P + "de430.bsp")):
    with solar_system_ephemeris.set(eph):
        out[label] = t.light_travel_time(src, kind="barycentric", location=geo).to(u.s).value
d = (out["DE430"] - out["DE405"]) * 1e6
print(f"mean {d.mean():+.4f} us, peak-to-peak {np.ptp(d):.4f} us")
# Expect: mean +0.3769 us, peak-to-peak 0.0016 us. A constant offset, not a drift:
# 2.4% of one HRC spec bin, and it cannot distort anything within an observation.
# Note de405.bsp lives under a_old_versions/; astropy's own "de405" name 404s.
```

```python
# 10. The known-answer test: obsid 5644 is ACIS Timed Exposure at 0.44 s, not 3.2 s.
#     This is the counter-example to "TE means 3.2 s" and to filtering on data_mode.
import boto3, zlib, io
from botocore import UNSIGNED
from botocore.client import Config
from astropy.io import fits

c = boto3.client("s3", config=Config(signature_version=UNSIGNED))
key = "chandra/data/byobsid/4/5644/primary/acisf05644N004_evt2.fits.gz"
r = c.get_object(Bucket="nasa-heasarc", Key=key, Range="bytes=0-600000")
raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(r["Body"].read())
h = fits.open(io.BytesIO(raw))[1].header  # warns about truncation; the header is complete
for k in ("DETNAM", "DATAMODE", "READMODE", "EXPTIME", "TIMEDEL", "ONTIME", "OBJECT"):
    print(f"{k:<9} = {h[k]!r}")
# Expect: DETNAM 'ACIS-7', DATAMODE 'GRADED', READMODE 'TIMED', EXPTIME 0.4,
#         TIMEDEL 0.44104, ONTIME 75131.2, OBJECT 'CXOM82 J095550.2+694047'.
# Nyquist period 0.88 s, so a 1.37 s ULX-pulsar spin is sampled ~3.1 times per cycle.
# The catalogue says only data_mode = 'TE_006AC', which reveals none of this.
```
