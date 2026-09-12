# Adding Chandra (ACIS + HRC) to `heasarc_retrieve_pipeline`

> **Handoff document.** Written 2026-09-12 against `heasarc_retrieve_pipeline` on branch
> `various_fixes` (HEAD `1153f58`). **Steps 1 to 5 have landed**; steps 6 onwards are
> still the agreed design rather than a report on work done. It is written to be picked up
> cold, by a person or a session with no memory of the conversation that produced it.
> Every number in it was measured against the live HEASARC archive, the live CXC conda
> channel, or real Chandra data files on 2026-09-12; the snippets under *Reproducing the
> archive facts* re-derive them, so none of it has to be taken on trust.
>
> **Steps 6 onwards need CIAO, which is not installed on Matteo's machine.** That is
> where this stops: everything doable without a CIAO installation is in, and step 6 is
> the first line that cannot be written honestly without one.
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

## Where this stands

**Last updated 2026-09-12, after step 10 and the acceptance test.** Branch `various_fixes`,
everything unpushed.

| | |
|---|---|
| Landed | Steps 1–7 and 10, eight commits, `5377cfd` → `98635d2` |
| Left | Steps 8, 9, 11, 12, 13 |
| Code | `src/heasarc_retrieve_pipeline/chandra.py`, `src/heasarc_retrieve_pipeline/ciao.py` |
| Tests | `tests/test_chandra.py` (202 + 19 doctests), `tests/test_ciao.py` (41), all offline |
| CIAO | 4.18.0 + CALDB 4.12.4 in the `ciao` micromamba environment, driven by `subprocess` from `py313-x64` — the pipeline never enters CIAO's Python |

**The acceptance test passes on both verification observations.** Run through the module end
to end on 2026-09-12: `5644` gives `P = 1.3453202 s` at `Z²₁ = 59.32` (6.2 σ after trials),
`8190` gives `P = 1.3504294 s` at `Z²₁ = 26.55` (2.7 σ) — both within 0.2 σ of Liu 2024,
from a blind 34 mHz search. Numbers, method and the two folded profiles in
*The known-answer test*. **Search with `-N 1` and without `--fast`**; the reasons are there
too.

**Three deviations from this plan are in force and two of them want a verdict:**

1. `ciao.run` grew an `args=` parameter, and `ciao_environment` now defaults `CALDB` to the
   tree beside the installation. Both are in `f202ab1`; neither was in step 5. This machine
   exports `CALDB=/Users/meo/azure_software/caldb/` for HEASOFT, which has no Chandra data
   in it, so without the default every CIAO task would silently use the wrong calibration.
   *Not controversial, but recorded.*
2. **`psfsize_srcs`' `NEAR_CHIP_EDGE` is not used**, because it is wrong on every ACIS
   subarray. Details in step 6.
3. **The aspect solution is not barycentred**, against step 10's text. Details and the
   argument in step 10. **This one needs Matteo's yes or no.**

**Two environment notes for whoever picks this up:**

* `micromamba` is at `/opt/homebrew/bin/micromamba` with `MAMBA_ROOT_PREFIX=$HOME/mamba`.
* The `ciao` environment shipped **numpy 2.5.3**, which removed `np.chararray` and so broke
  `pycrates` and with it *every* Python CIAO tool — `psfsize_srcs`, `specextract`,
  `chandra_repro`, `acis_set_ardlib`. Pinned to **2.4.6** (inside CIAO's declared
  `>=2.3.5,<3`) with Matteo's approval. If a fresh install fails at `import pycrates`, this
  is why.

---

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

### Which ACIS configuration an observation is

**Measured 2026-09-12, while step 2 was being written. The plan did not have this, and
assumed `DETNAM` would answer it.** It does not.

`DETNAM` is a chip list, and its digits are chip identifiers: **0–3 are the ACIS-I array**
(I0–I3) and **4–9 are the ACIS-S array** (S0–S5). The aimpoint is I3 — chip 3 — for ACIS-I
and S3 — chip 7 — for ACIS-S. An observation routinely reads out chips from both arrays:
`ACIS-012367` is the whole ACIS-I array with S2 and S3 alongside it, and `ACIS-235678` is
an ACIS-S observation that happens to include I2 and I3.

So both aimpoint chips are frequently on at once, and no rule written on the chip set can
say which one the telescope was focused on. Over **150 randomly chosen archived ACIS
observations, 75 of each configuration as `chanmaster.detector` labels them, 50 had both
chip 3 and chip 7 reading out** — a third of the sample — and those 50 span both
configurations.

What does answer it is `SIM_Z`, where the Science Instrument Module was parked:

| Catalogue | `SIM_Z` range (75 each) | Median (= nominal aimpoint) |
|---|---|---|
| `ACIS-I` | −238.274 … −214.099 | **−233.587** |
| `ACIS-S` | −195.973 … −182.134 | **−190.143** |

The two do not overlap: **18.126 mm** separate the most positive ACIS-I from the most
negative ACIS-S. `chandra.ACIS_SIM_Z_THRESHOLD = -205.0` sits in that gap with about 9 mm
of margin either way. An observation that does not offset the SIM sits exactly at its
nominal aimpoint, which is why the medians and the nominals coincide.

HRC needs none of this: its `DETNAM` *is* the configuration, `HRC-I` or `HRC-S`.

**Two smaller header facts, also measured while writing step 2.** Continuous Clocking
reads `READMODE = 'CONTINUOUS'`, `DATAMODE = 'CC33_FAINT'`, **`TIMEDEL = 0.00285`** (2.85
ms) and carries no `EXPTIME` at all — a third instance of the lesson that the nominal 3.2 s
describes almost nothing. And the archive's local download directory is the OBSID
*unpadded* (`byobsid/8/6298/` → `<input>/6298`), while its file names are padded
(`hrcf06298`), so directories and stems use different spellings of the same number. Both
are pinned by tests.

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

Ordered so that everything testable without CIAO comes first. Steps 1–5 need no CIAO
installed at all, and all five are now in; step 6 is where an installation becomes
unavoidable.

### ~~Step 1 — `MISSION_CONFIG` entry and the download filter~~ **Done, `5377cfd`.**

**One deviation, agreed with Matteo 2026-09-12.** The `MISSION_CONFIG` entry did *not*
land here: registering a mission requires an `obsid_processing` callable, and
`process_chandra_obsid` does not exist until step 12, so this step would have had to ship
a function that raises. XMM did not do that either — its filter landed in `a122c21` and
its `MISSION_CONFIG` entry nine commits later in `cda3cf0` — so **Chandra follows the same
order and the entry moves to step 12.** What did land here is the filter, both regexes,
and `chanmaster`'s 24 real columns recorded in `test_core.py`'s `CATALOGUE_COLUMNS`, so
the all-mission guards cover Chandra the moment step 12 registers it.

The repro route was measured while the filter was written and the plan did not have the
numbers: it keeps **79%, 77% and 78%** of the three directories.

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

### ~~Step 2 — `chandra.py`: config, paths, detector and mode parsing~~ **Done, `e3814b1`.**

The plan underestimated this step in one place: it assumed the detector label could be
read off `DETNAM`. It cannot — see *Which ACIS configuration an observation is*, below,
which is a measurement made while writing this step and added to the plan afterwards.

`DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./", products="archive",
caldb=None, psf_ecf=0.9, psf_energy_kev=1.0, src_radius_arcsec=None,
bkg_inner_factor=1.5, bkg_outer_factor=3.0, flare_sigma=3.0,
hrc_veto_ratio_threshold=0.99, pileup_percentile=90.0)`.

`src_radius_arcsec=None` means "ask `psfsize_srcs`"; a number overrides it.

Path helpers mirroring `xmm.py`'s: `chandra_base_output_path`, `chandra_product_output_path`,
`chandra_pipeline_output_path`, and finders for each product family.

*Test:* offline, against a fabricated directory tree.

*Commit:* `Add chandra.py: configuration, output paths and product discovery`

### ~~Step 3 — the time-resolution record~~ **Done, `335f747`.**

The `chandra_time_resolution` function described above, its `TimeResolution` dataclass, and
the `dtf1` reader. Pure Python, no CIAO, no network.

*Test:* every branch, from small recorded arrays. The two real cases are the fixtures:
`VALID/TOTAL = 0.296` at 228.8 triggers/s must give 4.37 ms, and `VALID/TOTAL = 1.000`
must give 15.625 µs **and not** 16.67 ms. That second assertion is the whole point of the
function and it is the first test to write.

*Commit:* `Say what time resolution a Chandra observation can actually support`

### ~~Step 4 — the archive front end~~ **Done, `2cc8199`.**

`chandra_archive_front_end(obsid, config)` → one `Observation`, or `None` when the
directory holds no level-2 event list. Reads `DETNAM`, `GRATING`, `DATAMODE`, `READMODE`,
`TIMEDEL`, `CALDBVER`; locates the companion files; records the calibration-staleness
diagnostic.

*Test:* offline, against fabricated headers for the three measured configurations.

*Commit:* `Read a Chandra observation off the archive's level-2 products`

### ~~Step 5 — `src/heasarc_retrieve_pipeline/ciao.py`~~ **Done, `9f39121`.**

Mirrors `sas.py`'s public shape; standalone **by decision**, duplicating it a third time.

```python
HAS_CIAO      # ASCDS_INSTALL set and `dmlist` on PATH
CIAO_LOCK     # threading.RLock(), same reasoning as heasoft.HEASOFT_LOCK
IN_PLACE      # marker class, copied from heasoft.py:259
run(name, *, produces, log_to=None, capture=False, env=None, cwd=None, **params)
ciao_environment(obsid, config)   # per-observation PFILES — see the ardlib hazard
```

Module docstring says why this duplicates `sas.py` rather than sharing with it, and each
copied helper carries a one-line pointer to its twin. The `produces=` guard at
`tests/test_heasoft.py:401` is repeated in `tests/test_ciao.py`, which is where
`test_sas.py` keeps its copy too — each runner's guard lives beside that runner's
tests rather than in one list that has to be remembered.

*Tests* (`tests/test_ciao.py`, offline, `subprocess.run` monkeypatched): argv order and
`k=v` formatting; a `dmcopy` filter expression containing `[`, `]`, `#` and spaces
survives unchanged; a non-zero return code raises naming the task; a zero return code with
a missing output raises; `ciao_environment` never mutates `os.environ` and gives two
OBSIDs two different `PFILES`.

**One deviation, flagged 2026-09-12.** `run` also took `capture` and `cwd`, which the
signature above did not have. They are carried over from `sas.run` so the two runners
are the same shape, and Chandra needs both for the reasons XMM did: `dmlist` answers on
standard output and nowhere else, so `capture` is the only way to read it; and
`specextract` writes the file names it was *given* into `BACKFILE`, `RESPFILE` and
`ANCRFILE`, where a FITS header card holds 80 characters, so `cwd` is what lets the
caller pass short names. Both are tested rather than left as untested spare parts.

Two things not in the plan came out of writing it. `chandra_repro` produces a
*directory*, so `_check_outputs` had to keep the SAS version's "a directory must exist
and hold at least one entry" branch rather than only checking files. And `PFILES` needs
`$ASCDS_INSTALL/contrib/param` on the fallback beside `$ASCDS_INSTALL/param`, because
the contributed scripts — `chandra_repro` among them — keep their parameters there.

*Commit:* `Add ciao.py: run CIAO tasks with checked outputs and per-observation PFILES`

### ~~Step 6 — position and extraction regions~~ **Done, `caddefd`.**

**One deviation.** `psfsize_srcs` also returns a `NEAR_CHIP_EDGE` flag, and it is wrong on
every ACIS subarray: `check_chip_edge` computes the top of the window as `(NROWS-1) - edge`
instead of `FIRSTROW + NROWS - 1 - edge`, so for `5644` (`FIRSTROW = 449`, `NROWS = 128`,
`edge = 32`) the window runs 481 → 95 and *every* position is flagged. The column is not
read. `chandra_chip_edge` computes the margin itself from `FIRSTROW`/`NROWS` and reports a
**distance in chip pixels**, not a boolean, which is more useful anyway: `5644` has 47.95 px
of clearance, `8190` has 27.22 px — genuinely inside the 32-px dither amplitude.

The `src_radius_arcsec = None` default is vindicated on the two verification observations:
`5644` at 0.29′ off-axis gets **0.830″**, `8190` at 3.58′ gets **2.186″**. A single fixed
radius would have been wrong for one of them.


`dmcoords` converts the given RA/Dec to sky and chip coordinates; `psfsize_srcs` gives the
radius enclosing `psf_ecf` of the counts at `psf_energy_kev`. Background is an annulus
with the configured factors, as for XMM. For CC mode the regions are 1-D strips in the
surviving spatial coordinate, the direct analogue of XMM's `RAWX` strips.

Chandra's PSF grows from about one arcsecond on-axis to over ten at eight arcminutes
off-axis, so a fixed radius — XMM's approach — would be wrong at both ends. That is why
the default is `None`.

*Commit:* `Size Chandra extraction regions from the PSF at the source's off-axis angle`

### ~~Step 7 — flare screening and the cleaned event list~~ **Done, `2633ac0`.**

Two things the plan did not anticipate, both found on real data and both now tested:

* **`dmextract` emits bins outside the observation's GTI** with `EXPOSURE = 0` and
  `COUNT_RATE = 0` — 16 of 393 on `5644`. Read at face value they are the quietest bins in
  the observation and drag the threshold down. `read_chandra_lightcurve` turns them into
  `NaN`.
* **A curve shorter than the observation must not cut the uncovered stretch.** Intersecting
  the flare intervals with the observation GTI silently discarded good time wherever the
  light curve did not reach. `good_intervals` is now bounded by the union of the two spans,
  so an uncovered stretch is *kept*.

`[exclude sky=circle(...)]` does not compose with other Data Model filters — "cannot mix
EXCLUDE and FILTER". Region algebra, `[sky=field()-circle(...)]`, does.

The default is as gentle as intended: `5644` loses 200 s of 75 131 (0.3%), `8190` loses
nothing. `dmcopy "evt[@gti]"` **intersects** with the existing GTI rather than replacing
it, and the recorded exposure matches the output file's `ONTIME` exactly.


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

### ~~Step 10 — barycentring~~ **Done, `98635d2`.**

**This step was the plan's one open dependency, and it was settled by measurement on
2026-09-12. The answer is not the one the plan first assumed.**

**One deviation, and it needs Matteo's sign-off.** The text below says the aspect solution
must be barycentred alongside the events. It is **not**, deliberately. Nothing in this
architecture ever pairs the barycentred events with an aspect solution: pile-up and
`specextract` run on the *uncorrected* cleaned list, which is the only list an asol belongs
with, and the barycentred list exists solely to be folded. Barycentring the asol as well
would write ~17 MB of dead weight per observation to guard against a mixing that cannot
happen. Say the word and it goes back in.

Measured on both verification observations: `TIMESYS = TDB`, `TIMEREF = SOLARSYSTEM`,
`PLEPHEM = JPL-DE405`, corrections of **−280.89 s** (`5644`) and **−207.16 s** (`8190`)
applied to every HDU including the GTI blocks. `axbary` will not read a **gzipped input**
event or aspect file ("Failed to re-open output file", error 112) — it reads a gzipped
*orbit* file fine. Ours come from `dmcopy` uncompressed, so nothing had to change.

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

`HAS_CIAO` probes `ASCDS_INSTALL` in the environment and `dmlist` on `PATH`, which is the
shape `HAS_SAS` settled on after `import pysas` proved to be the wrong probe. It is
resolved once, at import, so a session that installs CIAO *after* importing the package
has to restart the interpreter.

So the install, when the 2.3 GB is available:

```bash
micromamba create -n ciao -c https://cxc.cfa.harvard.edu/conda/ciao -c conda-forge \
    ciao ciao-contrib caldb_main sherpa
```

and the check that it reached the process, which is what every step from 6 on assumes:

```bash
python -c "from heasarc_retrieve_pipeline import ciao; print(ciao.HAS_CIAO)"
```

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

One caution stands: **`DATAMODE = GRADED`.** ACIS graded mode telemeters grade and total
pulse height only. It is fine for timing, which is what is wanted here, but it constrains
spectroscopy and CTI correction. Step 9 should detect `GRADED` and say so in the report
rather than producing a spectrum that looks ordinary and is not.

#### The test was run, by hand, on 2026-09-12 — and it passes

**The pulsation is recovered.** This was done before any of this module exists, using only
the archive, astropy and HENDRICS, precisely to find out whether the plan's central claim
survives contact with real data. It does.

| | |
|---|---|
| **Measured frequency** | **0.7433174711 Hz** |
| **Measured period** | **1.3453202 s** |
| Liu 2024 (doi:10.3847/1538-4357/ad17c7), via Matteo | 1.345317(2) s |
| Difference | **+3.19 µs, i.e. +1.6 σ of the quoted error** |
| Z²₂ power (0.98″ extraction) | **69.27** |
| Significance | **7.6 σ single-trial, 5.9 σ after full trials correction** |
| Liu 2024's reported significance | 7.7 σ |
| Pulsed amplitude | **10.8 ± 1.5 %** |

Matteo warned that Liu may have used a slightly different orbital ephemeris, so the period
could move by more than its error bar. +1.6 σ is comfortably inside that.

**The detection is robust to the extraction region.** All three radii return the *identical*
frequency, and only the significance moves with the counts:

| Radius | Events | Z²₂ | Pulsed amplitude |
|---|---|---|---|
| 1.5 px (0.74″) | 9 743 | 50.16 | 10.15 ± 1.71 % |
| 2.0 px (0.98″) | 11 796 | **69.27** | 10.84 ± 1.51 % |
| 3.0 px (1.48″) | 13 753 | 81.83 | 10.91 ± 1.39 % |

**And the source is now pinned down: it is M82 X-2, not the header target.** The two are
4.63″ apart and both are bright — X-1 carries 51 573 counts within 1″, X-2 11 796 — but
the signal is at X-2's position, at X-2's period. The header's `OBJECT` is M82 X-1, so
**an implementation that extracted at `RA_TARG`/`DEC_TARG` would have found nothing.**
That is the sharpest possible vindication of the rule that the position asked for is the
position extracted, never the header's.

**What made it work, in order of how easily each could have been got wrong:**

1. **Deorbiting is not optional.** Without it, the same data give Z²₂ = 25.68, no
   candidate, and an upper limit of 9.1 % — i.e. a *non-detection of a real 10.8 % signal*.
   Over 75 ks the orbit drifts the pulse by roughly 33 cycles. Nor can an `fdot` search
   absorb it: a third of an orbit is not a constant frequency derivative.
2. **`PBDOT` is not a refinement.** Extrapolating the ephemeris back 1 728 orbits to 2005,
   the orbital-decay term alone is **0.0863 orbits** of correction. Omit it and the fold is
   wrong by 8.6 % of an orbit. The residual uncertainty after applying it is 0.168 spin
   cycles, dominated by `PBDOT`'s own error — tolerable, and it is why the recovered period
   sits 1.6 σ off rather than dead on.
3. **The spacecraft term in the barycentring matters.** Chandra is up to 125 000 km from
   Earth. The geocentre correction drifts 2.1 s across this observation (1.6 spin cycles);
   the spacecraft-to-geocentre term adds another 0.129 s (0.10 cycles). Barycentring to the
   geocentre alone would cost a tenth of a cycle of smearing.
4. **The 0.44104 s frame time is fast enough, and had to be read rather than assumed.**
   3.05 samples per cycle. The frame acts as a 0.328-phase boxcar, attenuating the
   fundamental by ~0.83 — visible in the profile as a broad, near-sinusoidal shape, and no
   obstacle to detection.

**Reproduced with:** events and `orbitf*_eph1.fits` straight from S3; barycentring in
astropy (DE405, Roemer + Shapiro at the geocentre, plus the interpolated spacecraft term,
plus TT→TDB); extraction at M82 X-2's position; `HENzsearch --fast -N 2 -p orbital_decay.par`.
**The ephemeris used was `~/tmp/nustar_2026/orbital_decay.par` (1 159 bytes, 2026-09-05)**,
which is *not* the file Matteo pointed at on Google Drive (1 148 bytes, 2026-09-08) — that
one is unreadable from this machine's sandbox. The detection is strong enough that the
difference plainly does not matter for *whether* the signal is there, but the newer file
should be used before the recovered period is quoted anywhere.

**Consequence for the plan: the ACIS timing branch is de-risked before it is written.**
Step 5's resolution logic, step 10's barycentring and the position rule have all now been
exercised end to end on real data with a known answer. What remains is to put this inside
the module, not to find out whether it works. `5644` should become the module's
highest-value integration test.

#### Run again through the module, on 2026-09-12, on `5644` **and** `8190`

Everything above was done by hand. This was done by the module: `chandra_archive_front_end`
→ `chandra_extraction_regions` → `chandra_clean_event_list` → `chandra_barycenter`, then
HENDRICS on what came out. **Both observations return the published period.**

**Use `Z²₁`, not `Z²₂`.** Matteo's call, and the profiles below show why: they are
single-peaked and close to sinusoidal, so a second harmonic adds no signal. Worse, at
`TIMEDEL = 0.44104 s` the second harmonic (1.487 Hz) sits *above* the Nyquist frequency
(1.134 Hz), so `n = 2` is summing aliased noise into the statistic. The `--fast` accelerated
search compounds this: over the same band it reports `Z²₁ = 30.30` for `5644` where an exact
fdot = 0 search reports **59.32**, because its coarse phase binning attenuates the very peak
it is looking for. **Search `-N 1`, and without `--fast` unless an `fdot` is actually needed
— the accelerated search costs 8× the trials and, here, half the power.**

| | `5644` | `8190` |
|---|---|---|
| Extraction | 0.830″ (PSF at 0.29′ off-axis) | 2.186″ (PSF at 3.58′ off-axis) |
| Events | 10 577 | 19 025 |
| Exposure after screening | 74 933 s | 58 179 s |
| Barycentric correction | −280.89 s | −207.16 s |
| **Measured period** | **1.3453202 s** | **1.3504294 s** |
| Liu 2024 | 1.345321(5) s | 1.350429(4) s |
| Difference | **−0.2 σ** | **+0.1 σ** |
| **Z²₁** | **59.32** | **26.55** |
| Highest other peak in the band | 14.60 | 21.58 |
| Significance after 2 554 / 1 977 trials | **6.2 σ** | **2.7 σ** |
| Sinusoid amplitude | 10.6 ± 1.4 % | 5.3 ± 1.0 % |
| Liu 2024's amplitude | 12 ± 2 % | 6 ± 2 % (lower limit) |

`5644` is unambiguous: the peak stands at 59.32 in a band whose next-highest excursion is
14.60. `8190` is exactly what Matteo said it would be — **lower significance**. Liu 2024
declares it at 3.3 σ; this search puts it at 2.7 σ after trials, and its peak is only 5
units of `Z²₁` above the tallest noise peak in the band. On its own it would not be claimed.

**What makes `8190` a verification rather than a coincidence is that nothing about the
answer was fed in.** A blind search over 0.722–0.756 Hz — 34 mHz, wide enough to hold every
frequency this source has ever shown — puts its tallest peak **0.1 σ** from a period
published independently, and its amplitude within 0.4 σ of the published amplitude. The
probability that a noise peak lands inside a 4 µs window by chance is ~10⁻³.

The two observations also bracket the spin-down: 1.3453202 s in 2005-08, 1.3504294 s in
2007-06, +5.1 ms in 655 days.

`8190` is off-axis and blended, which is why its amplitude is half `5644`'s and why Liu
calls theirs a lower limit. **Restricting to Liu's own 2–8 keV band reproduces Liu's own
number**: at the same 2.186″ radius, `Z²₁` rises from 26.55 to **30.00**, the tallest noise
peak in the band drops from 21.58 to 18.02, and the significance after trials becomes
**3.24 σ** against Liu 2024's declared 3.3 σ. The period moves by 4 µs, to +1.1 σ of theirs.
So the band is worth having — but the module's default, whole band at the PSF radius,
already recovers the signal, and that default is what was being tested.

*Figure:* `/private/tmp/hrp_chandra/verify/m82x2_profiles.pdf` — the two folded
profiles, both single-peaked. The whole verification workspace is under that directory.

#### A by-product: the astropy barycentring route is no longer hypothetical

Open item 1 offered a third option for the DE430 question — computing the barycentric
correction ourselves — and dismissed it as "real work with its own validation". **That work
has now been done**, in about forty lines, and validated in the strongest available way: it
recovered a published pulsation at the published period. So if Matteo prefers DE430 over
`axbary`'s DE405, the route exists and is proven. This does **not** overturn the `axbary`
decision — a CIAO task is still less to maintain than our own ephemeris code — but the
fallback is now real rather than notional.

### Cost

The three long observations are the bulk. At the HRC ratio measured on `17661` — 88.7 MB
kept of 264.5 MB, for 29.8 ks — the 266 ks should come to roughly 700–800 MB after the
filter, against something over 2 GB unfiltered. That is an ordinary overnight batch, not a
special arrangement.

---

## Open items, to settle on the first run with CIAO

1. ~~**Does `barycorr` handle Chandra correctly?**~~ **Settled 2026-09-12: it does not.**
   HEASOFT has no Chandra orbit reader, `axbary` is the route, and the DE405 it forces
   costs a constant 0.377 µs. Full workings in *Step 10*. The astropy fallback, floated
   here as untested, was then written and used to recover the `5644` pulsation — so it is
   proven, not notional, if Matteo wants DE430 after all. Only that preference is open.
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
7. **A pulsation survey of the fast-frame ACIS-S observations of M82, once the module
   runs end to end.** Added by Matteo 2026-09-12, after `5644` and `8190` were both
   recovered. Item 5 asks *how much* fast-timing ACIS data the archive holds; this asks
   what is *in* it. The two verification observations are short by the standards of what
   is available:

   | obsid | detector | `data_mode` | exposure | date |
   |---|---|---|---|---|
   | `5644` | ACIS-S | `TE_006AC` | 75.1 ks | 2005-08-17 |
   | `8190` | ACIS-S | `TE_003C4` | 58.2 ks | 2007-06-02 |
   | **`10542`** | ACIS-S | `TE_0085E` | **120.2 ks** | 2009-06-24 |
   | **`10543`** | ACIS-S | `TE_0085E` | **120.0 ks** | 2009-07-02 |
   | **`10544`** | ACIS-S | `TE_0085E` | **74.5 ks** | 2009-07-08 |

   `10542`/`10543`/`10544` are the obvious first targets: the same detector, one `data_mode`
   between them, and 315 ks together — more than four times the counts of `5644`, which is
   the difference between a marginal detection and a measurement. Their **frame time is not
   yet known**: the catalogue's `TE_xxxxx` code does not decode to one (`5644` and `8190`
   carry different codes and the same `TIMEDEL = 0.44104 s`), so it takes an event-header
   read per observation, exactly the survey item 5 describes. Select on `TIMEDEL` short
   enough to sample 1.35 s — say `TIMEDEL < 0.5 s`, three or more samples per cycle — and
   not on the mode string.

   The search itself is the one run for the acceptance target, unchanged: deorbit with
   `orbital_decay.par`, `axbary`, then **Z²₁**. Use `n = 1`: the profile is close to
   sinusoidal, and at these frame times the second harmonic sits above the Nyquist
   frequency, so `n = 2` is buying aliased noise, not signal. See *The known-answer test*.

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
