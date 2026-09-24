# Adding Swift/XRT fluxes to `heasarc_retrieve_pipeline`

> **Handoff document.** Written 2026-09-24 on branch `various_fixes`. It is written to be
> picked up cold, by a person or a session with no memory of the conversation that
> produced it. **Step 1 has landed** (`a236706`, `6dbe7de`). Step 0, a probe of the UKSSDC
> product builder, is half done: one job has been analysed and a second one is pending
> (see *Where step 0 stands*). Nothing else has been started.
>
> Decisions marked **decided** were made by Matteo and should not be relitigated without
> him.
>
> Not part of the Sphinx build: like `xmm_integration_plan.md`, it is listed in
> `docs/conf.py`'s `exclude_patterns`. Keep it updated as the steps land. Move what is
> learned into `docs/technical_details.rst`, and delete this file when every step is done.

## Goal

Analyse Swift/XRT (the X-ray Telescope on Swift) data, for **fluxes only**. The XRT
timing modes are out of scope. The acceptance test is to reproduce §2 of Brightman et al.
2019, ApJ 873, 115 (Figs. 2–4):

- 227 XRT observations of M82, 2012–2016.
- Source region: a 49″ circle containing all of M82's sources. Background: a nearby
  circle of the same size.
- Grouped to at least 1 count per bin with GRPPHA. XSPEC fit over 0.2–10 keV of
  `zwabs*powerlaw` with z = 0.00067, using the Cash statistic.
- Reported quantity: the **observed 0.5–8 keV flux**, plotted against days since
  2012-01-01.
- The flux is (1–2)×10⁻¹¹ erg cm⁻² s⁻¹ for most of the period, rising to about 5×10⁻¹¹
  after day 1150 (an X-1 flare).
- Lomb–Scargle periodogram over 10–1000 d with 10⁴ frequencies, in three epochs:
  150–700, 700–1150 and 1150–1800 days. Peaks at **61.0 d** (epoch 1) and **56.5 d**
  (epoch 2), and none in epoch 3.
- Epoch folding in 8 phase bins, with T0 = 2012-01-01. The error bars are the 1σ spread
  (Fig. 4).

**Archive fact.** `swiftmastr` has **288** observations within 0.2° of
(148.96, 69.68) with MJD between 55927 and 57754 and `xrt_exposure > 0`. All of them have
PC-mode (Photon Counting, the imaging mode) exposure, 603 ks in total. Why the paper has
227 is unknown, and explaining it is an acceptance target. Query used:

```python
from astroquery.heasarc import Heasarc
q = Heasarc.query_tap(
    "SELECT obsid, name, start_time, xrt_exposure, xrt_expo_pc, xrt_expo_wt, ra, dec "
    "FROM swiftmastr WHERE CONTAINS(POINT('ICRS',ra,dec),CIRCLE('ICRS',148.96,69.68,0.2))=1 "
    "AND start_time BETWEEN 55927 AND 57754 AND xrt_exposure > 0 ORDER BY start_time"
).to_table()
```

## Decisions

- **decided** — Flux fitting lives in a **generic** PyXspec module,
  `spectral_fit.py`, which any mission can use. Done.
- **decided** — The Lomb–Scargle and folding analysis goes in **local numbered scripts**
  outside the package, like the RXTE M82 bundle on `/Volumes/Elements/m82_rxte/`. The
  package produces the per-observation flux table.
- **decided** — If we reduce locally, it is `xrtpipeline` with a **local** Swift XRT
  CALDB (calibration database), not the remote one. Ask Matteo before downloading it,
  giving its size.
- **Matteo's suggestion, adopted as the primary route** — use `swifttools`
  (https://www.swift.ac.uk/API/), which drives the UKSSDC XRT product builder
  (Evans et al. 2009) on Leicester's servers, rather than reinventing the reduction.
- Matteo is **registered** with the builder as `matteo.bachetti@inaf.it`. This is the
  `userID` for every request. Read it from config or an environment variable; never write
  it into the code.
- Commits are allowed; **never push**.

## Route

Build one spectrum per observation with the builder, then refit each one locally with
`spectral_fit.fit_flux` in the paper's configuration. The builder's own fits are kept as
a cross-check: its flux band is fixed at 0.3–10 keV and its model is
`TBabs*zTBabs*powerlaw`. The local `xrtpipeline` route (step 5) is kept as a fallback and
as validation on a few observations.

What the builder offers (read from the source of `swifttools` 4.0.5, then confirmed by
job 319830):

- `addSpectrum(timeslice="obsid", whichData="user", useObs="<comma-separated obsids>",
  srcrad=<pixels>, redshift=..., galactic=True)` returns one tar file per observation.
  Each contains:
  - `<name>pc.pi`: grouped with minimum 1 count, channels 0–29 (below 0.3 keV) marked
    bad, and `BACKFILE`/`RESPFILE`/`ANCRFILE` set as bare file names;
  - `pcsource.pi` and `pcback.pi`, unbinned;
  - `pc.rmf`, `pc.arf`, a `.areas` file describing the regions, and `models/*.xcm`.

  WT-mode (Windowed Timing) files come too, where they exist; ignore them.
- `retrieveSpectralFits()` returns, per observation and mode: `obsFlux` with bounds,
  `unabsFlux`, `gamma`, `nh`, `cstat`, `dof`, `meantime` and `exposure`.
- `addLightCurve(binMeth="obsid", minEnergy=..., maxEnergy=...)` returns one count rate
  per observation. It is a free sanity check on the flux curve.
- Global pile-up thresholds: `pcPupRate` and `wtPupRate`. See the finding below.
- `copyOldJob(jobID, becomeThis=True)` reattaches to a submitted job from any machine,
  for the same `userID`.

## Steps (one commit each, tests first)

0. **Probe the builder** (manual, no commit). See *Where step 0 stands*.
1. ~~**`spectral_fit.py` (generic)**~~ — done, `a236706` + `6dbe7de`.
   `fit_flux(spectrum, background, response, arf, model, parameters, frozen, fit_band,
   flux_band, statistic="cstat", delta_stat=2.706, min_counts=10)` returns the flux, its
   bounds, `error_flags`, stat and dof, counts, net rate, exposure, the free parameters and
   `reason`.
   - The flux comes from `cflux` placed outside the absorption, so it is the *observed*
     flux. Its errors come from XSPEC's `error` command.
   - `companion_files()` resolves `BACKFILE`/`RESPFILE`/`ANCRFILE` relative to the
     spectrum, because the package forbids `os.chdir` (`test_prefect_wiring`).
   - XSPEC prompting is switched off during the load. Otherwise a companion file named
     relative to the spectrum makes XSPEC prompt for a new name, and with nobody to
     answer, the whole load fails.
   - It refuses a spectrum without a response.
   - Tests: `tests/test_spectral_fit.py`, marked `heasoft` and skipped without PyXspec.
     They simulate spectra with `fakeit` on a diagonal response the test writes itself.
   - **Verified:** it reproduces the builder's own fit of 00091489001 to 0.03%:
     6.690e-11 against 6.692e-11, with an identical C-stat of 126.06 for 158 degrees of
     freedom (`TBabs*zTBabs*powerlaw`, `abund wilm`, `xsect vern`, 0.3–10 keV).
2. **`swift.py`: the builder client.**
   - `request_xrt_spectra(obsids, ra, dec, config)` builds the request, submits it, waits
     and downloads into `<out>/swift_xrt/<jobID>/`. It also saves the output of
     `retrieveSpectralFits()` as JSON.
   - It records the job ID *before* waiting, so a crash reattaches with `copyOldJob`
     instead of resubmitting.
   - `swifttools` becomes an optional dependency (a new extra). Observations are selected
     with our own astroquery query of `swiftmastr`. This route does **not** go through
     `MISSION_CONFIG` or the HEASARC download.
   - Tests: request construction, which swifttools can do offline, and parsing the tar
     layout from a tiny fixture.
   - The request must carry the pile-up setting that step 0 settles.
3. **Flux table.** Refit every PC spectrum with `fit_flux` in the paper's configuration:
   `zwabs*powerlaw`, `zwabs.Redshift=0.00067` frozen, 0.5–8 keV flux.
   - Fit range: the builder already marks channels below 0.3 keV bad, so the paper's
     0.2 keV lower bound cannot be reproduced. Use 0.3–10 keV and document the difference.
   - Output: `swift_xrt_fluxes.csv`, one row per observation, with obsid, MJD of the
     midpoint, exposure, net rate, our flux and bounds, the builder's flux and its band,
     and the fit flags.
   - Tests: two fake records produce the table.
4. **Docs.** A Swift section in `docs/technical_details.rst`. It covers:
   - the builder route, and what the service decides that we do not control;
   - the pile-up finding;
   - W-stat;
   - the 0.3 keV floor.

   Add anything odd to `docs/known_issues.rst`.
5. **(Deferred, optional) local route.** This adds:
   - a `MISSION_CONFIG["swift"]` entry: table `swiftmastr`, `expo_column` `xrt_exposure`,
     `name_column` `name`;
   - a download filter that keeps only `xrt/` and `auxil/`, modelled on
     `rxte.rxte_download_filter`;
   - `xrtpipeline` (following `nustar.nu_run_l2_pipeline`), then `xrtproducts` and
     `xrtmkarf` with the exposure map.

   The tools are present in `henv313_x86`, but the local CALDB (`~/devel/CALDB`) has
   **no Swift XRT data yet**. Run it on a handful of observations to validate the
   builder's spectra.

## Where step 0 stands

**Job 319830** was analysed. It used obsids 00091489001–003, `srcrad=21`, the redshift,
the default pile-up setting, and an obsid-binned light curve over 0.5–8 keV. Findings:

- It took about **1 hour** from submission to download, mostly waiting in the queue. The
  size is about **4 MB per observation**, so about 1.2 GB for all 288.
- `srcrad=21` **is honoured**: the outer radius is 49.56″, which is 21 × 2.36″.
- **The builder applied a pile-up correction that is wrong for M82.**
  - It judged the PC data piled up, excised the inner 23.6″, and kept an annulus from
    23.6″ to 49.56″.
  - It corrected for a point-source PSF (point-spread function) with a "Corr factor" of
    11.39. The correction is folded into the ARF, which peaks at 14.5 cm², against about
    110 cm² for XRT.
  - M82 is several sources plus diffuse emission, so the correction inflates the flux.
    The builder's result for 00091489001 is an observed 0.3–10 keV flux of 6.7×10⁻¹¹,
    against the paper's (1–2)×10⁻¹¹ in 0.5–8 keV.
- The background is an annulus about 94 times the source area. It contributes about 1%
  of the source-region counts, so where it sits hardly matters.
- The light curve's summed PC rate (Hard + Soft) is about 2 c/s, which is also
  pile-up-corrected. Treat it the same way.

**Job 319833 is pending.** It asks for the same 3 observations and the spectrum only,
with `pcPupRate=1000.0` and `wtPupRate=1000.0` to switch the pile-up correction off.
Submitted about 13:20 on 2026-09-24, from Matteo's Mac. To resume on another machine:

```python
import json
from swifttools.ukssdc.xrt_prods import XRTProductRequest

r = XRTProductRequest("matteo.bachetti@inaf.it", silent=True)
r.copyOldJob(319833, becomeThis=True)
print(r.complete, r.status, r.URL)
if r.complete:
    r.downloadProducts(".", format="tar.gz", clobber=True)
    json.dump(r.retrieveSpectralFits(returnData=True), open("spec_fits.json", "w"),
              indent=1, default=str)
```

Then unpack `spec.tar.gz` and the per-observation `Obs_*.tar.gz` files, and check:

1. The `.areas` file shows a full circle of 49.56″ (no annulus), and the ARF peaks
   near 110 cm². If not, `pcPupRate` did not switch the correction off. Look for another
   switch, or fall back to step 5.
2. Refit `Obs_*pc.pi` with `fit_flux` in the paper's configuration (below). If the
   0.5–8 keV fluxes land near (1–2)×10⁻¹¹, the route works. Go on to step 2. Days 110–120
   after 2012-01-01 fall just before the paper's epoch 1 and are still in the
   low-flux period.
3. If they are still far off, stop and discuss with Matteo before choosing between the
   builder and the local route.

A second worry to keep in mind: the paper did **no** pile-up correction. If some
observations truly are piled up (X-1 flared after day 1150), the paper's fluxes are low
there too. For a reproduction, match the paper; flag it in the docs.

The script used for job 319830 (it crashed on a `print` *after* submitting; the job was
recovered with `copyOldJob`):

```python
from swifttools.ukssdc.xrt_prods import XRTProductRequest

obs = "00091489001,00091489002,00091489003"
r = XRTProductRequest("matteo.bachetti@inaf.it", silent=False)
r.setGlobalPars(name="M82_probe", targ="00091489", getT0=True, RA=148.9627, Dec=69.6793,
                centroid=False, useSXPS=False, posErr=1)   # 319833 adds pcPupRate=1000.0, wtPupRate=1000.0
r.addSpectrum(whichData="user", useObs=obs, timeslice="obsid", srcrad=21,
              hasRedshift=True, redshift=0.00067, galactic=True)
r.addLightCurve(binMeth="obsid", whichData="user", useObs=obs, minEnergy=0.5, maxEnergy=8.0)
r.submit()                     # r.submitError raises if submission *succeeded*: don't print it
```

`targ` is the Swift target ID, zero-padded to 8 digits: the first 8 digits of the obsid.
The 288 observations span **ten** target IDs, so the full request needs all of them
(counts from `swiftmastr`, 2026-09-24):

| target_id | name | observations |
|---|---|---|
| 00032503 | M82X-1 | 149 |
| 00033123 | SN_M82 | 69 |
| 00092202 | M82X-1 | 51 |
| 00091489 | NGC3034 | 7 |
| 00081780 | M82_X2 | 3 |
| 00081887 | M82_ULXS | 3 |
| 00080426 | M82 | 2 |
| 00081460 | M82 | 2 |
| 00033124 | SN_M82 | 1 |
| 00081978 | M82_ULXS | 1 |

The paper says it kept the SN 2014J observations (the 70 `SN_M82` rows), so those are
not what separates 288 from 227. The position used here is M82 X-2.

Refitting a builder spectrum in the paper's configuration:

```python
from heasarc_retrieve_pipeline.spectral_fit import fit_flux

r = fit_flux("Obs_00091489001pc.pi", model="zwabs*powerlaw",
             parameters={"zwabs.Redshift": 0.00067}, frozen=["zwabs.Redshift"],
             fit_band=(0.3, 10.0), flux_band=(0.5, 8.0))
```

To reproduce the builder's own numbers instead, set `xspec.Xset.abund = "wilm"` and
`xspec.Xset.xsect = "vern"`. Then fit `TBabs*zTBabs*powerlaw`, with `TBabs.nH` frozen
at the Galactic value from `models/*.xcm`, over 0.3–10 keV with the flux in 0.3–10 keV.

## Reproduction bundle (local, not version-controlled)

Numbered, resumable stages, like `/Volumes/Elements/m82_rxte/`:

1. `01_catalog`: the `swiftmastr` query above, and the target IDs.
2. `02_request`: the builder job or jobs.
3. `03_fit`: builds the flux table.
4. `04_lomb_scargle`: astropy `LombScargle` with the paper's grid and epochs.
5. `05_fold`: 8 bins.
6. `06_compare`: Figs. 2–4. Use 7″ or 3.5″ widths and 7 pt labels.

Put it on the Elements drive (the internal disk had only 14 GB free).

**Acceptance targets:**
- (a) The flux levels in Fig. 2.
- (b) LS peaks near 61.0 d and 56.5 d, and none in epoch 3.
- (c) Folded profiles like Fig. 4.
- (d) An explanation of 288 against 227.

## Environments (Matteo's Mac; adapt elsewhere)

- **PyXspec:** `henv313` and `henv313_x86` both have PyXspec 2.1.5 with XSPEC 12.15.1.
  Initialise with `export CONDA_PREFIX=<env>; source $CONDA_PREFIX/bin/heainit.sh`.
  *Append* to `PYTHONPATH`, never overwrite it: heainit puts `xspec` and `heasoftpy`
  there.
- **Running the XSPEC tests:**
  `python -m pytest src/heasarc_retrieve_pipeline/tests/test_spectral_fit.py -p no:cacheprovider`
  in an initialised `henv313`.
- **Offline suite:** in `py313-x64`, 1876 passed and 38 skipped after `6dbe7de`.
- **swifttools** was not installed in any environment. The probes used an unpacked
  `swifttools-4.0.5` wheel on `PYTHONPATH`. `pip install swifttools` is the real
  dependency.
- **Checks before each commit:** pre-commit (`ruff format` rewrites files), and
  `sphinx-build -W -E -b html docs <build>` for the docs.
