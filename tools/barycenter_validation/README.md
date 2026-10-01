# Validating the `barycenter` package against the mission tools

`validate.py` takes one real reduction per mission that the pipeline had barycentered with
the mission's own tool, barycenters the same input with the `barycenter` package and the
same settings, and compares the two outputs column by column. The settings come from the
references themselves: `barycorr`'s parameter history in the output header (NuSTAR, RXTE),
or the step's diagnostics record (XMM, Chandra). Inputs are only read.

```bash
~/mamba/envs/py313-x64/bin/python validate.py --outdir <scratch>
```

## Inputs (as of 2026-10-01)

| case | input | reference made with |
|---|---|---|
| NuSTAR | `out_flares/90901333002/nu90901333002A01_cl_src1.evt` | `barycorr`, DE430, CALDB clock `nuCclock20100101v230` |
| RXTE | `~/tmp/m82_rxte/scout/bary_test/gx.evt` | `barycorr`, DE430, default clock (`gx_bary_caldb.evt`) |
| XMM | `~/tmp/m82_xmm/0657801901/event_cl/..._pn_S003_imaging_cl.evt` | SAS `barycen`, DE430 |
| Chandra | `~/tmp/m82_acis/5644/event_cl/chandra05644_aciss_timed_cl.evt` | CIAO `axbary`, DE405 (`refframe=ICRS`) |

XMM, Chandra and RXTE are at M82 X-2 (148.96267, 69.67931); NuSTAR at the position
`barycorr` recorded.

## Results, `barycenter` 1.0.0, package minus reference

| case | column | n | mean (ns) | max abs (ns) |
|---|---|---|---|---|
| NuSTAR | EVENTS/TIME | 24446 | +31.5 | 59.6 |
| NuSTAR | GTI/START, STOP | 1212 | +32.1, +30.9 | 59.6 |
| RXTE | XTE_SE/TIME | 53689 | +15.1 | 119.2 |
| RXTE | GTI/START, STOP | 1 | -59.6, +59.6 | 59.6 |
| XMM | EVENTS/TIME | 165104 | -62.5 | 119.2 |
| XMM | EXPOSU/TIME | 1766829 | -62.4 | 119.2 |
| XMM | GTI and STDGTI | 2544 | -68 to -74 | 119.2 |
| Chandra | EVENTS/TIME | 281781 | -0.0 | 59.6 |
| Chandra | GTI/START, STOP | 2 | +14.9 | 29.8 |

Every mean is inside the package's 100 ns target. The maxima are one or two steps of a
64-bit float at these mission elapsed times (29.8 ns near 2e8 s, 59.6 ns near 4e8 s,
119.2 ns near 5e8 s), the finest difference the reference files can record.

## What differs, and is not an error

- **Auxiliary time columns.** `barycorr` leaves NuSTAR's `BADPIX` times on spacecraft
  time, and `barycen` leaves XMM's `HKAUX` housekeeping times alone; the package corrects
  every `TIME` column in the file. Nothing in the pipeline reads either after
  barycentering. They are listed as "not compared". The NaNs in XMM's `HKAUX` are already in
  the input.
- **RXTE `clockfile=NONE` does skip the clock.** `gx_bary.evt`, made with
  `clockfile=NONE`, is 59.6 µs from both the package and the default-clock run. So
  `barycorr` does apply `tdc.dat` by default and does not apply it with `NONE`. The
  pipeline never passed `NONE`, and the package always applies `tdc.dat`.

## DE430 minus DE405 on Chandra 5644

At the end the script also barycenters 5644 with DE430. Against DE405 that is a mean
**-1.006 µs**, varying by 0.06 µs over the observation. `chandra.py` quotes **+0.377 µs**
(`DE405_MINUS_DE430_US`), measured on obsid 6298 at geocentre. The two are different
observations, at different epochs, so this is no contradiction in itself: the offset
depends on epoch and direction and is not one pipeline-wide constant. It only matters
for the `axbary` fallback, since the package route now uses DE430 directly.
