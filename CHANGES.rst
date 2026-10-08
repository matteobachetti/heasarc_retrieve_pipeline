=========
Changelog
=========

Changes are recorded as small snippets in ``docs/changes/`` and assembled into this file
at release time with `towncrier <https://towncrier.readthedocs.io>`__ (see
``docs/changes/README.rst``). Everything below the marker line that follows was written
by hand from the git history, because no changelog was kept before PR #11; it is grouped
by release tag and by pull request, and is a summary, not a list of commits.

.. towncrier release notes start

Before PR #11 (2025-07 to 2026-09)
----------------------------------

Everything merged into ``main`` after the ``v0.2`` tag and before PR #11. No release was
tagged in this period.

Direct commits to ``main`` after PR #10 (2026-09-04)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Data that was already decrypted is no longer downloaded and decrypted again: a sentinel
  file marks it.
- ``nupipeline`` and ``nuproducts`` write their output to one log file per observation.
- The diagnostics page logs the start and the end of its write, with how long it took.
- An attempt to shut down Prefect's ``ProcessPoolTaskRunner`` explicitly was tried and
  reverted: Prefect runs a duplicate of the object, so closing ours does nothing.

PR #10: NuSTAR spectra, flare filtering, diagnostics, split and merge (2026-07 to 2026-09)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*NuSTAR products*

- NuSTAR spectra are extracted with ``nuproducts``, from source regions chosen
  automatically on the sky image (best region, with a maximum radius), for mode-01 and
  for spacecraft-science mode-06 data.
- FPMA and FPMB are extracted over one shared good time interval and combined into one
  spectrum per observing mode, with the "case B" correction documented.
- Merged final event lists are written as FITS files; the barycentric correction averages
  only mode-01 positions.
- Solar flares are filtered from source and background, using the GOES light curve (which
  is now written to disk) and the flare catalogue, plus a cut on the GOES flux itself.
  The effect is plotted and recorded as numbers.
- The A+B merge no longer keeps events that only one module saw.

*Downloading*

- The archive index is read from ``href`` instead of the link text, every downloaded file
  is checked against its size at the archive, and a bad one stops the run.
- The S3 listing is paginated and uses the sizes it already carries.
- NuSTAR observations with zero exposure are not downloaded.
- Proprietary-period data can be downloaded as PGP-encrypted archives and decrypted with
  ``gpg``, using the passphrases in ``~/.heasarc_retrieve_pgp_keys``.

*Robustness and concurrency*

- The legacy fallback code path was deleted, ``barycenter_file`` was unified, and the
  Prefect tasks and futures were cleaned up (real futures, task names that no longer
  crash).
- Every path is absolute and nothing steers the pipeline with ``chdir`` any more.
- Each observation runs in its own process, with its own parameter files and directory,
  and ``retrieve_and_process_data`` accepts a list of OBSIDs.
- All HEASOFT calls go through one locked entry point that notices when a tool fails and
  makes every call declare what it produces.
- A short workspace on local scratch keeps file names within HEASOFT's length limits;
  names that HEASOFT would silently truncate are refused.
- One failing observation costs only that observation. Observations with no spacecraft
  science data (including slews) and files too faint to place a region on are skipped and
  recorded, not treated as crashes.

*Diagnostics, split and merge*

- Every observation gets one HTML page showing the whole reduction (plotly figures, no
  more JPEGs written next to the data), and the run gets an index linking all of them.
  Each step records what it did, how the extraction region was chosen, and what the source
  separation found.
- New commands: ``hrp-split-obsid`` splits a finished observation into time segments,
  ``hrp-merge-obsids`` merges several observations, ``hrp-report`` rebuilds the pages and
  ``hrp-check-roundtrip`` checks a split against its parent, on a copy.

*Packaging and development*

- The package moved under ``src/``; only what is needed at runtime is a dependency, and
  the imaging stack is imported where it is used.
- Added ruff and pre-commit, opt-in markers for the expensive tests, a CI split into a
  fast required suite and slower scheduled jobs, a documentation build in CI, and a check
  of the behaviour of the HEASOFT tools that the test doubles assume.
- ``heasoftpy`` ``allow_failure`` is requested per call instead of process-wide.
- Documented the pipeline, its known issues, and every module.

PR #9: CI on micromamba (2026-01-02)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- The GitHub workflow installs its environment with micromamba.

PR #8: newer astropy (2026-01-02)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Updated astropy and added Python 3.14 to the test matrix.

PR #6: S3 migration and RXTE queries (2025-12-29)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Contributed by aDNAn-itis.

- Data can be downloaded from S3.
- The RXTE query and file search were corrected, with a warning for observations in
  binned mode.
- Extra flags are passed through to the NuSTAR processing function.
- The minimum Python version is now 3.10.

0.2 (2025-06-24)
----------------

PR #4: new astroquery (2025-06)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- File names and paths come from the new astroquery functionality, with workarounds for
  its ``_last_catalog_name`` problem.
- Downloads from different sources are split, S3 is used when possible, and recursive S3
  download is tested.
- Dependencies were updated (astroquery was added) and the minimum Python version raised.

Directly on ``main``
^^^^^^^^^^^^^^^^^^^^

- Complete (not only source) event files are barycentred as well.

PR #2: tests and CI (2025-05)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Remote-data tests by default, a test position and data path fixed, GitHub Actions and
  tox configured, version information added.

0.1 (2024-06-24)
----------------

First tagged state of the NuSTAR pipeline, developed from 2023-06 onwards.

- Finds NuSTAR observations in the HEASARC tables by source name or position, and
  downloads them.
- Runs the NuSTAR pipeline and extracts products, with GTI handling, barycentring of
  source and background, and background files.
- Machinery to avoid re-running finished work, handling of race conditions, and more
  robust behaviour when an observation fails.
- Fixed the coordinates used for the barycentric correction.
