"""
SVOM: ECLAIRs and GRM, from the French Science Center (FSC).

SVOM's data are not at HEASARC. The catalogue and the files are at the FSC, reached through
the SVOM team's own packages -- ECPI's ``ecpi_find_files`` and ``svom.messaging`` -- which
live in an environment of their own (the ``eclair`` micromamba environment, Python 3.12)
and are never imported here. This module drives them as subprocesses, the way
:mod:`heasarc_retrieve_pipeline.ciao` drives CIAO, and plugs into the run through the
``catalogue``, ``work_items`` and ``download`` entries of ``MISSION_CONFIG``.

Both instruments share the catalogue and the auxiliary files:

* **One catalogue, by ObsID.** ECLAIRs and GRM files of the same pointing carry the same
  ``OBS_ID``. ``ecpi_find_files`` searches ECLAIRs files by position (it knows no GRM
  product), and its one-row-per-file answer is grouped into one row per ObsID.
* **Orbit, attitude and SAA by time.** ``SVO-ORB-CNV`` and ``SVO-ATT-CNV`` come one per
  downlink pass, the SAA tables (South Atlantic Anomaly entry and exit times) one per
  week. An event file can start minutes before its own pass's orbit file, inside
  the previous one's, so every file overlapping the events (with :data:`AUX_MARGIN_S` on
  either side) is fetched, whatever its pass.

What the run needs from the environment, like ``HEADAS`` for HEASOFT:

``SVOM_ECPI_PYTHON``
    The Python of the environment with ECPI and ``svom.messaging``; ``ecpi_find_files`` is
    looked for beside it.
``SVOM_ECPI_ENV``
    A shell file setting the FSC credentials (``KC_USERNAME`` and friends), sourced
    before every call and never read here. Default ``~/.ecpi``.
``FSC_URL``
    The FSC server. Default :data:`DEFAULT_FSC_URL`, production: without it ECPI goes to
    the integration server and the login fails.
"""

import json
import os
import subprocess
import tempfile

import numpy as np
from astropy.table import Table, vstack
from astropy.time import Time

from .diagnostics import catalogue_row
from .utils import get_logger

#: The helper run inside the ECPI environment; see its docstring.
HELPER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "svom_fsc.py")

#: The production FSC server.
DEFAULT_FSC_URL = "https://fsc.svom.org"

#: Event products per instrument. ECLAIRs' GTIs come in a file of their own.
EVENT_PRODUCTS = {
    "eclairs": ("ECL-EVT-CAL", "ECL-GTI-CAL"),
    "grm": ("GRM-EVT",),
}

#: Orbit and attitude, one file of each per pass, and the SAA entry and exit times, one
#: table per week, made for ECLAIRs (there is none for GRM): ``DEEP`` is the core of the
#: anomaly, ``EXT`` the wide region around it.
AUX_PRODUCTS = ("SVO-ORB-CNV", "SVO-ATT-CNV", "SAA-DEEP-ECL", "SAA-EXT-ECL")

#: How far beyond the events' span orbit and attitude files are looked for. Event files
#: were seen starting up to 5 minutes before their pass's orbit file.
AUX_MARGIN_S = 600.0

#: Default search radius around the ECLAIRs pointing, per instrument. The GRM detectors
#: point 30 degrees off the ECLAIRs axis, and see far off their own axes.
DEFAULT_RADIUS_DEG = {"eclairs": 45.0, "grm": 70.0}

#: The primary-header keywords that identify one product, beside its acronym.
IDENTIFYING_KEYWORDS = ("OBS_ID", "BURST_ID", "PASS_ID")

DEFAULT_CONFIG = dict(out_data_path="./", input_data_path="./")


def ecpi_python():
    """The ECPI environment's Python, from ``SVOM_ECPI_PYTHON``, or ``None``."""
    return os.environ.get("SVOM_ECPI_PYTHON") or None


def has_ecpi():
    """
    Whether ECPI can be called: ``SVOM_ECPI_PYTHON`` names an executable.

    Returns
    -------
    bool
    """
    python = ecpi_python()
    return python is not None and os.access(python, os.X_OK)


def ecpi_command(argv, env_file=None):
    """
    The argument vector that runs ``argv`` with the FSC credentials loaded.

    The credentials file is a shell file, so it is sourced by ``bash`` and the command is
    ``exec``-ed in the same shell. Its name is passed as an argument, not pasted into the
    script, so no file name can change what the shell runs. Without a credentials file
    the command runs as it is.

    Examples
    --------
    >>> ecpi_command(["python", "-V"], env_file="/nonexistent")
    ['python', '-V']
    >>> ecpi_command(["python", "-V"], env_file=__file__)[:2]
    ['bash', '-c']
    """
    if env_file is None:
        env_file = os.path.expanduser(os.environ.get("SVOM_ECPI_ENV", "~/.ecpi"))
    if not os.path.exists(env_file):
        return list(argv)
    script = 'source "$1" > /dev/null && shift && exec "$@"'
    return ["bash", "-c", script, "svom-ecpi", env_file, *argv]


def ecpi_environment(environ=None):
    """
    The process environment, with ``FSC_URL`` defaulting to production.

    Examples
    --------
    >>> ecpi_environment({})["FSC_URL"]
    'https://fsc.svom.org'
    >>> ecpi_environment({"FSC_URL": "https://example.invalid"})["FSC_URL"]
    'https://example.invalid'
    """
    env = dict(os.environ if environ is None else environ)
    env.setdefault("FSC_URL", DEFAULT_FSC_URL)
    return env


def run_ecpi(argv, cwd=None):
    """
    Run a command in the ECPI environment and return its standard output.

    Raises
    ------
    RuntimeError
        If ECPI is not configured, or the command fails; the message ends with the last
        lines of its standard error.
    """
    if not has_ecpi():
        raise RuntimeError(
            "SVOM data need ECPI: set SVOM_ECPI_PYTHON to the Python of the environment "
            "that has it (and svom.messaging)."
        )
    done = subprocess.run(
        ecpi_command(argv), env=ecpi_environment(), cwd=cwd, capture_output=True, text=True
    )
    if done.returncode != 0:
        tail = "\n".join(done.stderr.strip().splitlines()[-10:])
        raise RuntimeError(f"{os.path.basename(argv[0])} failed ({done.returncode}):\n{tail}")
    return done.stdout


def helper_argv(*args):
    """The helper, run with the ECPI Python in isolated mode."""
    return [ecpi_python(), "-I", HELPER, *[str(a) for a in args]]


def find_files_argv(
    position=None, radius_deg=None, obsid=None, mjd_start=None, mjd_stop=None, python=None
):
    """
    The ``ecpi_find_files`` call for ECLAIRs event files, by position or by ObsID.

    Dates are MJD (UTC), passed as ISO dates. ``ecpi_find_files`` is beside ``python``,
    by default :func:`ecpi_python`.

    Examples
    --------
    >>> find_files_argv(obsid="1000000001", python="/env/bin/python")
    ['/env/bin/ecpi_find_files', '--acronym', 'ECL-EVT-CAL', '--obsid', '1000000001']
    >>> find_files_argv(mjd_start=60980.0, python="/env/bin/python")[-2:]
    ['--date-min', '2025-11-01T00:00:00.000']
    """
    argv = [
        os.path.join(os.path.dirname(python or ecpi_python()), "ecpi_find_files"),
        "--acronym",
        "ECL-EVT-CAL",
    ]
    if obsid is not None:
        argv += ["--obsid", str(obsid)]
    if position is not None:
        argv += ["--ra", repr(float(position.ra.deg)), "--dec", repr(float(position.dec.deg))]
        argv += ["--radius", repr(float(radius_deg))]
    for flag, mjd in (("--date-min", mjd_start), ("--date-max", mjd_stop)):
        if mjd is not None:
            argv += [flag, Time(mjd, format="mjd", scale="utc").isot]
    return argv


def find_ecl_files(**kwargs):
    """
    ``ecpi_find_files``' table of ECLAIRs event files, one row per file.

    Takes the arguments of :func:`find_files_argv`. An empty table if nothing is found.
    """
    with tempfile.TemporaryDirectory(prefix="svom_find_") as workdir:
        table_file = os.path.join(workdir, "files.tsv")
        argv = find_files_argv(**kwargs)
        argv += ["--table-file", table_file, "--url-file", os.path.join(workdir, "urls.txt")]
        run_ecpi(argv, cwd=workdir)
        if not os.path.exists(table_file) or os.path.getsize(table_file) == 0:
            return Table()
        return Table.read(table_file, format="ascii.tab")


def _mean_direction(ra_deg, dec_deg):
    """Mean of sky directions, as unit vectors, so that RA wraps correctly at 0/360."""
    ra, dec = np.radians(ra_deg), np.radians(dec_deg)
    x, y, z = (
        np.mean(np.cos(dec) * np.cos(ra)),
        np.mean(np.cos(dec) * np.sin(ra)),
        np.mean(np.sin(dec)),
    )
    return np.degrees(np.arctan2(y, x)) % 360, np.degrees(np.arctan2(z, np.hypot(x, y)))


def _number(value):
    """A table cell as a float: ``None``, ``"None"`` and masked cells are NaN."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def group_by_obsid(files):
    """
    One catalogue row per ObsID, from ``ecpi_find_files``' one row per file.

    ``ra``/``dec`` are the mean pointing of the files that have one, ``tstart``/``tstop``
    the span of all of them (SVOM seconds), ``passes`` the downlink passes they came in.

    Parameters
    ----------
    files : astropy.table.Table
        With ``OBS_ID``, ``PASS_ID``, ``TSTART``, ``TSTOP``, ``RA_PNT``, ``DEC_PNT`` and
        ``PROGRAM``.

    Returns
    -------
    astropy.table.Table
        ``obsid`` (str), ``ra``, ``dec``, ``tstart``, ``tstop``, ``n_files``, ``passes``,
        ``program``, sorted by ``tstart``.

    Examples
    --------
    >>> files = Table(rows=[
    ...     (1000000001.0, "000001_HBX", 10.0, 20.0, 359.0, 0.0, "ToO"),
    ...     (1000000001.0, "000002_HBX", 30.0, 40.0, 1.0, 0.0, "ToO")],
    ...     names=["OBS_ID", "PASS_ID", "TSTART", "TSTOP", "RA_PNT", "DEC_PNT", "PROGRAM"])
    >>> row = group_by_obsid(files)[0]
    >>> str(row["obsid"]), round(float(row["ra"]) % 360, 6), float(row["tstop"]), str(row["passes"])
    ('1000000001', 0.0, 40.0, '000001_HBX,000002_HBX')
    """
    names = ["obsid", "ra", "dec", "tstart", "tstop", "n_files", "passes", "program"]
    if len(files) == 0:
        return Table(names=names, dtype=[str, float, float, float, float, int, str, str])
    obsids = np.array([str(int(_number(o))) for o in files["OBS_ID"]])
    rows = []
    for obsid in np.unique(obsids):
        mine = files[obsids == obsid]
        ra = np.array([_number(v) for v in mine["RA_PNT"]])
        dec = np.array([_number(v) for v in mine["DEC_PNT"]])
        good = np.isfinite(ra) & np.isfinite(dec)
        mean_ra, mean_dec = _mean_direction(ra[good], dec[good]) if good.any() else (np.nan, np.nan)
        rows.append(
            (
                obsid,
                mean_ra,
                mean_dec,
                np.nanmin([_number(v) for v in mine["TSTART"]]),
                np.nanmax([_number(v) for v in mine["TSTOP"]]),
                len(mine),
                ",".join(sorted({str(p) for p in mine["PASS_ID"]})),
                str(mine["PROGRAM"][0]),
            )
        )
    table = Table(rows=rows, names=names)
    table.sort("tstart")
    return table


def svom_catalogue(
    instrument, position=None, radius_deg=None, mjd_start=None, mjd_stop=None, obsids=None
):
    """
    The ``"catalogue"`` hook: SVOM ObsIDs near a position, or the ObsIDs given.

    Parameters
    ----------
    instrument : ``"eclairs"`` or ``"grm"``
        Chooses the default radius, :data:`DEFAULT_RADIUS_DEG`.
    position : astropy.coordinates.SkyCoord, optional
    radius_deg : float, optional
        Around the ECLAIRs pointing.
    mjd_start, mjd_stop : float, optional
    obsids : list of str, optional
        Instead of a position.

    Returns
    -------
    astropy.table.Table
        See :func:`group_by_obsid`.
    """
    if obsids is not None:
        found = [find_ecl_files(obsid=obsid) for obsid in obsids]
        found = [table for table in found if len(table)]
        return group_by_obsid(vstack(found) if found else Table())
    if radius_deg is None:
        radius_deg = DEFAULT_RADIUS_DEG[instrument]
    return group_by_obsid(
        find_ecl_files(
            position=position, radius_deg=radius_deg, mjd_start=mjd_start, mjd_stop=mjd_stop
        )
    )


def svom_work_items(result_table, source_position):
    """
    The ``"work_items"`` hook: one item per ObsID.

    There is no URL to look up: the download searches the FSC by ObsID itself, and the
    ``url`` is only a label, ``fsc:<obsid>``. Without a source position each ObsID is
    barycentred at its mean ECLAIRs pointing.
    """
    items = []
    for row in result_table:
        obsid = str(row["obsid"])
        if source_position is not None:
            ra, dec = source_position.ra.deg, source_position.dec.deg
        else:
            ra, dec = float(row["ra"]), float(row["dec"])
        items.append(
            dict(obsid=obsid, url=f"fsc:{obsid}", ra=ra, dec=dec, catalogue=catalogue_row(row))
        )
    return items


def search_products(acronym, pairs=(), tmin=None, tmax=None):
    """Products of one acronym at the FSC, through the helper: a list of dicts."""
    args = ["search", acronym, *pairs]
    if tmin is not None:
        args += ["--tmin", repr(float(tmin))]
    if tmax is not None:
        args += ["--tmax", repr(float(tmax))]
    return json.loads(run_ecpi(helper_argv(*args)))


def latest_versions(products):
    """
    The highest ``PROC_VER`` of each product, told apart by acronym, ObsID, burst and pass.

    Examples
    --------
    >>> old = dict(acronym="GRM-EVT", url="a", header=dict(OBS_ID=1, PROC_VER=1))
    >>> new = dict(acronym="GRM-EVT", url="b", header=dict(OBS_ID=1, PROC_VER=2))
    >>> [p["url"] for p in latest_versions([old, new])]
    ['b']
    """
    best = {}
    for product in products:
        header = product["header"]
        key = (product["acronym"],) + tuple(header.get(k) for k in IDENTIFYING_KEYWORDS)
        if key not in best or (header.get("PROC_VER") or 0) > (
            best[key]["header"].get("PROC_VER") or 0
        ):
            best[key] = product
    return list(best.values())


def download_plan(instrument, obsid):
    """
    Every file to download for one ObsID: its event products, then orbit, attitude and
    SAA.

    The auxiliary files (:data:`AUX_PRODUCTS`) are all those overlapping the events' span, widened by
    :data:`AUX_MARGIN_S`, whatever their pass. An ObsID with no event products for this
    instrument gives an empty plan.
    """
    events = []
    for acronym in EVENT_PRODUCTS[instrument]:
        events += search_products(acronym, [f"OBS_ID={obsid}"])
    events = latest_versions(events)
    if not events:
        return []
    tmin, tmax = aux_window(events)
    aux = []
    for acronym in AUX_PRODUCTS:
        aux += search_products(acronym, tmin=tmin, tmax=tmax)
    return events + latest_versions(aux)


def aux_window(events, margin=AUX_MARGIN_S):
    """
    The interval orbit and attitude files must cover: the events' span plus a margin.

    Examples
    --------
    >>> events = [dict(header=dict(TSTART=100.0, TSTOP=200.0)),
    ...           dict(header=dict(TSTART=150.0, TSTOP=400.0))]
    >>> aux_window(events, margin=10)
    (90.0, 410.0)
    """
    tstart = min(float(p["header"]["TSTART"]) for p in events)
    tstop = max(float(p["header"]["TSTOP"]) for p in events)
    return tstart - margin, tstop + margin


def download_criteria(product):
    """
    ``KEY=VALUE`` pairs that pick exactly this product and version at the FSC.

    Examples
    --------
    >>> download_criteria(dict(header=dict(OBS_ID=None, BURST_ID=None, PASS_ID="000001_HBX",
    ...                                    PROC_VER=1)))
    ['PASS_ID=000001_HBX', 'PROC_VER=1']
    """
    header = product["header"]
    return [
        f"{key}={header[key]}"
        for key in IDENTIFYING_KEYWORDS + ("PROC_VER",)
        if header.get(key) is not None
    ]


def local_name(product):
    """The archive's own file name, from the end of the product's URL."""
    return os.path.basename(product["url"])


def svom_download(instrument, obsid, url, outdir, config, test=False):
    """
    The ``"download"`` hook: fetch one ObsID's files into ``<outdir>/<obsid>``.

    Files already there are not downloaded again. With ``test``, only the plan is made
    and logged.
    """
    logger = get_logger()
    plan = download_plan(instrument, obsid)
    logger.info(f"{obsid}: {len(plan)} SVOM {instrument} files to download")
    if test:
        return plan
    destination = os.path.join(outdir, obsid)
    for product in plan:
        name = local_name(product)
        if os.path.exists(os.path.join(destination, name)):
            logger.info(f"{obsid}: {name} already downloaded")
            continue
        run_ecpi(
            helper_argv("get", product["acronym"], destination, name, *download_criteria(product))
        )
    return plan


def process_svom_obsid(instrument, obsid, config=None, ra=None, dec=None, flags=None):
    """Placeholder until the reductions land (steps S4 and S5 of the SVOM plan)."""
    raise NotImplementedError(f"The SVOM {instrument} reduction is not written yet.")
