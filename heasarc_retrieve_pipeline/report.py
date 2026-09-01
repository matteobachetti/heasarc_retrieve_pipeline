"""
One interactive page per observation, built from what the reduction recorded.

The pipeline used to leave its evidence scattered: a JPEG next to every event file, a
``skipped_inputs.txt`` in the observation directory, a few ``*_DONE.TXT`` markers, and a
log. After a run of 56 observations that is some 1800 files and no way to see any one
observation whole. This module joins the three records the pipeline keeps --

* ``diagnostics/*.json`` and their ``.npz`` siblings, from
  :mod:`heasarc_retrieve_pipeline.diagnostics`: the status, timing and numbers of every
  step;
* ``skipped_inputs.txt``, from :func:`heasarc_retrieve_pipeline.utils.read_skipped_inputs`:
  which inputs were skipped and why;
* ``.steps/<name>.json``, when they exist: what each step produced

-- into a single ``<OBSID>/diagnostics.html`` that opens in a browser, with plots that
zoom.

Plotly is imported **inside** the functions that need it, never at module scope, so that
importing this package does not require it. Every figure is serialised with
``include_plotlyjs=False`` against one shared ``plotly.min.js`` at the output root: the
bundle is 4.8 MB, and inlining it in 56 pages would be 270 MB of identical bytes.

The pages are opened as ``file://`` URLs, so nothing may be fetched at render time --
browsers block ``fetch()`` against ``file://``. Every number a figure needs is in the page.
"""

import glob
import html
import json
import os
import string
import sys

import numpy as np

from .diagnostics import (
    canonical_metadata,
    diagnostics_path,
    read_arrays,
    read_manifest,
    read_records,
)
from .utils import get_logger, read_skipped_inputs


PLOTLY_BUNDLE = "plotly.min.js"
"""Name of the shared plotly bundle, written once at the output root."""

STATUS_COLOURS = {
    "done": "#2a9d8f",
    "skipped": "#e9c46a",
    "failed": "#e76f51",
    "running": "#8ecae6",
}
"""One colour per step status, used by the timeline and the index."""

STEP_TITLES = {
    "observation": "The observation as a whole",
    "l2_pipeline": "Level 2 pipeline",
    "recover_spacecraft_science": "Spacecraft science recovery",
    "separate_sources": "Source separation",
    "source_region": "Extraction region",
    "source_position": "Source position",
    "join_source_data": "Source join",
    "flare_filtering": "Solar-flare filtering",
    "calculate_spectra": "Spectral extraction",
}
"""Readable names for the steps, for the timeline and the section headings."""


PAGE = string.Template(
    """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>$title</title>
<script src="$bundle"></script>
<style>
body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; margin: 0 auto;
       max-width: 1100px; padding: 1.5rem; color: #222; background: #fff; }
h1 { margin-bottom: 0.2rem; font-size: 1.6rem; }
h2 { margin-top: 2.2rem; font-size: 1.2rem; border-bottom: 1px solid #ddd;
     padding-bottom: 0.2rem; }
h3 { margin-top: 1.4rem; font-size: 1rem; font-weight: 600; color: #444; }
.subtitle { color: #666; margin-top: 0; }
table { border-collapse: collapse; margin: 0.6rem 0; font-size: 0.9rem; }
th, td { text-align: left; padding: 0.25rem 0.8rem 0.25rem 0; vertical-align: top; }
th { color: #666; font-weight: 600; }
tr + tr td, tr + tr th { border-top: 1px solid #eee; }
.status { display: inline-block; padding: 0.05rem 0.45rem; border-radius: 0.6rem;
          color: #fff; font-size: 0.8rem; }
.empty { color: #888; font-style: italic; }
.error { font-family: ui-monospace, monospace; font-size: 0.8rem; white-space: pre-wrap;
         background: #fdf1ee; border-left: 3px solid #e76f51; padding: 0.5rem 0.8rem; }
footer { margin-top: 3rem; color: #888; font-size: 0.8rem; }
</style>
</head>
<body>
$body
<footer>Written by heasarc_retrieve_pipeline.report</footer>
</body>
</html>
"""
)
"""The whole page. ``string.Template`` rather than jinja2: one substitution, no logic."""


def _plotly():
    """
    The plotly modules, imported on use.

    Returns
    -------
    tuple
        ``(graph_objects, plotly.io)``.
    """
    import plotly.graph_objects as go
    import plotly.io as pio

    return go, pio


def _blank(fig, height=420):
    """Strip the theme and the margins from a figure, and give it a sensible size."""
    fig.update_layout(
        template="none",
        height=height,
        margin=dict(l=60, r=20, t=40, b=45),
        plot_bgcolor="#fff",
        paper_bgcolor="#fff",
        font=dict(size=12),
    )
    fig.update_xaxes(showline=True, linecolor="#bbb", ticks="outside", gridcolor="#f0f0f0")
    fig.update_yaxes(showline=True, linecolor="#bbb", ticks="outside", gridcolor="#f0f0f0")
    return fig


def _records_of(records, step):
    """Every record of one step, in the order they were written."""
    return [record for record in records if record.get("step") == step]


def _label(record):
    """``step`` or ``step (key)``, for a figure title or a timeline row."""
    title = STEP_TITLES.get(record.get("step"), record.get("step", "?"))
    key = record.get("key")
    return f"{title} ({key})" if key else title


# --------------------------------------------------------------------------- figures


def timeline_figure(records):
    """
    Every step of the reduction as a bar, in the order it ran, coloured by status.

    The bar's length is how long the step took and its position is when it started,
    relative to the first step, so a step that took an hour is visibly an hour. A run
    killed part way through leaves its last step as ``running``, which is how the page
    names the step the reduction died in.

    Parameters
    ----------
    records : list of dict
        As returned by :func:`heasarc_retrieve_pipeline.diagnostics.read_records`.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    go, _ = _plotly()

    fig = go.Figure()
    if not records:
        return _blank(fig, height=120)

    origin = min(record.get("started") or 0.0 for record in records)
    for status in ("done", "skipped", "failed", "running"):
        rows = [record for record in records if record.get("status") == status]
        if not rows:
            continue
        fig.add_trace(
            go.Bar(
                y=[_label(record) for record in rows],
                # A step that took no measurable time still has to be visible.
                x=[max(record.get("duration_s") or 0.0, 0.5) for record in rows],
                base=[(record.get("started") or origin) - origin for record in rows],
                orientation="h",
                name=status,
                marker_color=STATUS_COLOURS.get(status, "#999"),
                hovertext=[
                    f"{_label(record)}<br>{status}"
                    + (f"<br>{record['reason']}" if record.get("reason") else "")
                    + (f"<br>{record['error']}" if record.get("error") else "")
                    + f"<br>{record.get('duration_s') or 0.0:.1f} s"
                    for record in rows
                ],
                hoverinfo="text",
            )
        )

    fig.update_layout(
        barmode="overlay",
        xaxis_title="seconds since the first step started",
        legend=dict(orientation="h", y=1.12, x=0),
    )
    fig.update_yaxes(autorange="reversed", automargin=True)
    return _blank(fig, height=max(180, 34 * len(records) + 90))


def separation_figure(record, arrays):
    """
    The sky image the source separation worked on, with the peaks it found.

    The image's horizontal axis is sky ``Y`` and its vertical axis is sky ``X``. That is
    not a mistake in this function: ``image_from_table`` histograms ``(Y, X)`` and
    transposes, and the peak coordinates follow the same convention -- see the notes in
    :func:`heasarc_retrieve_pipeline.image_utils.image_from_table`.

    Peaks above the acceptance threshold are drawn as filled circles and those below it as
    open ones, so a source that was found and then rejected is visible as such.

    Parameters
    ----------
    record : dict
        A ``separate_sources`` record.
    arrays : dict of numpy.ndarray
        Its array payload.

    Returns
    -------
    plotly.graph_objects.Figure or None
        ``None`` if the record has no image -- a file with too few events, for instance.
    """
    go, _ = _plotly()

    if not arrays or "image" not in arrays:
        return None

    # The image is smoothed counts read off a colour bar. Two decimals is more than the
    # eye can use, and single precision is more than two decimals needs -- plotly encodes
    # a float64 array as twice the base64 of a float32 one, and a real observation puts
    # sixteen of these on one page.
    image = np.round(np.asarray(arrays["image"], dtype=float), 2).astype(np.float32)
    x = _centres(arrays.get("image_x"), image.shape[1])
    y = _centres(arrays.get("image_y"), image.shape[0])

    fig = go.Figure(
        go.Heatmap(
            z=image,
            x=x,
            y=y,
            colorscale="Cividis",
            zmin=float(np.median(image)),
            colorbar=dict(title="counts/bin", thickness=12),
            hovertemplate="Y %{x:.0f}, X %{y:.0f}: %{z:.1f}<extra></extra>",
        )
    )

    peaks = np.asarray(arrays.get("peaks", np.zeros((0, 2))), dtype=float)
    fluxes = np.asarray(arrays.get("peak_fluxes", np.zeros(len(peaks))), dtype=float)
    threshold = (record.get("values") or {}).get("acceptance_threshold")
    if len(peaks):
        accepted = (
            fluxes >= threshold if threshold is not None else np.ones(len(peaks), bool)
        )
        for keep, name, symbol in (
            (accepted, "accepted", "circle"),
            (~accepted, "below the threshold", "circle-open"),
        ):
            if not np.any(keep):
                continue
            fig.add_trace(
                go.Scatter(
                    x=peaks[keep, 1],
                    y=peaks[keep, 0],
                    mode="markers",
                    name=name,
                    marker=dict(symbol=symbol, size=11, color="#e76f51",
                                line=dict(width=2, color="#e76f51")),
                    hovertext=[
                        f"X {peak[0]:.0f}, Y {peak[1]:.0f}<br>{flux:.0f} counts"
                        for peak, flux in zip(peaks[keep], fluxes[keep])
                    ],
                    hoverinfo="text",
                )
            )

    fig.update_layout(
        xaxis_title="sky Y (pixels)",
        yaxis_title="sky X (pixels)",
        legend=dict(orientation="h", y=1.1, x=0),
    )
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    return _blank(fig, height=520)


def radial_profile_figure(record, arrays):
    """
    The radial profile the extraction radius was chosen from, with the PSF for comparison.

    The chosen radius is marked. A profile that follows the PSF is a point source; one that
    does not is either extended or not the source the observation was pointed at.

    Parameters
    ----------
    record : dict
        A ``source_region`` record.
    arrays : dict of numpy.ndarray
        Its array payload.

    Returns
    -------
    plotly.graph_objects.Figure or None
        ``None`` if no profile was recorded -- a region read back from disk on a rerun has
        none, because nothing was measured.
    """
    go, _ = _plotly()

    if not arrays or "radius" not in arrays:
        return None

    radius = np.asarray(arrays["radius"], dtype=float)
    profile = np.asarray(arrays["profile"], dtype=float)
    error = np.asarray(arrays.get("profile_error", np.zeros_like(profile)), dtype=float)
    psf = np.asarray(arrays.get("psf_profile", np.zeros_like(profile)), dtype=float)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=radius,
            y=profile,
            error_y=dict(array=error, thickness=1, width=0),
            mode="markers+lines",
            name="measured",
            marker=dict(size=5, color="#264653"),
            line=dict(width=1, color="#264653"),
        )
    )
    if np.any(psf):
        fig.add_trace(
            go.Scatter(x=radius, y=psf, mode="lines", name="expected PSF",
                       line=dict(width=1.5, color="#e9c46a", dash="dash"))
        )

    values = record.get("values") or {}
    rlimit = values.get("rlimit", values.get("rlimit_snr"))
    if rlimit is not None:
        fig.add_vline(
            x=float(rlimit),
            line=dict(color="#e76f51", width=1.5),
            annotation_text=f"{float(rlimit):.0f}″",
            annotation_position="top right",
        )

    fig.update_layout(xaxis_title="radius (arcsec)", yaxis_title="surface brightness",
                      legend=dict(orientation="h", y=1.12, x=0))
    fig.update_yaxes(type="log")
    return _blank(fig)


def gti_figure(record, arrays):
    """
    What was joined into what, as one row of intervals per file.

    One row per input file's good time intervals, then the OR-merged row for each module,
    then the AND-merged row for the pair. A module whose intervals do not overlap the
    other's produces a combined row that is visibly shorter than either.

    Parameters
    ----------
    record : dict
        A ``join_source_data`` record.
    arrays : dict of numpy.ndarray
        Its array payload.

    Returns
    -------
    plotly.graph_objects.Figure or None
        ``None`` if no intervals were recorded.
    """
    go, _ = _plotly()

    if not arrays:
        return None

    values = record.get("values") or {}
    rows = []
    for fpm in ("A", "B"):
        names = values.get(f"inputs_{fpm}") or []
        for i in range(len(names)):
            key = f"gti_{fpm}_in_{i}"
            if key in arrays:
                rows.append((names[i], arrays[key], "#8ecae6"))
        if f"gti_{fpm}_out" in arrays:
            rows.append((f"FPM{fpm} merged (OR)", arrays[f"gti_{fpm}_out"], "#219ebc"))
    if "gti_combined" in arrays:
        rows.append((values.get("combined", "combined (AND)"),
                     arrays["gti_combined"], "#023047"))

    rows = [(name, np.atleast_2d(gti), colour) for name, gti, colour in rows]
    rows = [row for row in rows if row[1].size]
    if not rows:
        return None

    origin = min(float(np.min(gti[:, 0])) for _, gti, _ in rows)

    # One bar per good time interval, and a real observation has a few thousand of them
    # per row. The row is identified by its number and named once, in the tick labels:
    # repeating a thirty-character file name once per interval, as a categorical y axis
    # would, was three quarters of a megabyte on the first page this drew.
    fig = go.Figure()
    for index, (name, gti, colour) in enumerate(rows):
        fig.add_trace(
            go.Bar(
                y=np.full(len(gti), index, dtype=np.int16),
                x=np.round(gti[:, 1] - gti[:, 0], 3).astype(np.float32),
                base=np.round(gti[:, 0] - origin, 3),
                orientation="h",
                marker_color=colour,
                showlegend=False,
                hovertemplate=f"{name}<br>%{{x:.0f}} s<extra></extra>",
            )
        )

    fig.update_layout(barmode="overlay",
                      xaxis_title="seconds since the earliest good time interval")
    fig.update_yaxes(
        tickmode="array",
        tickvals=list(range(len(rows))),
        ticktext=[name for name, _, _ in rows],
        autorange="reversed",
        automargin=True,
    )
    return _blank(fig, height=max(200, 30 * len(rows) + 90))


def flare_figure(record, arrays):
    """
    The three panels of the solar-flare filtering, on one shared time axis.

    The GOES X-ray flux on top, then the 3--10 keV light curve where solar stray light
    lands, then 10--79 keV as the control: flares do not produce hard X-rays at NuSTAR's
    aperture, so the bottom panel should look the same before and after. The removed
    intervals are shaded across all three.

    Parameters
    ----------
    record : dict
        A ``flare_filtering`` record.
    arrays : dict of numpy.ndarray
        Its array payload.

    Returns
    -------
    plotly.graph_objects.Figure or None
        ``None`` if no light curves were recorded.
    """
    go, _ = _plotly()
    from plotly.subplots import make_subplots

    if not arrays or not any(key.startswith("lc_") for key in arrays):
        return None

    bands = [("3_10", "3–10 keV (solar stray light)"),
             ("10_79", "10–79 keV (control)")]
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                        row_heights=[0.26, 0.37, 0.37])

    if "goes_time" in arrays:
        for column, colour, name in (("goes_xrsb", "#e76f51", "GOES 1–8 Å"),
                                     ("goes_xrsa", "#264653", "GOES 0.5–4 Å")):
            if column in arrays:
                fig.add_trace(
                    go.Scatter(x=arrays["goes_time"], y=arrays[column], mode="lines",
                               name=name, line=dict(width=1, color=colour)),
                    row=1, col=1,
                )
        fig.update_yaxes(type="log", title_text="W m<sup>-2</sup>", row=1, col=1)

    values = record.get("values") or {}
    for row, (band, title) in enumerate(bands, start=2):
        for when, colour, name in (("before", "#aaa", "before"),
                                   ("after", "#2a9d8f", "after")):
            time = arrays.get(f"lc_{band}_{when}_time")
            if time is None:
                continue
            fig.add_trace(
                go.Scatter(
                    x=time,
                    y=arrays[f"lc_{band}_{when}_rate"],
                    error_y=dict(array=arrays.get(f"lc_{band}_{when}_rate_err"),
                                 thickness=0.7, width=0),
                    mode="markers",
                    name=f"{title} {name}",
                    marker=dict(size=4, color=colour),
                ),
                row=row, col=1,
            )
        chi2 = values.get(f"chi2_dof_{band}")
        label = f"{title} rate (s<sup>-1</sup>)"
        if chi2:
            label += f"<br>χ²/dof {chi2[0]:.2f} → {chi2[1]:.2f}"
        fig.update_yaxes(title_text=label, title_font_size=10, row=row, col=1)

    removed = np.atleast_2d(np.asarray(arrays.get("removed", np.zeros((0, 2))), float))
    for start, stop in removed.reshape(-1, 2):
        fig.add_vrect(x0=start, x1=stop, fillcolor="#e76f51", opacity=0.16,
                      line_width=0, layer="below")

    fig.update_xaxes(title_text="mission elapsed time (s)", row=3, col=1)
    fig.update_layout(legend=dict(orientation="h", y=1.06, x=0, font=dict(size=10)))
    return _blank(fig, height=780)


def _centres(edges, n):
    """
    Bin centres for ``n`` bins, from ``n + 1`` edges.

    Returns a plain index when there are no edges, or when there are not enough of them:
    a picture with the wrong axis numbers is worse than one with none.
    """
    if edges is None:
        return np.arange(n)
    edges = np.asarray(edges, dtype=float)
    if edges.size == n + 1:
        return (edges[:-1] + edges[1:]) / 2.0
    if edges.size == n:
        return edges
    return np.arange(n)


# ----------------------------------------------------------------------------- HTML


def _table(rows, header=None):
    """A small HTML table from ``(name, value)`` pairs. Everything is escaped."""
    if not rows:
        return '<p class="empty">nothing recorded</p>'
    out = ["<table>"]
    if header:
        out.append("<tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in header) + "</tr>")
    for row in rows:
        out.append("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in row) + "</tr>")
    out.append("</table>")
    return "\n".join(out)


def _badge(status):
    """A coloured status pill."""
    colour = STATUS_COLOURS.get(status, "#999")
    return f'<span class="status" style="background:{colour}">{html.escape(str(status))}</span>'


def _figure_html(fig):
    """
    One figure as a ``<div>``, with no library of its own.

    ``include_plotlyjs=False`` for every figure: the bundle is loaded once by the page's
    own ``<script src>``, whose relative path this module computes. Plotly's own
    ``"directory"`` mode emits a bare ``src="plotly.min.js"`` and copies nothing, which on
    a page one directory below the bundle points at a file that does not exist.
    """
    _, pio = _plotly()
    return pio.to_html(fig, full_html=False, include_plotlyjs=False,
                       config=dict(displaylogo=False, responsive=True))


def read_step_stamps(obsid, outdir):
    """
    The completion-model step stamps of an observation, when there are any.

    These are written only on success, by a design that is not implemented yet
    (``docs/completion_model_plan.md``); this reads whatever is there and says nothing
    when there is nothing, so the page renders the same either way.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    outdir : str
        Run output directory.

    Returns
    -------
    list of (str, dict)
        ``(name, stamp)`` pairs, sorted by name.
    """
    stamps = []
    for path in sorted(glob.glob(os.path.join(outdir, obsid, ".steps", "*.json"))):
        try:
            with open(path) as fobj:
                stamps.append((os.path.basename(path)[: -len(".json")], json.load(fobj)))
        except (OSError, ValueError) as error:
            get_logger().warning(f"Ignoring unreadable step stamp {path}: {error}")
    return stamps


def observation_summary(obsid, outdir):
    """
    Everything the run recorded about one observation, joined.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    outdir : str
        Run output directory -- the one that holds ``<OBSID>/``.

    Returns
    -------
    dict
        ``obsid``, ``manifest``, ``metadata``, ``records``, ``skipped``, ``stamps`` and
        ``outcome``. Every one of them is empty rather than absent when the observation
        recorded nothing, so a page can always be built.
    """
    config = dict(out_data_path=outdir)
    directory = diagnostics_path(obsid, config)
    manifest = read_manifest(directory) or {}
    records = read_records(directory)

    return dict(
        obsid=obsid,
        manifest=manifest,
        metadata=canonical_metadata(manifest.get("catalogue") or {}),
        records=records,
        skipped=read_skipped_inputs(obsid, config),
        stamps=read_step_stamps(obsid, outdir),
        outcome=outcome_of(records),
    )


def outcome_of(records):
    """
    One word for how an observation went.

    ``failed`` if any step failed, ``running`` if any step never finished -- which is what
    a killed run leaves behind -- ``done`` if anything finished, and ``no records`` if the
    observation never started.

    Parameters
    ----------
    records : list of dict

    Returns
    -------
    str
    """
    statuses = {record.get("status") for record in records}
    if not statuses:
        return "no records"
    for status in ("failed", "running"):
        if status in statuses:
            return status
    return "done"


def _parameter_rows(summary):
    """The observation's own parameters, from the manifest, in a fixed order."""
    manifest = summary["manifest"]
    metadata = summary["metadata"]
    rows = [("OBSID", summary["obsid"])]
    for label, value in (
        ("Target", metadata.get("source_name")),
        ("Mission", manifest.get("mission")),
        ("RA, Dec (deg)", _position(manifest)),
        ("Exposure (s)", metadata.get("exposure")),
        ("Observation date", metadata.get("date")),
        ("Public date", metadata.get("public_date")),
        ("Cycle", metadata.get("cycle")),
        ("Downloaded from", manifest.get("url")),
        ("Recorded at", manifest.get("written_iso")),
    ):
        if value not in (None, ""):
            rows.append((label, value))
    return rows


def _position(manifest):
    """``ra, dec`` as one cell, or None when the manifest has no position."""
    ra, dec = manifest.get("ra"), manifest.get("dec")
    if ra is None or dec is None:
        return None
    return f"{float(ra):.5f}, {float(dec):.5f}"


def _step_rows(records):
    """One row per step for the summary table under the timeline."""
    rows = []
    for record in records:
        detail = record.get("reason") or record.get("error") or ""
        duration = record.get("duration_s")
        rows.append(
            (
                _label(record),
                record.get("status", "?"),
                "" if duration is None else f"{duration:.1f}",
                detail,
            )
        )
    return rows


def _step_table(records):
    """The step table, with the status column rendered as a coloured pill."""
    if not records:
        return '<p class="empty">this observation recorded no steps</p>'
    out = ["<table>", "<tr><th>step</th><th>status</th><th>seconds</th><th></th></tr>"]
    for name, status, duration, detail in _step_rows(records):
        out.append(
            f"<tr><td>{html.escape(name)}</td><td>{_badge(status)}</td>"
            f"<td>{html.escape(duration)}</td><td>{html.escape(detail)}</td></tr>"
        )
    out.append("</table>")
    return "\n".join(out)


def observation_body(summary, directory):
    """
    The body of one observation's page.

    Parameters
    ----------
    summary : dict
        From :func:`observation_summary`.
    directory : str
        The observation's diagnostics directory, for reading the array payloads.

    Returns
    -------
    str
        HTML. Every figure that could not be built is simply absent; nothing raises.
    """
    obsid = summary["obsid"]
    records = summary["records"]
    metadata = summary["metadata"]

    title = metadata.get("source_name") or summary["manifest"].get("mission") or ""
    parts = [
        f"<h1>{html.escape(str(obsid))}</h1>",
        f'<p class="subtitle">{html.escape(str(title))} &mdash; '
        f"{_badge(summary['outcome'])}</p>",
        "<h2>Observation</h2>",
        _table(_parameter_rows(summary)),
        "<h2>Steps</h2>",
    ]

    figures = []
    if records:
        figures.append(timeline_figure(records))
    parts.append("$FIGURE" if records else "")
    parts.append(_step_table(records))

    for record in records:
        if record.get("status") == "failed" and record.get("traceback"):
            parts.append(f"<h3>{html.escape(_label(record))} failed</h3>")
            parts.append(f'<pre class="error">{html.escape(record["traceback"])}</pre>')

    sections = (
        ("Source separation", "separate_sources", separation_figure),
        ("Extraction regions", "source_region", radial_profile_figure),
        ("Joining", "join_source_data", gti_figure),
        ("Solar-flare filtering", "flare_filtering", flare_figure),
    )
    for heading, step, builder in sections:
        drawn = []
        for record in _records_of(records, step):
            fig = _safe(builder, record, read_arrays(directory, record))
            if fig is not None:
                drawn.append((record, fig))
        if not drawn:
            continue
        parts.append(f"<h2>{heading}</h2>")
        for record, fig in drawn:
            parts.append(f"<h3>{html.escape(record.get('key') or _label(record))}</h3>")
            parts.append("$FIGURE")
            figures.append(fig)

    parts.append("<h2>Skipped inputs</h2>")
    parts.append(
        _table([(item, reason) for item, reason in summary["skipped"]],
               header=("input", "why it was skipped"))
        if summary["skipped"]
        else '<p class="empty">nothing was skipped</p>'
    )

    if summary["stamps"]:
        parts.append("<h2>What each step produced</h2>")
        parts.append(
            _table(
                [(name, ", ".join(map(str, stamp.get("outputs", []))) or "")
                 for name, stamp in summary["stamps"]],
                header=("step", "outputs"),
            )
        )

    body = "\n".join(part for part in parts if part)
    return _splice(body, figures)


def _splice(body, figures):
    """Replace each ``$FIGURE`` placeholder, in order, with a serialised figure."""
    out = []
    rest = body
    for fig in figures:
        head, _, rest = rest.partition("$FIGURE")
        out.append(head)
        out.append(_figure_html(fig))
    out.append(rest)
    return "".join(out)


def _safe(builder, *args):
    """Build a figure, or log and return ``None``. A picture must not lose a page."""
    try:
        return builder(*args)
    except Exception as error:
        get_logger().warning(f"Could not build the {builder.__name__} figure: {error}")
        return None



def run_timeline_figure(summaries):
    """
    When each observation ran and how long it took, coloured by how it ended.

    The whole run in one picture: how much of it was actually parallel, which observations
    took the time, and where the failures fell. It is drawn from the ``observation``
    record, so an observation that never started simply has no bar.

    Parameters
    ----------
    summaries : list of dict
        From :func:`observation_summary`.

    Returns
    -------
    plotly.graph_objects.Figure or None
        ``None`` if no observation recorded when it ran.
    """
    go, _ = _plotly()

    rows = []
    for summary in summaries:
        for record in _records_of(summary["records"], "observation"):
            if record.get("started") is not None:
                rows.append((summary["obsid"], record))
    if not rows:
        return None

    origin = min(record["started"] for _, record in rows)
    fig = go.Figure()
    for status in ("done", "skipped", "failed", "running"):
        chosen = [(obsid, r) for obsid, r in rows if r.get("status") == status]
        if not chosen:
            continue
        fig.add_trace(
            go.Bar(
                y=[obsid for obsid, _ in chosen],
                x=[max(r.get("duration_s") or 0.0, 1.0) for _, r in chosen],
                base=[r["started"] - origin for _, r in chosen],
                orientation="h",
                name=status,
                marker_color=STATUS_COLOURS.get(status, "#999"),
                hovertemplate="%{y}<br>%{x:.0f} s<extra></extra>",
            )
        )

    fig.update_layout(barmode="overlay",
                      xaxis_title="seconds since the first observation started",
                      legend=dict(orientation="h", y=1.06, x=0))
    # An OBSID is a number as far as a plotting library is concerned, and 30702012004 on a
    # numeric axis comes out labelled 30.702012004B. It is a name.
    fig.update_yaxes(type="category", autorange="reversed", automargin=True)
    return _blank(fig, height=max(220, 26 * len(rows) + 100))


def _index_rows(summaries):
    """One row per observation for the index table, in the order given."""
    rows = []
    for summary in summaries:
        records = summary["records"]
        statuses = [record.get("status") for record in records]
        observation = _records_of(records, "observation")
        rows.append(
            dict(
                obsid=summary["obsid"],
                target=summary["metadata"].get("source_name") or "",
                outcome=summary["outcome"],
                reason=(observation[0].get("reason") or observation[0].get("error") or "")
                if observation
                else "",
                exposure=summary["metadata"].get("exposure"),
                duration=observation[0].get("duration_s") if observation else None,
                steps=len(records),
                skipped_steps=statuses.count("skipped"),
                failed_steps=statuses.count("failed"),
                skipped_inputs=len(summary["skipped"]),
            )
        )
    return rows


def index_body(summaries):
    """
    The body of the run index.

    Parameters
    ----------
    summaries : list of dict
        From :func:`observation_summary`, in the order they should be listed.

    Returns
    -------
    str
        HTML, with the figure already spliced in.
    """
    rows = _index_rows(summaries)
    counts = {}
    for row in rows:
        counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1
    tally = ", ".join(f"{n} {name}" for name, n in sorted(counts.items()))

    parts = [
        "<h1>Reduction run</h1>",
        f'<p class="subtitle">{len(rows)} observation(s): {html.escape(tally)}</p>',
    ]

    figures = []
    fig = _safe(run_timeline_figure, summaries)
    if fig is not None:
        parts.append("$FIGURE")
        figures.append(fig)

    header = ("OBSID", "target", "outcome", "exposure (s)", "took (s)", "steps",
              "skipped steps", "failed steps", "skipped inputs", "")
    out = ["<h2>Observations</h2>", "<table>",
           "<tr>" + "".join(f"<th>{html.escape(name)}</th>" for name in header) + "</tr>"]
    for row in rows:
        link = f"{row['obsid']}/diagnostics.html"
        out.append(
            "<tr>"
            f'<td><a href="{html.escape(link)}">{html.escape(row["obsid"])}</a></td>'
            f"<td>{html.escape(str(row['target']))}</td>"
            f"<td>{_badge(row['outcome'])}</td>"
            f"<td>{_number(row['exposure'], 0)}</td>"
            f"<td>{_number(row['duration'], 1)}</td>"
            f"<td>{row['steps']}</td>"
            f"<td>{row['skipped_steps']}</td>"
            f"<td>{row['failed_steps']}</td>"
            f"<td>{row['skipped_inputs']}</td>"
            f"<td>{html.escape(str(row['reason'])[:80])}</td>"
            "</tr>"
        )
    out.append("</table>")
    parts.extend(out)

    return _splice("\n".join(parts), figures)


def _number(value, digits):
    """A number for a table cell, or an empty cell when there is none."""
    if value is None:
        return ""
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return html.escape(str(value))


def write_index(outdir, obsids=None):
    """
    Write ``<outdir>/index.html``, one row per observation, linking every page.

    Built from whatever is on disk, so it works on a run that was killed: an observation
    that has a manifest and nothing else is listed with no records, which is itself the
    statement that it never started.

    Parameters
    ----------
    outdir : str
        Run output directory.
    obsids : list of str, optional
        Which observations to list, in order. Defaults to every observation directory
        found under ``outdir``.

    Returns
    -------
    str
        The path written.
    """
    if obsids is None:
        obsids = observation_directories(outdir)
    summaries = [observation_summary(obsid, outdir) for obsid in obsids]

    path = os.path.join(outdir, "index.html")
    os.makedirs(outdir, exist_ok=True)
    _write(path, PAGE.substitute(title="Reduction run", bundle=PLOTLY_BUNDLE,
                                 body=index_body(summaries)))
    return path


def write_observation_page(obsid, outdir):
    """
    Write ``<outdir>/<OBSID>/diagnostics.html``.

    Safe to call on an observation that recorded nothing: the page is then its OBSID and
    an empty step table, which is itself the useful statement that nothing ran.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    outdir : str
        Run output directory.

    Returns
    -------
    str
        The path written.
    """
    summary = observation_summary(obsid, outdir)
    directory = diagnostics_path(obsid, dict(out_data_path=outdir))
    body = observation_body(summary, directory)

    path = os.path.join(outdir, obsid, "diagnostics.html")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # The bundle lives at the run root, one level up from the observation directory.
    page = PAGE.substitute(title=f"{obsid} diagnostics",
                           bundle=os.path.join("..", PLOTLY_BUNDLE),
                           body=body)
    _write(path, page)
    return path


def write_plotly_bundle(outdir):
    """
    Write the shared ``plotly.min.js`` at the run root, once.

    Every page loads this one file. It is 4.8 MB; inlining it in each page of a 56
    observation run would be 270 MB of identical bytes, and the pages are opened from
    disk, where nothing can be fetched from a CDN.

    Parameters
    ----------
    outdir : str
        Run output directory.

    Returns
    -------
    str
        The path written.
    """
    from plotly.offline import get_plotlyjs

    path = os.path.join(outdir, PLOTLY_BUNDLE)
    os.makedirs(outdir, exist_ok=True)
    _write(path, get_plotlyjs())
    return path


def _write(path, text):
    """Write a file whole or not at all, as ``record_skipped_input`` does."""
    import tempfile

    directory = os.path.dirname(path) or "."
    handle, temporary = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(handle, "w") as fobj:
            fobj.write(text)
        os.replace(temporary, path)
    except BaseException:
        if os.path.exists(temporary):
            os.unlink(temporary)
        raise


def main(argv=None):
    """
    ``python -m heasarc_retrieve_pipeline.report <outdir>`` -- rebuild the pages.

    Rebuilds from whatever records are on disk, which is the difference between a crashed
    run leaving nothing to look at and one you can browse.

    Parameters
    ----------
    argv : list of str, optional
        Defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        Process exit status.
    """
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print(__doc__.strip().splitlines()[0])
        print("usage: hrp-report <output directory>")
        return 2

    outdir = os.path.abspath(argv[0])
    obsids = observation_directories(outdir)
    for obsid in obsids:
        write_observation_page(obsid, outdir)
    index = write_index(outdir, obsids)
    write_plotly_bundle(outdir)
    print(f"{len(obsids)} observation page(s); open {index}")
    return 0


def observation_directories(outdir):
    """
    Every observation directory under a run root, in order.

    An observation is any subdirectory holding a ``diagnostics`` directory or a
    ``skipped_inputs.txt``: enough for there to be something to say about it.

    Parameters
    ----------
    outdir : str
        Run output directory.

    Returns
    -------
    list of str
        OBSIDs.
    """
    obsids = []
    for name in sorted(os.listdir(outdir)) if os.path.isdir(outdir) else []:
        path = os.path.join(outdir, name)
        if not os.path.isdir(path):
            continue
        if os.path.isdir(os.path.join(path, "diagnostics")) or os.path.exists(
            os.path.join(path, "skipped_inputs.txt")
        ):
            obsids.append(name)
    return obsids


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
