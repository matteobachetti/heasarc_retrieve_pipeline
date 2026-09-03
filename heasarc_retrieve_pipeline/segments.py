"""
Cutting a finished observation into time segments.

A source changes state part-way through an observation and you want the two halves fitted
separately. This module does that on a tree the pipeline has already reduced: nothing here
re-runs ``nupipeline``, and nothing regenerates a response.

Two kinds of product get split, by two different routes.

Spectra
    ``nuproducts`` already takes a ``usrgtifile``, which is how
    :func:`~heasarc_retrieve_pipeline.nustar.calculate_spectra` applies the flare-free
    GTI. Handing it a *segment's* GTI instead is the whole of the spectral split. Run with
    ``runmkarf=no runmkrmf=no`` it skips the slow part -- response generation, which is
    what makes ``nuproducts`` take minutes -- while HEASOFT still does the region
    filtering, the background spectrum, ``BACKSCAL``, the exposure and the GTI extension.
    The segments are then consistent with the unsplit products by construction rather than
    by this module reimplementing the region and grade filters. The segment spectra point
    at the parent's existing ``.arf`` and ``.rmf``.

Event lists
    Pure astropy. :func:`~heasarc_retrieve_pipeline.utils.apply_gti` already drops the
    events, rewrites the GTI extension and rescales ``ONTIME``/``LIVETIME``/``EXPOSURE``;
    :func:`~heasarc_retrieve_pipeline.utils.update_time_bounds` corrects what the file
    says about *when* it was taken.

Everything is written into the parent observation's own tree with a ``_seg<N>`` suffix::

    <OBSID>/products/nu<OBSID>A01_sr_seg1.pha     source spectrum
    <OBSID>/products/nu<OBSID>A01_bk_seg1.pha     background spectrum
    <OBSID>/products/nu<OBSID>A01_grp_seg1.pha    grouped, the one you fit
    <OBSID>/nu<OBSID>A_src1_bary_seg1.evt         event list

That is not tidiness, it is what makes the responses free: ``RESPFILE``, ``ANCRFILE`` and
``BACKFILE`` are bare file names resolved relative to the spectrum's own directory, so a
segment sitting next to its parent can point at the parent's 68 MB ``.rmf`` with no copy,
no symbolic link and no path rewriting.

``N`` split times give ``N + 1`` segments, numbered in time order. The numbering never
shifts: a split time outside the observation leaves an empty segment rather than
renumbering the others, because ``seg2`` has to mean the same stretch of the observation
in every run.

Command line::

    hrp-split-obsid <out_data_path> <OBSID> <MJD> [<MJD> ...]

See ``docs/technical_details.rst`` for the scientific rationale, and
``docs/known_issues.rst`` for known defects.
"""

import argparse
import glob
import os
import re
import sys

import numpy as np
from astropy.io import fits

from . import heasoft
from .diagnostics import diagnostics_path, record_step
from .nustar import (
    nu_base_output_path,
    nu_pipeline_output_path,
    nu_product_output_path,
    spectral_input_files,
)
from .utils import (
    apply_gti,
    get_logger,
    gti_extension_index,
    gti_to_array,
    intersect_intervals,
    met_from_mjd,
    mjd_from_met,
    read_gti,
    record_skipped_input,
    rootname,
    segment_bounds,
    splitext_improved,
    time_system,
    update_time_bounds,
)

__all__ = [
    "event_files_to_split",
    "insert_tag",
    "main",
    "reference_file",
    "resolve_split_times",
    "segment_tag",
    "split_event_files",
    "split_obsid",
    "split_spectra",
    "write_gti_file",
]


#: Matches the suffix :func:`segment_tag` adds, so that a rerun does not split its own
#: output into segments of segments.
SEGMENT_RE = re.compile(r"_seg\d+$")


def segment_tag(number):
    """
    The suffix identifying a segment.

    Parameters
    ----------
    number : int
        One-based segment number.

    Returns
    -------
    str

    Examples
    --------
    >>> segment_tag(1)
    'seg1'
    """
    return f"seg{number}"


def insert_tag(path, tag):
    """
    Add a suffix to a file name, before its extension.

    Compression suffixes are handled by
    :func:`~heasarc_retrieve_pipeline.utils.splitext_improved`, so a ``.evt.gz`` keeps
    both parts.

    Parameters
    ----------
    path : str
        File name or path.
    tag : str
        Suffix to insert, without the leading underscore.

    Returns
    -------
    str

    Examples
    --------
    >>> insert_tag("products/nu80002092006A01_sr.pha", "seg1")
    'products/nu80002092006A01_sr_seg1.pha'
    >>> insert_tag("nu80002092006A_src1_bary.evt.gz", "seg2")
    'nu80002092006A_src1_bary_seg2.evt.gz'
    """
    root, extension = splitext_improved(path)
    return f"{root}_{tag}{extension}"


def reference_file(obsid, config):
    """
    The event file whose time keywords define an observation's MJD conversion.

    The first file :func:`~heasarc_retrieve_pipeline.nustar.spectral_input_files` yields,
    which is a mode-01 file whenever there is one.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    str
        Path of the event file.

    Raises
    ------
    ValueError
        If the observation has no cleaned event file.
    """
    for _, infile in spectral_input_files(obsid, config):
        return infile
    raise ValueError(f"{obsid} has no cleaned event file to take a time reference from")


def resolve_split_times(obsid, config, split_mjds, scale=None):
    """
    Turn the MJDs the user asked for into mission elapsed times.

    Also returns what those times are in civil time, so that a wrong ``scale`` is visible
    in the log before anything is written -- NuSTAR's ``TIMESYS`` is ``TT``, and MJD 56689
    TT is 67.184 s earlier than MJD 56689 UTC.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    split_mjds : list of float
        Where to cut.
    scale : str, optional
        Time scale the MJDs are given in. ``None``, the default, means the file's own
        ``TIMESYS``; pass ``"utc"`` for a date read off something labelled in civil time.

    Returns
    -------
    mets : list of float
        Mission elapsed times, on the ``TIME + TIMEZERO`` scale.
    utc : list of float
        The same instants as UTC MJDs, for reporting.
    timesys : str
        The file's own time scale, for reporting.
    """
    with fits.open(reference_file(obsid, config)) as hdul:
        mets = [float(met_from_mjd(mjd, hdul, scale=scale)) for mjd in split_mjds]
        utc = [float(mjd_from_met(met, hdul, scale="utc")) for met in mets]
        timesys = time_system(hdul)
    return mets, utc, timesys


def write_gti_file(template, outfile, gti):
    """
    Write a GTI file holding ``gti``, borrowing every keyword from an existing one.

    Copying a real GTI file rather than building one from scratch is what keeps
    ``MJDREFI``, ``TIMESYS``, ``TIMEUNIT`` and everything else ``nuproducts`` reads
    exactly as HEASOFT wrote them. Only the rows change.

    ``gti`` is on the ``TIME + TIMEZERO`` scale, as
    :func:`~heasarc_retrieve_pipeline.utils.read_gti` reports it; the template extension's
    ``TIMEZERO`` is subtracted back out before writing, so the file reads the same way it
    came in.

    Parameters
    ----------
    template : str
        An existing GTI file, or an event file, to take the header from.
    outfile : str
        Where to write. Overwritten if it exists.
    gti : array-like or table
        The intervals to write.

    Returns
    -------
    numpy.ndarray
        The intervals written, shape ``(N, 2)``.
    """
    gti = gti_to_array(gti)
    with fits.open(template) as hdul:
        index = gti_extension_index(hdul)
        source = hdul[index]
        timezero = float(source.header.get("TIMEZERO", 0.0))
        replacement = fits.BinTableHDU.from_columns(
            [
                fits.Column(name="START", format="D", array=gti[:, 0] - timezero),
                fits.Column(name="STOP", format="D", array=gti[:, 1] - timezero),
            ],
            header=source.header,
        )
        out = fits.HDUList([fits.PrimaryHDU(header=hdul[0].header), replacement])
        out.writeto(outfile, overwrite=True)
    return gti


def _source_gti(infile):
    """
    The GTI a segment of ``infile`` is cut out of, and the file to take a header from.

    The parent run left ``<root>_noflares.gti`` next to each event file -- the file's own
    intervals with the solar flares already taken out, which is what ``nuproducts`` was
    given and therefore what the segments have to be carved from. Falling back to the
    event file's own GTI would silently put the flares back in.
    """
    noflares = os.path.join(
        os.path.dirname(infile), rootname(os.path.basename(infile)) + "_noflares.gti"
    )
    if os.path.exists(noflares):
        with fits.open(noflares) as hdul:
            index = gti_extension_index(hdul)
            timezero = float(hdul[index].header.get("TIMEZERO", 0.0))
            return gti_to_array(hdul[index].data) + timezero, noflares
    with fits.open(infile) as hdul:
        return read_gti(hdul), infile


def split_spectra(obsid, config, bounds, rec=None):
    """
    Re-extract each of an observation's spectra over each segment.

    One ``nuproducts`` call per input event file per non-empty segment, with the segment's
    GTI as ``usrgtifile`` and the responses turned off. The parent's ``.arf`` and ``.rmf``
    are then named in the output's ``RESPFILE`` and ``ANCRFILE``.

    The extraction regions are the parent's, looked up next to each event file exactly as
    :func:`~heasarc_retrieve_pipeline.nustar.calculate_spectra` does. A file whose regions
    are missing is skipped and recorded, deliberately rather than re-measured: a segment
    has to use the region the reused response was computed for.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    bounds : array-like
        ``(N, 2)`` segment bounds, as
        :func:`~heasarc_retrieve_pipeline.utils.segment_bounds` returns.
    rec : StepRecord, optional
        Diagnostics record to write counts into.

    Returns
    -------
    list of str
        Base names of the grouped spectra written.
    """
    logger = get_logger()
    indir = nu_pipeline_output_path(obsid, config=config)
    outdir = nu_product_output_path(obsid, config=config)
    os.makedirs(outdir, exist_ok=True)

    written = []
    skipped = []
    for fpm, infile in spectral_input_files(obsid, config):
        root_name = rootname(os.path.basename(infile))
        stem = root_name[: -len("_cl")] if root_name.endswith("_cl") else root_name
        filedir = os.path.dirname(infile)

        this_src = os.path.join(filedir, root_name + "_src.reg")
        this_bkg = os.path.join(filedir, root_name + "_bkg.reg")
        if not os.path.exists(this_src) or not os.path.exists(this_bkg):
            logger.warning(f"No extraction region next to {infile}, skipping")
            record_skipped_input(
                obsid,
                config,
                infile,
                "no extraction region from the parent run; a segment must reuse the "
                "region its response was computed for",
            )
            skipped.append(os.path.basename(infile))
            continue

        parent_gti, template = _source_gti(infile)

        for number, segment in enumerate(np.asarray(bounds, dtype=float), start=1):
            tag = segment_tag(number)
            segment_gti = intersect_intervals(parent_gti, [segment])
            if not segment_gti.size:
                logger.info(f"{tag} of {os.path.basename(infile)} has no good time")
                skipped.append(f"{os.path.basename(infile)}:{tag}")
                continue

            gti_file = os.path.join(filedir, f"{root_name}_{tag}.gti")
            write_gti_file(template, gti_file, segment_gti)

            params = dict(
                indir=indir,
                infile=infile,
                instrument=f"FPM{fpm}",
                steminputs="nu" + obsid,
                # Tagged so that nuproducts' own default-named outputs -- the plot it
                # insists on writing, above all -- cannot land on the parent's.
                stemout=f"{stem}_{tag}",
                srcregionfile=this_src,
                bkgregionfile=this_bkg,
                outdir=outdir,
                clobber="yes",
                # The whole point: the responses are the expensive part and they do not
                # depend on the time cut. See the caveat in the module documentation.
                runmkarf="no",
                runmkrmf="no",
                extended="no",
                rungrppha="yes",
                grpmincounts=20,
                grppibadlow=35,
                grppibadhigh=1909,
                usrgtifile=gti_file,
                phafile=os.path.join(outdir, f"{stem}_sr_{tag}.pha"),
                bkgphafile=os.path.join(outdir, f"{stem}_bk_{tag}.pha"),
                grpphafile=os.path.join(outdir, f"{stem}_grp_{tag}.pha"),
                # The parent already has these and they would only be the segment's
                # again; nothing reads them and they cost time to make.
                lcfile="NONE",
                bkglcfile="NONE",
                imagefile="NONE",
            )
            logger.info(f"Extracting {tag} of {os.path.basename(infile)}")
            heasoft.run(
                "nuproducts",
                params,
                produces=params["grpphafile"],
                noprompt=True,
                clobber=True,
                verbose=True,
            )

            point_at_parent_response(
                [params["phafile"], params["grpphafile"]],
                f"{stem}_sr.rmf",
                f"{stem}_sr.arf",
            )
            _remove_plot(outdir, f"{stem}_{tag}")
            written.append(os.path.basename(params["grpphafile"]))

    if rec is not None:
        rec.value(spectra=written, skipped=skipped)
    return written


def point_at_parent_response(phafiles, rmf, arf):
    """
    Name the parent's response files in a segment spectrum.

    ``runmkarf=no runmkrmf=no`` leaves ``nuproducts`` with nothing to put in ``RESPFILE``
    and ``ANCRFILE``, so it writes ``none``. Both are bare file names resolved relative to
    the spectrum's own directory, and the segments sit in the same directory as the
    parent, so naming them is the whole of the job.

    Done with astropy rather than ``fthedit``: it is two keywords on a file this package
    owns, it is four HEASOFT subprocesses saved per segment, and it can be tested without
    a HEASOFT installation.

    Parameters
    ----------
    phafiles : list of str
        Spectra to edit, in place. Missing files are ignored -- ``grppha`` may not have
        produced one.
    rmf, arf : str
        Base names of the parent's redistribution matrix and ancillary response.
    """
    for path in phafiles:
        if not os.path.exists(path):
            continue
        with fits.open(path, mode="update") as hdul:
            for hdu in hdul:
                if "RESPFILE" in hdu.header:
                    hdu.header["RESPFILE"] = rmf
                if "ANCRFILE" in hdu.header:
                    hdu.header["ANCRFILE"] = arf


def _remove_plot(outdir, stem):
    """
    Delete the plot ``nuproducts`` writes whether or not it was asked to.

    ``plotdevice`` has no "off" setting. Four figures per event file per segment, none of
    them looked at, is what commit 710c1d5 got rid of everywhere else.
    """
    for suffix in ("_ph.gif", "_ph.ps"):
        path = os.path.join(outdir, stem + suffix)
        if os.path.exists(path):
            os.unlink(path)


def event_files_to_split(obsid, config):
    """
    The observation's merged and barycentred event lists.

    The per-source and background files in the observation's top-level directory: what
    :func:`~heasarc_retrieve_pipeline.nustar.join_source_data` merged and
    :func:`~heasarc_retrieve_pipeline.nustar.barycenter_data` corrected, which is what
    timing is done on. The Level-2 files under ``event_pipe`` and ``split`` are left
    alone -- the spectra reach them through a GTI, so there is nothing to gain by
    duplicating them.

    Files that are themselves segments are excluded, so the tool is safe to re-run.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.

    Returns
    -------
    list of str
        Paths, sorted.
    """
    basedir = nu_base_output_path(obsid, config=config)
    found = set()
    for pattern in (f"nu{obsid}*_src*.evt*", f"nu{obsid}*_back*.evt*"):
        found.update(glob.glob(os.path.join(basedir, pattern)))
    return sorted(
        path
        for path in found
        if not path.endswith(".gpg")
        and SEGMENT_RE.search(rootname(os.path.basename(path))) is None
    )


def split_event_files(obsid, config, bounds, rec=None):
    """
    Cut the observation's merged event lists into segments.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    config : dict
        Must contain ``out_data_path``.
    bounds : array-like
        ``(N, 2)`` segment bounds.
    rec : StepRecord, optional
        Diagnostics record to write counts into.

    Returns
    -------
    list of str
        Base names of the event files written.
    """
    logger = get_logger()
    written = []
    empty = []
    for path in event_files_to_split(obsid, config):
        for number, segment in enumerate(np.asarray(bounds, dtype=float), start=1):
            tag = segment_tag(number)
            outfile = insert_tag(path, tag)
            with fits.open(path) as hdul:
                segment_gti = intersect_intervals(read_gti(hdul), [segment])
                if not segment_gti.size:
                    empty.append(f"{os.path.basename(path)}:{tag}")
                    continue
                stats = apply_gti(hdul, segment_gti)
                update_time_bounds(hdul, segment_gti)
                hdul.writeto(outfile, overwrite=True)
            logger.info(
                f"{os.path.basename(outfile)}: {stats['nevents_after']} events, "
                f"{stats['ontime_after']:.1f} s"
            )
            written.append(os.path.basename(outfile))

    if rec is not None:
        rec.value(event_files=written, empty_segments=empty)
    return written


def split_obsid(obsid, config, split_mjds, scale=None, spectra=True, events=True):
    """
    Cut one finished observation into time segments.

    Parameters
    ----------
    obsid : str
        Observation identifier. The observation must already be reduced.
    config : dict
        Must contain ``out_data_path``.
    split_mjds : list of float
        Where to cut. ``N`` of them give ``N + 1`` segments.
    scale : str, optional
        Time scale the MJDs are given in; ``None`` means the files' own ``TIMESYS``.
    spectra, events : bool, optional
        Which products to split.

    Returns
    -------
    dict
        ``bounds``, ``split_mets``, ``spectra`` and ``event_files``.
    """
    from .nustar import observation_time_span

    logger = get_logger()
    mets, utc, timesys = resolve_split_times(obsid, config, split_mjds, scale=scale)
    tstart, tstop, _, _ = observation_time_span(obsid, config)
    # Open at both ends on purpose. Only the cut times the caller asked for carry any
    # information; where the data begin and end is a different answer for every file, and
    # each one is intersected with its own GTI below. Closing these on the observation
    # span -- which comes from the mode-01 file -- clipped the mode-06 CHU files, whose
    # good time reaches past both ends of it, and silently lost up to 720 s of exposure.
    bounds = segment_bounds(-np.inf, np.inf, mets)

    for mjd, met, as_utc in zip(split_mjds, mets, utc):
        logger.info(
            f"MJD {mjd} ({'file ' + timesys.upper() if scale is None else scale.upper()})"
            f" is MET {met:.6f}, which is MJD {as_utc:.8f} UTC"
        )
    logger.info(f"{obsid} runs {tstart:.3f} to {tstop:.3f}: {len(bounds)} segment(s)")

    with record_step(
        diagnostics_path(obsid, config), obsid, "split_obsid"
    ) as rec:
        rec.value(
            split_mjds=[float(m) for m in split_mjds],
            split_mets=mets,
            split_mjds_utc=utc,
            timesys=timesys,
            # JSON has no infinity -- json.dumps would write a bare ``Infinity``, which
            # Python reads back but nothing else does. ``null`` says "open end", which is
            # what the outer edges of the first and last segment mean.
            bounds=[
                [None if not np.isfinite(edge) else float(edge) for edge in segment]
                for segment in np.asarray(bounds, dtype=float)
            ],
        )
        written_spectra = split_spectra(obsid, config, bounds) if spectra else []
        written_events = split_event_files(obsid, config, bounds) if events else []
        rec.value(spectra=written_spectra, event_files=written_events)

    return {
        "bounds": bounds,
        "split_mets": mets,
        "spectra": written_spectra,
        "event_files": written_events,
    }


def main(argv=None):
    """
    ``hrp-split-obsid <data_dir> <OBSID> <MJD> [<MJD> ...]``.

    Parameters
    ----------
    argv : list of str, optional
        Defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        Process exit status.
    """
    parser = argparse.ArgumentParser(
        prog="hrp-split-obsid",
        description=__doc__.strip().splitlines()[0],
    )
    parser.add_argument("data_dir", help="parent directory containing {OBSID}/event_pipe subdirectories")
    parser.add_argument("obsid", help="observation to split; must already be reduced")
    parser.add_argument(
        "mjd", nargs="+", type=float, help="where to cut; N of them give N + 1 segments"
    )
    parser.add_argument(
        "--utc",
        action="store_true",
        help=(
            "read the MJDs as UTC rather than in the files' own TIMESYS. NuSTAR is TT, "
            "and the two differ by 67 s -- use this for a date read off a light curve "
            "labelled in civil time"
        ),
    )
    parser.add_argument(
        "--no-spectra", action="store_true", help="do not re-extract the spectra"
    )
    parser.add_argument(
        "--no-events", action="store_true", help="do not split the event lists"
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    import logging

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = {"out_data_path": os.path.abspath(args.data_dir)}
    result = split_obsid(
        args.obsid,
        config,
        args.mjd,
        scale="utc" if args.utc else None,
        spectra=not args.no_spectra,
        events=not args.no_events,
    )
    print(
        f"{len(result['bounds'])} segment(s): "
        f"{len(result['spectra'])} spectra, {len(result['event_files'])} event file(s)"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
