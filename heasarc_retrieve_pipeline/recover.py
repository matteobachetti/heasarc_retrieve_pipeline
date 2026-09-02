"""
Measuring an observation that nobody recorded.

The reduction writes down what each step found, and the report draws the observation's
page from that. Two kinds of observation have nothing for it to draw:

* one reduced before this package recorded anything at all, which is every tree produced
  up to now; and
* one whose records were lost -- a diagnostics directory deleted to save space, say.

Their products are still on disk. The event files, the region files and the joined files
are exactly what the reduction measured the first time, so this module measures them
again and writes the datasets that were missing. After that they are read like any other:
recovery is a one-off, and the second page build costs nothing.

What this is not
----------------

It is **not** a way of running the reduction again. Nothing here calls HEASOFT, writes an
event file, or touches a product. It reads what is there and records what it finds.

It is also not a second implementation of the reduction's measurements. Where the
reduction has a function that measures without writing --
:func:`~heasarc_retrieve_pipeline.image_utils.measure_sources_in_file` is the one that
exists for this -- recovery calls it, so the page cannot drift away from the pipeline
that made the data.

What it cannot know
-------------------

A recovered dataset is a measurement of the *outputs*, so anything that was only ever a
property of the run is gone for good: how long a step took, whether it was retried, the
parameters it was called with. Extraction radii are the ones this package uses by
default, which is what the run will have used unless it was told otherwise. The records
recovery writes therefore carry no ``duration_s`` worth reading and are marked
``from_earlier_outputs``, so the page says plainly that this run did not do the work.

Examples
--------
>>> import tempfile
>>> outdir = tempfile.mkdtemp()
>>> recover_observation("90101201002", outdir)
[]
"""

import os

from .diagnostics import diagnostics_path, read_records, record_step
from .image_utils import measure_sources_in_file
from .nustar import nu_pipeline_output_path, separation_candidates, split_path
from .utils import get_logger, rootname

__all__ = ["measured_steps", "recover_observation", "recover_separations"]

#: The extraction radii :func:`~heasarc_retrieve_pipeline.nustar.separate_sources` uses
#: unless a caller says otherwise. A recovered measurement has no way of knowing what the
#: run that made the files was told, so it records these and says it recorded them.
DEFAULT_REGION_SIZE = 30
DEFAULT_BACK_REGION_SIZE = 55


def measured_steps(directory):
    """
    Which steps already have a dataset, so that recovery can leave them alone.

    Parameters
    ----------
    directory : str
        An observation's diagnostics directory.

    Returns
    -------
    set of tuple
        ``(step, key)`` for every record that carries a payload. A record with no payload
        does not count: a step that skipped and inherited nothing is exactly the gap this
        module is here to fill.
    """
    return {
        (record["step"], record.get("key") or "")
        for record in read_records(directory)
        if record.get("arrays")
    }


def recover_separations(obsid, outdir, measured=None):
    """
    Measure the field of every cleaned event file that has no separation dataset.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    outdir : str
        The run's output root -- the directory that holds ``<OBSID>/``.
    measured : set, optional
        From :func:`measured_steps`. Computed if not given.

    Returns
    -------
    list of str
        The event files that were measured, in the order they were read.
    """
    config = dict(out_data_path=outdir)
    directory = diagnostics_path(obsid, config)
    if measured is None:
        measured = measured_steps(directory)
    logger = get_logger()

    recovered = []
    for source in (nu_pipeline_output_path(obsid, config), split_path(obsid, config)):
        if not os.path.isdir(source):
            continue
        for event_file in separation_candidates(source):
            key = rootname(os.path.basename(event_file))
            if ("separate_sources", key) in measured:
                continue
            logger.info(f"Recovering the source separation of {event_file}")
            with record_step(directory, obsid, "separate_sources", key=key) as rec:
                rec.from_earlier_outputs()
                # Said before measuring, so that the more specific skip a file with too
                # few events records for itself wins over this one.
                rec.skip("measured from the files an earlier run left")
                measure_sources_in_file(
                    event_file,
                    region_size=DEFAULT_REGION_SIZE,
                    back_region_size=DEFAULT_BACK_REGION_SIZE,
                    rec=rec,
                )
            recovered.append(event_file)
    return recovered


def recover_observation(obsid, outdir):
    """
    Fill in every dataset an observation is missing.

    Safe to call on an observation that is already complete: it reads the records, finds
    nothing missing, and does nothing. Safe to call on one that was never reduced at all,
    or that failed half way -- there is simply less to find.

    Parameters
    ----------
    obsid : str
        Observation identifier.
    outdir : str
        The run's output root.

    Returns
    -------
    list of str
        Everything that was measured, for the log and for the tests. Empty when there was
        nothing to do, which is the normal case.

    Notes
    -----
    One recovery that fails must not cost the others, nor the page: each is logged and
    passed over. A page with a figure missing is a great deal better than no page.
    """
    directory = diagnostics_path(obsid, dict(out_data_path=outdir))
    measured = measured_steps(directory)
    logger = get_logger()

    recovered = []
    for recovery in (recover_separations,):
        try:
            recovered.extend(recovery(obsid, outdir, measured=measured))
        except Exception as error:
            logger.warning(f"Could not recover {recovery.__name__} for {obsid}: {error}")
    return recovered
