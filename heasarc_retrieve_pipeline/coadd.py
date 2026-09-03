"""
Co-adding spectra with HEASOFT ``addspec``.

The plumbing shared by everything in this package that adds one spectrum to another:
merging several observations of a source (:mod:`heasarc_retrieve_pipeline.combine`),
combining the two focal-plane modules of one observation, and the segment round trip
(:mod:`heasarc_retrieve_pipeline.roundtrip`). It sits below all of them so that none has to
import another.

``addspec`` warns, in its own help, that adding PHA datasets is a dangerous exercise, and
names the only two cases where it may be valid:

A) the same detector observing a source at different times, and
B) different but *identical* detectors observing at the same time.

Merging observations is case A. Combining FPMA with FPMB is case B, and the difference
matters, because ``addspec`` "implicitly assumes case A) in that it adds together the
exposure times of the PHA files instead of the effective areas". A case-B co-addition
therefore comes out claiming an exposure that is not a real elapsed time, and reporting
half the count rate the two detectors actually collected.

:func:`apply_case_b_scaling` is the correction, and it is a pure relabelling. Measured on
``90901333002``: ``addspec`` writes ``EXPOSURE`` as the sum of its inputs (103961.4 s from
52206.0 and 51755.4) and the response as their exposure-weighted *mean*, so what XSPEC
folds is

    EXPOSURE * RSP = (T_A + T_B) * (A_A*T_A + A_B*T_B)/(T_A + T_B) = A_A*T_A + A_B*T_B,

which is the true expected number of counts. Halving ``EXPOSURE`` and setting ``AREASCAL``
to 2 leaves that product untouched -- so no fit changes by a digit -- while making the file
say what it means: a real livetime, and an effective area that is the sum of the two
modules' rather than their average. It is a header edit; the response, which is 68 MB, is
never rewritten.

The same measurement showed that ``ONTIME``, ``LIVETIME``, ``TELAPSE`` and ``DEADC`` are
absent from an ``addspec`` output altogether, so there is nothing else to put right, and
that ``BACKSCAL`` comes out as 1.0 on both the spectrum and its background -- ``addspec``
folds the two modules' differing background areas into the scaling of the background
itself. The background's own ``EXPOSURE`` is deliberately inflated by a factor of a
thousand by the ``mathpha`` call that builds it ("exposure time increased to avoid
rounding errors"), and it is left alone: the correction cancels out of the background term,
so touching it would gain nothing and only obscure ``addspec``'s bookkeeping.

Why the inputs are staged
    ``addspec`` reads each spectrum's ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE``, and
    resolves them **relative to the current working directory**. See :func:`stage_inputs`
    for the bug this works around and for why the working directory has to move.
"""

import contextlib
import os
import shutil

from astropy.io import fits

from . import heasoft
from .utils import get_logger

__all__ = [
    "GROUPING_COMMAND",
    "apply_case_b_scaling",
    "run_addspec",
    "stage_inputs",
    "working_directory",
]

#: The grouping :func:`~heasarc_retrieve_pipeline.nustar.calculate_spectra` applies, in
#: the form ``grppha`` takes it. 20 counts per bin is the usual minimum for chi-squared
#: to be approximately valid; channels outside 3.0-78.0 keV are marked bad, via
#: ``E = 0.04 * PI + 1.6``.
GROUPING_COMMAND = "group min 20 & bad 0-34 & bad 1910-4095 & exit"


@contextlib.contextmanager
def working_directory(path):
    """
    Run a block with the process's working directory somewhere else.

    The working directory belongs to the whole process, and steering a pipeline step by
    changing it is exactly what ``test_prefect_wiring`` forbids. This is the one exception,
    and it is forced by the ``addspec`` bug described in :func:`stage_inputs`: a background
    spectrum has to be named without a directory, so the only way to say *which* background
    spectrum is to be standing in its directory.

    :data:`~heasarc_retrieve_pipeline.heasoft.HEASOFT_LOCK` is held throughout, which is
    what makes it safe: every HEASOFT call in this package goes through that lock, so no
    other tool can run while the directory is moved. It is re-entrant, so the
    :func:`~heasarc_retrieve_pipeline.heasoft.run` calls inside the block take it again
    without deadlocking.

    Parameters
    ----------
    path : str
        Directory to change into.
    """
    with heasoft.HEASOFT_LOCK:
        previous = os.getcwd()
        os.chdir(path)
        try:
            yield path
        finally:
            os.chdir(previous)


def stage_inputs(spectra, stagedir):
    """
    Gather the spectra of a merge into one directory, with pointers ``addspec`` can read.

    This exists to work around one specific ``addspec`` bug, and the shape of the
    workaround follows exactly from the shape of the bug. ``addspec`` co-adds the
    backgrounds by building a ``mathpha`` expression out of the ``BACKFILE`` values and
    spawning it -- but, unlike the expression it builds for the source spectra, it does
    **not** quote the operands::

        mathpha "expr='/path/nu..._sr.pha'+'/path/nu..._sr.pha'"          quoted, fine
        mathpha "expr=(/path/nu..._bk.pha*31.5)+(/path/nu..._bk.pha*31.5)"  not quoted

    ``mathpha`` reads the second as arithmetic, so every ``/`` in the path is a division
    operator and the run dies on ``fitsio 4.060 error message: could not open the named
    file``. A ``BACKFILE`` must therefore contain no directory at all, which leaves being
    in the right directory as the only way to say which file is meant.

    That is the whole of the constraint, so the staging is no wider than it. Measured, not
    assumed: with only ``BACKFILE`` made bare, ``addspec`` completes and writes its
    ``.rsp`` while the list file holds absolute paths and ``RESPFILE``/``ANCRFILE`` are
    absolute too.

    So each source spectrum is *copied* -- the originals must not be touched -- and in the
    copy ``BACKFILE`` is reduced to a bare name while ``RESPFILE`` and ``ANCRFILE`` are
    made absolute, pointing back at the parent's own responses. Only the background
    spectra are linked into the directory; the 68 MB ``.rmf`` files are never linked or
    copied at all.

    The file names already carry the OBSID, so spectra from different observations cannot
    collide here.

    Parameters
    ----------
    spectra : list of str
        Source spectra to stage.
    stagedir : str
        Directory to build. Created if it is not there.

    Returns
    -------
    list of str
        Base names of the staged spectra, in the order given, for the list file
        ``addspec`` reads.
    """
    os.makedirs(stagedir, exist_ok=True)
    logger = get_logger()
    staged = []

    for path in spectra:
        source = os.path.dirname(path)
        name = os.path.basename(path)
        destination = os.path.join(stagedir, name)
        shutil.copy(path, destination)

        with fits.open(destination, mode="update") as hdul:
            for hdu in hdul:
                for keyword in ("BACKFILE", "RESPFILE", "ANCRFILE"):
                    value = str(hdu.header.get(keyword, "none")).strip()
                    if not value or value.lower() in ("none", "no"):
                        continue
                    referenced = os.path.basename(value)
                    original = os.path.join(source, referenced)
                    if keyword == "BACKFILE":
                        # Bare, and linked in beside us: mathpha would read a path as
                        # arithmetic. This is the only keyword that has to be handled.
                        hdu.header[keyword] = referenced
                        _link(original, os.path.join(stagedir, referenced))
                    else:
                        hdu.header[keyword] = os.path.abspath(original)

        staged.append(name)
        logger.debug(f"Staged {name} for merging")

    return staged


def _link(source, destination):
    """
    Point ``destination`` at ``source``, quietly doing nothing if it is already there.

    A symbolic link rather than a copy: a merge only reads the background spectra. Falls
    back to copying where linking is not available.
    """
    if os.path.exists(destination) or os.path.islink(destination):
        return
    if not os.path.exists(source):
        get_logger().warning(f"{source} is named by a spectrum but is not there")
        return
    try:
        os.symlink(os.path.abspath(source), destination)
    except OSError:  # pragma: no cover - only on filesystems without symbolic links
        shutil.copy(source, destination)


def run_addspec(spectra, outdir, root, stagename, qaddrmf=True):
    """
    Co-add a list of spectra with ``addspec``, and leave the results in ``outdir``.

    The staging, the list file, the two HEASOFT calls and the tidying up, which are the
    same whether the spectra being added come from different observations or from the two
    focal-plane modules of one.

    Parameters
    ----------
    spectra : list of str
        Paths of the source spectra to co-add. Fewer than two is a programming error;
        callers are expected to have skipped that case, because ``addspec`` on a single
        file is a slow and lossy copy.
    outdir : str
        Directory the outputs are moved into.
    root : str
        Base name of the outputs: ``<root>.pha``, ``.bak``, ``.rsp`` and ``_grp.pha``.
    stagename : str
        Name of the staging directory, made inside ``outdir`` and removed afterwards.
    qaddrmf : bool, optional
        Combine the responses into a ``<root>.rsp``. Off leaves the caller to point the
        result at a response made elsewhere, which is what the segments do -- see
        :func:`~heasarc_retrieve_pipeline.segments.point_at_parent_response`.

    Returns
    -------
    list of str
        Base names of the files written into ``outdir``, including the ``_inputs.lis``
        record of what went in.
    """
    logger = get_logger()
    stagedir = os.path.join(outdir, stagename)
    staged = stage_inputs(spectra, stagedir)

    listfile = os.path.join(stagedir, root + ".lis")
    with open(listfile, "w") as fobj:
        fobj.write("".join(f"{basename}\n" for basename in staged))

    logger.info(f"Co-adding {len(staged)} spectra into {root}.pha")

    # addspec resolves the files its inputs name against the working directory, so this is
    # the only place it can be run from. See the module documentation.
    with working_directory(stagedir):
        heasoft.run(
            "addspec",
            produces=os.path.join(stagedir, root + ".pha"),
            infil=os.path.basename(listfile),
            outfil=root,
            qaddrmf="yes" if qaddrmf else "no",
            qsubback="yes",
            clobber="yes",
            noprompt=True,
        )

        # grppha writes the pointers through from its input, so the grouped spectrum comes
        # out naming the .bak and .rsp addspec just made.
        heasoft.run(
            "grppha",
            produces=os.path.join(stagedir, root + "_grp.pha"),
            infile=root + ".pha",
            outfile="!" + root + "_grp.pha",
            comm=GROUPING_COMMAND,
            noprompt=True,
        )

    written = []
    for suffix in (".pha", ".bak", ".rsp", "_grp.pha"):
        source = os.path.join(stagedir, root + suffix)
        if os.path.exists(source):
            shutil.move(source, os.path.join(outdir, root + suffix))
            written.append(root + suffix)

    # The list file is the record of what was co-added, so it is kept; the rest of the
    # staging directory is copies and symbolic links that would only confuse anyone reading
    # the products directory later.
    shutil.move(listfile, os.path.join(outdir, f"{root}_inputs.lis"))
    shutil.rmtree(stagedir, ignore_errors=True)
    written.append(f"{root}_inputs.lis")
    return written


def apply_case_b_scaling(paths, n_modules):
    """
    Turn an ``addspec`` output from case A into case B.

    ``addspec`` adds the exposure times of its inputs. That is right for the same detector
    at different times and wrong for two detectors at the same time, where what is added is
    the effective area. Halving the exposure and declaring an ``AREASCAL`` of two says the
    latter, and leaves ``EXPOSURE * AREASCAL`` -- which is all XSPEC folds -- exactly as it
    was. See the module documentation for the measurement behind this.

    The divisor is the number of *modules*, never the number of files. A product built from
    mode 01 and several mode-06 CHU subsets has many more inputs than modules, and those
    subsets are disjoint in time: their exposures genuinely do add, and only the FPMA/FPMB
    axis is the simultaneous one. This is why
    :func:`~heasarc_retrieve_pipeline.nustar.paired_spectral_inputs` refuses to let an
    unpaired file into a combined product -- it keeps the divisor equal to two.

    Parameters
    ----------
    paths : list of str
        Spectra to edit, in place. Missing files are ignored. The background is not one of
        them: the correction cancels out of the background term, and ``addspec`` has
        already inflated its exposure by a thousand for its own reasons.
    n_modules : int
        How many focal-plane modules went in, which is the factor by which ``addspec``
        over-counted the exposure.

    Returns
    -------
    list of str
        The paths that were changed.
    """
    logger = get_logger()
    changed = []
    for path in paths:
        if not os.path.exists(path):
            continue
        with fits.open(path, mode="update") as hdul:
            for hdu in hdul:
                if "EXPOSURE" not in hdu.header:
                    continue
                hdu.header["EXPOSURE"] = hdu.header["EXPOSURE"] / n_modules
                hdu.header["AREASCAL"] = n_modules
                hdu.header.add_history(
                    f"EXPOSURE/{n_modules}, AREASCAL={n_modules}: addspec adds exposures, "
                    "but these detectors observed at the same time"
                )
        changed.append(path)
        logger.debug(f"Rescaled {os.path.basename(path)} for {n_modules} modules")
    return changed
