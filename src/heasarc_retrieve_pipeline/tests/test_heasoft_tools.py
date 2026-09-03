"""What the real ftools actually do, checked against a real HEASOFT.

Everywhere else in the suite the ftools are recorded doubles: fast, offline, and shaped
by what someone believed the tool does. Several of those beliefs are load-bearing --
``ftmerge`` concatenating without sorting, ``ftmerge`` taking the exposure keywords from
its *first* input, ``ftmerge`` refusing an output that already exists with return code
105, ``ftmgtime`` writing an extension called ``STDGTI`` that then has to be renamed. The
pipeline is built on all four. This module is where they are checked against the tool
rather than against a comment.

It is deliberately narrow. Only the tools that need no CALDB and run in a tenth of a
second are here; ``nupipeline``, ``nuproducts``, ``nusplitsc``, ``nicerl2`` and
``barycorr`` need calibration files and minutes of CPU, and stay stubbed. And the stubs
that remain elsewhere are not redundant: they assert *how* a tool is called -- that
``addspec`` gets relative paths because absolute ones overflow its buffer, that there is
one ``addspec`` per module and per mode -- which a real run cannot express.

Everything here is skipped unless ``HEADAS`` is set and ``heasoftpy`` imports; see
``conftest.py``.
"""

import os

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import heasoft

pytestmark = pytest.mark.heasoft


@pytest.fixture(scope="module", autouse=True)
def private_pfiles(tmp_path_factory):
    """Keep these tests out of the user's ``~/pfiles``.

    ``heasoftpy`` rewrites ``<PFILES>/<tool>.par`` around every call. Sharing that
    directory with whatever else is running is the failure this package exists to avoid,
    and a test suite is no exception.
    """
    directory = tmp_path_factory.mktemp("pfiles")
    heasoft.use_private_pfiles(str(directory))
    return str(directory)


def write_event_file(path, times, gti, ontime=None):
    """A minimal event file with an EVENTS and a GTI extension."""
    times = np.asarray(times, dtype=float)
    gti = np.asarray(gti, dtype=float)
    ontime = float(np.sum(gti[:, 1] - gti[:, 0])) if ontime is None else ontime

    events = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="TIME", format="D", array=times),
            fits.Column(name="PI", format="J", array=np.arange(times.size)),
        ],
        name="EVENTS",
    )
    events.header["TIMEZERO"] = 0.0
    events.header["ONTIME"] = ontime
    events.header["LIVETIME"] = ontime
    events.header["EXPOSURE"] = ontime

    gti_hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=gti[:, 0]),
            fits.Column(name="STOP", format="D", array=gti[:, 1]),
        ],
        name="GTI",
    )
    fits.HDUList([fits.PrimaryHDU(), events, gti_hdu]).writeto(path, overwrite=True)
    return str(path)


def write_spectrum(path, counts, exposure=1000.0):
    """A minimal OGIP type-I PHA file, complete enough for grppha and addspec."""
    counts = np.asarray(counts, dtype=np.int32)
    spectrum = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CHANNEL", format="J", array=np.arange(counts.size)),
            fits.Column(name="COUNTS", format="J", array=counts, unit="count"),
        ],
        name="SPECTRUM",
    )
    header = spectrum.header
    header["HDUCLASS"] = "OGIP"
    header["HDUCLAS1"] = "SPECTRUM"
    header["HDUCLAS2"] = "TOTAL"
    header["HDUCLAS3"] = "COUNT"
    header["HDUVERS"] = "1.2.1"
    header["TELESCOP"] = "NuSTAR"
    header["INSTRUME"] = "FPMA"
    header["FILTER"] = "NONE"
    header["EXPOSURE"] = exposure
    header["AREASCAL"] = 1.0
    header["BACKSCAL"] = 1.0
    header["CORRSCAL"] = 1.0
    header["BACKFILE"] = "NONE"
    header["CORRFILE"] = "NONE"
    header["RESPFILE"] = "NONE"
    header["ANCRFILE"] = "NONE"
    header["CHANTYPE"] = "PI"
    header["DETCHANS"] = counts.size
    header["POISSERR"] = True
    fits.HDUList([fits.PrimaryHDU(), spectrum]).writeto(path, overwrite=True)
    return str(path)


@pytest.fixture
def two_event_files(tmp_path):
    """Two overlapping event files, out of time order between them.

    FPMA's good time is ``[0, 10]`` and FPMB's is ``[3, 12]``, as in the observations the
    merge was written for: the modules start and stop within seconds of each other, not
    together. The exposures differ so that "which input did the keyword come from" has an
    answer.
    """
    first = write_event_file(str(tmp_path / "a.evt"), [1.0, 5.0, 9.0], [[0, 10]], ontime=10.0)
    second = write_event_file(str(tmp_path / "b.evt"), [2.0, 6.0], [[3, 12]], ontime=9.0)
    return first, second


class TestFtmerge:
    """What ``merge_event_files`` gets from the tool, and what it has to fix afterwards."""

    def test_it_concatenates_the_event_tables_without_sorting(self, tmp_path, two_event_files):
        first, second = two_event_files
        out = str(tmp_path / "m.evt")

        heasoft.run("ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO")

        with fits.open(out) as hdul:
            times = list(hdul["EVENTS"].data["TIME"])
        # Second file's events come after the first file's, not in time order. This is why
        # merge_event_files runs ftsort straight afterwards.
        assert times == [1.0, 5.0, 9.0, 2.0, 6.0]

    def test_it_takes_the_exposure_keywords_from_the_first_input(self, tmp_path, two_event_files):
        first, second = two_event_files
        out = str(tmp_path / "m.evt")

        heasoft.run("ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO")

        with fits.open(out) as hdul:
            header = hdul["EVENTS"].header
        # 10.0 is the first file's; the second's is 9.0. The combined file's exposure is
        # therefore wrong until something else fixes it -- see issue 34 in
        # docs/known_issues.rst.
        assert header["ONTIME"] == 10.0
        assert header["LIVETIME"] == 10.0
        assert header["EXPOSURE"] == 10.0

    def test_it_drops_the_gti_extension(self, tmp_path, two_event_files):
        first, second = two_event_files
        out = str(tmp_path / "m.evt")

        heasoft.run("ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO")

        with fits.open(out) as hdul:
            names = [hdu.name for hdu in hdul]
        # With copyall=NO only the first extension survives, so the merged GTI has to be
        # built separately by ftmgtime and put back with fappend.
        assert names == ["PRIMARY", "EVENTS"]

    def test_it_refuses_an_output_that_already_exists(self, tmp_path, two_event_files):
        first, second = two_event_files
        out = str(tmp_path / "m.evt")
        heasoft.run("ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO")

        with pytest.raises(RuntimeError) as excinfo:
            heasoft.run(
                "ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO"
            )

        # This is what a re-run of a half-finished join hits, and what the "join twice"
        # tests in test_nustar.py are about.
        assert "105" in str(excinfo.value)
        assert "already exists" in str(excinfo.value)

    def test_it_still_refuses_when_heasoftpy_raises_instead_of_returning(
        self, tmp_path, two_event_files, monkeypatch
    ):
        """The same refusal, with ``heasoftpy`` configured the way CI has it.

        ``allow_failure`` decides whether a non-zero exit is returned or raised, and the
        two ``heasoftpy`` builds in use disagree about the default. This sets it the way
        the HEASARC conda channel's build does -- which is what made the test above fail
        there -- and asks for the same ``RuntimeError`` anyway.
        """
        monkeypatch.setattr(heasoft.hsp.Config, "allow_failure", False)

        first, second = two_event_files
        out = str(tmp_path / "m.evt")
        heasoft.run("ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO")

        with pytest.raises(RuntimeError) as excinfo:
            heasoft.run(
                "ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO"
            )

        assert "ftmerge" in str(excinfo.value)
        assert "105" in str(excinfo.value)

    def test_a_leading_bang_overwrites(self, tmp_path, two_event_files):
        first, second = two_event_files
        out = str(tmp_path / "m.evt")
        heasoft.run("ftmerge", produces=out, infile=first, outfile=out, copyall="NO")

        heasoft.run(
            "ftmerge",
            produces="!" + out,
            infile=f"{first},{second}",
            outfile="!" + out,
            copyall="NO",
        )

        with fits.open(out) as hdul:
            assert len(hdul["EVENTS"].data) == 5


def test_ftsort_puts_the_rows_in_order(tmp_path, two_event_files):
    first, second = two_event_files
    out = str(tmp_path / "m.evt")
    heasoft.run("ftmerge", produces=out, infile=f"{first},{second}", outfile=out, copyall="NO")

    heasoft.run("ftsort", produces="!" + out, infile=out, outfile="!" + out, columns="TIME")

    with fits.open(out) as hdul:
        times = list(hdul["EVENTS"].data["TIME"])
    assert times == sorted(times)
    assert times == [1.0, 2.0, 5.0, 6.0, 9.0]


class TestFtmgtime:
    """The merged good time, and the extension name it comes out with."""

    def test_merge_and_gives_the_intersection(self, tmp_path, two_event_files):
        first, second = two_event_files
        out = str(tmp_path / "merged.gti")

        heasoft.run(
            "ftmgtime",
            produces=out,
            ingtis=f"{first}[GTI],{second}[GTI]",
            outgti=out,
            merge="AND",
            chatter=5,
        )

        with fits.open(out) as hdul:
            rows = [(row["START"], row["STOP"]) for row in hdul[1].data]
        assert rows == [(3.0, 10.0)]

    def test_the_extension_it_writes_is_not_called_gti(self, tmp_path, two_event_files):
        first, second = two_event_files
        out = str(tmp_path / "merged.gti")

        heasoft.run(
            "ftmgtime",
            produces=out,
            ingtis=f"{first}[GTI],{second}[GTI]",
            outgti=out,
            merge="AND",
            chatter=5,
        )

        with fits.open(out) as hdul:
            names = [hdu.name for hdu in hdul]
        # STDGTI, not GTI -- which is the entire reason merge_gtis runs fthedit next.
        assert names == ["PRIMARY", "STDGTI"]


def test_fthedit_renames_the_extension_in_place(tmp_path, two_event_files):
    first, second = two_event_files
    out = str(tmp_path / "merged.gti")
    heasoft.run(
        "ftmgtime",
        produces=out,
        ingtis=f"{first}[GTI],{second}[GTI]",
        outgti=out,
        merge="AND",
        chatter=5,
    )

    heasoft.run(
        "fthedit",
        produces=heasoft.IN_PLACE(out),
        infile=out + "+1",
        keyword="EXTNAME",
        operation="a",
        value="GTI",
    )

    with fits.open(out) as hdul:
        assert [hdu.name for hdu in hdul] == ["PRIMARY", "GTI"]


def test_fappend_adds_the_gti_back_to_the_event_file(tmp_path, two_event_files):
    first, second = two_event_files
    events = str(tmp_path / "m.evt")
    gti = str(tmp_path / "merged.gti")
    heasoft.run(
        "ftmerge", produces=events, infile=f"{first},{second}", outfile=events, copyall="NO"
    )
    heasoft.run(
        "ftmgtime",
        produces=gti,
        ingtis=f"{first}[GTI],{second}[GTI]",
        outgti=gti,
        merge="AND",
        chatter=5,
    )

    heasoft.run("fappend", produces=heasoft.IN_PLACE(events), infile=f"{gti}[1]", outfile=events)

    with fits.open(events) as hdul:
        assert len(hdul) == 3
        rows = [(row["START"], row["STOP"]) for row in hdul[2].data]
    assert rows == [(3.0, 10.0)]


def test_the_merge_leaves_events_outside_the_merged_gti(tmp_path):
    """The defect the AND merge exists to clean up, reproduced with the real tools.

    On observation 90901333002 two events out of 62705 sat 0.66 s and 0.77 s past the end
    of a good time interval: FPMB kept recording for a fraction of a second after FPMA
    stopped, ``ftmgtime merge=AND`` correctly cut the good time back to the overlap, and
    ``ftmerge`` -- which knows nothing about GTIs -- concatenated the events anyway.
    """
    first = write_event_file(str(tmp_path / "a.evt"), [5.0], [[0, 10]])
    second = write_event_file(str(tmp_path / "b.evt"), [5.0, 10.5], [[0, 11]])
    events = str(tmp_path / "m.evt")
    gti = str(tmp_path / "merged.gti")

    heasoft.run(
        "ftmerge", produces=events, infile=f"{first},{second}", outfile=events, copyall="NO"
    )
    heasoft.run(
        "ftmgtime",
        produces=gti,
        ingtis=f"{first}[GTI],{second}[GTI]",
        outgti=gti,
        merge="AND",
        chatter=5,
    )

    with fits.open(events) as hdul:
        times = np.asarray(hdul["EVENTS"].data["TIME"], dtype=float)
    with fits.open(gti) as hdul:
        stop = float(hdul[1].data["STOP"][-1])

    assert stop == 10.0
    assert np.any(times > stop), "the event past the merged good time should still be there"


class TestGrppha:
    """The grouping step of the coadd, run for real."""

    @pytest.fixture
    def spectrum(self, tmp_path):
        rng = np.random.default_rng(0)
        return write_spectrum(str(tmp_path / "src.pha"), rng.poisson(5.0, 4096))

    def test_it_writes_grouping_and_quality_columns(self, tmp_path, spectrum):
        from heasarc_retrieve_pipeline.coadd import GROUPING_COMMAND

        out = str(tmp_path / "grp.pha")

        heasoft.run(
            "grppha",
            produces=out,
            infile=spectrum,
            outfile="!" + out,
            comm=GROUPING_COMMAND,
            noprompt=True,
        )

        with fits.open(out) as hdul:
            data = hdul["SPECTRUM"].data
            names = data.columns.names
            bad = int(np.sum(data["QUALITY"] != 0))
        assert "GROUPING" in names and "QUALITY" in names
        # `bad 0-34 & bad 1910-4095` in GROUPING_COMMAND: 35 + 2186 channels.
        assert bad == 35 + 2186


def test_addspec_adds_the_exposures_of_its_inputs(tmp_path):
    """``addspec`` is what makes a coadd, and the exposure is what it is judged on."""
    rng = np.random.default_rng(1)
    write_spectrum(str(tmp_path / "s1.pha"), rng.poisson(5.0, 4096), exposure=1000.0)
    write_spectrum(str(tmp_path / "s2.pha"), rng.poisson(5.0, 4096), exposure=500.0)
    (tmp_path / "list.txt").write_text("s1.pha\ns2.pha\n")

    from heasarc_retrieve_pipeline.coadd import working_directory

    # addspec resolves the names in its list file against the working directory, which is
    # why the pipeline runs it from the staging directory and hands it relative paths.
    with working_directory(str(tmp_path)):
        heasoft.run(
            "addspec",
            produces=str(tmp_path / "sum.pha"),
            infil="list.txt",
            outfil="sum",
            qaddrmf="no",
            qsubback="no",
            clobber="yes",
            noprompt=True,
        )

    with fits.open(str(tmp_path / "sum.pha")) as hdul:
        assert hdul["SPECTRUM"].header["EXPOSURE"] == 1500.0


def test_a_tool_that_succeeds_without_writing_is_still_an_error(tmp_path, two_event_files):
    """The check that a zero return code does not buy.

    ``ftlist`` prints to standard output and writes no file at all, which makes it a
    convenient stand-in for the real case this guards: ``ftmgtime`` handed an empty list
    of input GTIs exits 0, writes nothing, and lets the next tool take the blame.
    """
    first, _ = two_event_files

    with pytest.raises(RuntimeError, match="did not create"):
        heasoft.run("ftlist", produces=str(tmp_path / "nothing.txt"), infile=first, option="H")


def test_the_private_pfiles_directory_is_the_one_being_used(private_pfiles, two_event_files):
    """A tool call must not have moved PFILES back to the shared directory."""
    first, _ = two_event_files
    heasoft.run("ftlist", produces=heasoft.IN_PLACE(first), infile=first, option="H")

    assert os.environ["PFILES"].startswith(private_pfiles + ";")
