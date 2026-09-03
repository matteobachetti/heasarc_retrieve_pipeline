"""
Offline tests for :mod:`heasarc_retrieve_pipeline.combine`.

No HEASOFT and no network. ``addspec`` and ``grppha`` are stubbed; what is checked is the
staging that has to happen around them, which is where the real difficulty is -- see the
module documentation on why ``addspec`` cannot be handed absolute paths.
"""

import os

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import coadd, combine

OBSIDS = ["80002092002", "80002092004"]


def make_spectrum(path, stem, exposure=1000.0, backfile=None, respfile=None, ancrfile=None):
    """A source spectrum shaped like one nuproducts wrote, pointing at its neighbours."""
    spectrum = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CHANNEL", format="J", array=np.arange(16)),
            fits.Column(name="COUNTS", format="J", array=np.arange(16)),
        ],
        name="SPECTRUM",
    )
    spectrum.header["EXPOSURE"] = exposure
    spectrum.header["BACKFILE"] = stem + "_bk.pha" if backfile is None else backfile
    spectrum.header["RESPFILE"] = stem + "_sr.rmf" if respfile is None else respfile
    spectrum.header["ANCRFILE"] = stem + "_sr.arf" if ancrfile is None else ancrfile
    spectrum.header["DETCHANS"] = 16
    fits.HDUList([fits.PrimaryHDU(), spectrum]).writeto(path, overwrite=True)


def make_products(base, obsid, modes=("01",), chus=()):
    """One observation's products directory: spectra and the files they name."""
    products = os.path.join(base, obsid, "products")
    os.makedirs(products, exist_ok=True)
    stems = []
    for fpm in "AB":
        for mode in modes:
            stems.append(f"nu{obsid}{fpm}{mode}")
        for chu in chus:
            stems.append(f"nu{obsid}{fpm}06_chu{chu}_N")
    for stem in stems:
        make_spectrum(os.path.join(products, stem + "_sr.pha"), stem)
        for suffix in ("_bk.pha", "_sr.rmf", "_sr.arf"):
            with open(os.path.join(products, stem + suffix), "w") as fobj:
                fobj.write(stem + suffix)
    return products


@pytest.fixture
def tree(tmp_path):
    for obsid in OBSIDS:
        make_products(tmp_path, obsid, modes=("01",), chus=("12",))
    return {"out_data_path": str(tmp_path)}


class StubAddspec:
    """``addspec`` and ``grppha``, recorded rather than run.

    The outputs are written into the working directory the caller changed into, which is
    the behaviour the staging exists to satisfy -- a stub that wrote them anywhere else
    would let a bug in that arrangement through.
    """

    def __init__(self):
        self.calls = []
        self.cwds = []

    def __call__(self, name, *args, produces=None, **kwargs):
        self.calls.append((name, dict(kwargs)))
        self.cwds.append(os.getcwd())
        if name == "addspec":
            root = kwargs["outfil"]
            # Real FITS, because a merged per-module spectrum is itself an input to the
            # co-addition of the two modules, and that one stages and rescales it.
            make_spectrum(
                root + ".pha",
                root,
                exposure=2000.0,
                backfile=root + ".bak",
                respfile=root + ".rsp",
                ancrfile="none",
            )
            make_spectrum(root + ".bak", root, exposure=2000000.0)
            with open(root + ".rsp", "w") as fobj:
                fobj.write(root + ".rsp")
        elif name == "grppha":
            make_spectrum(kwargs["outfile"].lstrip("!"), "grouped", exposure=2000.0)


@pytest.fixture
def stub(monkeypatch):
    stub = StubAddspec()
    # coadd is what actually spawns the tools now; combine only decides what to add.
    monkeypatch.setattr(coadd.heasoft, "run", stub)
    return stub


class TestMergeName:
    def test_default_names_the_ends(self):
        assert combine.merge_name(OBSIDS) == "merged_80002092002_80002092004"

    def test_an_explicit_name_wins(self):
        assert combine.merge_name(OBSIDS, "vela_2013") == "vela_2013"

    @pytest.mark.parametrize("bad", ["a+b", "a-b", "a*b", "run(1)", "with space"])
    def test_arithmetic_in_a_name_is_refused(self, bad):
        """mathpha, which addspec spawns, would read these as operators."""
        with pytest.raises(ValueError, match="mathpha"):
            combine.merge_name(OBSIDS, bad)


class TestSourceSpectra:
    def test_it_finds_both_modes(self, tree):
        found = combine.source_spectra(OBSIDS[0], tree)
        assert sorted(os.path.basename(path) for _, path in found) == [
            "nu80002092002A01_sr.pha",
            "nu80002092002A06_chu12_N_sr.pha",
            "nu80002092002B01_sr.pha",
            "nu80002092002B06_chu12_N_sr.pha",
        ]

    def test_the_modules_are_reported(self, tree):
        found = combine.source_spectra(OBSIDS[0], tree)
        assert sorted({fpm for fpm, _ in found}) == ["A", "B"]

    def test_mode01_only_leaves_the_chu_subsets_out(self, tree):
        found = combine.source_spectra(OBSIDS[0], tree, mode01_only=True)
        assert all("chu" not in path for _, path in found)
        assert len(found) == 2

    def test_a_segment_spectrum_is_not_an_input(self, tree):
        """Co-adding a segment with the observation it came from double-counts it."""
        products = os.path.join(tree["out_data_path"], OBSIDS[0], "products")
        make_spectrum(
            os.path.join(products, "nu80002092002A01_sr_seg1.pha"), "nu80002092002A01"
        )
        found = combine.source_spectra(OBSIDS[0], tree)
        # The base name, not the path: pytest's temporary directory is named after the
        # test, so "seg" appears in the path of every file here.
        assert all("seg" not in os.path.basename(path) for _, path in found)

    def test_another_observations_files_are_ignored(self, tree):
        products = os.path.join(tree["out_data_path"], OBSIDS[0], "products")
        make_spectrum(os.path.join(products, "nu99999999999A01_sr.pha"), "nu99999999999A01")
        found = combine.source_spectra(OBSIDS[0], tree)
        assert all("99999999999" not in path for _, path in found)


class TestStageInputs:
    """Everything addspec will look for, in the one directory it will look in."""

    def spectra(self, tree):
        return [path for _, path in combine.source_spectra(OBSIDS[0], tree)]

    def test_the_spectra_are_copied_not_moved(self, tree, tmp_path):
        original = self.spectra(tree)
        combine.stage_inputs(original, str(tmp_path / "stage"))
        assert all(os.path.exists(path) for path in original)

    def test_the_background_pointer_becomes_a_bare_name(self, tree, tmp_path):
        """The whole point of staging. ``addspec`` interpolates ``BACKFILE`` into a
        ``mathpha`` expression unquoted, so a ``/`` in it is read as division."""
        stage = str(tmp_path / "stage")
        staged = combine.stage_inputs(self.spectra(tree), stage)
        with fits.open(os.path.join(stage, staged[0])) as hdul:
            assert os.sep not in hdul["SPECTRUM"].header["BACKFILE"]

    def test_the_responses_stay_absolute(self, tree, tmp_path):
        """Only ``BACKFILE`` has the problem, so only ``BACKFILE`` is narrowed. Verified
        against HEASOFT: ``addspec`` builds its ``.rsp`` from absolute pointers."""
        stage = str(tmp_path / "stage")
        staged = combine.stage_inputs(self.spectra(tree), stage)
        with fits.open(os.path.join(stage, staged[0])) as hdul:
            header = hdul["SPECTRUM"].header
            for keyword in ("RESPFILE", "ANCRFILE"):
                assert os.path.isabs(header[keyword])
                assert os.path.exists(header[keyword])

    def test_everything_named_is_there_to_be_found(self, tree, tmp_path):
        stage = str(tmp_path / "stage")
        staged = combine.stage_inputs(self.spectra(tree), stage)
        for name in staged:
            with fits.open(os.path.join(stage, name)) as hdul:
                header = hdul["SPECTRUM"].header
                for keyword in ("BACKFILE", "RESPFILE", "ANCRFILE"):
                    # Bare names resolve inside the staging directory, which is where
                    # addspec runs; absolute ones resolve on their own.
                    assert os.path.exists(os.path.join(stage, header[keyword]))

    def test_the_originals_are_untouched(self, tree, tmp_path):
        original = self.spectra(tree)[0]
        with fits.open(original) as hdul:
            before = hdul["SPECTRUM"].header["RESPFILE"]
        combine.stage_inputs([original], str(tmp_path / "stage"))
        with fits.open(original) as hdul:
            assert hdul["SPECTRUM"].header["RESPFILE"] == before

    def test_the_background_is_linked_not_copied(self, tree, tmp_path):
        """A merge only reads it, and it has to be here under a bare name."""
        stage = str(tmp_path / "stage")
        combine.stage_inputs(self.spectra(tree)[:1], stage)
        background = [n for n in os.listdir(stage) if n.endswith("_bk.pha")][0]
        assert os.path.islink(os.path.join(stage, background))

    def test_the_big_responses_are_never_brought_in(self, tree, tmp_path):
        """An rmf is 68 MB. Nothing needs it here, so nothing links or copies it."""
        stage = str(tmp_path / "stage")
        combine.stage_inputs(self.spectra(tree), stage)
        brought_in = [n for n in os.listdir(stage) if n.endswith((".rmf", ".arf"))]
        assert brought_in == []

    def test_spectra_from_two_observations_do_not_collide(self, tree, tmp_path):
        both = self.spectra(tree) + [
            path for _, path in combine.source_spectra(OBSIDS[1], tree)
        ]
        staged = combine.stage_inputs(both, str(tmp_path / "stage"))
        assert len(set(staged)) == len(both)

    def test_a_missing_response_is_a_warning_not_a_crash(self, tree, tmp_path):
        spectrum = self.spectra(tree)[0]
        os.unlink(os.path.join(os.path.dirname(spectrum), "nu80002092002A01_sr.rmf"))
        combine.stage_inputs([spectrum], str(tmp_path / "stage"))


class TestWorkingDirectory:
    def test_it_changes_and_changes_back(self, tmp_path):
        before = os.getcwd()
        with combine.working_directory(str(tmp_path)) as here:
            assert os.path.realpath(os.getcwd()) == os.path.realpath(here)
        assert os.getcwd() == before

    def test_it_changes_back_after_a_failure(self, tmp_path):
        before = os.getcwd()
        with pytest.raises(RuntimeError):
            with combine.working_directory(str(tmp_path)):
                raise RuntimeError("boom")
        assert os.getcwd() == before


class TestMergeSpectra:
    def test_one_addspec_per_module_and_mode(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        roots = [
            params["outfil"] for name, params in stub.calls if name == "addspec"
        ]
        assert roots == [
            "vela_A01",
            "vela_A06",
            "vela_B01",
            "vela_B06",
            "vela_comb01",
            "vela_comb06",
            "vela_comb0106",
        ]

    def test_addspec_runs_in_the_staging_directory(self, tree, stub):
        """It resolves BACKFILE against the working directory, so this is load-bearing."""
        combine.merge_spectra(OBSIDS, tree, "vela")
        for cwd in stub.cwds:
            assert os.path.basename(cwd).startswith("_inputs_")

    def test_the_list_file_holds_bare_names(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        products = os.path.join(tree["out_data_path"], "vela", "products")
        listfile = os.path.join(products, "vela_A01_inputs.lis")
        lines = [line.strip() for line in open(listfile) if line.strip()]
        # One mode-01 spectrum from each of the two observations, and nothing of mode 06.
        assert lines == [
            f"nu{OBSIDS[0]}A01_sr.pha",
            f"nu{OBSIDS[1]}A01_sr.pha",
        ]
        assert all(os.sep not in line for line in lines)

    def test_the_staging_directory_is_cleaned_up(self, tree, stub):
        """Copies and symbolic links have no business surviving in products/."""
        combine.merge_spectra(OBSIDS, tree, "vela")
        products = os.path.join(tree["out_data_path"], "vela", "products")
        assert [n for n in os.listdir(products) if n.startswith("_inputs")] == []

    def test_the_response_is_combined_and_the_background_kept(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        params = dict(stub.calls)["addspec"]
        assert params["qaddrmf"] == "yes"
        assert params["qsubback"] == "yes"

    def test_the_outputs_land_in_the_products_directory(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        products = os.path.join(tree["out_data_path"], "vela", "products")
        for suffix in (".pha", ".bak", ".rsp", "_grp.pha"):
            assert os.path.exists(os.path.join(products, "vela_A01" + suffix))

    def test_the_grouping_matches_the_pipeline(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        params = dict(stub.calls)["grppha"]
        assert "group min 20" in params["comm"]
        assert "bad 0-34" in params["comm"]
        assert "bad 1910-4095" in params["comm"]

    def test_mode01_only_narrows_the_inputs(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela", mode01_only=True)
        products = os.path.join(tree["out_data_path"], "vela", "products")
        listfile = os.path.join(products, "vela_A01_inputs.lis")
        lines = [line.strip() for line in open(listfile) if line.strip()]
        assert len(lines) == 2
        assert not os.path.exists(os.path.join(products, "vela_A06_inputs.lis"))

    def test_a_module_with_one_spectrum_is_not_co_added(self, tmp_path, stub):
        """addspec on a single file would be a slow, lossy copy."""
        make_products(tmp_path, OBSIDS[0], modes=("01",))
        config = {"out_data_path": str(tmp_path)}
        written = combine.merge_spectra([OBSIDS[0]], config, "solo")
        assert written == {}
        assert stub.calls == []


class TestMergeObsids:
    def test_it_records_what_it_merged(self, tree, stub, monkeypatch):
        monkeypatch.setattr(combine, "merge_event_lists", lambda *a, **k: [])
        from heasarc_retrieve_pipeline.diagnostics import diagnostics_path, read_records

        result = combine.merge_obsids(OBSIDS, tree)

        assert result["name"] == "merged_80002092002_80002092004"
        records = read_records(diagnostics_path(result["name"], tree))
        record = [r for r in records if r["step"] == "merge_obsids"][0]
        assert record["status"] == "done"
        assert record["values"]["obsids"] == OBSIDS

    def test_the_merged_tree_looks_like_an_observation_to_the_report(
        self, tree, stub, monkeypatch
    ):
        """report.observation_directories counts anything with a diagnostics directory."""
        monkeypatch.setattr(combine, "merge_event_lists", lambda *a, **k: [])
        from heasarc_retrieve_pipeline.report import observation_directories

        result = combine.merge_obsids(OBSIDS, tree)
        assert result["name"] in observation_directories(tree["out_data_path"])

    def test_an_explicit_name_is_used(self, tree, stub, monkeypatch):
        monkeypatch.setattr(combine, "merge_event_lists", lambda *a, **k: [])
        result = combine.merge_obsids(OBSIDS, tree, name="vela_2013")
        assert result["name"] == "vela_2013"
        assert os.path.isdir(os.path.join(tree["out_data_path"], "vela_2013"))


class TestCombiningMergedModules:
    """FPMA and FPMB co-added across a merged dataset, one product per observing mode."""

    def products(self, tree):
        return os.path.join(tree["out_data_path"], "vela", "products")

    def test_the_modes_are_kept_apart_before_they_are_combined(self, tree, stub):
        """<NAME>_A used to be whichever mixture of 01 and 06 the observations held."""
        combine.merge_spectra(OBSIDS, tree, "vela")

        listfile = os.path.join(self.products(tree), "vela_A06_inputs.lis")
        lines = [line.strip() for line in open(listfile) if line.strip()]
        assert lines == [
            f"nu{OBSIDS[0]}A06_chu12_N_sr.pha",
            f"nu{OBSIDS[1]}A06_chu12_N_sr.pha",
        ]

    def test_one_product_per_flavour_is_written(self, tree, stub):
        written = combine.merge_spectra(OBSIDS, tree, "vela")

        assert {"comb01", "comb06", "comb0106"} <= set(written)
        for suffix in ("comb01", "comb06", "comb0106"):
            for end in (".pha", "_grp.pha", "_inputs.lis"):
                assert os.path.exists(os.path.join(self.products(tree), f"vela_{suffix}{end}"))

    def test_the_merged_modules_are_what_goes_in(self, tree, stub):
        """Not the observations' own spectra: those are already inside the merged ones."""
        combine.merge_spectra(OBSIDS, tree, "vela")

        listfile = os.path.join(self.products(tree), "vela_comb0106_inputs.lis")
        lines = [line.strip() for line in open(listfile) if line.strip()]
        assert lines == ["vela_A01.pha", "vela_B01.pha", "vela_A06.pha", "vela_B06.pha"]

    def test_the_exposure_is_corrected_for_two_modules(self, tree, stub):
        """addspec handed back 2000 s for two modules that observed at the same time."""
        combine.merge_spectra(OBSIDS, tree, "vela")

        header = fits.getheader(os.path.join(self.products(tree), "vela_comb01.pha"), 1)
        assert header["EXPOSURE"] == 1000.0
        assert header["AREASCAL"] == 2

    def test_a_dataset_with_one_module_is_not_combined(self, tree, stub):
        spectra = [
            ("A", os.path.join(tree["out_data_path"], obsid, "products", f"nu{obsid}A01_sr.pha"))
            for obsid in OBSIDS
        ]

        written = combine.merge_spectra(OBSIDS, tree, "vela", spectra=spectra)

        assert sorted(written) == ["A01"]

    def test_a_mode_present_for_one_module_only_is_left_out(self, tree, stub):
        """The case-B correction is a factor of two, so both modules or neither."""
        spectra = [
            (fpm, path)
            for obsid in OBSIDS
            for fpm, path in combine.source_spectra(obsid, tree)
            if not (fpm == "B" and "06" in os.path.basename(path))
        ]

        written = combine.merge_spectra(OBSIDS, tree, "vela", spectra=spectra)

        assert sorted(written) == ["A01", "A06", "B01", "comb01"]

    def test_combined_background_is_rebuilt_from_the_inputs(self, tree, stub):
        """addspec/mathpha inflates backgrounds by 1000; on pre-inflated inputs that
        overflows. The repair replaces the combined .bak with the sum of the per-module
        backgrounds.
        """
        combine.merge_spectra(OBSIDS, tree, "vela")

        products = self.products(tree)
        with fits.open(os.path.join(products, "vela_comb01.bak")) as hdul:
            bak = hdul[1]
            # Two inputs (A01, B01) each with EXPOSURE=2000000 → sum = 4000000
            assert bak.header["EXPOSURE"] == 4000000.0
            # COUNTS = sum of the two per-module backgrounds
            expected_counts = np.zeros(16, dtype=np.int32)
            for fn in ("vela_A01.bak", "vela_B01.bak"):
                with fits.open(os.path.join(products, fn)) as h:
                    expected_counts += h[1].data["COUNTS"]
            np.testing.assert_array_equal(bak.data["COUNTS"], expected_counts)

    def test_it_records_what_it_combined(self, tree, stub):
        from heasarc_retrieve_pipeline.diagnostics import diagnostics_path, record_step

        with record_step(diagnostics_path("vela", tree), "vela", "merge_obsids") as rec:
            combine.merge_spectra(OBSIDS, tree, "vela", rec=rec)

        from heasarc_retrieve_pipeline.diagnostics import read_records

        record = next(
            r
            for r in read_records(diagnostics_path("vela", tree))
            if r["step"] == "merge_obsids"
        )
        assert record["values"]["combined"]["comb01"] == "vela_comb01_grp.pha"
        assert record["values"]["combined_inputs"]["comb01"] == [
            "vela_A01.pha",
            "vela_B01.pha",
        ]


class TestMergeEventLists:
    def test_files_are_grouped_by_what_they_are(self, tree, monkeypatch):
        merged = []

        def stub_merge(paths, outfile, gti_operation="OR"):
            merged.append((sorted(os.path.basename(p) for p in paths), outfile, gti_operation))
            return outfile

        monkeypatch.setattr(combine.merge_event_files, "fn", stub_merge, raising=False)

        for obsid in OBSIDS:
            basedir = os.path.join(tree["out_data_path"], obsid)
            for name in (f"nu{obsid}A_src1_bary.evt", f"nu{obsid}A_back_bary.evt"):
                open(os.path.join(basedir, name), "w").close()

        written = combine.merge_event_lists(OBSIDS, tree, "vela")

        assert sorted(written) == ["nuvelaA_back_bary.evt", "nuvelaA_src1_bary.evt"]
        assert all(operation == "OR" for _, _, operation in merged)
        assert all(len(paths) == 2 for paths, _, _ in merged)

    def test_a_product_only_one_observation_has_is_skipped(self, tree, monkeypatch):
        monkeypatch.setattr(
            combine.merge_event_files, "fn", lambda *a, **k: None, raising=False
        )
        basedir = os.path.join(tree["out_data_path"], OBSIDS[0])
        open(os.path.join(basedir, f"nu{OBSIDS[0]}A_src2_bary.evt"), "w").close()

        assert combine.merge_event_lists(OBSIDS, tree, "vela") == []


class TestCommandLine:
    def test_one_observation_is_not_a_merge(self, tree, stub, capsys):
        with pytest.raises(SystemExit):
            combine.main([tree["out_data_path"], OBSIDS[0]])
        assert "at least two" in capsys.readouterr().err

    def test_it_merges_and_reports(self, tree, stub, monkeypatch, capsys):
        monkeypatch.setattr(combine, "merge_event_lists", lambda *a, **k: [])
        status = combine.main([tree["out_data_path"]] + OBSIDS + ["--name", "vela"])
        assert status == 0
        assert "vela" in capsys.readouterr().out


class TestMergingAnExplicitListOfSpectra:
    """Co-adding files named by the caller rather than found by globbing.

    ``source_spectra`` deliberately refuses anything with a ``_seg<N>`` tag, so that a merge
    of observations cannot double-count one that was previously split -- see
    :class:`TestSourceSpectra` above. That guard is what makes the segment round trip
    unreachable through the ordinary entry point, and it is not going anywhere. This is the
    way in for a caller that has worked out for itself which files belong together, and
    ``roundtrip.py`` is the one that has.
    """

    def spectra_of(self, tree, obsid, fpm="A"):
        products = os.path.join(tree["out_data_path"], obsid, "products")
        return [(fpm, os.path.join(products, f"nu{obsid}{fpm}01_sr.pha"))]

    def test_the_named_spectra_are_the_ones_co_added(self, tree, stub):
        spectra = self.spectra_of(tree, OBSIDS[0]) + self.spectra_of(tree, OBSIDS[1])
        combine.merge_spectra(OBSIDS, tree, "explicit", spectra=spectra)

        products = os.path.join(tree["out_data_path"], "explicit", "products")
        lines = [
            line.strip()
            for line in open(os.path.join(products, "explicit_A01_inputs.lis"))
            if line.strip()
        ]
        assert lines == [os.path.basename(path) for _, path in spectra]

    def test_the_glob_is_not_consulted_at_all(self, tree, stub, monkeypatch):
        def refuse(*args, **kwargs):
            raise AssertionError("source_spectra must not be called")

        monkeypatch.setattr(combine, "source_spectra", refuse)
        spectra = self.spectra_of(tree, OBSIDS[0]) + self.spectra_of(tree, OBSIDS[1])
        combine.merge_spectra(OBSIDS, tree, "explicit", spectra=spectra)

        assert [name for name, _ in stub.calls if name == "addspec"] == ["addspec"]

    def test_a_segment_spectrum_can_be_named_even_though_it_is_never_found(
        self, tree, stub
    ):
        """The round trip in one test: the two halves of a split, co-added by name."""
        products = os.path.join(tree["out_data_path"], OBSIDS[0], "products")
        stem = f"nu{OBSIDS[0]}A01"
        segments = []
        for number in (1, 2):
            path = os.path.join(products, f"{stem}_sr_seg{number}.pha")
            make_spectrum(path, stem)
            segments.append(("A", path))

        found = [os.path.basename(path) for _, path in combine.source_spectra(OBSIDS[0], tree)]
        assert not [name for name in found if "_seg" in name]

        combine.merge_spectra([OBSIDS[0]], tree, "trip", spectra=segments)

        merged = os.path.join(tree["out_data_path"], "trip", "products")
        lines = [
            line.strip()
            for line in open(os.path.join(merged, "trip_A01_inputs.lis"))
            if line.strip()
        ]
        assert lines == [f"{stem}_sr_seg1.pha", f"{stem}_sr_seg2.pha"]

    def test_the_modules_are_still_kept_apart(self, tree, stub):
        spectra = self.spectra_of(tree, OBSIDS[0], "A") + self.spectra_of(
            tree, OBSIDS[1], "A"
        ) + self.spectra_of(tree, OBSIDS[0], "B") + self.spectra_of(tree, OBSIDS[1], "B")
        written = combine.merge_spectra(OBSIDS, tree, "explicit", spectra=spectra)

        assert sorted(written) == ["A01", "B01", "comb01"]


class TestCaseBScaling:
    """addspec adds exposures; two modules observing at once add effective area instead."""

    def spectrum(self, tmp_path, exposure=1000.0):
        path = str(tmp_path / "nu1_comb01.pha")
        make_spectrum(path, "nu1_comb01", exposure=exposure)
        return path

    def test_the_exposure_is_divided_by_the_number_of_modules(self, tmp_path):
        path = self.spectrum(tmp_path, exposure=1000.0)

        coadd.apply_case_b_scaling([path], 2)

        assert fits.getheader(path, 1)["EXPOSURE"] == 500.0

    def test_the_area_is_multiplied_by_the_number_of_modules(self, tmp_path):
        path = self.spectrum(tmp_path)

        coadd.apply_case_b_scaling([path], 2)

        assert fits.getheader(path, 1)["AREASCAL"] == 2

    def test_what_xspec_folds_is_left_exactly_as_it_was(self, tmp_path):
        """The whole correction is a relabelling: no fit may change by a digit."""
        path = self.spectrum(tmp_path, exposure=103961.35432021158)
        before = fits.getheader(path, 1)
        product = before["EXPOSURE"] * before.get("AREASCAL", 1)

        coadd.apply_case_b_scaling([path], 2)

        after = fits.getheader(path, 1)
        assert after["EXPOSURE"] * after["AREASCAL"] == product

    def test_it_says_in_the_file_what_it_did(self, tmp_path):
        path = self.spectrum(tmp_path)

        coadd.apply_case_b_scaling([path], 2)

        assert any("AREASCAL=2" in str(card) for card in fits.getheader(path, 1)["HISTORY"])

    def test_a_missing_file_is_not_an_error(self, tmp_path):
        assert coadd.apply_case_b_scaling([str(tmp_path / "nope.pha")], 2) == []
