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

from heasarc_retrieve_pipeline import combine

OBSIDS = ["80002092002", "80002092004"]


def make_spectrum(path, stem, exposure=1000.0):
    """A source spectrum shaped like one nuproducts wrote, pointing at its neighbours."""
    spectrum = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="CHANNEL", format="J", array=np.arange(16)),
            fits.Column(name="COUNTS", format="J", array=np.arange(16)),
        ],
        name="SPECTRUM",
    )
    spectrum.header["EXPOSURE"] = exposure
    spectrum.header["BACKFILE"] = stem + "_bk.pha"
    spectrum.header["RESPFILE"] = stem + "_sr.rmf"
    spectrum.header["ANCRFILE"] = stem + "_sr.arf"
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
            for suffix in (".pha", ".bak", ".rsp"):
                with open(root + suffix, "w") as fobj:
                    fobj.write(root + suffix)
        elif name == "grppha":
            with open(kwargs["outfile"].lstrip("!"), "w") as fobj:
                fobj.write("grouped")


@pytest.fixture
def stub(monkeypatch):
    stub = StubAddspec()
    monkeypatch.setattr(combine.heasoft, "run", stub)
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

    def test_the_pointers_become_bare_names(self, tree, tmp_path):
        stage = str(tmp_path / "stage")
        staged = combine.stage_inputs(self.spectra(tree), stage)
        with fits.open(os.path.join(stage, staged[0])) as hdul:
            header = hdul["SPECTRUM"].header
            for keyword in ("BACKFILE", "RESPFILE", "ANCRFILE"):
                assert os.sep not in header[keyword]

    def test_everything_named_is_there_to_be_found(self, tree, tmp_path):
        stage = str(tmp_path / "stage")
        staged = combine.stage_inputs(self.spectra(tree), stage)
        for name in staged:
            with fits.open(os.path.join(stage, name)) as hdul:
                header = hdul["SPECTRUM"].header
                for keyword in ("BACKFILE", "RESPFILE", "ANCRFILE"):
                    assert os.path.exists(os.path.join(stage, header[keyword]))

    def test_the_originals_are_untouched(self, tree, tmp_path):
        original = self.spectra(tree)[0]
        with fits.open(original) as hdul:
            before = hdul["SPECTRUM"].header["RESPFILE"]
        combine.stage_inputs([original], str(tmp_path / "stage"))
        with fits.open(original) as hdul:
            assert hdul["SPECTRUM"].header["RESPFILE"] == before

    def test_the_responses_are_linked_not_copied(self, tree, tmp_path):
        """An rmf is 68 MB and a merge only reads it."""
        stage = str(tmp_path / "stage")
        combine.stage_inputs(self.spectra(tree)[:1], stage)
        rmf = [n for n in os.listdir(stage) if n.endswith(".rmf")][0]
        assert os.path.islink(os.path.join(stage, rmf))

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
    def test_one_addspec_per_module(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        assert [name for name, _ in stub.calls if name == "addspec"] == [
            "addspec",
            "addspec",
        ]

    def test_addspec_runs_in_the_staging_directory(self, tree, stub):
        """It resolves BACKFILE against the working directory, so this is load-bearing."""
        combine.merge_spectra(OBSIDS, tree, "vela")
        for cwd in stub.cwds:
            assert os.path.basename(cwd).startswith("_inputs_FPM")

    def test_the_list_file_holds_bare_names(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        products = os.path.join(tree["out_data_path"], "vela", "products")
        listfile = os.path.join(products, "vela_A_inputs.lis")
        lines = [line.strip() for line in open(listfile) if line.strip()]
        assert len(lines) == 4
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
            assert os.path.exists(os.path.join(products, "vela_A" + suffix))

    def test_the_grouping_matches_the_pipeline(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela")
        params = dict(stub.calls)["grppha"]
        assert "group min 20" in params["comm"]
        assert "bad 0-34" in params["comm"]
        assert "bad 1910-4095" in params["comm"]

    def test_mode01_only_narrows_the_inputs(self, tree, stub):
        combine.merge_spectra(OBSIDS, tree, "vela", mode01_only=True)
        products = os.path.join(tree["out_data_path"], "vela", "products")
        listfile = os.path.join(products, "vela_A_inputs.lis")
        lines = [line.strip() for line in open(listfile) if line.strip()]
        assert len(lines) == 2

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
