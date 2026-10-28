"""SVOM retrieval: the ECPI runner, the catalogue, and what is downloaded for one ObsID.

ECPI lives in an environment of its own and needs FSC credentials, neither of which CI
has. Everything but the last test therefore replaces ``svom.run_ecpi`` with a function
that answers as the FSC does (answers recorded 2026-10-28, numbers synthetic).
"""

import json
import os
import subprocess

import pytest
from astropy.coordinates import SkyCoord
from astropy.table import Table

from heasarc_retrieve_pipeline import core, svom

FIND_FILES_TABLE = """\
CARD\tPASS_ID\tOBS_ID\tBURST_ID\tEARTHFOV\tSTACK\tPROGRAM\tOBJECTID\tSOURCEID\tDATE-BEG\tDATE-END\tDATE\tEXPOSURE\tTSTART\tTSTOP\tRA_PNT\tDEC_PNT\tROLL_PNT
ECL-EVT-CAL\t000002_SYC\t1000000002.0\tn.a.\tnan\tnan\tGeneralProgram\tn.a.\tn.a.\t-\t-\t-\tNone\t5000.0\t6000.0\t10.0\t20.0\tNone
ECL-EVT-CAL\t000001_HBX\t1000000001.0\tn.a.\tnan\tnan\tGeneralProgram\tn.a.\tn.a.\t-\t-\t-\tNone\t1000.0\t2000.0\t30.0\t40.0\tNone
ECL-EVT-CAL\t000003_KUX\t1000000001.0\tsb26000001\tnan\tnan\tCoreProgram\tn.a.\tn.a.\t-\t-\t-\tNone\t2000.0\t3000.0\tNone\tNone\tNone
"""


def product(acronym, tstart, tstop, obsid=None, pass_id=None, burst=None, version=1):
    """One product as the helper's ``search`` returns it."""
    header = dict(
        CARD=acronym,
        OBS_ID=obsid,
        BURST_ID=burst,
        PASS_ID=pass_id,
        TSTART=tstart,
        TSTOP=tstop,
        PROC_VER=version,
    )
    url = f"/sdb/{acronym}/{acronym}_{obsid}_{pass_id}_v{version}.fits"
    return dict(acronym=acronym, url=url, header=header)


class FakeFSC:
    """Answers the runner's calls from a dictionary of products, and records them."""

    def __init__(self, products, find_table=FIND_FILES_TABLE):
        self.products = products
        self.find_table = find_table
        self.calls = []

    def __call__(self, argv, cwd=None):
        self.calls.append(list(argv))
        if os.path.basename(argv[0]) == "ecpi_find_files":
            with open(argv[argv.index("--table-file") + 1], "w") as table:
                table.write(self.find_table)
            return ""
        command, acronym = argv[3], argv[4]
        if command == "get":
            outdir, name = argv[5], argv[6]
            os.makedirs(outdir, exist_ok=True)
            open(os.path.join(outdir, name), "w").close()
            return json.dumps(dict(path=os.path.join(outdir, name)))
        return json.dumps(self.products.get(acronym, []))

    def searched(self, acronym):
        return [call for call in self.calls if call[3:5] == ["search", acronym]]


@pytest.fixture
def ecpi(monkeypatch):
    """A fake ECPI Python, so the argument vectors can be built."""
    monkeypatch.setenv("SVOM_ECPI_PYTHON", "/env/bin/python")


class TestRunner:
    def test_the_credentials_file_is_sourced_and_its_name_is_not_run(self, tmp_path):
        """The file sets the variables the command sees; a name full of shell syntax is
        passed as data, never pasted into the script."""
        env_file = tmp_path / "x; touch pwned; $(touch pwned2)"
        env_file.write_text("export KC_USERNAME=someone\n")

        argv = svom.ecpi_command(["printenv", "KC_USERNAME"], env_file=str(env_file))
        out = subprocess.run(argv, cwd=tmp_path, capture_output=True, text=True).stdout

        assert out.strip() == "someone"
        assert not (tmp_path / "pwned").exists() and not (tmp_path / "pwned2").exists()

    def test_without_ecpi_the_error_says_what_to_set(self, monkeypatch):
        """No silent fallback to a Python without ECPI."""
        monkeypatch.delenv("SVOM_ECPI_PYTHON", raising=False)

        with pytest.raises(RuntimeError, match="SVOM_ECPI_PYTHON"):
            svom.run_ecpi(["ecpi_find_files"])


class TestCatalogue:
    def test_files_are_grouped_into_one_row_per_obsid(self, ecpi, monkeypatch):
        """Span over all files, pointing from those that have one, sorted by time."""
        monkeypatch.setattr(svom, "run_ecpi", FakeFSC({}))

        table = svom.svom_catalogue("eclairs", position=SkyCoord(83.6, 22.0, unit="deg"))

        assert list(table["obsid"]) == ["1000000001", "1000000002"]
        first = table[0]
        assert (first["tstart"], first["tstop"], first["n_files"]) == (1000.0, 3000.0, 2)
        assert (first["ra"], first["dec"]) == pytest.approx((30.0, 40.0))
        assert first["passes"] == "000001_HBX,000003_KUX"

    @pytest.mark.parametrize("instrument, radius", [("eclairs", "45.0"), ("grm", "70.0")])
    def test_each_instrument_has_its_own_default_radius(
        self, ecpi, monkeypatch, instrument, radius
    ):
        """GRM's detectors point 30 degrees off the ECLAIRs axis, so it searches wider."""
        fsc = FakeFSC({})
        monkeypatch.setattr(svom, "run_ecpi", fsc)

        svom.svom_catalogue(instrument, position=SkyCoord(83.6, 22.0, unit="deg"))

        argv = fsc.calls[0]
        assert argv[argv.index("--radius") + 1] == radius

    def test_the_search_by_obsid_and_the_work_items(self, ecpi, monkeypatch):
        """``fsc:<obsid>`` items, barycentred at the pointing when no source is given."""
        fsc = FakeFSC({})
        monkeypatch.setattr(svom, "run_ecpi", fsc)

        table = core.MISSION_CONFIG["svom_grm"]["catalogue"](obsids=["1000000001"])
        items = svom.svom_work_items(table[:1], None)

        assert fsc.calls[0][fsc.calls[0].index("--obsid") + 1] == "1000000001"
        assert items[0]["url"] == "fsc:1000000001"
        assert (items[0]["ra"], items[0]["dec"]) == pytest.approx((30.0, 40.0))


class TestDownload:
    PRODUCTS = {
        "GRM-EVT": [
            product("GRM-EVT", 1000.0, 5000.0, obsid=7, version=1),
            product("GRM-EVT", 1000.0, 5000.0, obsid=7, version=2),
        ],
        # The pass of the events starts late; the one before it covers the start.
        "SVO-ORB-CNV": [
            product("SVO-ORB-CNV", 1200.0, 9000.0, pass_id="000002_HBX"),
            product("SVO-ORB-CNV", -3000.0, 1200.0, pass_id="000001_KUX"),
        ],
        "SVO-ATT-CNV": [product("SVO-ATT-CNV", 1200.0, 9000.0, pass_id="000002_HBX")],
        # Weekly SAA entry/exit tables, covering the events whatever their pass.
        "SAA-DEEP-ECL": [product("SAA-DEEP-ECL", -5e5, 5e5)],
        "SAA-EXT-ECL": [product("SAA-EXT-ECL", -5e5, 5e5)],
    }
    AUX = ["SVO-ORB-CNV"] * 2 + ["SVO-ATT-CNV", "SAA-DEEP-ECL", "SAA-EXT-ECL"]

    def test_events_then_every_auxiliary_file_around_them(self, ecpi, monkeypatch):
        """The latest event version, and orbit, attitude and SAA files searched by time
        with the margin, so the previous pass's orbit file comes too."""
        fsc = FakeFSC(self.PRODUCTS)
        monkeypatch.setattr(svom, "run_ecpi", fsc)

        plan = svom.download_plan("grm", 7)

        assert [p["acronym"] for p in plan] == ["GRM-EVT"] + self.AUX
        assert plan[0]["header"]["PROC_VER"] == 2
        orbit_search = fsc.searched("SVO-ORB-CNV")[0]
        assert orbit_search[-4:] == ["--tmin", "400.0", "--tmax", "5600.0"]

    def test_an_obsid_without_events_downloads_nothing(self, ecpi, monkeypatch):
        """No GRM file for this pointing: no orbit or attitude search either."""
        fsc = FakeFSC({})
        monkeypatch.setattr(svom, "run_ecpi", fsc)

        assert svom.download_plan("grm", 7) == []
        assert not fsc.searched("SVO-ORB-CNV")

    def test_the_download_hook_fetches_into_the_obsid_directory_once(
        self, ecpi, monkeypatch, tmp_path
    ):
        """Each file once, by the URL its search returned (an SAA table has no ObsID or
        pass to pick it by); a rerun skips them."""
        fsc = FakeFSC(self.PRODUCTS)
        monkeypatch.setattr(svom, "run_ecpi", fsc)
        download = core.MISSION_CONFIG["svom_grm"]["download"]

        download("7", "fsc:7", str(tmp_path), {})
        gets = [call for call in fsc.calls if call[3] == "get"]
        download("7", "fsc:7", str(tmp_path), {})

        assert len(gets) == 6
        assert len(os.listdir(tmp_path / "7")) == 6
        urls = [p["url"] for products in self.PRODUCTS.values() for p in products]
        assert [call[4] for call in gets] == [urls[1]] + urls[2:]
        assert len([call for call in fsc.calls if call[3] == "get"]) == 6

    def test_a_test_run_only_makes_the_plan(self, ecpi, monkeypatch, tmp_path):
        """``test=True`` searches but downloads nothing."""
        fsc = FakeFSC(self.PRODUCTS)
        monkeypatch.setattr(svom, "run_ecpi", fsc)

        svom.svom_download("grm", "7", "fsc:7", str(tmp_path), {}, test=True)

        assert not [call for call in fsc.calls if call[3] == "get"]


@pytest.mark.remote_data
@pytest.mark.skipif(not svom.has_ecpi(), reason="SVOM_ECPI_PYTHON is not set")
def test_the_fsc_answers_a_grm_search_and_a_plan():
    """Live: a day of GRM-EVT products, and the plan for the first ObsID found has its
    event file and its orbit, attitude and SAA files."""
    found = svom.search_products("GRM-EVT", tmin=280000000.0, tmax=280086400.0)
    assert found and {"acronym", "url", "header"} <= set(found[0])

    plan = svom.download_plan("grm", found[0]["header"]["OBS_ID"])

    assert {p["acronym"] for p in plan} == {"GRM-EVT", *svom.AUX_PRODUCTS}


def test_an_empty_answer_is_an_empty_catalogue(ecpi, monkeypatch):
    """Nothing near the source: a table with the right columns and no rows, not an error."""
    monkeypatch.setattr(svom, "run_ecpi", FakeFSC({}, find_table=""))

    table = svom.svom_catalogue("grm", position=SkyCoord(0, 0, unit="deg"))

    assert len(table) == 0 and "obsid" in table.colnames
    assert isinstance(table, Table)
