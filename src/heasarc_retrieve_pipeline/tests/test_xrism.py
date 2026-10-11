import os
import re

from heasarc_retrieve_pipeline.xrism import (
    find_xrism_inputs,
    xrism_base_output_path,
    xrism_download_filter,
)


CONFIG = {"out_data_path": "out"}
OBSID = "101002010"

# The whole archive listing of 101002010 (the Crab, 2024-10-03) except log/ and xtend/,
# recorded on 2026-10-11.
LISTING_101002010 = [
    "auxil/xa101002010.att.gz",
    "auxil/xa101002010.cat.gz",
    "auxil/xa101002010.com.gz",
    "auxil/xa101002010.ehk.gz",
    "auxil/xa101002010.mkf.gz",
    "auxil/xa101002010.orb.gz",
    "auxil/xa101002010.tim.gz",
    "auxil/xa101002010_fff.cat",
    "auxil/xa101002010_gen.gti.gz",
    "auxil/xa101002010gen_a0.hk.gz",
    "resolve/event_cl/xa101002010rsl_p0px0000_cl.evt.gz",
    "resolve/event_cl/xa101002010rsl_p0px1000_cl.evt.gz",
    "resolve/event_uf/xa101002010rsl_a0pxpr_uf.evt.gz",
    "resolve/event_uf/xa101002010rsl_el.gti.gz",
    "resolve/event_uf/xa101002010rsl_p0px1000_uf.evt.gz",
    "resolve/event_uf/xa101002010rsl_px1000_exp.gti.gz",
    "resolve/hk/xa101002010rsl_a0.hk1.gz",
    "resolve/products/xa101002010rsl_p0px1000_src.lc.gz",
    "resolve/products/xa101002010rsl_p0px1000_sky.img.gz",
    "xtend/event_cl/xa101002010xtd_p031200010_cl.evt.gz",
]
KEPT_101002010 = [
    "auxil/xa101002010.orb.gz",
    "resolve/event_cl/xa101002010rsl_p0px0000_cl.evt.gz",
    "resolve/event_cl/xa101002010rsl_p0px1000_cl.evt.gz",
]


def what_a_filter_keeps(arguments, entries, prefix="xrism/data/obs/1/101002010/"):
    """
    Run a download filter over a recorded listing, the way a transport would: against the
    whole remote name, as an HTTPS URL and as an S3 key, which must agree. This is
    test_rxte.py's helper of the same name, over XRISM's archive paths.
    """
    include = re.compile(arguments["re_include"])
    kept = {}
    for flavour, base in [
        ("https", "https://heasarc.gsfc.nasa.gov/FTP/" + prefix),
        ("s3", prefix),
    ]:
        kept[flavour] = [entry for entry in entries if include.search(base + entry)]
    assert kept["https"] == kept["s3"], "the filter reads an S3 key and a URL differently"
    return kept["s3"]


class TestXrismPaths:
    def test_the_output_directory_is_the_obsid_under_out_data_path(self):
        assert xrism_base_output_path(config=CONFIG, obsid=OBSID) == os.path.join("out", OBSID)


class TestTheDownloadFilter:
    def test_it_keeps_exactly_what_the_reduction_reads(self):
        """The orbit file and every cleaned Resolve event file; not the unfiltered events,
        the 33 MB attitude file, the products or anything from Xtend."""
        kept = what_a_filter_keeps(xrism_download_filter({}), LISTING_101002010)

        assert kept == KEPT_101002010

    def test_uncompressed_copies_are_kept_too(self):
        """A local or SciServer copy may have been gunzipped."""
        entries = ["auxil/xa101002010.orb", "resolve/event_cl/xa101002010rsl_p0px1000_cl.evt"]

        assert what_a_filter_keeps(xrism_download_filter({}), entries) == entries


class TestFindingTheInputs:
    def make_observation(self, tmp_path, names=KEPT_101002010):
        root = tmp_path / OBSID
        for name in names:
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"")
        return root

    def test_every_cleaned_file_and_the_orbit_are_found(self, tmp_path):
        found = find_xrism_inputs(str(self.make_observation(tmp_path)))

        assert [os.path.basename(f) for f in found["event_files"]] == [
            "xa101002010rsl_p0px0000_cl.evt.gz",
            "xa101002010rsl_p0px1000_cl.evt.gz",
        ]
        assert os.path.basename(found["orbit_file"]) == "xa101002010.orb.gz"

    def test_an_observation_without_resolve_data_is_reported_not_raised(self, tmp_path):
        """Calibration pointings such as 000001150 have only unfiltered Resolve events."""
        found = find_xrism_inputs(
            str(self.make_observation(tmp_path, ["auxil/xa000001150.orb.gz"]))
        )

        assert found["event_files"] == []

    def test_the_macos_sidecar_files_are_not_mistaken_for_data(self, tmp_path):
        """Copies on an exFAT drive get a ``._`` sidecar next to every file."""
        root = self.make_observation(tmp_path)
        (root / "resolve" / "event_cl" / "._xa101002010rsl_p0px1000_cl.evt.gz").write_bytes(b"")
        (root / "auxil" / "._xa101002010.orb.gz").write_bytes(b"")

        found = find_xrism_inputs(str(root))

        assert len(found["event_files"]) == 2
        assert os.path.basename(found["orbit_file"]) == "xa101002010.orb.gz"
