"""Offline tests for the download layer.

Every test in ``test_pipeline.py`` needs the network. These do not: the archive index is
a real page captured from HEASARC, and the transfers are stubbed at the one function that
touches the network.
"""

import os
import re
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from heasarc_retrieve_pipeline import core
from heasarc_retrieve_pipeline.core import (
    MISSION_CONFIG,
    download_node,
    get_remote_directory_listing,
    file_needs_download,
    obsid_query,
    parse_directory_index,
    recursive_download_s3,
    s3_key_destination,
)

BASE_URL = "https://heasarc.gsfc.nasa.gov/FTP/nustar/data/obs/00/8/80002092008/"

# The real index page of 80002092008, captured verbatim. Apache writes the column-sort
# links and the "Parent Directory" link as <a> elements too, which is what made the
# text-based parser invent five entries per directory.
ARCHIVE_INDEX_HTML = """\
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /FTP/nustar/data/obs/00/8/80002092008</title>
 </head>
 <body>
<h1>Index of /FTP/nustar/data/obs/00/8/80002092008</h1>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                       <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/back.gif" alt="[PARENTDIR]"> <a href="/FTP/nustar/data/obs/00/8/">Parent Directory</a>                                -   
<img src="/icons/folder.gif" alt="[DIR]"> <a href="auxil/">auxil/</a>                     2020-11-06 20:20    -   
<img src="/icons/folder.gif" alt="[DIR]"> <a href="event_cl/">event_cl/</a>                  2020-11-06 20:20    -   
<img src="/icons/folder.gif" alt="[DIR]"> <a href="event_uf/">event_uf/</a>                  2020-11-06 20:20    -   
<img src="/icons/folder.gif" alt="[DIR]"> <a href="hk/">hk/</a>                        2020-11-06 20:20    -   
<img src="/icons/compressed.gif" alt="[   ]"> <a href="nu80002092008.cat.gz">nu80002092008.cat.gz</a>       2020-11-06 20:20  3.4K  
<img src="/icons/text.gif" alt="[TXT]"> <a href="pipe.log">pipe.log</a>                   2020-11-06 20:20  1.7M  
<hr></pre>
</body></html>
"""


class TestParseDirectoryIndex:
    """What an Apache index page actually lists."""

    def test_the_column_sort_links_are_not_files(self):
        entries = parse_directory_index(ARCHIVE_INDEX_HTML, BASE_URL)

        for spurious in "Name", "Last%20modified", "Size", "Description":
            assert BASE_URL + spurious not in entries

    def test_the_parent_directory_link_is_not_followed(self):
        entries = parse_directory_index(ARCHIVE_INDEX_HTML, BASE_URL)

        assert not any("Parent" in entry for entry in entries)
        assert not any(entry.endswith("/8/") for entry in entries)

    def test_the_real_subdirectories_come_back_with_their_slash(self):
        entries = parse_directory_index(ARCHIVE_INDEX_HTML, BASE_URL)

        for name in "auxil", "event_cl", "event_uf", "hk":
            assert BASE_URL + name + "/" in entries

    def test_the_real_files_come_back(self):
        entries = parse_directory_index(ARCHIVE_INDEX_HTML, BASE_URL)

        assert BASE_URL + "nu80002092008.cat.gz" in entries
        assert BASE_URL + "pipe.log" in entries

    def test_nothing_else_is_listed(self):
        assert len(parse_directory_index(ARCHIVE_INDEX_HTML, BASE_URL)) == 6

    def test_every_entry_stays_under_the_directory(self):
        """An href that escapes the directory is the recursion hazard."""
        entries = parse_directory_index(ARCHIVE_INDEX_HTML, BASE_URL)

        assert all(entry.startswith(BASE_URL) for entry in entries)

    def test_a_relative_parent_reference_is_dropped(self):
        html = '<a href="../">Up</a><a href="real.evt">real.evt</a>'

        assert parse_directory_index(html, BASE_URL) == [BASE_URL + "real.evt"]

    def test_an_absolute_url_elsewhere_is_dropped(self):
        html = '<a href="https://example.com/x.evt">x</a><a href="real.evt">real.evt</a>'

        assert parse_directory_index(html, BASE_URL) == [BASE_URL + "real.evt"]

    def test_a_space_in_a_name_is_encoded(self):
        html = '<a href="a file.evt">a file.evt</a>'

        assert parse_directory_index(html, BASE_URL) == [BASE_URL + "a%20file.evt"]

    def test_a_page_with_no_links_gives_nothing(self):
        assert parse_directory_index("<html><body>empty</body></html>", BASE_URL) == []


class TestFileNeedsDownload:
    """The policy: what to do about a file that is already on disk.

    A local tree is a mirror of the archive, and the archive is authoritative, so a file
    of the wrong size is worthless rather than precious.
    """

    def test_a_missing_file_is_downloaded(self, tmp_path):
        needed, reason = file_needs_download(str(tmp_path / "absent.evt"), 100)

        assert needed
        assert "not" in reason.lower()

    def test_a_file_of_the_right_size_is_kept(self, tmp_path):
        path = tmp_path / "good.evt"
        path.write_bytes(b"x" * 100)

        needed, _ = file_needs_download(str(path), 100)

        assert not needed

    def test_a_short_file_is_downloaded_again(self, tmp_path):
        path = tmp_path / "short.evt"
        path.write_bytes(b"x" * 40)

        needed, _ = file_needs_download(str(path), 100)

        assert needed

    def test_a_long_file_is_downloaded_again(self, tmp_path):
        """Wrong is wrong in either direction: a longer file is not the archive's."""
        path = tmp_path / "long.evt"
        path.write_bytes(b"x" * 160)

        needed, _ = file_needs_download(str(path), 100)

        assert needed

    def test_an_empty_file_is_downloaded_again(self, tmp_path):
        path = tmp_path / "empty.evt"
        path.write_bytes(b"")

        needed, _ = file_needs_download(str(path), 100)

        assert needed

    def test_the_reason_names_both_sizes(self, tmp_path):
        path = tmp_path / "short.evt"
        path.write_bytes(b"x" * 40)

        _, reason = file_needs_download(str(path), 100)

        assert "40" in reason and "100" in reason

    def test_an_unknown_expected_size_keeps_the_file(self, tmp_path):
        """Some servers send no Content-Length. That is not evidence of a bad file."""
        path = tmp_path / "unverifiable.evt"
        path.write_bytes(b"x" * 40)

        needed, reason = file_needs_download(str(path), None)

        assert not needed
        assert "verif" in reason.lower()


BASE = "https://heasarc.gsfc.nasa.gov/FTP/nustar/data/obs/00/8"
NODE = BASE + "/80002092008/nu80002092008.cat.gz"


def stub_transfer(content, expected_size=None):
    """Stand in for the one function that touches the network.

    ``_download_pysmartdl`` is where pySmartDL is called; substituting it substitutes the
    network, not an optional dependency -- pysmartdl is a hard dependency and installs
    fine. ``expected_size`` defaults to the length actually written, i.e. a good transfer.
    """

    def transfer(url, dest):
        with open(dest, "wb") as fobj:
            fobj.write(content)
        return dest, len(content) if expected_size is None else expected_size

    return transfer


class TestDownloadNode:
    def test_a_good_transfer_returns_the_local_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr(core, "_download_pysmartdl", stub_transfer(b"x" * 10))

        result = download_node.fn(NODE, BASE, str(tmp_path))

        assert result == str(tmp_path / "80002092008" / "nu80002092008.cat.gz")
        assert os.path.getsize(result) == 10

    def test_a_failed_transfer_raises(self, tmp_path, monkeypatch):
        def explode(url, dest):
            raise OSError("connection reset by peer")

        monkeypatch.setattr(core, "_download_pysmartdl", explode)

        with pytest.raises(RuntimeError, match="connection reset"):
            download_node.fn(NODE, BASE, str(tmp_path))

    def test_a_short_transfer_raises_and_leaves_nothing_behind(self, tmp_path, monkeypatch):
        """The archive says 100 bytes, 40 arrive. That file must not survive."""
        monkeypatch.setattr(
            core, "_download_pysmartdl", stub_transfer(b"x" * 40, expected_size=100)
        )
        local = tmp_path / "80002092008" / "nu80002092008.cat.gz"

        with pytest.raises(RuntimeError, match="40"):
            download_node.fn(NODE, BASE, str(tmp_path))

        assert not local.exists()

    def test_part_files_are_cleaned_up_after_a_failure(self, tmp_path, monkeypatch):
        """pySmartDL downloads into <dest>.000, <dest>.001, ... and combines at the end."""
        local = tmp_path / "80002092008" / "nu80002092008.cat.gz"

        def explode(url, dest):
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            for part in range(3):
                with open(f"{dest}.{part:03d}", "wb") as fobj:
                    fobj.write(b"x")
            raise OSError("interrupted")

        monkeypatch.setattr(core, "_download_pysmartdl", explode)

        with pytest.raises(RuntimeError):
            download_node.fn(NODE, BASE, str(tmp_path))

        assert list(local.parent.glob("*.0*")) == []

    def test_an_existing_file_of_the_right_size_is_left_alone(self, tmp_path, monkeypatch):
        local = tmp_path / "80002092008" / "nu80002092008.cat.gz"
        local.parent.mkdir(parents=True)
        local.write_bytes(b"x" * 100)
        monkeypatch.setattr(core, "remote_file_size", lambda url: 100)
        monkeypatch.setattr(core, "_download_pysmartdl", stub_transfer(b"NEW"))

        assert download_node.fn(NODE, BASE, str(tmp_path)) is None
        assert local.read_bytes() == b"x" * 100

    def test_an_existing_file_of_the_wrong_size_is_replaced(self, tmp_path, monkeypatch):
        """The case a killed run leaves behind: a file that exists but is not the file."""
        local = tmp_path / "80002092008" / "nu80002092008.cat.gz"
        local.parent.mkdir(parents=True)
        local.write_bytes(b"x" * 40)
        monkeypatch.setattr(core, "remote_file_size", lambda url: 100)
        monkeypatch.setattr(core, "_download_pysmartdl", stub_transfer(b"y" * 100))

        result = download_node.fn(NODE, BASE, str(tmp_path))

        assert result == str(local)
        assert local.read_bytes() == b"y" * 100

    def test_verification_can_be_turned_off(self, tmp_path, monkeypatch):
        local = tmp_path / "80002092008" / "nu80002092008.cat.gz"
        local.parent.mkdir(parents=True)
        local.write_bytes(b"x" * 40)

        def no_network(url):
            raise AssertionError("verify=False must not ask the archive anything")

        monkeypatch.setattr(core, "remote_file_size", no_network)

        assert download_node.fn(NODE, BASE, str(tmp_path), verify=False) is None

    def test_an_unverifiable_existing_file_is_accepted(self, tmp_path, monkeypatch):
        local = tmp_path / "80002092008" / "nu80002092008.cat.gz"
        local.parent.mkdir(parents=True)
        local.write_bytes(b"x" * 40)
        monkeypatch.setattr(core, "remote_file_size", lambda url: None)

        assert download_node.fn(NODE, BASE, str(tmp_path)) is None
        assert local.read_bytes() == b"x" * 40

    def test_a_filtered_node_returns_none(self, tmp_path, monkeypatch):
        def no_network(url, dest):
            raise AssertionError("a filtered node must not be fetched")

        monkeypatch.setattr(core, "_download_pysmartdl", no_network)

        assert download_node.fn(NODE, BASE, str(tmp_path), test_str="_uf") is None

    def test_test_mode_transfers_nothing(self, tmp_path, monkeypatch):
        def no_network(url, dest):
            raise AssertionError("test mode must not fetch")

        monkeypatch.setattr(core, "_download_pysmartdl", no_network)

        result = download_node.fn(NODE, BASE, str(tmp_path), test=True)

        assert result == str(tmp_path / "80002092008" / "nu80002092008.cat.gz")
        assert not os.path.exists(result)


PREFIX = "nustar/data/obs/09/9/90901333002/"


class TestS3KeyDestination:
    """Where a bucket key lands locally, and whether it is wanted at all."""

    def test_the_obsid_becomes_the_top_local_directory(self, tmp_path):
        dest = s3_key_destination(PREFIX + "auxil/nu1_att.fits.gz", PREFIX, str(tmp_path))

        assert dest == str(tmp_path / "90901333002" / "auxil" / "nu1_att.fits.gz")

    def test_a_key_not_matching_re_include_is_dropped(self, tmp_path):
        dest = s3_key_destination(
            PREFIX + "auxil/nu1_att.fits.gz",
            PREFIX,
            str(tmp_path),
            re_include=re.compile(r"evt"),
        )

        assert dest is None

    def test_a_key_matching_re_exclude_is_dropped(self, tmp_path):
        dest = s3_key_destination(
            PREFIX + "event_cl/nu1A02_cl.evt.gz",
            PREFIX,
            str(tmp_path),
            re_exclude=re.compile(r"[AB]0[2-5]"),
        )

        assert dest is None

    def test_exclude_beats_include(self, tmp_path):
        dest = s3_key_destination(
            PREFIX + "event_cl/nu1A02_cl.evt.gz",
            PREFIX,
            str(tmp_path),
            re_include=re.compile(r"evt"),
            re_exclude=re.compile(r"[AB]0[2-5]"),
        )

        assert dest is None

    def test_the_test_str_filter_still_applies(self, tmp_path):
        dest = s3_key_destination(
            PREFIX + "event_cl/nu1A01_cl.evt.gz", PREFIX, str(tmp_path), test_str="_uf"
        )

        assert dest is None

    def test_no_filters_keeps_everything(self, tmp_path):
        dest = s3_key_destination(PREFIX + "hk/nu1A_fpm.hk.gz", PREFIX, str(tmp_path))

        assert dest is not None


class StubS3Client:
    """Enough of a boto3 S3 client to exercise the listing and the transfers.

    boto3 is a hard dependency and imports fine offline; what is missing offline is the
    *bucket*, which is what this stands in for.
    """

    def __init__(self, pages):
        self.pages = pages
        self.downloaded = []

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, **kwargs):
                return iter(client.pages)

        return Paginator()

    def download_file(self, bucket, key, dest):
        self.downloaded.append(key)
        with open(dest, "wb") as fobj:
            fobj.write(b"x" * self._size_of(key))

    def _size_of(self, key):
        for page in self.pages:
            for obj in page.get("Contents", []):
                if obj["Key"] == key:
                    return obj["Size"]
        raise KeyError(key)


def page(*entries):
    return {"Contents": [{"Key": key, "Size": size} for key, size in entries]}


class TestRecursiveDownloadS3:
    def test_keys_beyond_the_first_page_are_downloaded(self, tmp_path, monkeypatch):
        """list_objects_v2 returns at most 1000 keys; the rest are on later pages."""
        client = StubS3Client(
            [
                page((PREFIX + "auxil/first.fits.gz", 10)),
                page((PREFIX + "hk/second.hk.gz", 20)),
            ]
        )
        monkeypatch.setattr(core, "_s3_client", lambda: client)

        results = recursive_download_s3.fn(f"s3://nasa-heasarc/{PREFIX}", str(tmp_path))

        assert len(client.downloaded) == 2
        assert len(results) == 2

    def test_an_empty_page_ends_the_listing_cleanly(self, tmp_path, monkeypatch):
        client = StubS3Client([page((PREFIX + "auxil/only.fits.gz", 10)), {}])
        monkeypatch.setattr(core, "_s3_client", lambda: client)

        results = recursive_download_s3.fn(f"s3://nasa-heasarc/{PREFIX}", str(tmp_path))

        assert len(results) == 1

    def test_a_local_file_of_the_right_size_is_not_fetched(self, tmp_path, monkeypatch):
        client = StubS3Client([page((PREFIX + "auxil/there.fits.gz", 10))])
        monkeypatch.setattr(core, "_s3_client", lambda: client)
        local = tmp_path / "90901333002" / "auxil" / "there.fits.gz"
        local.parent.mkdir(parents=True)
        local.write_bytes(b"y" * 10)

        recursive_download_s3.fn(f"s3://nasa-heasarc/{PREFIX}", str(tmp_path))

        assert client.downloaded == []
        assert local.read_bytes() == b"y" * 10

    def test_a_local_file_of_the_wrong_size_is_fetched_again(self, tmp_path, monkeypatch):
        """The listing carries Size for every key, so this check costs nothing here."""
        client = StubS3Client([page((PREFIX + "auxil/short.fits.gz", 10))])
        monkeypatch.setattr(core, "_s3_client", lambda: client)
        local = tmp_path / "90901333002" / "auxil" / "short.fits.gz"
        local.parent.mkdir(parents=True)
        local.write_bytes(b"y" * 4)

        recursive_download_s3.fn(f"s3://nasa-heasarc/{PREFIX}", str(tmp_path))

        assert client.downloaded == [PREFIX + "auxil/short.fits.gz"]
        assert local.read_bytes() == b"x" * 10

    def test_an_incomplete_transfer_raises(self, tmp_path, monkeypatch):
        client = StubS3Client([page((PREFIX + "auxil/lies.fits.gz", 100))])
        client.download_file = lambda bucket, key, dest: open(dest, "wb").write(b"z" * 40)
        monkeypatch.setattr(core, "_s3_client", lambda: client)

        with pytest.raises(RuntimeError, match="40"):
            recursive_download_s3.fn(f"s3://nasa-heasarc/{PREFIX}", str(tmp_path))

        assert not (tmp_path / "90901333002" / "auxil" / "lies.fits.gz").exists()

    def test_test_mode_transfers_nothing(self, tmp_path, monkeypatch):
        client = StubS3Client([page((PREFIX + "auxil/x.fits.gz", 10))])
        monkeypatch.setattr(core, "_s3_client", lambda: client)

        results = recursive_download_s3.fn(f"s3://nasa-heasarc/{PREFIX}", str(tmp_path), test=True)

        assert client.downloaded == []
        assert len(results) == 1


class StubIndexServer:
    """A tiny fake archive: a mapping of URL to the HTML index it serves."""

    def __init__(self, pages):
        self.pages = pages
        self.requested = []

    def urlopen(self, req):
        url = req.full_url
        self.requested.append(url)
        if url not in self.pages:
            raise HTTPError(url, 404, "Not Found", None, None)

        class Response:
            def read(inner):
                return self.pages[url].encode()

        return Response()


def index_page(base, *names):
    """An index listing ``names`` under ``base``, in the Apache shape we parse."""
    links = "".join(f'<a href="{name}">{name}</a>\n' for name in names)
    return f"<html><body><pre>{links}</pre></body></html>"


class TestWalkRemoteDirectory:
    """The recursion under a directory listing, with no network."""

    def serve(self, monkeypatch, pages):
        server = StubIndexServer(pages)
        monkeypatch.setattr("urllib.request.urlopen", server.urlopen)
        return server

    def test_the_files_of_a_flat_directory_come_back(self, monkeypatch):
        self.serve(monkeypatch, {BASE_URL: index_page(BASE_URL, "a.evt", "b.evt")})

        assert core.walk_remote_directory(BASE_URL) == [
            BASE_URL + "a.evt",
            BASE_URL + "b.evt",
        ]

    def test_a_subdirectory_is_descended_into(self, monkeypatch):
        sub = BASE_URL + "event_cl/"
        self.serve(
            monkeypatch,
            {
                BASE_URL: index_page(BASE_URL, "event_cl/", "pipe.log"),
                sub: index_page(sub, "cl.evt"),
            },
        )

        assert core.walk_remote_directory(BASE_URL) == [
            sub,
            sub + "cl.evt",
            BASE_URL + "pipe.log",
        ]

    def test_every_directory_is_fetched_exactly_once(self, monkeypatch):
        sub = BASE_URL + "event_cl/"
        deep = sub + "deeper/"
        server = self.serve(
            monkeypatch,
            {
                BASE_URL: index_page(BASE_URL, "event_cl/"),
                sub: index_page(sub, "deeper/"),
                deep: index_page(deep, "cl.evt"),
            },
        )

        core.walk_remote_directory(BASE_URL)

        assert server.requested == [BASE_URL, sub, deep]

    def test_an_http_error_gives_nothing(self, monkeypatch):
        self.serve(monkeypatch, {})

        assert core.walk_remote_directory(BASE_URL) is None

    def test_a_subdirectory_that_errors_does_not_sink_the_listing(self, monkeypatch):
        self.serve(monkeypatch, {BASE_URL: index_page(BASE_URL, "gone/", "pipe.log")})

        assert core.walk_remote_directory(BASE_URL) == [
            BASE_URL + "gone/",
            BASE_URL + "pipe.log",
        ]

    def test_the_task_returns_what_the_walk_returns(self, monkeypatch):
        self.serve(monkeypatch, {BASE_URL: index_page(BASE_URL, "a.evt")})

        assert get_remote_directory_listing.fn(BASE_URL) == [BASE_URL + "a.evt"]


class TestThePageWriteIsTimed:
    """
    A worker that stalls writing a page has to say so.

    Batch runs have sat for hours between an observation's last step and its task run
    finishing, with nothing in the log in between. That window holds exactly two things:
    :func:`~heasarc_retrieve_pipeline.core.write_page` and Prefect's own finalisation of
    the task run, and nothing told them apart. These two lines bracket ours.
    """

    OBSID = "90202038002"

    def a_page_writer(self, monkeypatch, page):
        """Put ``page`` in place of the real page builder, which needs plotly."""
        from heasarc_retrieve_pipeline import report

        monkeypatch.setattr(report, "write_observation_page", page)

    def test_the_start_is_logged_before_the_page_is_built(self, monkeypatch, caplog):
        """Said first, so that a stall inside the builder still names the observation."""
        seen = []

        def page(obsid, outdir):
            seen.append(caplog.text)

        self.a_page_writer(monkeypatch, page)

        with caplog.at_level("INFO", logger="heasarc_retrieve_pipeline"):
            core.write_page(self.OBSID, "/nowhere")

        assert f"Writing the diagnostics page for {self.OBSID}" in seen[0]

    def test_the_end_says_how_long_it_took(self, monkeypatch, caplog):
        self.a_page_writer(monkeypatch, lambda obsid, outdir: None)

        with caplog.at_level("INFO", logger="heasarc_retrieve_pipeline"):
            core.write_page(self.OBSID, "/nowhere")

        assert re.search(rf"Wrote the diagnostics page for {self.OBSID} in \d+\.\d s", caplog.text)

    def test_a_page_that_raises_is_still_timed_and_still_passed_over(self, monkeypatch, caplog):
        """The reduction goes on: a reporting failure must not fail the observation."""

        def page(obsid, outdir):
            raise ValueError("no records")

        self.a_page_writer(monkeypatch, page)

        with caplog.at_level("INFO", logger="heasarc_retrieve_pipeline"):
            core.write_page(self.OBSID, "/nowhere")

        assert re.search(
            rf"Could not write the diagnostics page for {self.OBSID} after \d+\.\d s: "
            r"ValueError: no records",
            caplog.text,
        )
        assert "Wrote the diagnostics page" not in caplog.text


# Recorded from the live HEASARC TAP service on 2026-09-07 with
#
#     SELECT column_name FROM TAP_SCHEMA.columns WHERE table_name='<table>'
#
# (the service quotes ``"time"`` and ``"__row"``; the quotes are stripped here). The
# point of keeping the whole schema rather than the interesting parts is that the
# assertion below is then exact: every column a mission asks for is one the catalogue
# really has. ``xmmmaster`` is recorded before XMM is a mission, so the guard is in
# place on the day the mission is added.
CATALOGUE_COLUMNS = {
    "numaster": set(
        """
        __row __x_ra_dec __y_ra_dec __z_ra_dec abstract bii caldb_version category_code
        comments coordinated copi_fname copi_lname country cycle data_gap dec end_time
        exposure_a exposure_b instrument_mode issue_flag lii name nupsdout obs_type
        observation_mode obsid ontime_a ontime_b pi_fname pi_lname priority prnb
        processing_date public_date ra roll_angle slew_mode software_version
        solar_activity spacecraft_mode status subject_category time title
        """.split()
    ),
    "nicermastr": set(
        """
        __row __x_ra_dec __y_ra_dec __z_ra_dec abstract bii caldb_version category_code
        coordinated cycle dec end_time exposure facility galactic_nh lii mpu0_exposure
        mpu1_exposure mpu2_exposure mpu3_exposure mpu4_exposure mpu5_exposure
        mpu6_exposure name num_fpm num_processed obs_type obsid orig_target_id pi_fname
        pi_lname prnb processing_date processing_status processing_version public_date ra
        remarks software_version subject_category target_class target_dec target_id
        target_ra time time_awarded title
        """.split()
    ),
    "xtemaster": set(
        """
        __row __x_ra_dec __y_ra_dec __z_ra_dec archived_date bii cycle dec duration
        exposure hexte_anglea hexte_angleb hexte_dwella hexte_dwellb hexte_energya
        hexte_energyb hexte_modea hexte_modeb lii observed_date obsid pca_config1
        pca_config2 pca_config3 pca_config4 pca_config5 pca_config6 pi_fname pi_lname
        pi_no priority prnb processed_date ra scheduled_date status subject_category
        tar_no target_name time time_awarded
        """.split()
    ),
    "xmmmaster": set(
        """
        __row __x_ra_dec __y_ra_dec __z_ra_dec bii class data_in_heasarc dec
        distribution_date duration end_time estimated_exposure lii mos1_mode mos1_num
        mos1_time mos2_mode mos2_num mos2_time name obsid odf_date om_mode om_num om_time
        pi_fname pi_lname pi_title pn_mode pn_num pn_time pno pps_flag pps_version
        process_date process_status public_date ra rgs1_mode rgs1_num rgs1_time
        rgs2_mode rgs2_num rgs2_time sas_version scheduled_duration status
        subject_category time xmm_revolution
        """.split()
    ),
}


class TestTheObsidQueryAsksEachCatalogueForItsOwnColumns:
    """
    The rest of the ``obsid_query`` tests are in ``test_concurrency.py``; this one is
    here because it is a schema guard that costs nothing, and that file is deselected
    unless ``--run-slow`` is given.

    ``cycle`` is the reason the guard exists. It used to be written into the query text
    for every mission, and ``xmmmaster`` does not have it, so the XMM query failed before
    it reached the archive.
    """

    def selected_columns(self, mission):
        query = obsid_query("1", mission)
        selected = query.split("SELECT", 1)[1].split("FROM", 1)[0]
        return [column.strip() for column in selected.split(",") if column.strip()]

    @pytest.mark.parametrize("mission", sorted(MISSION_CONFIG))
    def test_every_column_a_mission_asks_for_exists_in_its_catalogue(self, mission):
        table = MISSION_CONFIG[mission]["table"]

        assert set(self.selected_columns(mission)) <= CATALOGUE_COLUMNS[table]

    @pytest.mark.parametrize("mission", sorted(MISSION_CONFIG))
    def test_a_mission_asks_for_no_column_twice(self, mission):
        """``rxte`` named ``cycle`` in "additional" while the query text named it too."""
        columns = self.selected_columns(mission)

        assert len(columns) == len(set(columns))

    def test_a_new_mission_has_to_record_its_catalogue_schema(self):
        """Otherwise the guard above silently passes over it."""
        tables = {config["table"] for config in MISSION_CONFIG.values()}

        assert tables <= set(CATALOGUE_COLUMNS)


class TestThePerMissionDownloadFilter:
    """
    Some missions want only part of an observation directory.

    ``recursive_download`` has taken ``re_include`` and ``re_exclude`` all along and
    nothing ever passed them. A mission declares a ``"download_filter"`` callable in
    ``MISSION_CONFIG``, the run's config chooses which filter it returns, and missions
    that declare nothing download whole directories exactly as before.
    """

    def a_mission_like_nustar(self, monkeypatch, **extra):
        """Register a fictional mission, so the real ones are not disturbed."""
        monkeypatch.setitem(
            core.MISSION_CONFIG, "fictional", dict(MISSION_CONFIG["nustar"], **extra)
        )
        return "fictional"

    def test_a_mission_that_declares_no_filter_downloads_the_whole_directory(self):
        assert core.mission_download_filter("nustar", {}) == {}

    def test_a_mission_that_declares_one_gets_it(self, monkeypatch):
        mission = self.a_mission_like_nustar(
            monkeypatch, download_filter=lambda config: {"re_include": r"\.evt"}
        )

        assert core.mission_download_filter(mission, {}) == {"re_include": r"\.evt"}

    def test_the_run_config_is_what_chooses_the_filter(self, monkeypatch):
        """XMM's filter differs between the PPS and the ODF route, which is a config key."""
        mission = self.a_mission_like_nustar(
            monkeypatch, download_filter=lambda config: {"re_include": config["products"]}
        )

        assert core.mission_download_filter(mission, {"products": "pps"}) == {"re_include": "pps"}
        assert core.mission_download_filter(mission, {"products": "odf"}) == {"re_include": "odf"}

    def test_a_filter_may_name_only_the_arguments_the_download_takes(self, monkeypatch):
        """A misspelt key would silently download the whole gigabyte instead of 40 MB."""
        mission = self.a_mission_like_nustar(
            monkeypatch, download_filter=lambda config: {"re_includes": r"\.evt"}
        )

        with pytest.raises(ValueError, match="re_includes"):
            core.mission_download_filter(mission, {})


# The top level of an XMM observation directory at HEASARC, as Apache writes it. This one
# is Her X-1 0153950401, which has both a PPS directory and an ODF one.
XMM_TOP_LEVEL_HTML = """\
<html><head><title>Index of /FTP/xmm/data/rev0/0153950401</title></head><body>
<h1>Index of /FTP/xmm/data/rev0/0153950401</h1>
<table><tr><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th></tr>
<tr><td><a href="/FTP/xmm/data/rev0/">Parent Directory</a></td><td>&nbsp;</td></tr>
<tr><td><a href="4XMM/">4XMM/</a></td><td>2024-11-08 03:12</td></tr>
<tr><td><a href="ODF/">ODF/</a></td><td>2024-11-08 03:12</td></tr>
<tr><td><a href="PPS/">PPS/</a></td><td>2024-11-08 03:14</td></tr>
<tr><td><a href="om_mosaic/">om_mosaic/</a></td><td>2024-11-08 03:12</td></tr>
</table></body></html>
"""

XMM_URL = "https://heasarc.gsfc.nasa.gov/FTP/xmm/data/rev0/0153950401/"


class TestListingOneArchiveDirectory:
    """
    One request, one directory, no recursion.

    ``get_remote_directory_listing`` walks a whole tree, which is what a download wants
    and far more than a question about the tree's shape needs. Deciding whether an
    observation has been reduced at the archive is such a question, and it is asked
    before the download that would answer it expensively.

    A directory that cannot be listed is ``None`` and not an empty list. The difference
    matters to every caller: "there is nothing here" is a fact about the archive, and "I
    could not look" is a fact about the network, and only the first is worth acting on.
    """

    def a_page(self, monkeypatch, html):
        monkeypatch.setattr(core, "_fetch_directory_index", lambda url: html)

    def test_the_subdirectories_come_back_named(self, monkeypatch):
        self.a_page(monkeypatch, XMM_TOP_LEVEL_HTML)

        assert core.list_archive_directory(XMM_URL) == ["4XMM/", "ODF/", "PPS/", "om_mosaic/"]

    def test_a_directory_keeps_its_slash(self, monkeypatch):
        """Which is how a caller tells a subdirectory from a file, here as everywhere
        else in this module."""
        self.a_page(monkeypatch, XMM_TOP_LEVEL_HTML)

        assert all(entry.endswith("/") for entry in core.list_archive_directory(XMM_URL))

    def test_nothing_below_the_top_level_is_listed(self, monkeypatch):
        """The whole point: 0153950401 holds 461 files, and this asks about four names."""
        self.a_page(monkeypatch, XMM_TOP_LEVEL_HTML)

        assert not [e for e in core.list_archive_directory(XMM_URL) if "/" in e.rstrip("/")]

    def test_a_url_without_a_trailing_slash_works_too(self, monkeypatch):
        self.a_page(monkeypatch, XMM_TOP_LEVEL_HTML)

        assert core.list_archive_directory(XMM_URL.rstrip("/")) == [
            "4XMM/",
            "ODF/",
            "PPS/",
            "om_mosaic/",
        ]

    def test_a_directory_that_cannot_be_reached_is_none(self, monkeypatch):
        self.a_page(monkeypatch, None)

        assert core.list_archive_directory(XMM_URL) is None

    def test_an_http_error_is_not_an_empty_directory(self, monkeypatch):
        def fetch(url):
            raise HTTPError(url, 404, "Not Found", {}, None)

        monkeypatch.setattr(core, "_fetch_directory_index", fetch)

        assert core.list_archive_directory(XMM_URL) is None

    def test_s3_lists_the_same_names(self, monkeypatch):
        """The bucket has no directories, so subdirectories are common prefixes and the
        delimiter is what stops the listing from returning every key in the tree."""
        asked = {}

        def list_objects_v2(**kwargs):
            asked.update(kwargs)
            return {
                "CommonPrefixes": [
                    {"Prefix": "xmm/data/rev0/0153950401/ODF/"},
                    {"Prefix": "xmm/data/rev0/0153950401/PPS/"},
                ],
                "Contents": [{"Key": "xmm/data/rev0/0153950401/README.txt"}],
            }

        monkeypatch.setattr(
            core, "_s3_client", lambda: SimpleNamespace(list_objects_v2=list_objects_v2)
        )

        entries = core.list_archive_directory("s3://nasa-heasarc/xmm/data/rev0/0153950401/")

        assert entries == ["ODF/", "PPS/", "README.txt"]
        assert asked["Delimiter"] == "/"
        assert asked["Prefix"] == "xmm/data/rev0/0153950401/"

    def test_a_bucket_that_cannot_be_read_is_none(self, monkeypatch):
        def broken():
            raise OSError("no route to host")

        monkeypatch.setattr(core, "_s3_client", broken)

        assert core.list_archive_directory("s3://nasa-heasarc/xmm/x/") is None

    def test_a_local_directory_is_listed_from_disk(self, tmp_path):
        """The SciServer transport, where the archive is a mounted filesystem."""
        (tmp_path / "PPS").mkdir()
        (tmp_path / "ODF").mkdir()
        (tmp_path / "MANIFEST.1").write_text("x")

        assert core.list_archive_directory(str(tmp_path)) == ["MANIFEST.1", "ODF/", "PPS/"]

    def test_a_local_directory_that_is_not_there_is_none(self, tmp_path):
        assert core.list_archive_directory(str(tmp_path / "nowhere")) is None


class TestThePerMissionConfigResolution:
    """
    Some missions cannot know their configuration until they have looked at the archive.

    XMM is the one: ``xmmmaster`` says whether an observation was reduced at the archive,
    and it is wrong often enough to matter -- 0973390101 is flagged as reduced and has no
    PPS directory mirrored at HEASARC at all. A run that trusted the flag would download
    five megabytes of housekeeping and then report an observation with no data in it.

    Missions that declare nothing are handed their configuration back unchanged, which is
    what every mission did before this existed.
    """

    def a_mission_like_nustar(self, monkeypatch, **extra):
        monkeypatch.setitem(
            core.MISSION_CONFIG, "fictional", dict(MISSION_CONFIG["nustar"], **extra)
        )
        return "fictional"

    def test_a_mission_that_declares_nothing_keeps_its_config(self):
        config = {"products": "pps"}

        assert core.mission_resolve_config("nustar", config, "https://x/") == config

    def test_a_mission_that_declares_one_gets_to_change_its_config(self, monkeypatch):
        mission = self.a_mission_like_nustar(
            monkeypatch, resolve_config=lambda config, url: dict(config, products="odf")
        )

        assert core.mission_resolve_config(mission, {"products": "pps"}, "https://x/") == {
            "products": "odf"
        }

    def test_the_url_is_what_it_is_given_to_look_at(self, monkeypatch):
        seen = []
        mission = self.a_mission_like_nustar(
            monkeypatch, resolve_config=lambda config, url: seen.append(url) or config
        )

        core.mission_resolve_config(mission, {}, "s3://nasa-heasarc/xmm/x/")

        assert seen == ["s3://nasa-heasarc/xmm/x/"]

    def test_something_that_is_not_a_configuration_is_refused(self, monkeypatch):
        """A hook that forgets to return would otherwise fail three steps later, inside
        the mission's own reduction, under a name that has nothing to do with it."""
        mission = self.a_mission_like_nustar(monkeypatch, resolve_config=lambda config, url: None)

        with pytest.raises(TypeError, match="fictional"):
            core.mission_resolve_config(mission, {}, "https://x/")


class TestTheResolvedConfigReachesTheRun:
    """The wiring: what the hook decides is what the download filters on, and what the
    mission's own reduction is handed."""

    def a_run_that_records_what_it_saw(self, monkeypatch):
        seen = {}

        def recursive_download(url, outdir, **kwargs):
            seen["download"] = kwargs
            return []

        monkeypatch.setattr(core, "recursive_download", recursive_download)
        return seen

    def test_the_filter_sees_the_resolved_config(self, tmp_path, monkeypatch):
        seen = self.a_run_that_records_what_it_saw(monkeypatch)
        monkeypatch.setitem(
            core.MISSION_CONFIG,
            "fictional",
            dict(
                MISSION_CONFIG["nustar"],
                resolve_config=lambda config, url: dict(config, products="odf"),
                download_filter=lambda config: {"re_include": config["products"]},
            ),
        )

        core.download_and_process_observation.fn(
            "0153950401",
            "https://example.invalid/0153950401",
            83.0,
            22.0,
            str(tmp_path),
            "fictional",
            str(tmp_path / "pfiles"),
            str(tmp_path / "work"),
            test=True,
        )

        assert seen["download"]["re_include"] == "odf"


class TestTheDownloadFilterReachesTheDownload:
    """The wiring, from ``MISSION_CONFIG`` down to the call that fetches the files."""

    def a_download_that_records_its_arguments(self, monkeypatch):
        seen = {}

        def recursive_download(url, outdir, **kwargs):
            seen.update(kwargs)
            return []

        monkeypatch.setattr(core, "recursive_download", recursive_download)
        return seen

    def download(self, tmp_path, mission):
        core.download_and_process_observation.fn(
            "80002092008",
            "https://example.invalid/80002092008",
            83.0,
            22.0,
            str(tmp_path),
            mission,
            str(tmp_path / "pfiles"),
            str(tmp_path / "work"),
            test=True,
        )

    def test_a_mission_without_a_filter_passes_none(self, tmp_path, monkeypatch):
        seen = self.a_download_that_records_its_arguments(monkeypatch)

        self.download(tmp_path, "nustar")

        assert "re_include" not in seen
        assert "re_exclude" not in seen

    def test_a_mission_with_a_filter_passes_it(self, tmp_path, monkeypatch):
        seen = self.a_download_that_records_its_arguments(monkeypatch)
        monkeypatch.setitem(
            core.MISSION_CONFIG,
            "fictional",
            dict(
                MISSION_CONFIG["nustar"],
                download_filter=lambda config: {"re_include": r"EVLI", "re_exclude": r"\.PNG$"},
            ),
        )

        self.download(tmp_path, "fictional")

        assert seen["re_include"] == r"EVLI"
        assert seen["re_exclude"] == r"\.PNG$"
