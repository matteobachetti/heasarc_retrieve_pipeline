"""Offline tests for the download layer.

Every test in ``test_pipeline.py`` needs the network. These do not: the archive index is
a real page captured from HEASARC, and the transfers are stubbed at the one function that
touches the network.
"""

import os
import re

import pytest

from heasarc_retrieve_pipeline import core
from heasarc_retrieve_pipeline.core import (
    download_node,
    file_needs_download,
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
            PREFIX + "auxil/nu1_att.fits.gz", PREFIX, str(tmp_path),
            re_include=re.compile(r"evt"),
        )

        assert dest is None

    def test_a_key_matching_re_exclude_is_dropped(self, tmp_path):
        dest = s3_key_destination(
            PREFIX + "event_cl/nu1A02_cl.evt.gz", PREFIX, str(tmp_path),
            re_exclude=re.compile(r"[AB]0[2-5]"),
        )

        assert dest is None

    def test_exclude_beats_include(self, tmp_path):
        dest = s3_key_destination(
            PREFIX + "event_cl/nu1A02_cl.evt.gz", PREFIX, str(tmp_path),
            re_include=re.compile(r"evt"), re_exclude=re.compile(r"[AB]0[2-5]"),
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
        client = StubS3Client([
            page((PREFIX + "auxil/first.fits.gz", 10)),
            page((PREFIX + "hk/second.hk.gz", 20)),
        ])
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

        results = recursive_download_s3.fn(
            f"s3://nasa-heasarc/{PREFIX}", str(tmp_path), test=True
        )

        assert client.downloaded == []
        assert len(results) == 1
