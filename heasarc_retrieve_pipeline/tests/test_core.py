"""Offline tests for the download layer.

Every test in ``test_pipeline.py`` needs the network. These do not: the archive index is
a real page captured from HEASARC, and the transfers are stubbed at the one function that
touches the network.
"""

import os

import pytest

from heasarc_retrieve_pipeline.core import parse_directory_index

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
