"""
Offline tests for the RXTE/PCA reduction.

The file names are real: 90171-01-01-00 was listed in full from the public S3 mirror of
the HEASARC archive on 2026-09-17. It is one of the two long pointings at M82, and it was
chosen because it holds three GoodXenon event files, not one.
"""

import os
import re

import numpy as np
from astropy.io import fits
from astropy.table import Table

from heasarc_retrieve_pipeline import rxte
from heasarc_retrieve_pipeline.rxte import (
    rxte_base_output_path,
    rxte_download_filter,
    find_rxte_inputs,
    process_rxte_obsid,
    rxte_pcu_gtis,
    rxte_screened_events,
)


CONFIG = {"out_data_path": "out"}
OBSID = "10408-01-05-000"

# 90171-01-01-00, 45.7 ks: 251 files and 50.8 MB in the archive. The nine files the
# reduction reads are 10.9 MB of it. The largest of the rest are HEXTE (12.9 MB), the other
# PCA modes, and the calibration copy (7.8 MB).
LISTING_90171_01_01_00 = [
    "FIAC_15de310c-15de8c7b",
    "FIAE_15de310c-15de8c7b",
    "FICA_00000000-21e17c86",
    "FICC_15de310c-15de8c7b",
    "FIED_15de310c-15de8c7b",
    "FIFD_15de310c-15de8c7b",
    "FIGS_15de310c-15de8c7b",
    "FIHX_15de310c-15de8c7b",
    "FIIG_15de310c-15de8c7b",
    "FIIP_15de310c-15de8c7b",
    "FIOE_15de310c-15de8c7b",
    "FIPC_15de310c-15de8c7b",
    "FIPS_15de310c-15de8c7b",
    "FISP_15de310c-15de8c7b",
    "FIST_15de310c-15de8c7b",
    "FMI",
    "ace/FHd4_15de310c-15de8c7c.gz",
    "ace/FHd5_15de310c-15de8c7b.gz",
    "ace/FHd6_15de310c-15de8c7c.gz",
    "ace/FHd7_15de310c-15de8c7b.gz",
    "ace/FHd8_15de310c-15de8c7c.gz",
    "ace/FHd9_15de310c-15de8c7b.gz",
    "ace/FHda_15de310c-15de8c78.gz",
    "ace/FHdb_15de310c-15de8c7b.gz",
    "ace/FHdc_15de310c-15de8c7c.gz",
    "ace/FHdd_15de310c-15de8c7c.gz",
    "ace/FHde_15de310c-15de8c78.gz",
    "ace/FHdf_15de310c-15de8c7b.gz",
    "ace/FHe0_15de310c-15de8c70.gz",
    "ace/FHe1_15de310c-15de8c7b.gz",
    "ace/FHe2_15de310c-15de8c78.gz",
    "ace/FHe3_15de310c-15de8c7b.gz",
    "ace/FHe4_15de310c-15de8c70.gz",
    "ace/FHe5_15de310c-15de8c7b.gz",
    "ace/FHe6_15de310c-15de8c7c.gz",
    "ace/FHe7_15de310c-15de8c7c.gz",
    "ace/FHe8_15de310c-15de8c78.gz",
    "ace/FHe9_15de310c-15de8c78.gz",
    "acs/FH0d_15de310c-15de8c7b.gz",
    "acs/FH0e_15de310c-15de8c7d.gz",
    "acs/FH0f_15de310c-15de8c7d.gz",
    "acs/FH10_15de310c-15de8c7d.gz",
    "acs/FH11_15de310c-15de8c78.gz",
    "acs/FH12_15de310c-15de8c78.gz",
    "acs/FH13_15de310c-15de8c78.gz",
    "acs/FH14_15de310c-15de8c70.gz",
    "acs/FH15_15de310c-15de8c7b.gz",
    "acs/FH16_15de310c-15de8c70.gz",
    "acs/FH17_15de310c-15de8c78.gz",
    "acs/FH18_15de310c-15de8c78.gz",
    "acs/FH1a_15de310c-15de8c7c.gz",
    "acs/FH1b_15de310c-15de8c78.gz",
    "acs/FH1c_15de310c-15de8c78.gz",
    "acs/FH1d_15de310c-15de8c60.gz",
    "acs/FH1e_15de310c-15de8c7b.gz",
    "acs/FH6e_15de310c-15de8c7b.gz",
    "acs/FH9d_15de310c-15de8c7b.gz",
    "cal/00README",
    "cal/FICA.txt",
    "cal/FICA_00000000-21e17c86",
    "cal/XTECALDB",
    "cal/asm/.message",
    "cal/asm/caldb.indx",
    "cal/caldb.config",
    "cal/hexte/.message",
    "cal/hexte/bcf/collresp/hexte_98oct20_pwa.fov",
    "cal/hexte/bcf/collresp/hexte_98oct20_pwb.fov",
    "cal/hexte/caldb.indx",
    "cal/hexte/cpf/responses/hexte_98oct20_pwa.arf",
    "cal/hexte/cpf/responses/hexte_98oct20_pwb.arf",
    "cal/hexte_00may26_pwa.arf.gz",
    "cal/hexte_00may26_pwb013.arf.gz",
    "cal/pca/.message",
    "cal/pca/bcf/collresp/96jun05/p0coll_96jun05.fits",
    "cal/pca/bcf/collresp/96jun05/p1coll_96jun05.fits",
    "cal/pca/bcf/collresp/96jun05/p2coll_96jun05.fits",
    "cal/pca/bcf/collresp/96jun05/p3coll_96jun05.fits",
    "cal/pca/bcf/collresp/96jun05/p4coll_96jun05.fits",
    "cal/pca/bcf/collresp/96jun05/pcacoll_96jun05.fits",
    "cal/pca/bcf/e2c/pca_e2c_e03v04.fits",
    "cal/pca/bcf/eds/edsgcor_e04v00.fits",
    "cal/pca/caldb.indx",
    "cal/xh97mar20c_pwa.rmf.gz",
    "cal/xh97mar20c_pwb013.rmf.gz",
    "clock/FPclock_Day4246",
    "eds/FH2e_15de310c-15de8c76.gz",
    "fds/FH01_15de310c-15de8c76.gz",
    "fds/FH02_15de310c-15de8c76.gz",
    "fds/FH04_15de310c-15de8c7c.gz",
    "fds/FH05_15de310c-15de8c7c.gz",
    "fds/FH09_15de310c-15de8c6e.gz",
    "fds/FH0a_15de310c-15de8c6e.gz",
    "fds/FH0c_15de310c-15de8c64.gz",
    "fds/FH60_15de310c-15de8c64.gz",
    "fds/FHb4_15de310c-15de8c7b.gz",
    "gsace/FHc1_15de310c-15de8c78.gz",
    "gsace/FHc3_15de310c-15de8c70.gz",
    "hexte/FH53_15de310c-15de8c90.gz",
    "hexte/FH59_15de310c-15de8c90.gz",
    "hexte/FHfb_15de3140-15de8cc0.gz",
    "hexte/FHfc_15de3140-15de8cc0.gz",
    "hexte/FHfd_15de3180-15de8d00.gz",
    "hexte/FHfe_15de3180-15de8d00.gz",
    "hexte/FS50_15de310c-15de3460.gz",
    "hexte/FS50_15de4460-15de5a60.gz",
    "hexte/FS50_15de5a60-15de7210.gz",
    "hexte/FS50_15de7210-15de8a40.gz",
    "hexte/FS50_15de8a40-15de8c7b.gz",
    "hexte/FS52_15de4110-15de5110.gz",
    "hexte/FS52_15de5110-15de6110.gz",
    "hexte/FS52_15de6110-15de7110.gz",
    "hexte/FS52_15de7110-15de8110.gz",
    "hexte/FS52_15de8110-15de8c7b.gz",
    "hexte/FS54_15de310c-15de3400.gz",
    "hexte/FS54_15de3400-15de4400.gz",
    "hexte/FS54_15de6400-15de7400.gz",
    "hexte/FS54_15de7400-15de8400.gz",
    "hexte/FS55_15de3200-15de4200.gz",
    "hexte/FS55_15de4400-15de5600.gz",
    "hexte/FS55_15de5600-15de6600.gz",
    "hexte/FS55_15de6600-15de7600.gz",
    "hexte/FS55_15de7600-15de8600.gz",
    "hexte/FS55_15de8600-15de8c7b.gz",
    "hexte/FS56_15de310c-15de3450.gz",
    "hexte/FS56_15de3450-15de4460.gz",
    "hexte/FS56_15de4460-15de5a70.gz",
    "hexte/FS56_15de5a70-15de7230.gz",
    "hexte/FS56_15de7230-15de8a50.gz",
    "hexte/FS56_15de8a50-15de8c7b.gz",
    "hexte/FS58_15de3100-15de4100.gz",
    "hexte/FS58_15de4100-15de5110.gz",
    "hexte/FS58_15de5110-15de6120.gz",
    "hexte/FS58_15de6120-15de7120.gz",
    "hexte/FS58_15de7120-15de8120.gz",
    "hexte/FS58_15de8120-15de8c7b.gz",
    "ifog/FH77_15de310c-15de877a.gz",
    "ifog/FH78_15de310c-15de877b.gz",
    "ipsdu/FHc4_15de310c-15de8c7c.gz",
    "ipsdu/FHc6_15de310c-15de8c75.gz",
    "ipsdu/FHc8_15de3172-15de8c73.gz",
    "orbit/FPorbit_Day4246",
    "pca/FH5a_15de310c-15de8c88.gz",
    "pca/FH5b_15de310c-15de8c88.gz",
    "pca/FH5c_15de310c-15de8c88.gz",
    "pca/FH5d_15de310c-15de8c88.gz",
    "pca/FS37_15de310c-15de4b10.gz",
    "pca/FS37_15de5130-15de6154.gz",
    "pca/FS37_15de8050-15de8c74.gz",
    "pca/FS3b_15de310c-15de4b10.gz",
    "pca/FS3b_15de5140-15de6154.gz",
    "pca/FS3b_15de6920-15de7810.gz",
    "pca/FS3b_15de8050-15de8c74.gz",
    "pca/FS3f_15de310c-15de4ae0.gz",
    "pca/FS3f_15de8050-15de8c50.gz",
    "pca/FS46_15de310c-15de4ae0.gz",
    "pca/FS46_15de6920-15de77a0.gz",
    "pca/FS4a_15de310c-15de4b10.gz",
    "pca/FS4a_15de5140-15de6150.gz",
    "pca/FS4a_15de6920-15de7810.gz",
    "pca/FS4a_15de8060-15de8c7b.gz",
    "pca/FS4f_15de310c-15de4ae0.gz",
    "pca/FS4f_15de5140-15de6140.gz",
    "pca/FS4f_15de6920-15de77e0.gz",
    "pca/FS4f_15de8050-15de8c50.gz",
    "pca/GX_15de310c-15de4b10.evt.gz",
    "pca/GX_15de5130-15de6154.evt.gz",
    "pca/GX_15de6920-15de7810.evt.gz",
    "pse/FHca_15de310c-15de8c7c.gz",
    "pse/FHce_15de310c-15de8c77.gz",
    "pse/FHcf_15de310c-15de8c7b.gz",
    "spsdu/FH24_15de310c-15de8c75.gz",
    "spsdu/FH26_15de310c-15de8c7b.gz",
    "spsdu/FH28_15de310c-15de8c7b.gz",
    "spsdu/FH29_15de310c-15de8c7b.gz",
    "spsdu/FH79_15de310c-15de8c7b.gz",
    "spsdu/FH7a_15de310c-15de8c7b.gz",
    "spsdu/FH7b_15de310c-15de8c75.gz",
    "spsdu/FH7c_15de310c-15de8c7b.gz",
    "spsdu/FH7d_15de310c-15de8c7b.gz",
    "stdprod/FHee_15de310c-15de8c78.gz",
    "stdprod/FHef_15de310c-15de8c78.gz",
    "stdprod/FHf1_15de310c-15de8c7b.gz",
    "stdprod/FHf3_15de310c-15de8c78.gz",
    "stdprod/GIFS/x90171010100_xfl.gif",
    "stdprod/GIFS/xh90171010100_0net_pha.gif",
    "stdprod/GIFS/xh90171010100_1both_pha.gif",
    "stdprod/GIFS/xh90171010100_1net_lc.gif",
    "stdprod/GIFS/xh90171010100_1net_pha.gif",
    "stdprod/GIFS/xh90171010100_b0_pha.gif",
    "stdprod/GIFS/xh90171010100_b0b_lc.gif",
    "stdprod/GIFS/xh90171010100_b0c_lc.gif",
    "stdprod/GIFS/xh90171010100_b1b_lc.gif",
    "stdprod/GIFS/xh90171010100_n1b_lc.gif",
    "stdprod/GIFS/xh90171010100_n1c_lc.gif",
    "stdprod/GIFS/xh90171010100_rock.gif",
    "stdprod/GIFS/xh90171010100_s0_pha.gif",
    "stdprod/GIFS/xh90171010100_s0a_lc.gif",
    "stdprod/GIFS/xh90171010100_s1a_lc.gif",
    "stdprod/GIFS/xh90171010100_s1b_lc.gif",
    "stdprod/GIFS/xh90171010100_s1c_lc.gif",
    "stdprod/GIFS/xp90171010100_b2a_lc.gif",
    "stdprod/GIFS/xp90171010100_b2b_lc.gif",
    "stdprod/GIFS/xp90171010100_bkg_lc.gif",
    "stdprod/GIFS/xp90171010100_both_pha.gif",
    "stdprod/GIFS/xp90171010100_n2a_lc.gif",
    "stdprod/GIFS/xp90171010100_n2b_lc.gif",
    "stdprod/GIFS/xp90171010100_n2d_lc.gif",
    "stdprod/GIFS/xp90171010100_n2e_lc.gif",
    "stdprod/GIFS/xp90171010100_net_lc.gif",
    "stdprod/GIFS/xp90171010100_s2_pha.gif",
    "stdprod/GIFS/xp90171010100_s2a_lc.gif",
    "stdprod/GIFS/xp90171010100_s2b_lc.gif",
    "stdprod/GIFS/xp90171010100_s2c_lc.gif",
    "stdprod/GIFS/xp90171010100_s2d_lc.gif",
    "stdprod/GIFS/xp90171010100_s2e_lc.gif",
    "stdprod/GIFS/xp90171010100_src_lc.gif",
    "stdprod/hexte_00may26_pwa.arf.gz",
    "stdprod/hexte_00may26_pwb013.arf.gz",
    "stdprod/x90171010100.gti.gz",
    "stdprod/x90171010100.xfl.gz",
    "stdprod/xh90171010100_b0.pha.gz",
    "stdprod/xh90171010100_b0a.lc.gz",
    "stdprod/xh90171010100_b0b.lc.gz",
    "stdprod/xh90171010100_b0c.lc.gz",
    "stdprod/xh90171010100_b1.pha.gz",
    "stdprod/xh90171010100_b1a.lc.gz",
    "stdprod/xh90171010100_b1b.lc.gz",
    "stdprod/xh90171010100_b1c.lc.gz",
    "stdprod/xh90171010100_n0a.lc.gz",
    "stdprod/xh90171010100_n0c.lc.gz",
    "stdprod/xh90171010100_n1a.lc.gz",
    "stdprod/xh90171010100_n1b.lc.gz",
    "stdprod/xh90171010100_n1c.lc.gz",
    "stdprod/xh90171010100_s0a.lc.gz",
    "stdprod/xh90171010100_s0b.lc.gz",
    "stdprod/xh90171010100_s0c.lc.gz",
    "stdprod/xh90171010100_s1.pha.gz",
    "stdprod/xh90171010100_s1b.lc.gz",
    "stdprod/xh90171010100_s1c.lc.gz",
    "stdprod/xh97mar20c_pwb013.rmf.gz",
    "stdprod/xp90171010100.rsp.gz",
    "stdprod/xp90171010100_b2.pha.gz",
    "stdprod/xp90171010100_b2a.lc.gz",
    "stdprod/xp90171010100_b2c.lc.gz",
    "stdprod/xp90171010100_b2d.lc.gz",
    "stdprod/xp90171010100_n2a.lc.gz",
    "stdprod/xp90171010100_n2d.lc.gz",
    "stdprod/xp90171010100_n2e.lc.gz",
    "stdprod/xp90171010100_s2.pha.gz",
    "stdprod/xp90171010100_s2a.lc.gz",
    "stdprod/xp90171010100_s2b.lc.gz",
]

KEPT_90171_01_01_00 = [
    "orbit/FPorbit_Day4246",
    "pca/FS4a_15de310c-15de4b10.gz",
    "pca/FS4a_15de5140-15de6150.gz",
    "pca/FS4a_15de6920-15de7810.gz",
    "pca/FS4a_15de8060-15de8c7b.gz",
    "pca/GX_15de310c-15de4b10.evt.gz",
    "pca/GX_15de5130-15de6154.evt.gz",
    "pca/GX_15de6920-15de7810.evt.gz",
    "stdprod/x90171010100.xfl.gz",
]


def what_a_filter_keeps(arguments, entries, prefix="xte/data/archive/AO9/P90171/90171-01-01-00/"):
    """
    Run a download filter over a recorded listing, the way a transport would.

    Both transports match against the whole remote name: an HTTPS URL for one, a bucket key
    for the other. The filter has to read both the same way. This is test_xmm.py's
    helper of the same name, over RXTE's archive paths.
    """
    include = arguments.get("re_include", "")
    exclude = arguments.get("re_exclude", "")
    include = re.compile(include) if include else None
    exclude = re.compile(exclude) if exclude else None

    kept = {}
    for flavour, base in [
        ("https", "https://heasarc.gsfc.nasa.gov/FTP/" + prefix),
        ("s3", prefix),
    ]:
        kept[flavour] = [
            entry
            for entry in entries
            if (include is None or include.search(base + entry))
            and not (exclude is not None and exclude.search(base + entry))
        ]
    assert kept["https"] == kept["s3"], "the filter reads an S3 key and a URL differently"
    return kept["s3"]


class TestRxtePaths:
    def test_the_output_directory_is_the_obsid_under_out_data_path(self):
        assert rxte_base_output_path(config=CONFIG, obsid=OBSID) == os.path.join("out", OBSID)


class TestTheDownloadFilter:
    def test_it_keeps_exactly_what_the_reduction_reads(self):
        """
        Every GoodXenon event file, every Standard2 file, the filter file and the orbit
        file, and nothing else -- not HEXTE, not the calibration copy, not the
        per-observation clock file, which barycorr does not read for RXTE.
        """
        kept = what_a_filter_keeps(rxte_download_filter({}), LISTING_90171_01_01_00)

        assert kept == sorted(KEPT_90171_01_01_00)

    def test_the_filter_file_comes_but_not_its_preview_image(self):
        """
        stdprod/GIFS/x<obsid>_xfl.gif is the one near-miss in the listing: a plot of the
        filter file whose name also contains xfl.
        """
        entries = ["stdprod/GIFS/x90171010100_xfl.gif", "stdprod/x90171010100.xfl.gz"]

        assert what_a_filter_keeps(rxte_download_filter({}), entries) == [
            "stdprod/x90171010100.xfl.gz"
        ]

    def test_early_observations_with_shorter_time_stamps_are_kept_too(self):
        """
        File names carry the start and stop times in hexadecimal, and in 1997 those had
        seven digits rather than eight. Real names from 20303-02-06-00.
        """
        entries = [
            "orbit/FPorbit_Day1362",
            "pca/FS4a_703d500-703e480.gz",
            "pca/GX_703d4f0-703e2b0.evt.gz",
            "stdprod/x20303020600.xfl.gz",
        ]
        prefix = "xte/data/archive/AO2/P20303/20303-02-06-00/"

        assert what_a_filter_keeps(rxte_download_filter({}), entries, prefix) == entries


def write_filter_file(path, rows, timedel=16.0, timezero=3.37842846, t0=476110216.0):
    """
    A standard filter file with only the columns the screening reads.

    ``rows`` is one dict per 16-second sample; anything left out of a row takes a value
    that passes every cut, so a test can name the one quantity it is about.
    """
    n = len(rows)
    defaults = {"ELV": 45.0, "OFFSET": 0.001, "TIME_SINCE_SAA": 60.0}
    for pcu in range(5):
        defaults[f"PCU{pcu}_ON"] = 1
        defaults[f"ELECTRON{pcu}"] = 0.05

    columns = {
        name: np.array([row.get(name, value) for row in rows]) for name, value in defaults.items()
    }
    columns["Time"] = t0 + timedel * np.arange(n)
    columns["NUM_PCU_ON"] = sum(columns[f"PCU{pcu}_ON"] for pcu in range(5))

    hdu = fits.BinTableHDU(Table(columns), name="XTE_SA")
    hdu.header["TIMEDEL"] = timedel
    hdu.header["TIMEZERO"] = timezero
    hdu.header["TIMEPIXR"] = 0
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


class TestPerPcuGtis:
    def test_a_clean_stretch_is_one_interval_per_pcu(self, tmp_path):
        """
        Ten good samples give one interval running from the first sample's time to one
        TIMEDEL past the last, and every PCU that was on gets the same one.
        """
        path = write_filter_file(tmp_path / "x.xfl", [{}] * 10)

        gtis = rxte_pcu_gtis(path)

        assert sorted(gtis) == [0, 1, 2, 3, 4]
        for pcu in range(5):
            assert gtis[pcu].tolist() == [[476110219.37842846, 476110379.37842846]]

    def test_the_filter_file_timezero_is_added(self, tmp_path):
        """
        The filter file and the event file carry the same TIMEZERO, so the intervals are
        only comparable with event times if both get it. Leaving it off the filter side
        shifted every boundary by 3.4 s.
        """
        path = write_filter_file(tmp_path / "x.xfl", [{}] * 2, timezero=100.0)

        assert rxte_pcu_gtis(path)[2].tolist() == [[476110316.0, 476110348.0]]

    def test_a_pcu_that_was_off_gets_no_good_time(self, tmp_path):
        """A PCU switched off for the whole pointing keeps its key and gets no time."""
        path = write_filter_file(tmp_path / "x.xfl", [{"PCU3_ON": 0}] * 4)

        gtis = rxte_pcu_gtis(path)

        assert len(gtis[3]) == 0
        assert len(gtis[2]) == 1

    def test_the_pcus_get_different_intervals(self, tmp_path):
        """
        The point of screening per PCU: they are not on at the same times, so one interval
        list for the whole observation cannot describe the collecting area.
        """
        rows = [{}, {"PCU0_ON": 0}, {}, {}]
        path = write_filter_file(tmp_path / "x.xfl", rows)

        gtis = rxte_pcu_gtis(path)

        assert len(gtis[0]) == 2
        assert len(gtis[2]) == 1

    def test_the_standard_cuts_each_remove_their_sample(self, tmp_path):
        """
        Earth elevation, pointing offset, the SAA passage and the electron ratio: each cut
        on its own splits a clean stretch in two. The reduced screening this replaces
        applied only the first two.
        """
        for bad in [
            {"ELV": 5.0},
            {"OFFSET": 0.5},
            {"TIME_SINCE_SAA": 10.0},
            {"ELECTRON2": 0.5},
        ]:
            path = write_filter_file(tmp_path / "x.xfl", [{}, bad, {}])

            assert len(rxte_pcu_gtis(path)[2]) == 2, bad

    def test_a_negative_time_since_saa_is_good_time(self, tmp_path):
        """
        TIME_SINCE_SAA is negative when the satellite has not been through the anomaly in
        the recorded window. Reading it as "less than 30 minutes ago" would throw away a
        quarter of the 1997 and 2004 exposure for nothing.
        """
        path = write_filter_file(tmp_path / "x.xfl", [{}, {"TIME_SINCE_SAA": -20.0}, {}])

        assert len(rxte_pcu_gtis(path)[2]) == 1

    def test_a_nan_electron_ratio_is_not_good_time(self, tmp_path):
        """
        ELECTRONn is NaN in a handful of samples where the PCU is flagged on, and a NaN
        comparison is False, so the sample is dropped. Missing housekeeping is not
        evidence of a healthy detector.
        """
        path = write_filter_file(tmp_path / "x.xfl", [{}, {"ELECTRON2": np.nan}, {}])

        assert len(rxte_pcu_gtis(path)[2]) == 2

    def test_a_pointing_with_no_good_time_returns_empty_intervals(self, tmp_path):
        """An observation screened away entirely is empty, not missing: every PCU is
        still a key, so a caller does not have to tell "off" from "screened out"."""
        path = write_filter_file(tmp_path / "x.xfl", [{"ELV": 1.0}] * 4)

        gtis = rxte_pcu_gtis(path)

        assert sorted(gtis) == [0, 1, 2, 3, 4]
        assert all(len(g) == 0 for g in gtis.values())

    def test_only_the_requested_pcus_are_screened(self, tmp_path):
        """PCU0 lost its propane veto layer in 2000 and carries far more background, so
        the M82 search drops it; asking for a subset must not cost the others."""
        path = write_filter_file(tmp_path / "x.xfl", [{}] * 4)

        assert sorted(rxte_pcu_gtis(path, pcus=(2, 3, 4))) == [2, 3, 4]


def write_event_file(path, times, pcus, anodes=None, phas=None, timezero=3.37842846):
    """
    A GoodXenon event file with the columns and keywords the archive's own files carry.

    ``TIME`` is written relative to ``TIMEZERO``, as in the real files, so a test that
    passes absolute times gets them back only if the reduction adds it.
    """
    n = len(times)
    table = Table(
        {
            "TIME": np.asarray(times, dtype=float) - timezero,
            "Event": np.zeros((n, 24), dtype=bool),
            "PCUID": np.asarray(pcus, dtype=np.uint8),
            "ANODEID": np.asarray(anodes if anodes is not None else [10] * n, dtype=np.uint8),
            "PHA": np.asarray(phas if phas is not None else [50] * n, dtype=np.uint8),
        }
    )
    hdu = fits.BinTableHDU(table, name="XTE_SE")
    hdu.header["TIMEZERO"] = timezero
    hdu.header["TELESCOP"] = "XTE"
    hdu.header["INSTRUME"] = "PCA"
    hdu.header["DATAMODE"] = "GoodXenon_2s"
    hdu.header["TEVTB2"] = "(M[1]{1},S[Zero]{5},E[VPR]{1},D[0:4]{3},E[0:63]{6},C[0:255]{8})"
    hdu.header["MJDREFI"] = 49353
    hdu.header["MJDREFF"] = 0.000696574074
    hdu.header["TIMESYS"] = "TT"
    hdu.header["OBJECT"] = "M82_ULX"
    hdu.header["EXPOSURE"] = 99999.0
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)
    return str(path)


class TestScreenedEvents:
    def test_every_event_file_is_merged_in_time_order(self, tmp_path):
        """
        A GoodXenon pointing can hold several event files -- 25 of the 863 M82 pointings
        do -- and the first version of this module silently used only the first.
        """
        first = write_event_file(tmp_path / "GX_a.evt", [10.0, 30.0], [2, 2])
        second = write_event_file(tmp_path / "GX_b.evt", [20.0, 40.0], [2, 2])
        out = tmp_path / "cl.evt"

        rxte_screened_events([first, second], {2: [[0.0, 100.0]]}, str(out))

        with fits.open(out) as hdul:
            assert hdul["XTE_SE"].data["TIME"].tolist() == [10.0, 20.0, 30.0, 40.0]

    def test_an_event_is_screened_against_its_own_pcu(self, tmp_path):
        """
        The whole point of per-PCU intervals: PCU2 was collecting at t=50 and PCU3 was
        not, so the PCU3 event goes even though the observation was good time.
        """
        path = write_event_file(tmp_path / "GX_a.evt", [50.0, 50.0], [2, 3])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]], 3: [[200.0, 300.0]]}, str(out))

        with fits.open(out) as hdul:
            assert hdul["XTE_SE"].data["PCUID"].tolist() == [2]

    def test_a_pcu_that_was_not_asked_for_contributes_nothing(self, tmp_path):
        """Dropping PCU0 is done by leaving it out of the intervals, and must drop its
        events rather than passing them through unscreened."""
        path = write_event_file(tmp_path / "GX_a.evt", [50.0, 50.0], [0, 2])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]]}, str(out))

        with fits.open(out) as hdul:
            assert hdul["XTE_SE"].data["PCUID"].tolist() == [2]

    def test_the_gti_extension_is_the_union_over_the_pcus(self, tmp_path):
        """
        The GTI a timing tool reads has to say when there are events at all, which is the
        union. The area each unit contributed is recorded separately.
        """
        path = write_event_file(tmp_path / "GX_a.evt", [10.0, 250.0], [2, 3])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]], 3: [[200.0, 300.0]]}, str(out))

        with fits.open(out) as hdul:
            gti = hdul["GTI"].data
            assert gti["START"].tolist() == [0.0, 200.0]
            assert gti["STOP"].tolist() == [100.0, 300.0]
            assert hdul["GTI_PCU2"].data["STOP"].tolist() == [100.0]
            assert hdul["GTI_PCU3"].data["START"].tolist() == [200.0]

    def test_the_exposure_describes_the_screened_data(self, tmp_path):
        """
        The old output inherited EXPOSURE from the unfiltered file, so every rate computed
        from its header was wrong. Here it is the good time actually kept.
        """
        path = write_event_file(tmp_path / "GX_a.evt", [10.0, 250.0], [2, 3])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]], 3: [[200.0, 300.0]]}, str(out))

        with fits.open(out) as hdul:
            header = hdul["XTE_SE"].header
            assert header["EXPOSURE"] == 200.0
            assert header["ONTIME"] == 200.0
            assert (header["TSTART"], header["TSTOP"]) == (0.0, 300.0)

    def test_timezero_is_absorbed_into_the_times(self, tmp_path):
        """
        The output carries absolute times and TIMEZERO=0, so nothing downstream has to
        remember to add 3.4 s -- which is exactly the mistake the first version made.
        """
        path = write_event_file(tmp_path / "GX_a.evt", [50.0], [2])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]]}, str(out))

        with fits.open(out) as hdul:
            assert hdul["XTE_SE"].data["TIME"].tolist() == [50.0]
            assert hdul["XTE_SE"].header["TIMEZERO"] == 0.0

    def test_the_header_keeps_what_a_timing_tool_needs(self, tmp_path):
        """
        Stingray reads these files directly and calibrates PHA to keV from TEVTB2 and the
        epoch, so the mission keywords have to survive the screening.
        """
        path = write_event_file(tmp_path / "GX_a.evt", [50.0], [2])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]]}, str(out))

        with fits.open(out) as hdul:
            header = hdul["XTE_SE"].header
            assert header["TELESCOP"] == "XTE"
            assert header["INSTRUME"] == "PCA"
            assert header["MJDREFI"] == 49353
            assert "C[0:255]" in header["TEVTB2"]

    def test_the_redundant_event_column_is_dropped(self, tmp_path):
        """
        ``Event`` is the raw 24-bit field that PCUID, ANODEID and PHA were decoded from.
        Checked on 1.66 million archive events, the three columns reproduce it exactly, so
        keeping it only invites somebody to decode it a second time and differently.
        """
        path = write_event_file(tmp_path / "GX_a.evt", [50.0], [2])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]]}, str(out))

        with fits.open(out) as hdul:
            names = [c.name for c in hdul["XTE_SE"].columns]
            assert names == ["TIME", "PCUID", "ANODEID", "PHA"]

    def test_an_observation_screened_away_entirely_writes_nothing(self, tmp_path):
        """No good time means no product, and the caller is told so rather than handed an
        empty file that looks like a reduction."""
        path = write_event_file(tmp_path / "GX_a.evt", [50.0], [2])
        out = tmp_path / "cl.evt"

        assert rxte_screened_events([path], {2: []}, str(out)) is None
        assert not out.exists()


class TestFindingTheInputs:
    def make_pointing(self, tmp_path, obsid="94123-01-19-00"):
        """A pointing directory laid out the way the download filter leaves it."""
        root = tmp_path / obsid
        for part in ("pca", "stdprod", "orbit"):
            (root / part).mkdir(parents=True)
        (root / "pca" / "GX_b.evt.gz").write_bytes(b"")
        (root / "pca" / "GX_a.evt.gz").write_bytes(b"")
        (root / "pca" / "FS4a_a.gz").write_bytes(b"")
        (root / "stdprod" / "x94123011900.xfl.gz").write_bytes(b"")
        (root / "orbit" / "FPorbit_Day5510").write_bytes(b"")
        return root

    def test_it_finds_every_event_file_and_the_housekeeping(self, tmp_path):
        """All the GoodXenon files, the filter file the screening needs and the orbit
        file barycorr needs, and nothing from the other PCA data modes."""
        root = self.make_pointing(tmp_path)

        found = find_rxte_inputs(str(root))

        assert [os.path.basename(f) for f in found["event_files"]] == ["GX_a.evt.gz", "GX_b.evt.gz"]
        assert os.path.basename(found["filter_file"]) == "x94123011900.xfl.gz"
        assert os.path.basename(found["orbit_file"]) == "FPorbit_Day5510"

    def test_a_pointing_with_no_goodxenon_is_reported_not_raised(self, tmp_path):
        """Seven M82 pointings have no event-mode data at all; the batch has to walk past
        them rather than stop."""
        root = self.make_pointing(tmp_path)
        for path in (root / "pca").glob("GX_*"):
            path.unlink()

        assert find_rxte_inputs(str(root))["event_files"] == []

    def test_the_macos_sidecar_files_are_not_mistaken_for_data(self, tmp_path):
        """The archive copy lives on an exFAT drive, where macOS writes a ``._`` sidecar
        next to every file. They are not FITS and astropy chokes on them."""
        root = self.make_pointing(tmp_path)
        (root / "pca" / "._GX_a.evt.gz").write_bytes(b"")
        (root / "orbit" / "._FPorbit_Day5510").write_bytes(b"")

        found = find_rxte_inputs(str(root))

        assert [os.path.basename(f) for f in found["event_files"]] == ["GX_a.evt.gz", "GX_b.evt.gz"]
        assert os.path.basename(found["orbit_file"]) == "FPorbit_Day5510"


class TestTheGtiExtensionHeaders:
    def test_a_gti_extension_carries_the_time_keywords(self, tmp_path):
        """
        barycorr corrects a GTI extension only if it can read its time scale, and skips it
        in silence otherwise. Verified on a real pointing: without these keywords the
        events moved by 298.3 s and the intervals did not, so every interval boundary was
        five minutes wrong while the file still looked barycentred.
        """
        path = write_event_file(tmp_path / "GX_a.evt", [50.0], [2])
        out = tmp_path / "cl.evt"

        rxte_screened_events([path], {2: [[0.0, 100.0]]}, str(out))

        with fits.open(out) as hdul:
            for name in ("GTI", "GTI_PCU2"):
                header = hdul[name].header
                assert header["TIMESYS"] == "TT"
                assert header["TIMEUNIT"] == "s"
                assert header["MJDREFI"] == 49353
                assert header["HDUCLASS"] == "OGIP"
                assert header["HDUCLAS1"] == "GTI"
                assert (header["TSTART"], header["TSTOP"]) == (0.0, 100.0)


class TestTheFlowDefaults:
    def test_the_default_config_is_actually_used(self, tmp_path, monkeypatch):
        """
        ``config={}`` as a default never falls back to DEFAULT_CONFIG, because ``{}`` is
        not ``None``, so the flow raised KeyError on out_data_path before it did anything.
        Issue 27 in known_issues.rst.
        """
        monkeypatch.chdir(tmp_path)
        seen = {}

        def stub(raw_data_dir, obsid, config):
            seen["config"] = config
            return None

        monkeypatch.setattr(rxte, "reduce_observation", stub)

        assert process_rxte_obsid.fn("94123-01-19-00") is None
        assert seen["config"]["out_data_path"] == str(tmp_path)
