import os

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import barycenter as bary
from heasarc_retrieve_pipeline.barycenter import barycenter_file, barycentered_file_name


class TestBarycenteredFileName:
    """``_bary`` goes before the extension, whatever the extension is."""

    def test_a_nustar_event_file(self):
        assert barycentered_file_name("nu123A01_cl.evt") == "nu123A01_cl_bary.evt"

    def test_a_gzipped_file_stays_gzipped(self):
        """The compression suffix stays last: a gzipped input gives a gzipped output."""
        assert barycentered_file_name("nu123A01_cl.evt.gz") == "nu123A01_cl_bary.evt.gz"

    def test_a_fits_file(self):
        """Missions that call their event files something else are the reason this
        function exists: ``str.replace(".evt", "_bary.evt")`` leaves these untouched, and
        an output name equal to the input is worse than an ugly one."""
        assert barycentered_file_name("obs_events.fits") == "obs_events_bary.fits"

    def test_a_chandra_style_extension(self):
        assert barycentered_file_name("acisf_evt2.fits") == "acisf_evt2_bary.fits"

    def test_an_xmm_style_extension(self):
        assert barycentered_file_name("P0123_events.ds") == "P0123_events_bary.ds"

    def test_a_gzipped_fits_file(self):
        assert barycentered_file_name("obs.fits.gz") == "obs_bary.fits.gz"

    def test_directories_are_preserved(self):
        name = barycentered_file_name(os.path.join("out", "90901333002", "x.evt"))

        assert name == os.path.join("out", "90901333002", "x_bary.evt")

    def test_a_dot_in_a_directory_name_is_not_an_extension(self):
        """``str.replace`` would rename the directory instead of the file."""
        name = barycentered_file_name(os.path.join("out.evt", "x.evt"))

        assert name == os.path.join("out.evt", "x_bary.evt")

    def test_a_file_with_no_extension(self):
        assert barycentered_file_name("events") == "events_bary"


class TestCallingItOutsideAFlow:
    def test_an_existing_output_is_returned_without_a_prefect_run(self, tmp_path):
        """
        ``get_run_logger`` raises outside a flow, so the task could not be called through
        ``.fn`` at all -- a batch driver calling it directly got
        ``MissingContextError`` for every observation before it reached barycorr.
        """
        outfile = tmp_path / "x_bary.evt"
        outfile.write_bytes(b"already there")

        result = barycenter_file.fn(str(tmp_path / "x.evt"), "orbit", ra=1.0, dec=2.0)

        assert result == str(outfile)


#: How each mission's orbit file spells its columns, and in what unit (1 = metres, 1e3 =
#: kilometres): the shape the ``barycenter`` package reads, and nothing more.
TINY_ORBITS = {
    "NICER": (("X", "Y", "Z", "Vx", "Vy", "Vz"), 1.0, "ORBIT", 56658),
    "XMM": (("GEI_X", "GEI_Y", "GEI_Z", "VX", "VY", "VZ"), 1e3, "ORBIT", 50814),
    "CHANDRA": (("X", "Y", "Z", "Vx", "Vy", "Vz"), 1.0, "ORBITEPHEM", 50814),
}


def tiny_event_and_orbit_files(tmp_path, telescop="NICER"):
    """
    A 1000 s event file and a circular low-Earth orbit covering it, for one mission.

    Shared with the XMM and Chandra tests. Good enough for the ``barycenter`` package to
    run on, which is all it is for: accuracy is checked on real data.
    """
    names, unit, extname, mjdrefi = TINY_ORBITS[telescop]
    common = {"TELESCOP": telescop, "MJDREFI": mjdrefi, "MJDREFF": 7.775925925925930e-04}
    common.update(TIMESYS="TT", TIMEREF="LOCAL", TIMEUNIT="s")

    t = np.arange(-100.0, 1101.0, 10.0)
    phase = 2 * np.pi * t / 5600.0
    radius, speed = 6.9e6 / unit, 7.6e3 / unit
    orbit = fits.BinTableHDU.from_columns(
        [fits.Column(name="TIME", format="D", array=t)]
        + [
            fits.Column(name=name, format="D", array=values)
            for name, values in zip(
                names,
                (
                    radius * np.cos(phase),
                    radius * np.sin(phase),
                    np.zeros_like(t),
                    -speed * np.sin(phase),
                    speed * np.cos(phase),
                    np.zeros_like(t),
                ),
            )
        ],
        name=extname,
    )
    events = fits.BinTableHDU.from_columns(
        [fits.Column(name="TIME", format="D", array=np.linspace(100.0, 900.0, 50))],
        name="EVENTS",
    )
    gti = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="START", format="D", array=[50.0]),
            fits.Column(name="STOP", format="D", array=[950.0]),
        ],
        name="GTI",
    )
    for hdu in (orbit, events, gti):
        hdu.header.update(common)
    events.header.update(TSTART=50.0, TSTOP=950.0, RA_OBJ=83.63, DEC_OBJ=22.01)

    orbit_file, event_file = tmp_path / "tiny.orb", tmp_path / "tiny_cl.evt"
    fits.HDUList([fits.PrimaryHDU(), orbit]).writeto(orbit_file)
    fits.HDUList([fits.PrimaryHDU(header=fits.Header(common)), events, gti]).writeto(event_file)
    return str(event_file), str(orbit_file)


class TestToolChoice:
    def test_the_package_is_the_default(self, monkeypatch):
        monkeypatch.setattr(bary, "HAS_BARYCENTER", True)
        assert bary.barycenter_tool({}) == bary.PACKAGE_TOOL

    def test_without_the_package_the_official_tool_takes_over(self, monkeypatch):
        """A broken install of the package is a fallback with a warning, not a failed
        reduction that the mission's own tool could have finished."""
        monkeypatch.setattr(bary, "HAS_BARYCENTER", False)
        assert bary.barycenter_tool({}) == bary.OFFICIAL_TOOL

    def test_an_unknown_tool_is_refused(self):
        with pytest.raises(ValueError, match="barycenter_tool"):
            bary.barycenter_tool({"barycenter_tool": "axbary"})

    def test_a_kernel_file_cannot_go_to_the_official_tools(self):
        """barycorr and barycen open only the kernels they ship, by number."""
        with pytest.raises(ValueError, match="DEnnn"):
            bary.official_ephemeris_number("/data/de440.bsp")


@pytest.mark.skipif(not bary.HAS_BARYCENTER, reason="the barycenter package is not installed")
class TestWithThePackage:
    def test_events_and_gtis_move_together_onto_tdb(self, tmp_path):
        """
        The package route writes a TDB file and moves the GTI boundaries by the same
        correction as the events -- the thing barycorr silently failed to do for RXTE when
        a GTI extension lacked its time keywords. Astropy's built-in ephemeris keeps the
        test offline; accuracy against the official tools is checked on real data instead.
        """
        event_file, orbit_file = tiny_event_and_orbit_files(tmp_path)

        out = barycenter_file.fn(
            event_file, orbit_file, ra=83.63, dec=22.01, tool="barycenter", ephem="builtin"
        )

        with fits.open(event_file) as before, fits.open(out) as after:
            assert after["EVENTS"].header["TIMESYS"] == "TDB"
            shift = after["EVENTS"].data["TIME"] - before["EVENTS"].data["TIME"]
            gti_shift = after["GTI"].data["START"][0] - before["GTI"].data["START"][0]
        assert 0 < np.abs(shift).max() < 600
        assert np.isclose(gti_shift, shift[0], atol=0.01)
