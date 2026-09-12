"""What the real CIAO tasks actually do, checked against a real CIAO.

Everywhere else in the suite CIAO is a recorded double, shaped by what someone believed a
task does. Three of those beliefs are load-bearing for the Chandra reduction, and all
three were first measured on obsid ``5644``:

* ``dmcopy "evt2.fits[@flare.gti]"`` **intersects** the named table with the good times the
  file already carries, rather than replacing them -- which is why ``chandra_flare_gti``
  may write an interval wider than the observation and still get the right exposure.
* ``[exclude sky=...]`` beside any other filter is refused with "cannot mix EXCLUDE and
  FILTER", which is why ``chandra_flare_curve_filter`` cuts the source out with region
  algebra, ``field()-circle(...)``, instead.
* ``dmextract opt=ltc1`` writes a bin for every stretch between ``TSTART`` and ``TSTOP``,
  including the gaps in the good times, with ``EXPOSURE = 0`` and ``COUNT_RATE = 0`` --
  which is why ``read_chandra_lightcurve`` turns them into ``NaN``.

This module is where they are checked against the tool rather than against a comment. It
uses a small fabricated event list, so it needs no calibration and no download, and every
task runs in a fraction of a second. ``specextract``, ``psfsize_srcs``, ``pileup_map``,
``axbary`` and ``chandra_repro`` need calibration files or a real observation and stay
stubbed; they were verified end to end on ``5644`` and ``8190`` instead.

Everything here is skipped unless ``ASCDS_INSTALL`` is set and ``dmlist`` is on ``PATH``;
see ``conftest.py``. From an environment without CIAO's Python, putting CIAO on the path
is enough::

    export ASCDS_INSTALL=$HOME/mamba/envs/ciao PATH=$HOME/mamba/envs/ciao/bin:$PATH
"""

import os

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline import chandra, ciao

pytestmark = pytest.mark.ciao

#: The fabricated observation's own good times: a 2 000 s hole in a 10 000 s span.
OWN_GTI = [[0.0, 4000.0], [6000.0, 10000.0]]


def an_event_list(path, n=4000, seed=1):
    """
    A minimal ACIS-like level-2 event list: events, a sky vector, and a GTI block per chip.

    Only what the Data Model needs to parse a filter on it. ``MTYPE1``/``MFORM1`` declare
    ``sky`` as the vector of ``x`` and ``y``, which is how a real event file makes
    ``[sky=circle(...)]`` mean something. The events are drawn inside the good times, as a
    real level-2 file's are.
    """
    rng = np.random.default_rng(seed)
    starts, stops = np.array(OWN_GTI).T
    which = rng.integers(0, len(starts), n)
    time = np.sort(rng.uniform(starts[which], stops[which]))
    columns = [
        fits.Column("time", "1D", unit="s", array=time),
        fits.Column("ccd_id", "1I", array=np.full(n, 7)),
        fits.Column("x", "1E", unit="pixel", array=rng.uniform(3900.0, 4300.0, n)),
        fits.Column("y", "1E", unit="pixel", array=rng.uniform(3900.0, 4300.0, n)),
        fits.Column("energy", "1E", unit="eV", array=rng.uniform(300.0, 9000.0, n)),
    ]
    events = fits.BinTableHDU.from_columns(columns, name="EVENTS")
    ontime = float(np.sum(stops - starts))
    events.header.update(
        MTYPE1="sky",
        MFORM1="x,y",
        TELESCOP="CHANDRA",
        INSTRUME="ACIS",
        TIMESYS="TT",
        TIMEUNIT="s",
        MJDREF=50814.0,
        TIMEZERO=0.0,
        TSTART=0.0,
        TSTOP=10000.0,
        ONTIME=ontime,
        LIVETIME=ontime,
        EXPOSURE=ontime,
    )
    good = fits.BinTableHDU.from_columns(
        [
            fits.Column("START", "1D", unit="s", array=starts),
            fits.Column("STOP", "1D", unit="s", array=stops),
        ],
        name="GTI7",
    )
    good.header["CCD_ID"] = 7
    fits.HDUList([fits.PrimaryHDU(), events, good]).writeto(path, overwrite=True)
    return str(path)


@pytest.fixture
def workspace(tmp_path):
    """A fabricated observation, a configuration pointing into ``tmp_path``, and the
    per-observation CIAO environment the pipeline itself would use."""
    config = dict(chandra.DEFAULT_CONFIG, out_data_path=str(tmp_path / "out"))
    events = an_event_list(tmp_path / "evt2.fits")
    observation = chandra.Observation(
        obsid="5644",
        detector="aciss",
        grating="NONE",
        mode="timed",
        time_resolution=chandra.TimeResolution(0.44104, "acis_frame_time", ""),
        chips=(7,),
        event_list=events,
    )
    env = ciao.ciao_environment("5644", config)
    return observation, config, env


def good_times(path):
    """The intervals in the first GTI block of a file, as a list of pairs."""
    with fits.open(path) as hdul:
        block = next(hdu for hdu in hdul[1:] if hdu.name.startswith("GTI"))
        return [[float(a), float(b)] for a, b in zip(block.data["START"], block.data["STOP"])]


class TestApplyingGoodTimes:
    def test_a_named_table_is_intersected_with_the_files_own(self, workspace):
        """
        ``[0, 5000]`` against the file's ``[0, 4000] + [6000, 10000]`` leaves
        ``[0, 4000]``: the hole in the file's own good times is not filled in by a table
        that spans it. Replacing rather than intersecting would give 5 000 s.
        """
        observation, config, env = workspace

        cleaned = chandra.chandra_clean_event_list(
            observation, config, np.array([[-1000.0, 5000.0]]), env=env
        )

        assert good_times(cleaned) == [[0.0, 4000.0]]
        with fits.open(cleaned) as hdul:
            assert hdul["EVENTS"].header["ONTIME"] == pytest.approx(4000.0)
            assert hdul["EVENTS"].data["time"].max() < 4000.0

    def test_the_intervals_the_flare_cut_computed_are_the_ones_applied(self, workspace):
        observation, config, env = workspace
        wanted = np.array([[0.0, 2000.0], [2500.0, 4000.0], [6000.0, 10000.0]])

        cleaned = chandra.chandra_clean_event_list(observation, config, wanted, env=env)

        assert good_times(cleaned) == wanted.tolist()


class TestCuttingTheSourceOutOfTheField:
    def test_exclude_beside_another_filter_is_refused(self, workspace, tmp_path):
        """The belief ``chandra_flare_curve_filter`` is written around. Should a future
        CIAO accept this, the region algebra still works, but the comment is then wrong."""
        observation, _, env = workspace

        with pytest.raises(RuntimeError):
            ciao.run(
                "dmcopy",
                produces=str(tmp_path / "never.fits"),
                env=env,
                infile=observation.event_list + "[ccd_id=7][exclude sky=circle(4100,4100,20)]",
                outfile=str(tmp_path / "never.fits"),
                clobber=True,
            )

    def test_region_algebra_removes_the_circle_and_keeps_the_rest(self, workspace, tmp_path):
        observation, _, env = workspace
        outfile = str(tmp_path / "field.fits")
        radius = 20.0

        ciao.run(
            "dmcopy",
            produces=outfile,
            env=env,
            infile=observation.event_list
            + chandra.sky_filter(f"field()-circle(4100,4100,{radius:g})"),
            outfile=outfile,
            clobber=True,
        )

        with fits.open(observation.event_list) as original, fits.open(outfile) as kept:
            x, y = original["EVENTS"].data["x"], original["EVENTS"].data["y"]
            inside = np.hypot(x - 4100.0, y - 4100.0) < radius
            assert 0 < inside.sum() < len(x)
            assert len(kept["EVENTS"].data) == len(x) - inside.sum()


class TestTheFlareLightCurve:
    def _position_and_regions(self):
        position = chandra.SourcePosition(
            x=4100.0, y=4100.0, chip_id=7, chipx=500.0, chipy=500.0, theta_arcmin=0.3
        )
        regions = chandra.ExtractionRegions(
            source=chandra.sky_filter(chandra.circle_region(4100.0, 4100.0, 5.0)),
            background=chandra.sky_filter(chandra.annulus_region(4100.0, 4100.0, 7.5, 15.0)),
            radius_arcsec=5.0,
        )
        return position, regions

    def test_the_modules_own_filter_is_accepted_by_dmextract(self, workspace):
        """Energy band, chip, source cut out by region algebra, binned in time -- the
        whole string ``chandra_flare_curve_filter`` builds, parsed by the real tool."""
        observation, config, env = workspace
        position, regions = self._position_and_regions()

        curve = chandra.chandra_flare_lightcurve(observation, position, regions, config, env=env)

        with fits.open(curve) as hdul:
            assert hdul["LIGHTCURVE"].data["COUNTS"].sum() > 0

    def test_bins_in_a_gap_of_the_good_times_are_written_empty(self, workspace):
        """
        Four 500 s bins fall in the 2 000 s hole. ``dmextract`` writes them with no
        exposure and a zero rate, which read at face value are the quietest bins in the
        observation and drag the threshold down.
        """
        observation, config, env = workspace
        position, regions = self._position_and_regions()
        config = dict(config, flare_bin_seconds=500.0)

        path = chandra.chandra_flare_lightcurve(observation, position, regions, config, env=env)

        with fits.open(path) as hdul:
            data = hdul["LIGHTCURVE"].data
            in_gap = (data["TIME"] > 4000.0) & (data["TIME"] < 6000.0)
            assert in_gap.sum() == 4
            assert np.all(data["EXPOSURE"][in_gap] == 0)
            assert np.all(data["COUNT_RATE"][in_gap] == 0)
            assert np.all(data["EXPOSURE"][~in_gap] > 0)

    def test_the_reader_turns_those_bins_into_missing_values(self, workspace):
        observation, config, env = workspace
        position, regions = self._position_and_regions()
        config = dict(config, flare_bin_seconds=500.0)

        path = chandra.chandra_flare_lightcurve(observation, position, regions, config, env=env)
        curve = chandra.read_chandra_lightcurve(path)

        in_gap = (curve.time > 4000.0) & (curve.time < 6000.0)
        assert np.all(np.isnan(curve.rate[in_gap]))
        assert np.all(np.isfinite(curve.rate[~in_gap]))
        assert curve.cadence == pytest.approx(500.0)


class TestTheRunner:
    def test_dmlist_answers_on_standard_output(self, workspace):
        """Why ``capture`` exists: ``dmlist`` writes no file, so its output is the answer."""
        observation, _, env = workspace

        said = ciao.run(
            "dmlist",
            produces=[],
            capture=True,
            env=env,
            infile=observation.event_list,
            opt="counts",
        )

        assert said.stdout.split() == ["4000"]

    def test_parameter_files_are_written_to_the_observations_own_directory(self, workspace):
        """The ``ardlib.par`` hazard, checked where it happens: a task's parameter file
        lands in the private directory ``ciao_environment`` made, not in ``~/cxcds_param``."""
        observation, config, env = workspace
        private = env["PFILES"].split(";")[0]

        chandra.chandra_clean_event_list(observation, config, np.array(OWN_GTI), env=env)

        assert os.path.exists(os.path.join(private, "dmcopy.par"))
