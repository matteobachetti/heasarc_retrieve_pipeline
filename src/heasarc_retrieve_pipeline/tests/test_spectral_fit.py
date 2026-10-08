"""Flux measurement by spectral fitting, checked against spectra simulated by XSPEC itself.

Every test here needs PyXspec, which comes with HEASOFT and not with pip, so the module is
skipped whole without it. The spectra are faked on a small diagonal response written by
the test, so no CALDB is needed and a fit takes a fraction of a second.
"""

import numpy as np
import pytest
from astropy.io import fits

from heasarc_retrieve_pipeline.spectral_fit import fit_flux

xspec = pytest.importorskip("xspec")

pytestmark = pytest.mark.heasoft

#: What the spectra are simulated from: Brightman et al. (2019)'s M82 model.
MODEL = "zwabs*powerlaw"
TRUE = {"zwabs.nH": 0.5, "zwabs.Redshift": 0.00067, "powerlaw.PhoIndex": 1.8}


def write_diagonal_response(path, emin=0.2, emax=12.0, nchan=236, area=100.0):
    """Write an RMF that sends each energy bin to its own channel with ``area`` cm^2."""
    edges = np.linspace(emin, emax, nchan + 1)
    lo, hi = edges[:-1], edges[1:]
    chan = np.arange(nchan)
    matrix = fits.BinTableHDU.from_columns(
        [
            fits.Column("ENERG_LO", "E", array=lo),
            fits.Column("ENERG_HI", "E", array=hi),
            fits.Column("N_GRP", "I", array=np.ones(nchan)),
            fits.Column("F_CHAN", "J", array=chan),
            fits.Column("N_CHAN", "J", array=np.ones(nchan)),
            fits.Column("MATRIX", "E", array=np.full(nchan, area)),
        ],
        name="MATRIX",
    )
    ebounds = fits.BinTableHDU.from_columns(
        [
            fits.Column("CHANNEL", "J", array=chan),
            fits.Column("E_MIN", "E", array=lo),
            fits.Column("E_MAX", "E", array=hi),
        ],
        name="EBOUNDS",
    )
    for hdu in (matrix, ebounds):
        hdu.header.update(
            TELESCOP="TEST",
            INSTRUME="TEST",
            CHANTYPE="PI",
            DETCHANS=nchan,
            HDUCLASS="OGIP",
            HDUCLAS1="RESPONSE",
            HDUVERS="1.3.0",
            TLMIN4=0,
        )
    matrix.header.update(HDUCLAS2="RSP_MATRIX", HDUCLAS3="FULL", LO_THRES=0.0)
    ebounds.header.update(HDUCLAS2="EBOUNDS")
    fits.HDUList([fits.PrimaryHDU(), matrix, ebounds]).writeto(path, overwrite=True)
    return str(path)


def fake_spectrum(tmp_path, norm, exposure):
    """Simulate a spectrum of :data:`MODEL`; return it, its response and its true 0.5--8 keV flux.

    The response is returned separately because ``fakeit`` leaves ``RESPFILE`` blank when
    the path is over 68 characters, which pytest's temporary directories usually are.
    """
    rmf = write_diagonal_response(tmp_path / "diag.rmf")
    xspec.AllData.clear()
    xspec.AllModels.clear()
    model = xspec.Model(MODEL)
    for name, value in TRUE.items():
        comp, par = name.split(".")
        getattr(getattr(model, comp), par).values = value
    model.powerlaw.norm.values = norm
    xspec.AllData.dummyrsp(0.5, 8.0, 1500, "lin")
    xspec.AllModels.calcFlux("0.5 8.0")
    true_flux = model.flux[0]
    xspec.AllData.clear()
    spectrum = tmp_path / "fake.pha"
    xspec.AllData.fakeit(
        1,
        xspec.FakeitSettings(response=rmf, exposure=exposure, fileName=str(spectrum)),
        applyStats=True,
        noWrite=False,
    )
    xspec.AllData.clear()
    xspec.AllModels.clear()
    return str(spectrum), rmf, true_flux


def test_fit_flux_recovers_the_simulated_flux(tmp_path):
    """A few thousand counts of a known absorbed power law come back with the flux inside its errors."""
    spectrum, rmf, true_flux = fake_spectrum(tmp_path, norm=1e-2, exposure=2e4)

    result = fit_flux(
        spectrum,
        response=rmf,
        model=MODEL,
        parameters={"zwabs.Redshift": 0.00067},
        frozen=["zwabs.Redshift"],
        fit_band=(0.3, 10.0),
        flux_band=(0.5, 8.0),
    )

    assert result["reason"] == ""
    assert result["counts"] > 1000
    assert result["flux_lo"] < true_flux < result["flux_hi"]
    assert abs(result["flux"] / true_flux - 1) < 0.1
    assert result["statistic"] == "cstat"


def test_fit_flux_finds_companions_named_relative_to_the_spectrum(tmp_path, monkeypatch):
    """Files named by a bare RESPFILE, as the UKSSDC builder writes them, load from any directory."""
    spectrum, rmf, _ = fake_spectrum(tmp_path, norm=1e-2, exposure=1e3)
    with fits.open(spectrum, mode="update") as hdul:
        hdul[1].header["RESPFILE"] = "diag.rmf"
    monkeypatch.chdir(tmp_path.parent)

    result = fit_flux(spectrum, model=MODEL)

    assert result["reason"] == ""
    assert result["flux"] > 0


def test_fit_flux_refuses_a_nearly_empty_spectrum(tmp_path):
    """A spectrum with fewer counts than asked for returns NaN and says why, instead of a fit."""
    spectrum, rmf, _ = fake_spectrum(tmp_path, norm=1e-5, exposure=100)

    result = fit_flux(spectrum, response=rmf, model=MODEL, min_counts=20)

    assert np.isnan(result["flux"])
    assert "counts" in result["reason"]


def test_fit_flux_refuses_a_spectrum_without_a_response(tmp_path):
    """A spectrum whose RESPFILE is blank fails at once with a message, not inside XSPEC."""
    spectrum, _, _ = fake_spectrum(tmp_path, norm=1e-2, exposure=100)
    with fits.open(spectrum, mode="update") as hdul:
        hdul[1].header["RESPFILE"] = "none"

    with pytest.raises(ValueError, match="no response"):
        fit_flux(spectrum, model=MODEL)
