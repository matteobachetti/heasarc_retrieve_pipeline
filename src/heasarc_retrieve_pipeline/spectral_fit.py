"""
Fluxes by spectral fitting, with PyXspec.

The one entry point is :func:`fit_flux`. It fits a model to one spectrum and measures the
observed (absorbed) flux in a band with the ``cflux`` convolution model, which is the way
XSPEC recommends getting a flux *with its uncertainty*: ``flux err`` draws parameters
from the covariance matrix, which is a poor description of a fit to a few hundred counts,
whereas ``cflux`` makes the flux a fit parameter and ``error`` walks its likelihood
profile.

It is mission-agnostic: it takes whatever source and background spectra and responses it
is handed. Swift/XRT is its first user.

PyXspec comes with HEASOFT, not with pip, so it is imported inside the function and this
module imports without it.

**The statistic is not quite what it says.** With ``statistic="cstat"`` and a background
spectrum, XSPEC does not use the Cash statistic but the W-statistic, which treats the
background as Poisson data with its own nuisance model. That is the right thing for a
background measured from an off-source region, and it is what every published "C-stat
fit with background" in XSPEC actually is. W-stat is biased when background bins have zero
counts, which is why spectra are usually grouped to at least one count per bin before
being fitted.
"""

import math
import os

__all__ = ["fit_flux", "companion_files"]


def _parameter(model, name):
    """The PyXspec parameter ``"component.parameter"`` of ``model``."""
    component, parameter = name.split(".")
    return getattr(getattr(model, component), parameter)


def _first_norm(model):
    """The normalisation of the first additive component, which ``cflux`` makes redundant."""
    for component_name in model.componentNames:
        component = getattr(model, component_name)
        if "norm" in component.parameterNames:
            return component.norm
    raise ValueError(f"Model {model.expression} has no additive component with a norm")


#: The PHA keywords naming a spectrum's companion files, and what each is called here.
COMPANION_KEYWORDS = {"BACKFILE": "background", "RESPFILE": "response", "ANCRFILE": "arf"}


def companion_files(spectrum):
    """
    The background, response and effective-area files a spectrum's header names.

    OGIP keywords name them relative to the spectrum's own directory, while XSPEC looks
    them up relative to the working directory. Resolving them here avoids changing the
    working directory, which would change it for every thread of the process.

    Parameters
    ----------
    spectrum : str
        A PHA file.

    Returns
    -------
    dict
        ``background``, ``response`` and ``arf``: absolute paths, or ``None`` where the
        keyword is missing, blank or ``none``.

    Examples
    --------
    >>> import os, tempfile
    >>> from astropy.io import fits
    >>> d = tempfile.mkdtemp()
    >>> hdu = fits.BinTableHDU.from_columns([fits.Column("CHANNEL", "J", array=[0])])
    >>> hdu.header.update(BACKFILE="src_bkg.pha", RESPFILE="NONE")
    >>> fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(os.path.join(d, "src.pha"))
    >>> found = companion_files(os.path.join(d, "src.pha"))
    >>> found["background"] == os.path.join(d, "src_bkg.pha"), found["response"], found["arf"]
    (True, None, None)
    """
    from astropy.io import fits

    directory = os.path.dirname(os.path.abspath(spectrum))
    with fits.open(spectrum) as hdul:
        header = (hdul["SPECTRUM"] if "SPECTRUM" in hdul else hdul[1]).header
    found = {}
    for keyword, role in COMPANION_KEYWORDS.items():
        value = str(header.get(keyword, "")).strip()
        if value.lower() in ("", "none"):
            found[role] = None
        else:
            found[role] = os.path.join(directory, value)
    return found


def _empty(reason, **known):
    """A result with no measurement, saying why."""
    result = dict(
        flux=math.nan,
        flux_lo=math.nan,
        flux_hi=math.nan,
        error_flags="",
        statistic="",
        stat=math.nan,
        dof=0,
        counts=math.nan,
        net_rate=math.nan,
        exposure=math.nan,
        parameters={},
        reason=reason,
    )
    result.update(known)
    return result


def fit_flux(
    spectrum,
    background=None,
    response=None,
    arf=None,
    model="tbabs*powerlaw",
    parameters=None,
    frozen=(),
    fit_band=(0.3, 10.0),
    flux_band=(0.5, 8.0),
    statistic="cstat",
    delta_stat=2.706,
    min_counts=10,
):
    """
    Fit a model to a spectrum and measure its observed flux in a band, with errors.

    Parameters
    ----------
    spectrum : str
        The source spectrum (a PHA file). Its ``BACKFILE``, ``RESPFILE`` and ``ANCRFILE``
        keywords are used for whatever is not given below.
    background, response, arf : str, optional
        Override the spectrum's own background, response matrix and effective-area files.
    model : str
        An XSPEC model expression, e.g. ``"zwabs*powerlaw"``. It must contain at least
        one additive component with a ``norm``.
    parameters : dict, optional
        Starting values, keyed ``"component.parameter"``: ``{"zwabs.Redshift": 0.00067}``.
    frozen : sequence of str, optional
        Parameters, named as in ``parameters``, to keep fixed during the fit.
    fit_band : (float, float)
        Energies in keV outside which channels are ignored.
    flux_band : (float, float)
        The band of the reported flux, in keV. It may extend outside ``fit_band``, in
        which case the flux there is the model's extrapolation.
    statistic : str
        The XSPEC fit statistic. ``"cstat"`` becomes W-stat when there is a background;
        see the module notes.
    delta_stat : float
        The change of the statistic that defines the error bounds: 2.706 for 90 per cent
        (XSPEC's default), 1.0 for 1 sigma, both for one parameter of interest.
    min_counts : int
        Spectra with fewer total counts in ``fit_band`` than this are not fitted.

    Raises
    ------
    ValueError
        If the spectrum has no response, which would otherwise surface as XSPEC's
        "no energy defined range" when the fit starts.

    Returns
    -------
    dict
        ``flux``, ``flux_lo`` and ``flux_hi`` in erg cm^-2 s^-1; ``error_flags``, XSPEC's
        nine-letter error status string (all ``F`` when the error search went well);
        ``statistic``, ``stat`` and ``dof`` of the best fit; ``counts`` (total, source
        region) and ``net_rate`` in ``fit_band``; ``exposure``; ``parameters``, the
        best-fitting value of every free parameter; and ``reason``, empty for a good
        measurement and otherwise saying why there is none.
    """
    import xspec

    parameters = dict(parameters or {})
    spectrum = os.path.abspath(spectrum)
    companions = companion_files(spectrum)
    background = os.path.abspath(background) if background else companions["background"]
    response = os.path.abspath(response) if response else companions["response"]
    arf = os.path.abspath(arf) if arf else companions["arf"]

    xspec.Xset.chatter = 0
    xspec.Xset.logChatter = 0
    xspec.AllData.clear()
    xspec.AllModels.clear()
    prompting = xspec.Xset.allowPrompting
    try:
        # XSPEC resolves the keywords against the working directory, and when it cannot
        # find a file it asks for another name; with nobody to answer, the whole load
        # fails. Without prompting it loads the spectrum alone, and every companion is
        # set again below, by absolute path.
        xspec.Xset.allowPrompting = False
        data = xspec.Spectrum(spectrum)
        if background is not None:
            data.background = background
        if response is not None:
            data.response = response
        if arf is not None:
            data.response.arf = arf

        try:
            has_response = bool(data.response.rmf)
        except Exception:
            has_response = False
        if not has_response:
            raise ValueError(
                f"{spectrum} has no response. Pass one with response=, or check its "
                f"RESPFILE keyword: XSPEC's fakeit leaves it blank for paths over 68 "
                f"characters."
            )

        data.ignore(f"**-{fit_band[0]} {fit_band[1]}-**")
        xspec.AllData.ignore("bad")
        exposure = data.exposure
        net_rate, _, total_rate, _ = data.rate
        counts = total_rate * exposure
        known = dict(counts=counts, net_rate=net_rate, exposure=exposure)
        if counts < min_counts:
            return _empty(
                f"{counts:.0f} counts in {fit_band} keV, fewer than {min_counts}", **known
            )

        xspec.Fit.statMethod = statistic
        xspec.Fit.query = "yes"
        xspec.Fit.nIterations = 1000

        # First the model alone, to find where cflux should start.
        plain = xspec.Model(model)
        for name, value in parameters.items():
            _parameter(plain, name).values = value
        for name in frozen:
            _parameter(plain, name).frozen = True
        xspec.Fit.perform()
        start = {
            f"{c}.{p}": getattr(getattr(plain, c), p).values[0]
            for c in plain.componentNames
            for p in getattr(plain, c).parameterNames
        }
        xspec.AllModels.calcFlux(f"{flux_band[0]} {flux_band[1]}")
        first_flux = data.flux[0]
        if not first_flux > 0:
            return _empty("the best fit has no flux in the flux band", **known)

        # Then with cflux outermost, so it measures the observed, absorbed flux.
        xspec.AllModels.clear()
        wrapped = xspec.Model(f"cflux*({model})")
        for name, value in start.items():
            _parameter(wrapped, name).values = value
        for name in frozen:
            _parameter(wrapped, name).frozen = True
        wrapped.cflux.Emin.values = flux_band[0]
        wrapped.cflux.Emax.values = flux_band[1]
        wrapped.cflux.lg10Flux.values = math.log10(first_flux)
        _first_norm(wrapped).frozen = True
        xspec.Fit.perform()

        lg10flux = wrapped.cflux.lg10Flux
        xspec.Fit.error(f"maximum 1000 {delta_stat} {lg10flux.index}")
        low, high, flags = lg10flux.error

        free = {
            f"{c}.{p}": getattr(getattr(wrapped, c), p).values[0]
            for c in wrapped.componentNames
            for p in getattr(wrapped, c).parameterNames
            if not getattr(getattr(wrapped, c), p).frozen
        }
        return dict(
            flux=10 ** lg10flux.values[0],
            flux_lo=10**low,
            flux_hi=10**high,
            error_flags=flags,
            statistic=statistic,
            stat=xspec.Fit.statistic,
            dof=xspec.Fit.dof,
            parameters=free,
            reason="",
            **known,
        )
    finally:
        xspec.Xset.allowPrompting = prompting
        xspec.AllData.clear()
        xspec.AllModels.clear()
