"""
Image-based separation of sources and background in NuSTAR event files.

The entry point is :func:`filter_sources_in_images`. Given one cleaned event file it
builds a smoothed sky image, finds the bright peaks in it, and writes one event file per
accepted peak plus one background event file containing everything far from every peak.

The detection statistic
-----------------------

Candidate peaks come from ``skimage.feature.peak_local_max`` with an absolute threshold
of half the brightest pixel, so only peaks comparable in brightness to the brightest
object in the field are considered at all.

A candidate is then accepted if the number of events in its extraction circle exceeds
``median + MAD``, where both are computed over 300 circular apertures thrown at random
positions in the field by :func:`get_random_fluxes_in_img`. The median and the median
absolute deviation (``statsmodels.robust.mad``, rescaled so that it estimates a Gaussian
standard deviation) are used rather than the mean and standard deviation because some of
the random apertures inevitably land on real sources, and robust statistics limit the
damage.

This is in effect a **1-sigma cut**. It is deliberately worth being explicit that it is
not a calibrated detection significance: there is no Poisson treatment, no correction
for the number of trials, and the random apertures sample a field that contains the
sources themselves, which biases both estimators high. In practice the real gatekeeper
is the absolute threshold on the peaks; the flux cut mainly rejects peaks that are sharp
in the smoothed image but contain few counts.

Read this module as "split the obviously bright things in the field", not as a source
detection algorithm with a quantified false-alarm rate.

What the products are good for
------------------------------

Timing. The source files are plain circular extractions with no PSF or aperture
correction, no ancillary response and no exposure map, and the background file covers a
large and inhomogeneous part of the detector -- NuSTAR's background is dominated below
about 20 keV by aperture stray light, which varies strongly across the detector plane.
For spectroscopy, use the ``nuproducts`` path in
:mod:`heasarc_retrieve_pipeline.nustar` instead.

Coordinates are NuSTAR sky pixels throughout (1 pixel = 2.45 arcsec).
"""

from collections.abc import Iterable
import numpy as np
import copy
from astropy.table import Table
from astropy.io import fits
from astropy.visualization import hist
from matplotlib.figure import Figure
from skimage.feature import peak_local_max
from scipy.ndimage import gaussian_filter
from statsmodels.robust import mad


def image_from_table(table, bins, gaussian_filter_sigma=1.0):
    """
    Build a smoothed sky image from an event table.

    Parameters
    ----------
    table : astropy.table.Table or numpy.recarray
        Event table with ``X`` and ``Y`` columns, in sky pixels.
    bins : tuple of array_like
        ``(xbins, ybins)`` bin edges, passed straight to ``numpy.histogram2d``.
    gaussian_filter_sigma : float, optional
        Standard deviation, in bins, of the Gaussian smoothing kernel. Use 0 for no
        smoothing.

    Returns
    -------
    xbins, ybins : numpy.ndarray
        The bin edges, as returned by ``numpy.histogram2d``.
    img : numpy.ndarray
        The smoothed image, transposed so that it is indexed as ``img[y, x]``.

    Notes
    -----
    The histogram is built as ``histogram2d(table["Y"], table["X"])`` and the result is
    transposed, so the axis conventions need care when relating image indices back to sky
    coordinates.
    """
    hist, xbins, ybins = np.histogram2d(table["Y"], table["X"], bins=bins)

    img = gaussian_filter(hist, sigma=gaussian_filter_sigma)

    return xbins, ybins, img.T


def has_sky_position(table):
    """
    Mask of the events that were assigned a sky position.

    ``X`` and ``Y`` are zero -- or, rarely, negative -- for an event the aspect
    reconstruction could not place on the sky. Many NuSTAR observations contain a large
    pile-up of these at the origin, and every one of them has to be dropped before the
    field is imaged, before apertures are thrown at it, and before the background file is
    written. Both coordinates must be positive: an event with only one of them set is
    just as unplaced as one with neither.

    Parameters
    ----------
    table : astropy.table.Table, numpy.recarray or dict
        Event table with ``X`` and ``Y`` columns.

    Returns
    -------
    numpy.ndarray of bool
        True for the events with a usable sky position.

    Examples
    --------
    >>> table = {"X": np.array([0, 5, 0, 7]), "Y": np.array([0, 6, 3, 0])}
    >>> has_sky_position(table)
    array([False,  True, False, False])
    """
    return (table["X"] > 0) & (table["Y"] > 0)


def valid_table(table):
    """
    Drop events without a valid sky position.

    Parameters
    ----------
    table : astropy.table.Table or numpy.recarray
        Event table with ``X`` and ``Y`` columns.

    Returns
    -------
    Same type as ``table``
        The events :func:`has_sky_position` accepts.
    """
    return table[has_sky_position(table)]


def mask_around_region(table, coord, region_size=30):
    """
    Boolean mask of the events inside a circle.

    Parameters
    ----------
    table : astropy.table.Table or numpy.recarray
        Event table with ``X`` and ``Y`` columns, in sky pixels.
    coord : sequence of float
        ``(x, y)`` centre of the circle, in sky pixels.
    region_size : float, optional
        Radius of the circle, in sky pixels.

    Returns
    -------
    numpy.ndarray of bool
        True for the events inside the circle.

    Notes
    -----
    The coordinate differences are cast to the platform integer type before being squared.
    NuSTAR stores ``X`` and ``Y`` as 16-bit integers, and squaring a separation of more than
    about 180 pixels overflows that type and yields negative distances.
    """
    # Note the casting to standard int. Otherwise, it will
    # overflow and give negative numbers
    circle_of_coords = (
        np.array(table["X"] - coord[0]).astype(int) ** 2
        + np.array(table["Y"] - coord[1]).astype(int) ** 2
    )
    return circle_of_coords < region_size**2


def filter_table(table, coord, region_size=30):
    """
    Extract the events inside a circular region.

    Parameters
    ----------
    table : astropy.table.Table or numpy.recarray
        Event table with ``X`` and ``Y`` columns.
    coord : sequence of float
        ``(x, y)`` centre of the region, in sky pixels.
    region_size : float, optional
        Radius of the region, in sky pixels.

    Returns
    -------
    Same type as ``table``
        The events inside the region, excluding those without a valid sky position.
    """
    table = valid_table(table)
    table = table[mask_around_region(table, coord, region_size)]

    return table


def filter_table_outside_regions(table, coord_list, region_size=100):
    """
    Extract the events outside every one of a set of circular regions.

    This is how the background event file is built: everything far enough from every
    detected peak, including the peaks that failed the flux cut, so that sub-threshold
    sources contaminate neither the source files nor the background.

    Parameters
    ----------
    table : astropy.table.Table or numpy.recarray
        Event table with ``X`` and ``Y`` columns.
    coord_list : array_like
        ``(N, 2)`` array of region centres, in sky pixels. A single ``(x, y)`` pair is also
        accepted.
    region_size : float or array_like, optional
        Exclusion radius, in sky pixels. A scalar applies to every region; an array gives
        one radius per region.

    Returns
    -------
    Same type as ``table``
        The events outside all the regions, excluding those without a valid sky position.
    """
    if len(np.shape(coord_list)) < 2:
        coord_list = np.array([coord_list])
    if not isinstance(region_size, Iterable):
        region_size = np.ones(len(coord_list)) * region_size
    bad = ~has_sky_position(table)

    for i, coord in enumerate(coord_list):
        bad = bad | mask_around_region(table, coord, region_size[i])

    table_filt = table[~bad]

    return table_filt


def get_random_fluxes_in_img(table, region_size=30, n_rand=100):
    """
    Sample the counts in circular apertures at random positions in the field.

    Used to estimate the typical aperture counts and their scatter, from which the source
    acceptance threshold is derived. Aperture centres are drawn uniformly inside the
    bounding box of the valid events, staying ``3 * region_size`` away from its edges.

    Parameters
    ----------
    table : astropy.table.Table or numpy.recarray
        Event table with ``X`` and ``Y`` columns.
    region_size : float, optional
        Aperture radius, in sky pixels. Must match the radius used for the source
        extraction for the resulting threshold to be meaningful.
    n_rand : int, optional
        Number of apertures to draw.

    Returns
    -------
    list of int
        The number of events in each aperture.

    Notes
    -----
    The apertures are drawn anywhere in the field, including on the sources themselves, so
    the resulting median and scatter are biased high. The sampling is uniform over the
    bounding box, which does not account for vignetting or for the position dependence of
    the NuSTAR background.
    """
    placed = valid_table(table)
    xmin = np.min(placed["X"])
    ymin = np.min(placed["Y"])
    xmax = np.max(placed["X"])
    ymax = np.max(placed["Y"])

    fluxes = []
    for n in range(n_rand):
        x = np.random.uniform(xmin + 3 * region_size, xmax - 3 * region_size)
        y = np.random.uniform(ymin + 3 * region_size, ymax - 3 * region_size)

        table_filt = filter_table(table, [x, y], region_size=region_size)
        fluxes.append(len(table_filt))

    return fluxes


def filter_sources_in_images(eventfile, region_size=30, back_region_size=50):
    """
    Split a NuSTAR event file into per-source and background event files.

    The steps are:

    1. **Energy filter.** Events are converted from pulse-invariant channel to energy with
       the standard NuSTAR relation ``E [keV] = 0.04 * PI + 1.6`` -- channels are 40 eV
       wide and the first one starts at 1.6 keV -- and kept if ``3 <= E < 79`` keV, NuSTAR's
       nominal usable band. Events without a valid sky position are dropped. If fewer than
       20 events survive, the function gives up.
    2. **Image.** A 100x100 histogram in sky ``X``/``Y``, smoothed with a one-bin Gaussian.
       The grid spans the range of the surviving events, so one bin is roughly 10 sky pixels
       (about 25 arcsec) for a full field.
    3. **Peak detection.** ``peak_local_max`` with a minimum separation of 20 bins and an
       absolute threshold of half the brightest pixel.
    4. **Threshold.** The median and MAD of 300 random apertures give the acceptance
       threshold ``median + MAD``; see the module docstring for what that does and does not
       mean.
    5. **Extraction.** Accepted peaks are sorted by aperture counts in decreasing order, so
       ``_src1`` is always the brightest. Each gets its own event file; the background file
       holds everything outside ``back_region_size`` of *every* detected peak.

    A diagnostic JPEG is written next to each output file.

    Parameters
    ----------
    eventfile : str
        Path of a cleaned NuSTAR event file, optionally gzipped.
    region_size : float, optional
        Radius of the source extraction circles, in sky pixels (1 pixel = 2.45 arcsec).
    back_region_size : float, optional
        Radius, in sky pixels, of the region excluded around every detected peak when
        building the background file.

    Returns
    -------
    bool or None
        ``True`` if files were written, ``None`` if fewer than 20 events passed the energy
        and position filter.

    Notes
    -----
    The background estimate is computed with a hardcoded aperture radius of 30 sky pixels
    regardless of ``region_size``, so if the caller passes a different radius the threshold
    refers to a different aperture than the extraction does. The energy conversion is
    NuSTAR-specific. See the science caveats in ``docs/known_issues.rst``.
    """
    hdul = fits.open(eventfile)

    table = copy.deepcopy(hdul[1].data)

    energy = table["PI"] * 0.04 + 1.6
    good = (energy >= 3.0) & (energy < 79.0) & has_sky_position(table)

    if np.count_nonzero(good) < 20:
        hdul.close()
        return

    table = table[good]
    xmin = np.min(table["Y"])
    ymin = np.min(table["X"])
    xmax = np.max(table["Y"])
    ymax = np.max(table["X"])

    bins = (np.linspace(xmin, xmax, 100), np.linspace(ymin, ymax, 100))

    xbins, ybins, img = image_from_table(table, bins)

    dx = np.median(np.diff(xbins))
    dy = np.median(np.diff(ybins))

    # Comparison between image_max and im to find the coordinates of local maxima

    fluxes = get_random_fluxes_in_img(table, region_size=30, n_rand=300)

    median = np.median(fluxes)
    std = mad(fluxes)

    coordinates = peak_local_max(img, min_distance=20, threshold_abs=0.5 * np.max(img))

    # The first pixel in a FITS image is defined to range from 0.5 to 1.5,
    # with the center of the pixel at coordinate 1.0

    coordinates[:, 1] = coordinates[:, 1] * dx + dx + xmin
    coordinates[:, 0] = coordinates[:, 0] * dy + dy + ymin

    # ``Figure`` rather than ``pyplot``: a worker process reducing an observation should
    # not touch pyplot's global figure registry, or make it choose a display backend.
    fig = Figure()
    ax = fig.subplots()
    ax.pcolormesh(xbins, ybins, img, vmin=np.median(img))
    ax.plot(coordinates[:, 1], coordinates[:, 0], "r.")
    fig.savefig(eventfile.replace(".gz", "").replace(".evt", ".jpg"))

    region_fluxes = []
    for i, coord in enumerate(coordinates):
        table_filt = filter_table(table, coord, region_size=region_size)
        flux = len(table_filt)
        region_fluxes.append(flux)

    region_fluxes = np.asarray(region_fluxes)
    order = np.argsort(region_fluxes)
    coordinates = coordinates[order[::-1]]

    for i, coord in enumerate(coordinates):
        table_filt = filter_table(table, coord, region_size=region_size)
        flux = len(table_filt)
        print(median - std, flux, median + std)
        if flux < median + std:
            continue

        hdul[1].data = fits.BinTableHDU(table_filt).data
        # hdul[1].header = header
        hdul.writeto(
            eventfile.replace(".gz", "").replace(".evt", f"_src{i + 1}.evt"),
            overwrite=True,
        )

        x_filt, y_filt, img_filt = image_from_table(table_filt, bins, gaussian_filter_sigma=0)
        fig = Figure()
        ax = fig.subplots()
        ax.pcolormesh(x_filt, y_filt, img_filt, vmin=np.median(img))
        fig.savefig(eventfile.replace(".gz", "").replace(".evt", f"_src{i + 1}.jpg"))

    table_filt = filter_table_outside_regions(table, coordinates, region_size=back_region_size)

    hdul[1].data = fits.BinTableHDU(table_filt).data
    # hdul[1].header = header
    hdul.writeto(
        eventfile.replace(".gz", "").replace(".evt", f"_back.evt"),
        overwrite=True,
    )
    x_filt, y_filt, img_filt = image_from_table(table_filt, bins, gaussian_filter_sigma=0)
    fig = Figure()
    ax = fig.subplots()
    ax.pcolormesh(x_filt, y_filt, img_filt, vmin=np.median(img))
    fig.savefig(eventfile.replace(".gz", "").replace(".evt", f"_back.jpg"))
    hdul.close()
    return True
