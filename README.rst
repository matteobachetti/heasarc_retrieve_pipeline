Pipeline to automatically retrieve data from HEASARC
----------------------------------------------------

.. image:: https://github.com/matteobachetti/heasarc_retrieve_pipeline/actions/workflows/ci_tests.yml/badge.svg
    :target: https://github.com/matteobachetti/heasarc_retrieve_pipeline/actions/workflows/ci_tests.yml
    :alt: CI Tests

Query the HEASARC archive, download an observation, run the mission's reduction, and keep
a record of what happened -- as a `Prefect <https://www.prefect.io>`_ flow, so that
several observations can be reduced at once. NuSTAR is the mission that is worked out in
full, from ``nupipeline`` through split segments, merged event files and coadded spectra;
NICER and RXTE are further behind.

It is research software, written for a particular set of reductions and grown from there.
Read ``docs/known_issues.rst`` before trusting a number that comes out of it: the science
caveats are written down there rather than implied.

Installation
------------

.. code-block:: bash

    pip install .

That gives the archive query, the download transports and the flow machinery. The parts
that need more are optional, and are only imported when they are used:

==============  ==========================================================================
Extra           What it adds
==============  ==========================================================================
``imaging``     ``scikit-image``, ``scipy``, ``statsmodels`` -- splitting sources from
                background in sky images (``image_utils``)
``regions``     ds9 region files for the ``nuproducts`` spectral path
``s3``          ``boto3``, for the anonymous S3 download transport
``report``      ``plotly``, for the HTML observation pages written by ``hrp-report``
``solar``       ``sunpy``, for filtering out solar flares
``snr``         ``nustar_gen``, for signal-to-noise-optimised extraction regions. Not on
                PyPI, so this one installs from GitHub
``all``         everything above except ``snr``
``test``        ``pytest`` and ``pytest-astropy``
``docs``        ``sphinx`` and ``sphinx-automodapi``
==============  ==========================================================================

For example, ``pip install '.[all,test]'``.

**HEASOFT is a separate matter.** The reductions themselves are HEASOFT tools driven
through ``heasoftpy``, which pip cannot install; use the
`HEASARC conda channel <https://heasarc.gsfc.nasa.gov/docs/software/conda.html>`_ or a
source build, and set ``HEADAS`` and ``CALDB`` before running anything. Without it the
package still imports, queries and downloads -- it simply cannot reduce.

Command-line tools
------------------

============================  ================================================================
``hrp-split-obsid``           split a reduced observation into time segments
``hrp-merge-obsids``          merge segments back into one event file and spectrum
``hrp-report``                write the HTML page describing what a reduction did
``hrp-check-roundtrip``       check that splitting and merging returns what went in
============================  ================================================================

Running the tests
-----------------

.. code-block:: bash

    pytest --pyargs heasarc_retrieve_pipeline

Tests marked ``slow`` are deselected by default -- they are about half the runtime, and
they fork process pools -- so add ``--run-slow`` to include them. Tests marked ``heasoft``
run a real ftool and skip unless ``HEADAS`` is set. ``tox`` runs the same suite in a fresh
environment; ``tox -l -v`` lists what is available.

License
-------

This project is Copyright (c) Matteo Bachetti and licensed under
the terms of the BSD 3-Clause license. This package is based upon
the `Openastronomy packaging guide <https://github.com/OpenAstronomy/packaging-guide>`_
which is licensed under the BSD 3-clause licence. See the licenses folder for
more information.


Contributing
------------

We love contributions! heasarc_retrieve_pipeline is open source,
built on open source, and we'd love to have you hang out in our community.

**Imposter syndrome disclaimer**: We want your help. No, really.

There may be a little voice inside your head that is telling you that you're not
ready to be an open source contributor; that your skills aren't nearly good
enough to contribute. What could you possibly offer a project like this one?

We assure you - the little voice in your head is wrong. If you can write code at
all, you can contribute code to open source. Contributing to open source
projects is a fantastic way to advance one's coding skills. Writing perfect code
isn't the measure of a good developer (that would disqualify all of us!); it's
trying to create something, making mistakes, and learning from those
mistakes. That's how we all improve, and we are happy to help others learn.

Being an open source contributor doesn't just mean writing code, either. You can
help out by writing documentation, tests, or even giving feedback about the
project (and yes - that includes giving feedback about the contribution
process). Some of these contributions may be the most valuable to the project as
a whole, because you're coming to the project with fresh eyes, so you can see
the errors and assumptions that seasoned contributors have glossed over.

Note: This disclaimer was originally written by
`Adrienne Lowe <https://github.com/adriennefriend>`_ for a
`PyCon talk <https://www.youtube.com/watch?v=6Uj746j9Heo>`_, and was adapted by
heasarc_retrieve_pipeline based on its use in the README file for the
`MetPy project <https://github.com/Unidata/MetPy>`_.
