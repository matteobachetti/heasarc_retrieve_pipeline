# Licensed under a 3-clause BSD style license - see LICENSE.rst
import logging

from ._version import version as __version__

__all__ = ["__version__"]

logging.getLogger(__name__).info("heasarc_retrieve_pipeline version %s", __version__)
