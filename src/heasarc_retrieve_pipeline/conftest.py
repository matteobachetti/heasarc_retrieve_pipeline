"""
Test configuration shared by every test module in the package.

Two markers live here.

``slow``
    Deselected by default, run with ``--run-slow``. The bar for it is real time on a
    developer's machine: ``tests/test_concurrency.py`` alone is about half the runtime of
    the whole offline suite, because it forks a real process pool and starts a temporary
    Prefect server. Continuous integration runs it in a job of its own, so nothing is
    quietly skipped there.

``heasoft``
    Skipped unless a real HEASOFT installation is importable *and* ``HEADAS`` is set.
    These are the tests that call a real ftool rather than a recorded double.
"""

import os

# Most tests call Prefect tasks through ``.fn``, outside any flow run. Prefect's API log
# handler warns about that on every call; it has nothing to report to. This has to happen
# before Prefect is imported, and conftest.py is imported before any test module.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

import pytest  # noqa: E402

from . import heasoft  # noqa: E402


def pytest_addoption(parser):
    # pytest calls this hook twice for a conftest.py that is not at the rootdir -- once
    # while loading the initial conftests, and again when the plugin is registered and
    # the hook history is replayed. Measured on pytest 9.0.3; the second call raises
    # "option names already added". Adding the option once is all that is wanted.
    try:
        parser.addoption(
            "--run-slow",
            action="store_true",
            default=False,
            help="run the tests marked slow, which are deselected by default",
        )
    except ValueError:
        pass


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "slow: expensive test, deselected unless --run-slow is given"
    )
    config.addinivalue_line(
        "markers", "heasoft: needs a real HEASOFT installation, not a recorded double"
    )


def has_heasoft():
    """Whether a real HEASOFT is available to call.

    ``heasoftpy`` imports from ``$HEADAS/lib/python``, so importing it successfully
    already implies ``HEADAS`` -- but the ftools themselves are found through it, and a
    stale variable left over from an earlier shell is a real failure mode, so check both.
    """
    return heasoft.HAS_HEASOFT and bool(os.environ.get("HEADAS"))


def pytest_collection_modifyitems(config, items):
    run_slow = config.getoption("--run-slow")
    skip_heasoft = pytest.mark.skip(reason="needs a real HEASOFT installation ($HEADAS)")
    deselected = []
    kept = []

    heasoft_available = has_heasoft()

    for item in items:
        if "slow" in item.keywords and not run_slow:
            deselected.append(item)
            continue
        if "heasoft" in item.keywords and not heasoft_available:
            item.add_marker(skip_heasoft)
        kept.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept
