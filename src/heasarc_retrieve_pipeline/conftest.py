"""
Test configuration shared by every test module in the package.

Three markers live here.

``slow``
    Deselected by default, run with ``--run-slow``. The bar for it is real time on a
    developer's machine: ``tests/test_concurrency.py`` alone is about half the runtime of
    the whole offline suite, because it forks a real process pool and starts a temporary
    Prefect server. Continuous integration runs it in a job of its own, so nothing is
    quietly skipped there.

``heasoft``
    Skipped unless a real HEASOFT installation is importable *and* ``HEADAS`` is set.
    These are the tests that call a real ftool rather than a recorded double.

``ciao``
    Skipped unless ``ASCDS_INSTALL`` is set *and* ``dmlist`` is on ``PATH`` -- the same two
    questions :func:`heasarc_retrieve_pipeline.ciao.has_ciao` asks. These are the tests
    that call a real CIAO task. Continuous integration has no CIAO, so they run locally.
"""

import importlib.metadata
import os
import tempfile

# Most tests call Prefect tasks through ``.fn``, outside any flow run. Prefect's API log
# handler warns about that on every call; it has nothing to report to. This has to happen
# before Prefect is imported, and conftest.py is imported before any test module.
os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")


def private_prefect_home(prefect_version):
    """
    A ``PREFECT_HOME`` for this suite alone, named after the Prefect that will migrate it.

    ``PREFECT_HOME`` defaults to ``~/.prefect``, so every environment on a machine shares
    one SQLite database. A Prefect server starting runs ``alembic upgrade head`` on it, and
    a Prefect *older* than whatever last migrated it cannot start a server at all --
    ``Can't locate revision identified by ...``, then ``Application startup failed``. The
    suite needs a server more often than it looks: a task called outside a flow, which is
    what ``.fn`` on a flow whose body calls a task ends up doing, starts a temporary one.

    Keying the directory on the version is what makes this a fix rather than a reprieve.
    Emptying the shared database works until the next run in a newer environment migrates
    it forward again; two versions that never share a file cannot collide at all.

    Under the system temporary directory, so it is on local disk -- SQLite locking over a
    network-mounted home is unreliable -- and so it survives between runs, which means the
    migration happens once rather than on every invocation of pytest.

    Examples
    --------
    >>> private_prefect_home("3.7.4") == private_prefect_home("3.8.4")
    False
    >>> private_prefect_home("3.7.4").endswith("prefect-3.7.4")
    True
    """
    return os.path.join(
        tempfile.gettempdir(), f"heasarc_retrieve_pipeline-prefect-{prefect_version}"
    )


try:
    _PREFECT_VERSION = importlib.metadata.version("prefect")
except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    # Prefect is a hard dependency, so this is a broken installation rather than a
    # configuration. Leave a home that is still nobody else's and let the import fail.
    _PREFECT_VERSION = "absent"

# Like the setting above, this has to happen before Prefect is imported: its settings are
# resolved once, at import, and ``PREFECT_HOME`` is read then. ``setdefault``, so a run
# that wants a particular database -- the user's own, or a scratch one for a parallel
# reduction -- still says so from the outside. The directory is created here because it is
# read before anything would create it.
os.environ.setdefault("PREFECT_HOME", private_prefect_home(_PREFECT_VERSION))
os.makedirs(os.environ["PREFECT_HOME"], exist_ok=True)

import pytest  # noqa: E402

from . import ciao, heasoft  # noqa: E402


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
    config.addinivalue_line(
        "markers", "ciao: needs a real CIAO installation, not a recorded double"
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
    skip_ciao = pytest.mark.skip(reason="needs a real CIAO installation ($ASCDS_INSTALL)")
    deselected = []
    kept = []

    heasoft_available = has_heasoft()
    # Asked afresh rather than read off ``ciao.HAS_CIAO``: that flag is resolved when the
    # module is first imported, and conftest.py imports it before a test has had any chance
    # to change the environment. Nothing does today, but the probe is two lookups.
    ciao_available = ciao.has_ciao()

    for item in items:
        if "slow" in item.keywords and not run_slow:
            deselected.append(item)
            continue
        if "heasoft" in item.keywords and not heasoft_available:
            item.add_marker(skip_heasoft)
        if "ciao" in item.keywords and not ciao_available:
            item.add_marker(skip_ciao)
        kept.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept
