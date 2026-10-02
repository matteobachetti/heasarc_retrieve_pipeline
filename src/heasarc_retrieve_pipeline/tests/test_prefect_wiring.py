"""
Guards on how the package wires itself to Prefect.

These read the source rather than running it, so they cost nothing and cannot be
sidestepped by a code path that happens not to be exercised.
"""

import ast
import os
import pathlib
import string

import pytest

from .. import conftest


MODULES = sorted(
    p
    for p in pathlib.Path(__file__).resolve().parent.parent.glob("*.py")
    if p.name not in ("__init__.py", "_version.py")
)


def function_objects_in_wait_for(source):
    """
    Names passed to ``wait_for`` that are functions defined in the same module.

    Prefect expects futures or states. A bare function object is accepted and does
    nothing: the declared dependency neither orders the steps nor propagates a failure.

    Examples
    --------
    >>> function_objects_in_wait_for("def up(): pass\\ndown(wait_for=[up])")
    ['up']
    >>> function_objects_in_wait_for("def up(): pass\\nf = up.submit()\\ndown(wait_for=[f])")
    []
    """
    tree = ast.parse(source)
    defined = {
        n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "wait_for":
                continue
            elements = (
                keyword.value.elts
                if isinstance(keyword.value, (ast.List, ast.Tuple))
                else [keyword.value]
            )
            for element in elements:
                if isinstance(element, ast.Name) and element.id in defined:
                    offenders.append(element.id)
    return offenders


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_wait_for_never_gets_a_bare_function(path):
    """Measured on Prefect 3.8.4: with a function object the downstream body ran even
    though the upstream task had raised."""
    offenders = function_objects_in_wait_for(path.read_text())

    assert offenders == [], f"{path.name} passes function objects to wait_for: {offenders}"


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_every_wait_for_argument_comes_from_submit(path):
    """A future only bites once something resolves it, so the name in wait_for has to be
    one that ``.submit()`` produced -- not a plain value, and not a function."""
    tree = ast.parse(path.read_text())
    submitted = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "submit"
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "wait_for":
                continue
            elements = (
                keyword.value.elts
                if isinstance(keyword.value, (ast.List, ast.Tuple))
                else [keyword.value]
            )
            for element in elements:
                assert isinstance(element, ast.Name), f"{path.name}: wait_for takes a name"
                assert element.id in submitted, (
                    f"{path.name}: wait_for={element.id}, which no .submit() produced"
                )


def run_name_fields(source):
    """
    Every ``task_run_name``/``flow_run_name`` template, with the names it refers to.

    Yields ``(function, template, root_names)``. Prefect formats these with the call's
    arguments, so a name that is not a parameter raises ``KeyError`` at call time --
    which stays invisible for as long as the function is only called through ``.fn``.

    Examples
    --------
    >>> list(run_name_fields('@task(task_run_name="x_{a}")\\ndef f(a): pass'))
    [('f', 'x_{a}', ['a'])]
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            for keyword in decorator.keywords:
                if keyword.arg not in ("task_run_name", "flow_run_name"):
                    continue
                if not isinstance(keyword.value, ast.Constant):
                    continue
                template = keyword.value.value
                roots = [
                    field.split(".")[0].split("[")[0]
                    for _, field, _, _ in string.Formatter().parse(template)
                    if field
                ]
                yield node.name, template, roots


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_run_name_templates_only_name_real_parameters(path):
    """Measured on Prefect 3.8.4: a template naming something that is not a parameter
    raises KeyError when the task is called."""
    tree = ast.parse(path.read_text())
    parameters = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            parameters[node.name] = {a.arg for a in args.posonlyargs + args.args + args.kwonlyargs}

    for function, template, roots in run_name_fields(path.read_text()):
        for root in roots:
            assert root in parameters[function], (
                f"{path.name}: {function} run name {template!r} names {root!r}, "
                f"which is not one of its parameters"
            )


def heasoft_calls_without_produces(source):
    """
    ``heasoft.run``/``heasoft.run_task`` calls that do not say what they produce.

    A zero return code is not evidence that a file was written, so the wrapper checks --
    but only if the call site names the output.

    Examples
    --------
    >>> heasoft_calls_without_produces('heasoft.run("ftsort", infile="a")')
    ['ftsort']
    >>> heasoft_calls_without_produces('heasoft.run("ftsort", produces="b", infile="a")')
    []
    """
    tree = ast.parse(source)
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr in ("run", "run_task")):
            continue
        if not (isinstance(func.value, ast.Name) and func.value.id == "heasoft"):
            continue
        if any(keyword.arg == "produces" for keyword in node.keywords):
            continue
        first = node.args[0] if node.args else None
        offenders.append(first.value if isinstance(first, ast.Constant) else "?")
    return offenders


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_every_heasoft_call_says_what_it_produces(path):
    """Measured on a real run: ftmgtime with no input GTIs exits 0 and writes nothing, and
    the next tool takes the blame."""
    offenders = heasoft_calls_without_produces(path.read_text())

    assert offenders == [], f"{path.name}: no produces= on {offenders}"


def enclosing_function(tree, target):
    """Name of the function a node sits in, or ``None`` at module level."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(child is target for child in ast.walk(node)):
                return node.name
    return None


def chdir_calls(source):
    """
    Functions that call ``os.chdir``.

    The working directory belongs to the whole process. Changing it to steer where a step
    writes means no two observations can be reduced at the same time, and it makes every
    relative path in the configuration mean something different depending on when it is
    used.

    Examples
    --------
    >>> chdir_calls("def f():\\n    os.chdir('x')")
    ['f']
    >>> chdir_calls("def f():\\n    os.makedirs('x')")
    []
    """
    tree = ast.parse(source)
    callers = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "chdir":
            callers.append(enclosing_function(tree, node))
    return callers


#: Where a working directory may be set. ``prepare_worker`` is the ordinary case: a worker
#: process, once, before it runs anything, into a private directory of its own.
#:
#: ``coadd.working_directory`` is the exception, and it is forced from outside. ``addspec``
#: builds the ``mathpha`` expression that co-adds the backgrounds without quoting the
#: ``BACKFILE`` values, so a path in one is parsed as division and the run dies; a background
#: spectrum has to be named bare, and the only way to say which one is to be standing in its
#: directory. What makes it safe is ``heasoft.HEASOFT_LOCK``, held for as long as the
#: directory is moved: every HEASOFT call in this package goes through that lock, so none of
#: them can see the process standing somewhere else.
#:
#: That lock is now the whole of the argument. Until ``combine_module_spectra`` existed this
#: was reached only from ``hrp-merge-obsids``, a post-processing command running on a tree
#: the pipeline had already finished; it is now also reached from inside
#: ``process_nustar_obsid``, at the end, after the futures it depends on have been resolved.
CHDIR_ALLOWED_IN = {"prepare_worker", "working_directory"}


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_nothing_steers_the_pipeline_by_changing_directory(path):
    offenders = [name for name in chdir_calls(path.read_text()) if name not in CHDIR_ALLOWED_IN]

    assert offenders == [], f"{path.name}: os.chdir in {offenders}"


class TestTheSuiteGetsItsOwnPrefectDatabase:
    """``PREFECT_HOME`` defaults to ``~/.prefect``, one database for the whole machine.

    A Prefect server starting migrates that file to the schema of whichever Prefect started
    it, and a Prefect older than the file cannot start a server at all. The suite sees that
    as every test that touches a server waiting out a connection timeout -- and, worse, as a
    diagnostic record quietly missing everything the task that could not run would have
    written, because the caller logs a failed diagnostic rather than raising it. Neither
    symptom points at the cause.

    So the suite uses a database of its own, named after the Prefect that migrates it, and
    two versions never meet in one file. See ``conftest.py``.
    """

    def test_the_suite_is_not_using_the_shared_database(self):
        assert (
            pathlib.Path(os.environ["PREFECT_HOME"]).resolve()
            != pathlib.Path("~/.prefect").expanduser().resolve()
        )

    def test_two_prefect_versions_do_not_share_one_database(self):
        """The whole point: the older one must not meet what the newer one migrated."""
        assert conftest.private_prefect_home("3.7.4") != conftest.private_prefect_home("3.8.4")

    def test_the_directory_is_named_after_the_prefect_that_migrates_it(self):
        import prefect

        assert prefect.__version__ in conftest.private_prefect_home(prefect.__version__)

    def test_it_is_somewhere_prefect_can_write(self):
        assert os.path.isdir(os.environ["PREFECT_HOME"])
        assert os.access(os.environ["PREFECT_HOME"], os.W_OK)

    def test_the_one_the_developer_asked_for_wins(self, tmp_path):
        """``setdefault``, so a run that wants the real database, or a scratch one, says so."""
        source = pathlib.Path(conftest.__file__).read_text()

        assert 'os.environ.setdefault("PREFECT_HOME"' in source
