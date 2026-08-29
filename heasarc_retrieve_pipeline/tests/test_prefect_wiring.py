"""
Guards on how the package wires itself to Prefect.

These read the source rather than running it, so they cost nothing and cannot be
sidestepped by a code path that happens not to be exercised.
"""

import ast
import pathlib
import string

import pytest


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
    defined = {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}

    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "wait_for":
                continue
            elements = keyword.value.elts if isinstance(keyword.value, (ast.List, ast.Tuple)) else [keyword.value]
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
            elements = keyword.value.elts if isinstance(keyword.value, (ast.List, ast.Tuple)) else [keyword.value]
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
            parameters[node.name] = {
                a.arg for a in args.posonlyargs + args.args + args.kwonlyargs
            }

    for function, template, roots in run_name_fields(path.read_text()):
        for root in roots:
            assert root in parameters[function], (
                f"{path.name}: {function} run name {template!r} names {root!r}, "
                f"which is not one of its parameters"
            )
