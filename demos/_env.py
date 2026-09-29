"""Per-step environments, secrets, and settings for running the demos on Outerbounds.

    python three_ways.py --environment=fast-bakery run --with kubernetes

Every demo step carries one `@step_env(...)`. On a laptop (the default `local` environment)
it is a no-op, so the demos keep running in whatever environment you installed the
extension into. Under a virtual environment such as `fast-bakery`, it attaches:

* `@anaconda`, resolved from Anaconda's main channel, for every step it can serve;
* `@pypi` for steps that read through credential vending: `deltalake` is on conda-forge
  only, as an abi3 build that requires conda-forge's own Python;
* `@secrets` for the Databricks host and token;
* `@environment` forwarding the non-secret settings below, which would otherwise stay
  behind on the laptop.

The environment is read from the command line rather than a toggle variable because task
pods are started with the same `--environment` flag, so they see the same decorators.

Each demo directory links to this file, so `import _env` works from the directory the
demo is run from and Metaflow packages it with the flow.
"""

import os
import sys

from metaflow import environment, pypi, secrets

PYTHON = "3.12"
DATABRICKS_SECRET = "outerbounds.databricks"
ANACONDA_MAIN = "https://repo.anaconda.com/pkgs/main"

# The same versions on both channels: pickled pandas artifacts cross from @pypi steps into
# @anaconda steps.
_BASE = {"databricks-sdk": "0.117.0", "pandas": "2.2.3", "pyarrow": "24.0.0"}

_PYPI_GROUPS = {
    "vending": {"deltalake": "1.6.6", "duckdb": "1.5.5"},
}

FORWARDED_SETTINGS = (
    "METAFLOW_DATABRICKS_WAREHOUSE_ID",
    "DATABRICKS_WAREHOUSE_ID",
)

_VIRTUAL_ENVIRONMENTS = {"fast-bakery", "anaconda", "conda", "pypi"}


def _metaflow_environment(argv=None):
    argv = sys.argv if argv is None else argv
    for i, arg in enumerate(argv):
        if arg.startswith("--environment="):
            return arg.split("=", 1)[1]
        if arg == "--environment" and i + 1 < len(argv):
            return argv[i + 1]
    return os.environ.get("METAFLOW_DEFAULT_ENVIRONMENT", "local")


PLATFORM = _metaflow_environment() in _VIRTUAL_ENVIRONMENTS


def _compose(decorators):
    def apply(func):
        for deco in reversed(decorators):
            func = deco(func)
        return func

    return apply


def _env_decorator(kinds):
    unknown = set(kinds) - set(_PYPI_GROUPS)
    if unknown:
        raise ValueError("unknown step_env kind(s): %s" % ", ".join(sorted(unknown)))
    if not kinds:
        # Outerbounds-only, so importing it at module level breaks laptop runs on
        # open-source Metaflow.
        from metaflow import anaconda

        return anaconda(python=PYTHON, packages=dict(_BASE), channels=[ANACONDA_MAIN])
    packages = dict(_BASE)
    for kind in kinds:
        packages.update(_PYPI_GROUPS[kind])
    return pypi(python=PYTHON, packages=packages)


def step_env(*kinds):
    """Environment, secrets, and settings for one step.

    `kinds` add package groups to the Databricks SDK, pandas, and pyarrow: "vending"
    (deltalake and duckdb) resolves from PyPI, every other step from Anaconda's main
    channel.
    """
    if not PLATFORM:
        return lambda func: func

    decorators = [secrets(sources=[DATABRICKS_SECRET]), _env_decorator(kinds)]
    settings = {k: os.environ[k] for k in FORWARDED_SETTINGS if os.environ.get(k)}
    if settings:
        decorators.append(environment(vars=settings))
    return _compose(decorators)
