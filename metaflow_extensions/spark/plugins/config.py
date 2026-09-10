"""Configuration resolution for @spark.

Precedence, lowest to highest:

    1. backend defaults
    2. Metaflow config / environment variables (METAFLOW_SPARK_*, DATABRICKS_*)
    3. the flow-level config artifact named by the `config` attribute
    4. explicit decorator attributes

The flow-level artifact may be a dict, a JSON string, or an ``IncludeFile``, which
keeps the original ``spark_config`` idiom working.
"""

import json
import os

from .exceptions import SparkConfigError

DEFAULT_TIMEOUT_MINUTES = 60

# Keys that a legacy flat spark_config.json uses. Their presence at the top level
# means the config predates backend sections and targets EMR Serverless.
_LEGACY_EMR_KEYS = ("application-id", "execution-role")

_BACKEND_SECTIONS = ("databricks", "emr-serverless", "local")


def _deep_merge(base, override):
    """Recursively merge `override` into `base`, returning a new dict."""
    out = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        elif value is not None:
            out[key] = value
    return out


def _coerce(value, what):
    """Turn a dict, a JSON string, or an IncludeFile into a dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError as exc:
            raise SparkConfigError(
                "Could not parse %s as JSON: %s" % (what, exc)
            ) from exc
        if not isinstance(parsed, dict):
            raise SparkConfigError(
                "%s must be a JSON object, got %s." % (what, type(parsed).__name__)
            )
        return parsed
    raise SparkConfigError(
        "%s must be a dict, a JSON string, or an IncludeFile, got %s."
        % (what, type(value).__name__)
    )


def _normalize(config):
    """Move legacy flat EMR keys into their backend section."""
    config = dict(config)
    legacy = {k: config.pop(k) for k in _LEGACY_EMR_KEYS if k in config}
    if legacy:
        section = dict(config.get("emr-serverless") or {})
        # An explicit section wins over the legacy top-level key.
        for key, value in legacy.items():
            section.setdefault(key, value)
        config["emr-serverless"] = section
        config.setdefault("backend", "emr-serverless")
        # These were only ever meaningful for EMR.
        for key in ("s3-prefix",):
            if key in config:
                config["emr-serverless"].setdefault(key, config.pop(key))
    return config


def _env_config():
    """Read configuration out of the Metaflow config and the environment."""
    # Imported here rather than at module level: this module is reachable from the
    # plugin modules Metaflow itself imports while `metaflow/__init__` is still
    # executing, and a top-level metaflow import would make that circular.
    from metaflow.metaflow_config_funcs import from_conf

    config = {}

    backend = from_conf("SPARK_BACKEND") or os.environ.get("METAFLOW_SPARK_BACKEND")
    if backend:
        config["backend"] = backend
    mode = from_conf("SPARK_MODE") or os.environ.get("METAFLOW_SPARK_MODE")
    if mode:
        config["mode"] = mode

    databricks = {}
    # Honour the standard Databricks environment variables so that a workspace
    # already configured for the CLI or the SDK needs no extra Metaflow config.
    for key, envs in (
        ("host", ("METAFLOW_DATABRICKS_HOST", "DATABRICKS_HOST")),
        ("token", ("METAFLOW_DATABRICKS_TOKEN", "DATABRICKS_TOKEN")),
        ("profile", ("METAFLOW_DATABRICKS_PROFILE", "DATABRICKS_CONFIG_PROFILE")),
        ("client_id", ("METAFLOW_DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_ID")),
        (
            "client_secret",
            ("METAFLOW_DATABRICKS_CLIENT_SECRET", "DATABRICKS_CLIENT_SECRET"),
        ),
        ("cluster_id", ("METAFLOW_DATABRICKS_CLUSTER_ID", "DATABRICKS_CLUSTER_ID")),
        (
            "warehouse_id",
            ("METAFLOW_DATABRICKS_WAREHOUSE_ID", "DATABRICKS_WAREHOUSE_ID"),
        ),
        ("volume", ("METAFLOW_DATABRICKS_VOLUME",)),
        ("runtime_version", ("METAFLOW_DATABRICKS_RUNTIME_VERSION",)),
    ):
        for env in envs:
            value = os.environ.get(env)
            if value:
                databricks[key] = value
                break
    if databricks:
        config["databricks"] = databricks

    emr = {}
    for key, env in (
        ("application-id", "METAFLOW_SPARK_EMR_APPLICATION_ID"),
        ("execution-role", "METAFLOW_SPARK_EMR_EXECUTION_ROLE"),
        ("s3-prefix", "METAFLOW_SPARK_EMR_S3_PREFIX"),
    ):
        value = os.environ.get(env)
        if value:
            emr[key] = value
    if emr:
        config["emr-serverless"] = emr

    return config


def resolve_config(flow, config_attr, overrides, default_backend="local"):
    """Build the effective config dict for one @spark step.

    `config_attr` is either a dict/JSON string used directly, or the name of a flow
    attribute holding one. A missing named attribute is not an error: the backend
    may be fully configured by decorator attributes and the environment.
    """
    from_flow = {}
    if isinstance(config_attr, str):
        raw = getattr(flow, config_attr, None)
        if raw is not None:
            from_flow = _coerce(raw, "the '%s' artifact" % config_attr)
    elif config_attr is not None:
        from_flow = _coerce(config_attr, "the @spark(config=...) attribute")

    config = _normalize(_env_config())
    config = _deep_merge(config, _normalize(from_flow))
    config = _deep_merge(config, _normalize(overrides or {}))

    config.setdefault("backend", default_backend)
    config.setdefault("timeout", DEFAULT_TIMEOUT_MINUTES)
    for section in _BACKEND_SECTIONS:
        config.setdefault(section, {})
    config.setdefault("spark-parameters", {})
    return config


def backend_config(config, backend_name):
    """Return the merged config a single backend should see.

    Backend-neutral `spark-parameters` are merged underneath the backend's own, so a
    backend-specific value wins.
    """
    section = dict(config.get(backend_name) or {})
    neutral = config.get("spark-parameters") or {}
    if neutral:
        section["spark-parameters"] = _deep_merge(
            neutral, section.get("spark-parameters") or {}
        )
    section.setdefault("timeout", config.get("timeout"))
    if config.get("mode"):
        section.setdefault("mode", config["mode"])
    return section


def require(section, key, backend_name, hint=None):
    """Fetch a required config key, or raise a message that says how to set it."""
    value = section.get(key)
    if value in (None, ""):
        msg = "The '%s' backend requires '%s' to be set." % (backend_name, key)
        if hint:
            msg += "\n" + hint
        raise SparkConfigError(msg)
    return value
