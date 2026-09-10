import json

import pytest

from metaflow_extensions.spark.plugins.config import (
    DEFAULT_TIMEOUT_MINUTES,
    backend_config,
    require,
    resolve_config,
)
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError


class Flow:
    spark_config: object = None


def test_defaults_when_nothing_is_configured():
    config = resolve_config(Flow(), "spark_config", {})
    assert config["backend"] == "local"
    assert config["timeout"] == DEFAULT_TIMEOUT_MINUTES
    assert config["databricks"] == {}


def test_legacy_flat_emr_config_selects_emr_serverless():
    flow = Flow()
    flow.spark_config = {
        "application-id": "app-1",
        "execution-role": "arn:aws:iam::1:role/r",
        "s3-prefix": "s3://bucket/prefix",
        "spark-parameters": {"spark.executor.memory": "4g"},
    }
    config = resolve_config(flow, "spark_config", {})
    assert config["backend"] == "emr-serverless"
    assert config["emr-serverless"]["application-id"] == "app-1"
    assert config["emr-serverless"]["s3-prefix"] == "s3://bucket/prefix"


def test_json_string_config_is_parsed():
    flow = Flow()
    flow.spark_config = json.dumps({"backend": "databricks", "databricks": {"host": "h"}})
    config = resolve_config(flow, "spark_config", {})
    assert config["backend"] == "databricks"
    assert config["databricks"]["host"] == "h"


def test_decorator_overrides_beat_the_config_artifact():
    flow = Flow()
    flow.spark_config = {"backend": "emr-serverless", "timeout": 5}
    config = resolve_config(flow, "spark_config", {"backend": "databricks", "timeout": 90})
    assert config["backend"] == "databricks"
    assert config["timeout"] == 90


def test_none_overrides_do_not_clobber():
    flow = Flow()
    flow.spark_config = {"backend": "databricks"}
    config = resolve_config(flow, "spark_config", {"backend": None, "mode": None})
    assert config["backend"] == "databricks"


def test_env_is_below_the_config_artifact(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://env.databricks.com")
    monkeypatch.setenv("METAFLOW_SPARK_BACKEND", "databricks")
    flow = Flow()
    flow.spark_config = {"databricks": {"host": "https://flow.databricks.com"}}
    config = resolve_config(flow, "spark_config", {})
    assert config["backend"] == "databricks"
    assert config["databricks"]["host"] == "https://flow.databricks.com"


def test_env_fills_in_what_the_flow_omits(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-secret")
    flow = Flow()
    flow.spark_config = {"databricks": {"host": "https://flow.databricks.com"}}
    config = resolve_config(flow, "spark_config", {})
    assert config["databricks"]["token"] == "dapi-secret"


def test_bad_json_names_the_source():
    flow = Flow()
    flow.spark_config = "{not json"
    with pytest.raises(SparkConfigError, match="spark_config"):
        resolve_config(flow, "spark_config", {})


def test_backend_specific_spark_parameters_win():
    config = {
        "spark-parameters": {"spark.executor.memory": "4g", "spark.shuffle.spill": "true"},
        "databricks": {"spark-parameters": {"spark.executor.memory": "16g"}},
        "timeout": 30,
    }
    section = backend_config(config, "databricks")
    assert section["spark-parameters"] == {
        "spark.executor.memory": "16g",
        "spark.shuffle.spill": "true",
    }
    assert section["timeout"] == 30


def test_require_explains_how_to_set_the_key():
    with pytest.raises(SparkConfigError) as exc:
        require({}, "application-id", "emr-serverless", hint="Set it with ...")
    assert "application-id" in str(exc.value)
    assert "Set it with ..." in str(exc.value)
