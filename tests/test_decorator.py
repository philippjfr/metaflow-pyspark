import pytest

from metaflow_extensions.spark.plugins.decorator import (
    PySparkDecorator,
    SparkDecorator,
    _collect_inputs,
    _resolve,
    _run_session,
)
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError
from metaflow_extensions.spark.plugins.output import (
    FORMATS,
    strip_spark_attrs,
    validate_format,
)


class Flow:
    """Stands in for a FlowSpec instance; attributes are set as steps would set them."""


def attrs(**overrides):
    merged = dict(SparkDecorator.defaults)
    merged.update(overrides)
    return merged


# ----------------------------------------------------------------------
def test_backend_attributes_land_in_the_backend_section():
    config = _resolve(Flow(), attrs(backend="databricks", cluster_id="0101-abc"))
    assert config["backend"] == "databricks"
    assert config["databricks"]["cluster_id"] == "0101-abc"


def test_databricks_variants_share_one_config_section():
    config = _resolve(
        Flow(), attrs(backend="databricks-connect", cluster_id="0101-abc")
    )
    # The section is reachable both under the canonical name and the resolved backend
    # name, because backend_config() looks it up by the latter.
    assert config["databricks"]["cluster_id"] == "0101-abc"
    assert config["databricks-connect"]["cluster_id"] == "0101-abc"


def test_emr_attribute_names_are_translated_to_config_keys():
    config = _resolve(
        Flow(),
        attrs(
            backend="emr-serverless",
            application_id="app-1",
            execution_role="arn:aws:iam::1:role/r",
            s3_prefix="s3://bucket/p",
        ),
    )
    section = config["emr-serverless"]
    assert section["application-id"] == "app-1"
    assert section["execution-role"] == "arn:aws:iam::1:role/r"
    assert section["s3-prefix"] == "s3://bucket/p"


def test_mode_reaches_the_backend_section():
    config = _resolve(Flow(), attrs(backend="databricks", mode="job"))
    assert config["databricks"]["mode"] == "job"


def test_spark_parameters_stay_backend_neutral():
    config = _resolve(
        Flow(), attrs(backend="local", spark_parameters={"spark.ui.enabled": "false"})
    )
    assert config["spark-parameters"] == {"spark.ui.enabled": "false"}


def test_unset_attributes_do_not_appear_in_the_section():
    config = _resolve(Flow(), attrs(backend="databricks"))
    assert "cluster_id" not in config["databricks"]
    assert "photon" not in config["databricks"]


# ----------------------------------------------------------------------
def test_job_parameters_are_read_off_the_flow():
    flow = Flow()
    setattr(flow, "start_date", "2026-08-01")
    setattr(flow, "region", "eu")
    assert _collect_inputs(flow, ["start_date", "region"]) == {
        "start_date": "2026-08-01",
        "region": "eu",
    }


def test_a_missing_job_parameter_names_the_attribute():
    with pytest.raises(SparkConfigError) as exc:
        _collect_inputs(Flow(), ["start_date"])
    assert "start_date" in str(exc.value)
    assert "self.start_date" in str(exc.value)


# ----------------------------------------------------------------------
def test_every_advertised_output_format_validates():
    for fmt in FORMATS:
        validate_format(fmt)


def test_an_unknown_output_format_lists_the_valid_ones():
    with pytest.raises(SparkConfigError) as exc:
        validate_format("hdf5")
    assert "pandas" in str(exc.value)


# ----------------------------------------------------------------------
def make_decorator(cls, **kwargs):
    deco = cls(attributes=kwargs)
    deco.step_init(None, None, "features", [], None, None, None)
    return deco


def test_legacy_output_pandas_becomes_a_format():
    deco = make_decorator(PySparkDecorator, output_pandas=True)
    assert deco.attributes["output_format"] == "pandas"
    assert deco.attributes["backend"] == "emr-serverless"


def test_legacy_output_pyarrow_becomes_a_format():
    deco = make_decorator(PySparkDecorator, output_pyarrow=True)
    assert deco.attributes["output_format"] == "arrow"


def test_legacy_both_false_means_return_the_url():
    deco = make_decorator(PySparkDecorator, output_pandas=False, output_pyarrow=False)
    assert deco.attributes["output_format"] == "url"


def test_legacy_user_timeout_becomes_timeout():
    deco = make_decorator(PySparkDecorator, user_timeout=30)
    assert deco.attributes["timeout"] == 30


def test_legacy_spark_config_names_the_config_artifact():
    deco = make_decorator(PySparkDecorator, spark_config="my_config")
    assert deco.attributes["config"] == "my_config"


def test_explicit_output_format_wins_over_the_legacy_flags():
    deco = make_decorator(PySparkDecorator, output_pandas=True, output_format="polars")
    assert deco.attributes["output_format"] == "polars"


def test_a_non_callable_job_is_rejected_at_step_init():
    with pytest.raises(SparkConfigError, match="callable"):
        make_decorator(SparkDecorator, job="myjob.run")


def test_a_bad_output_format_is_rejected_at_step_init():
    with pytest.raises(SparkConfigError):
        make_decorator(SparkDecorator, output_format="hdf5")


# ----------------------------------------------------------------------
class PlanMetrics:
    """Stands in for the pyspark object Spark Connect's toPandas() puts in attrs."""


PlanMetrics.__module__ = "pyspark.sql.connect.client.core"


def _connect_frame():
    pandas = pytest.importorskip("pandas")
    frame = pandas.DataFrame({"bucket": [0, 1]})
    frame.attrs["metrics"] = [PlanMetrics()]
    frame.attrs["source"] = "orders"
    return frame


def test_pyspark_attrs_are_stripped_and_others_kept():
    frame = strip_spark_attrs(_connect_frame())
    assert frame.attrs == {"source": "orders"}


def test_a_session_step_strips_pyspark_attrs_from_its_artifacts():
    from contextlib import contextmanager
    from types import SimpleNamespace

    class Backend:
        @contextmanager
        def session(self, ctx):
            yield object()

    flow = Flow()

    def step_func():
        flow.summary = _connect_frame()

    ctx = SimpleNamespace(job_func=None)
    _run_session(None, Backend(), ctx, step_func, flow, attrs(), "pandas")
    assert "metrics" not in flow.summary.attrs
    assert not hasattr(flow, "spark")
