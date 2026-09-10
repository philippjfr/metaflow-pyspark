import pytest

from metaflow_extensions.spark.plugins.backends.databricks import (
    DEFAULT_MODE,
    _BY_MODE,
)
from metaflow_extensions.spark.plugins.backends.databricks.compute import (
    DEFAULT_RUNTIME_VERSION,
    EXISTING_CLUSTER,
    INSTANCE_POOL,
    NEW_CLUSTER,
    SERVERLESS,
    resolve_compute,
)
from metaflow_extensions.spark.plugins.backends.databricks.jobs import (
    _DatabricksRunBackend,
)
from metaflow_extensions.spark.plugins.backends.databricks.storage import (
    is_cloud_uri,
    is_volume_path,
    normalize_volume,
)
from metaflow_extensions.spark.plugins.context import JobState
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError

terminal_state = _DatabricksRunBackend._terminal_state


# ----------------------------------------------------------------------
# run-state classification. The original code conflated cancellation, failure, and
# control-plane trouble; every branch here is one of those distinctions.
# ----------------------------------------------------------------------
def test_api_21_result_state_is_authoritative():
    assert terminal_state("TERMINATED", "SUCCESS", {}) == JobState.SUCCESS
    assert terminal_state("TERMINATED", "FAILED", {}) == JobState.FAILED
    assert terminal_state("TERMINATED", "CANCELED", {}) == JobState.CANCELLED
    assert terminal_state("TERMINATED", "TIMEDOUT", {}) == JobState.FAILED


def test_skipped_and_internal_error_are_failures():
    assert terminal_state("SKIPPED", None, {}) == JobState.FAILED
    assert terminal_state("INTERNAL_ERROR", None, {}) == JobState.FAILED


def test_api_22_termination_details():
    assert (
        terminal_state("TERMINATED", None, {"code": "SUCCESS", "type": "SUCCESS"})
        == JobState.SUCCESS
    )
    assert (
        terminal_state("TERMINATED", None, {"code": "USER_CANCELED", "type": "CLIENT_ERROR"})
        == JobState.CANCELLED
    )
    assert (
        terminal_state("TERMINATED", None, {"code": "DRIVER_ERROR", "type": "CLIENT_ERROR"})
        == JobState.FAILED
    )
    assert (
        terminal_state("TERMINATED", None, {"code": "CLOUD_FAILURE", "type": "INTERNAL_ERROR"})
        == JobState.FAILED
    )


def test_terminated_with_no_outcome_is_not_invented_as_a_failure():
    assert terminal_state("TERMINATED", None, {}) == JobState.SUCCESS


# ----------------------------------------------------------------------
# compute shapes
# ----------------------------------------------------------------------
def test_nothing_configured_means_serverless():
    spec = resolve_compute({})
    assert spec.kind == SERVERLESS
    assert spec.task_fields() == {"environment_key": "metaflow-spark"}
    assert spec.submit_fields()["environments"][0]["spec"] == {"client": "3"}


def test_existing_cluster_shorthand():
    spec = resolve_compute({"cluster_id": "0101-abc"})
    assert spec.kind == EXISTING_CLUSTER
    assert spec.task_fields() == {"existing_cluster_id": "0101-abc"}
    assert spec.submit_fields() == {}


def test_instance_pool_omits_node_type():
    spec = resolve_compute({"instance_pool_id": "pool-1", "num_workers": 8})
    assert spec.kind == INSTANCE_POOL
    assert "node_type_id" not in spec.new_cluster
    assert spec.new_cluster["instance_pool_id"] == "pool-1"
    assert spec.new_cluster["num_workers"] == 8


def test_new_cluster_defaults_and_photon():
    spec = resolve_compute(
        {"runtime_version": "14.3.x-scala2.12", "photon": True}, cloud="azure"
    )
    assert spec.kind == NEW_CLUSTER
    assert spec.new_cluster["spark_version"] == "14.3.x-scala2.12"
    assert spec.new_cluster["node_type_id"] == "Standard_DS3_v2"
    assert spec.new_cluster["runtime_engine"] == "PHOTON"


def test_node_type_alone_builds_a_new_cluster():
    spec = resolve_compute({"node_type_id": "r5.4xlarge"})
    assert spec.kind == NEW_CLUSTER
    assert spec.new_cluster["spark_version"] == DEFAULT_RUNTIME_VERSION


def test_conflicting_compute_is_rejected():
    with pytest.raises(SparkConfigError, match="Conflicting compute"):
        resolve_compute({"serverless": True, "cluster_id": "0101-abc"})


def test_tags_land_on_the_new_cluster():
    spec = resolve_compute({"node_type_id": "r5.xlarge"}, tags={"metaflow_flow": "F"})
    assert spec.new_cluster["custom_tags"]["metaflow_flow"] == "F"


def test_spark_parameters_become_spark_conf():
    spec = resolve_compute(
        {"node_type_id": "r5.xlarge", "spark-parameters": {"spark.sql.shuffle.partitions": 200}}
    )
    assert spec.new_cluster["spark_conf"]["spark.sql.shuffle.partitions"] == "200"


def test_unknown_compute_string_is_rejected():
    with pytest.raises(SparkConfigError, match="not understood"):
        resolve_compute({"compute": "gpu"})


# ----------------------------------------------------------------------
# storage paths
# ----------------------------------------------------------------------
def test_volume_and_cloud_paths_are_distinguished():
    assert is_volume_path("/Volumes/main/default/staging")
    assert not is_volume_path("s3://bucket/key")
    assert is_cloud_uri("s3://bucket/key")
    assert is_cloud_uri("abfss://c@a.dfs.core.windows.net/p")
    assert not is_cloud_uri("/Volumes/main/default/staging")


def test_normalize_volume_strips_trailing_slashes():
    assert normalize_volume("/Volumes/main/default/staging/") == (
        "/Volumes/main/default/staging"
    )


def test_dbfs_style_volume_path_is_accepted():
    assert normalize_volume("dbfs:/Volumes/main/default/s") == "/Volumes/main/default/s"


# ----------------------------------------------------------------------
# mode dispatch
# ----------------------------------------------------------------------
def test_every_mode_maps_to_a_backend():
    assert DEFAULT_MODE in _BY_MODE
    for mode, backend in _BY_MODE.items():
        assert callable(backend), mode
