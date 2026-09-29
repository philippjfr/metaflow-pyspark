"""Tests for `query()`, the entry point over the SQL warehouse backend."""

import pyarrow as pa
import pytest
from databricks.sdk.service.sql import (
    ExternalLink,
    ResultData,
    StatementResponse,
    StatementState,
    StatementStatus,
)

import metaflow_extensions.spark.plugins.backends as backends_mod
import metaflow_extensions.spark.plugins.backends.databricks.sql as sql_mod
from metaflow_extensions.spark.plugins.exceptions import (
    SparkConfigError,
    SparkJobFailed,
)
from metaflow_extensions.spark.plugins.warehouse import query


class FakeService:
    def __init__(self, states=(StatementState.SUCCEEDED,), poll_error=None):
        self.states = list(states)
        self.poll_error = poll_error
        self.executed = []
        self.cancelled = []

    def execute_statement(self, statement, warehouse_id, **kwargs):
        self.executed.append(
            dict(kwargs, statement=statement, warehouse_id=warehouse_id)
        )
        return StatementResponse(
            statement_id="stmt-1", status=StatementStatus(state=StatementState.PENDING)
        )

    def get_statement(self, statement_id):
        if self.poll_error is not None:
            raise self.poll_error
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return StatementResponse(
            statement_id=statement_id,
            status=StatementStatus(state=state),
            result=ResultData(
                external_links=[ExternalLink(chunk_index=0, external_link="https://x")]
            ),
        )

    def cancel_execution(self, statement_id):
        self.cancelled.append(statement_id)

    def get_statement_result_chunk_n(self, statement_id, chunk_index):
        return ResultData(chunk_index=chunk_index)


@pytest.fixture
def service(monkeypatch):
    for env in ("DATABRICKS_WAREHOUSE_ID", "METAFLOW_DATABRICKS_WAREHOUSE_ID"):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setattr(backends_mod.time, "sleep", lambda seconds: None)

    fake = FakeService()

    class FakeClient:
        def __init__(self, config=None):
            self.sdk = type("Sdk", (), {"statement_execution": fake})()

        def warehouse_url(self, warehouse_id):
            return "https://workspace/sql/warehouses/%s" % warehouse_id

    monkeypatch.setattr(sql_mod, "DatabricksClient", FakeClient)
    monkeypatch.setattr(
        sql_mod,
        "_read_arrow_stream",
        lambda link: pa.table(
            {"order_date": ["2026-01-01"], "revenue": [10.0]}
        ).to_batches(),
    )
    return fake


def test_query_returns_the_result_in_the_requested_format(service):
    result = query("SELECT 1", warehouse_id="wh-1", output_format="arrow")
    assert result.column_names == ["order_date", "revenue"]
    assert query("SELECT 1", warehouse_id="wh-1").revenue.tolist() == [10.0]


def test_params_are_bound_not_formatted_into_the_statement(service):
    statement = "SELECT * FROM t WHERE since >= :since"
    query(statement, params={"since": "'; DROP TABLE t; --"}, warehouse_id="wh-1")
    executed = service.executed[0]
    assert executed["statement"] == statement
    assert executed["parameters"][0].value == "'; DROP TABLE t; --"


def test_explicit_warehouse_id_beats_the_environment(service, monkeypatch):
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "from-env")
    query("SELECT 1", warehouse_id="explicit", output_format="none")
    assert service.executed[0]["warehouse_id"] == "explicit"


def test_the_environment_supplies_the_default_warehouse(service, monkeypatch):
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "from-env")
    query("SELECT 1", output_format="none")
    assert service.executed[0]["warehouse_id"] == "from-env"


def test_a_flow_config_artifact_supplies_the_warehouse(service):
    class Flow:
        spark_config = {"databricks": {"warehouse_id": "from-flow"}}

    query("SELECT 1", flow=Flow(), output_format="none")
    assert service.executed[0]["warehouse_id"] == "from-flow"


def test_no_warehouse_anywhere_explains_how_to_set_one(service):
    with pytest.raises(SparkConfigError, match="DATABRICKS_WAREHOUSE_ID"):
        query("SELECT 1")
    assert service.executed == []


@pytest.mark.parametrize("output_format", ["table", "url", "spark"])
def test_reference_formats_are_rejected_up_front(service, output_format):
    with pytest.raises(SparkConfigError, match="not meaningful"):
        query("SELECT 1", warehouse_id="wh-1", output_format=output_format)
    assert service.executed == []


def test_a_failed_statement_raises_job_failed(service):
    service.states = [StatementState.FAILED]
    with pytest.raises(SparkJobFailed):
        query("SELECT nope", warehouse_id="wh-1")


def test_an_interrupt_cancels_the_statement(service):
    service.poll_error = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        query("SELECT 1", warehouse_id="wh-1")
    assert service.cancelled == ["stmt-1"]
