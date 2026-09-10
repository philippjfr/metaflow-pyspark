"""Unity Catalog tables as Metaflow artifacts.

The central idea: **the artifact is a table reference, not a copy of the table.**

    self.orders = UnityCatalogTable("main.retail.orders")

Assignment pins the table's current Delta version, so re-running the flow reads the same
bytes even if the table has moved on. That extends Metaflow's reproducibility guarantee
over data the customer governs in Unity Catalog, without copying it and without taking
governance away from UC.

Reads can happen two ways:

*Through Spark*, with ``to_spark(session)``, which is what you want when the data is
large or the work is a join.

*Without any cluster*, with ``to_arrow()``, which asks Unity Catalog to vend temporary,
scoped cloud credentials for the table's storage location and reads the Delta files
directly. Governance is preserved because UC issues the credentials and enforces the
grants; there is simply no Spark involved. For mid-size tables this is dramatically
cheaper than starting a cluster.

A table with deletion vectors enabled, the default on recent Databricks Runtimes for
many write patterns, cannot be read by the ``deltalake`` package's reader at all: it does
not support that protocol feature on any of its read methods, as of the latest release at
the time this was written (1.6.3). ``to_arrow()`` falls back to DuckDB's own Delta
reader for that case, which does apply deletion vectors correctly. Covers AWS and Azure
SAS credentials (see ``_read_via_duckdb``'s docstring for exactly how well-verified each
is); GCP has no DuckDB secret shape for the OAuth bearer token UC vends. Install
``duckdb`` to get the fallback; without it, or on GCP, a table with deletion vectors fails
with an explanation rather than a raw stack trace, pointing at ``to_spark()``.
"""

import os
import sys
import time

from ..exceptions import SparkBackendUnavailable, UnityCatalogError

UC_API = "/api/2.1/unity-catalog"

#: Vended credentials are short-lived. Re-vend slightly before expiry rather than
#: racing it, since a long read can outlive a token issued at the start.
CREDENTIAL_REFRESH_MARGIN_SECONDS = 60
DEFAULT_REGION = "us-east-1"


def _delta_protocol_error_types():
    """The exception type(s) that mean "this table's protocol is not supported".

    A tuple rather than a single class, and resolved lazily: `deltalake` is an
    optional dependency (the `catalog` extra), and an empty tuple here just means
    nothing gets special-cased, not an ImportError at module load.
    """
    try:
        from deltalake.exceptions import DeltaProtocolError

        return (DeltaProtocolError,)
    except ImportError:
        return ()


def _azure_storage_account(storage_location):
    """Pull the storage account name out of an ADLS Gen2 storage location.

    UC's Azure storage locations look like
    `abfss://<container>@<account>.dfs.core.windows.net/<path>`. Needed for the
    `AccountName=`/`BlobEndpoint=https://<account>.blob.core.windows.net;...`
    connection string DuckDB's Azure secret takes; note that is the *blob* endpoint
    even though the URL scheme is `abfss`, per Microsoft's own connection-string
    format.
    """
    if not storage_location:
        return None
    from urllib.parse import urlparse

    host = urlparse(storage_location).netloc.rsplit("@", 1)[-1]
    return host.split(".", 1)[0] if host else None


class UnityCatalogTable:
    """A pinned reference to a Unity Catalog table.

    Picklable, so it round-trips as a Metaflow artifact. Live clients and vended
    credentials are deliberately excluded from the pickled state: credentials expire,
    and a stale one in an artifact would be both useless and a leak.
    """

    def __init__(
        self,
        name,
        version=None,
        timestamp=None,
        client=None,
        pin=True,
        config=None,
    ):
        if name.count(".") != 2:
            raise UnityCatalogError(
                "Unity Catalog tables are named catalog.schema.table, got %r." % name
            )
        self.full_name = name
        self.catalog, self.schema, self.table = name.split(".")
        self.version = version
        self.timestamp = timestamp
        self.table_id = None
        self.storage_location = None
        self.table_type = None
        self.data_source_format = None

        self._client = client
        self._config = config or {}
        self._credentials = None
        self._credentials_expiry = 0

        self._describe()
        if pin and self.version is None and self.timestamp is None:
            self.version = self._current_version()

    # ------------------------------------------------------------------
    # pickling
    # ------------------------------------------------------------------
    def __getstate__(self):
        state = self.__dict__.copy()
        for key in ("_client", "_credentials", "_credentials_expiry"):
            state.pop(key, None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._client = None
        self._credentials = None
        self._credentials_expiry = 0
        self._config = getattr(self, "_config", {}) or {}

    def __repr__(self):
        pin = ""
        if self.version is not None:
            pin = " @v%s" % self.version
        elif self.timestamp:
            pin = " @%s" % self.timestamp
        return "UnityCatalogTable(%s%s)" % (self.full_name, pin)

    # ------------------------------------------------------------------
    @property
    def client(self):
        if self._client is None:
            from ..backends.databricks.client import DatabricksClient

            self._client = DatabricksClient(self._config)
        return self._client

    @property
    def pinned(self):
        return self.version is not None or self.timestamp is not None

    def _describe(self):
        try:
            info = self.client.api("GET", "%s/tables/%s" % (UC_API, self.full_name))
        except Exception as exc:
            raise UnityCatalogError(
                "Could not read Unity Catalog metadata for %s: %s\n"
                "Check that the table exists and that you have SELECT on it plus USE "
                "CATALOG and USE SCHEMA on its parents." % (self.full_name, exc)
            ) from exc
        self.table_id = info.get("table_id")
        self.storage_location = info.get("storage_location")
        self.table_type = info.get("table_type")
        self.data_source_format = info.get("data_source_format")

    def _current_version(self):
        """Read the table's current Delta version, without needing a cluster."""
        if self.data_source_format not in (None, "DELTA", "UNITY_CATALOG"):
            # Version pinning is a Delta feature. Say so instead of failing obscurely.
            return None
        try:
            delta = self._delta_table()
            return delta.version() if delta is not None else None
        except Exception as exc:
            # Silent divergence is the outcome to avoid: an unpinned reference reads
            # whatever the table looks like at call time, and that has to be visible,
            # not a `version=None` nobody notices until a reproduce run comes up empty.
            # warnings.warn() is not enough here: Metaflow's own CLI calls
            # warnings.filterwarnings("ignore") globally (metaflow/cli.py), which
            # silently swallows warnings from user code too. Write to stderr directly.
            print(
                "[UnityCatalogTable] %s was assigned without a pinned Delta version: %s\n"
                "to_spark() and to_arrow() will read the table's current state on every "
                "call instead of a fixed snapshot. This usually means credential "
                "vending is unavailable (no EXTERNAL USE SCHEMA, or deltalake is not "
                "installed); reading through to_spark() still works. Pin explicitly "
                "once you know the version, e.g. UnityCatalogTable(%r, version=N), "
                "using N from `DESCRIBE HISTORY %s` run through Spark."
                % (self.full_name, exc, self.full_name, self.full_name),
                file=sys.stderr,
            )
            return None

    def at_version(self, version):
        """Return a new reference pinned to `version`."""
        return UnityCatalogTable(
            self.full_name, version=version, client=self._client, config=self._config
        )

    def at_timestamp(self, timestamp):
        return UnityCatalogTable(
            self.full_name, timestamp=timestamp, client=self._client, config=self._config
        )

    def latest(self):
        """Return a new reference pinned to the table's current version."""
        return UnityCatalogTable(
            self.full_name, client=self._client, config=self._config
        )

    # ------------------------------------------------------------------
    # credential vending
    # ------------------------------------------------------------------
    def credentials(self, operation="READ"):
        """Ask Unity Catalog for temporary, scoped credentials for this table.

        This is the mechanism that lets governed data be read outside Databricks without
        going around governance: UC checks the grant and issues a short-lived credential
        for that table's storage only.
        """
        now = time.time()
        if self._credentials and now < self._credentials_expiry:
            return self._credentials

        if not self.table_id:
            raise UnityCatalogError(
                "No table_id for %s, so credentials cannot be vended." % self.full_name
            )
        try:
            response = self.client.api(
                "POST",
                "%s/temporary-table-credentials" % UC_API,
                body={"table_id": self.table_id, "operation": operation},
            )
        except Exception as exc:
            raise UnityCatalogError(
                "Unity Catalog refused to vend credentials for %s: %s\n"
                "Credential vending requires EXTERNAL USE SCHEMA on the schema (or "
                "equivalent) in addition to SELECT on the table. If your workspace does "
                "not allow it, read through Spark with to_spark() instead."
                % (self.full_name, exc)
            ) from exc

        expiry_ms = response.get("expiration_time")
        if expiry_ms:
            # The API reports an absolute epoch in milliseconds.
            self._credentials_expiry = (
                expiry_ms / 1000.0
            ) - CREDENTIAL_REFRESH_MARGIN_SECONDS
        else:
            self._credentials_expiry = now + 300
        self._credentials = response
        return response

    def storage_options(self, operation="READ"):
        """Translate vended credentials into delta-rs / object_store options."""
        creds = self.credentials(operation=operation)
        options = {}

        aws = creds.get("aws_temp_credentials")
        if aws:
            options.update(
                {
                    "AWS_ACCESS_KEY_ID": aws.get("access_key_id"),
                    "AWS_SECRET_ACCESS_KEY": aws.get("secret_access_key"),
                    "AWS_SESSION_TOKEN": aws.get("session_token"),
                }
            )
            options["AWS_REGION"] = self._resolve_region(aws)
            return {k: v for k, v in options.items() if v}

        sas = creds.get("azure_user_delegation_sas")
        if sas:
            return {"AZURE_STORAGE_SAS_TOKEN": sas.get("sas_token")}

        aad = creds.get("azure_aad")
        if aad:
            return {"AZURE_STORAGE_TOKEN": aad.get("aad_token")}

        gcp = creds.get("gcp_oauth_token")
        if gcp:
            return {"GOOGLE_BEARER_TOKEN": gcp.get("oauth_token")}

        raise UnityCatalogError(
            "Unity Catalog returned credentials in a form this version does not "
            "understand: %s. Read through Spark with to_spark() instead."
            % ", ".join(sorted(creds))
        )

    def _resolve_region(self, aws_credentials):
        """Find the bucket's region, which delta-rs needs but UC does not return."""
        for env in ("AWS_REGION", "AWS_DEFAULT_REGION"):
            if os.environ.get(env):
                return os.environ[env]
        location = self.storage_location or ""
        if location.startswith("s3://"):
            bucket = location[len("s3://") :].split("/")[0]
            try:
                import boto3

                client = boto3.client(
                    "s3",
                    aws_access_key_id=aws_credentials.get("access_key_id"),
                    aws_secret_access_key=aws_credentials.get("secret_access_key"),
                    aws_session_token=aws_credentials.get("session_token"),
                )
                region = client.get_bucket_location(Bucket=bucket).get(
                    "LocationConstraint"
                )
                # us-east-1 is reported as None for historical reasons.
                return region or "us-east-1"
            except Exception:
                pass
        return DEFAULT_REGION

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def _delta_table(self):
        try:
            from deltalake import DeltaTable
        except ImportError:
            raise SparkBackendUnavailable(
                "unity-catalog credential vending", "deltalake", extra="catalog"
            )
        if not self.storage_location:
            raise UnityCatalogError(
                "%s has no storage location, so it cannot be read without Spark. "
                "Managed views and foreign tables have to go through to_spark()."
                % self.full_name
            )
        kwargs = {"storage_options": self.storage_options()}
        if self.version is not None:
            kwargs["version"] = self.version
        return DeltaTable(self.storage_location, **kwargs)

    def to_arrow(self, columns=None, filters=None):
        """Read the table into an Arrow table with no cluster involved."""
        delta = self._delta_table()
        if self.timestamp is not None:
            delta.load_as_version(self.timestamp)
        try:
            dataset = delta.to_pyarrow_dataset()
            table = dataset.to_table(columns=columns, filter=filters)
        except _delta_protocol_error_types() as exc:
            table = self._read_via_duckdb(exc)
            if columns:
                table = table.select(columns)
            if filters is not None:
                table = table.filter(filters)
        return table

    def _translate_protocol_error(self, exc, duckdb_reason=None):
        """Turn a delta-rs protocol error into an explanation, not a stack trace.

        Deletion vectors are on by default for many tables created on recent
        Databricks Runtimes (predictive optimization enables them), and delta-rs's
        Python reader does not support them as of the pinned minimum
        (``deltalake>=0.18``) through at least 1.6.3, on any of its read methods
        (``to_pyarrow_dataset``, ``to_pyarrow_table``, ``to_pandas``): confirmed by
        reproducing this against a local table with deletion vectors enabled, not
        assumed from the error message alone. ``to_arrow()`` tries DuckDB's own Delta
        reader first, which does apply deletion vectors correctly (also confirmed by
        reproduction, not assumed); this is what is left to say when that has already
        failed too, or was not applicable (Azure and GCP; see ``_read_via_duckdb``).

        ``duckdb_reason`` says specifically why the fallback did not apply or did not
        work, so the log line itself answers "why didn't it fall back to DuckDB"
        without anyone having to go digging through a swallowed exception chain.
        """
        message = (
            "%s cannot be read without a cluster: %s\n"
            "Read through Spark instead with to_spark(), which applies whatever the "
            "table's protocol requires as part of its own scan."
            % (self.full_name, exc)
        )
        if duckdb_reason:
            message += "\n(DuckDB fallback not used: %s)" % duckdb_reason
        return UnityCatalogError(message)

    def _read_via_duckdb(self, original_exc):
        """Fall back to DuckDB's own Delta reader for a table delta-rs cannot read.

        This is a genuinely different implementation from `deltalake`'s Python
        bindings, not the same engine through a different API, and it applies
        deletion vectors where delta-rs's Python reader does not (verified locally:
        `deltalake` fails on a deletion-vector table on every read method it has,
        DuckDB's `delta_scan` reads the same table correctly). Using it here is what
        keeps "no cluster" true for these tables instead of conceding them to Spark.

        Wired up for AWS and Azure SAS credentials, the two shapes UC vends that map
        onto something DuckDB's secret model accepts. GCP does not: DuckDB's GCS
        secret needs HMAC keys, not the OAuth bearer token UC vends, and there is no
        documented parameter that takes one. Azure AAD tokens (`azure_aad`, vended
        when the storage credential is a managed identity / Access Connector rather
        than SAS-capable) have the same problem: DuckDB's Azure secret has no
        parameter for an already-obtained bearer token either. AWS is verified end to
        end, including against a real deletion-vector table (see
        tests/test_unity_catalog.py). Azure SAS was verified against a real Azure
        workspace and initially failed: the connection string needs `AccountName=`
        in it, not just `BlobEndpoint=`/`SharedAccessSignature=` (Microsoft's own
        documented minimal format for a SAS-based connection string, which omits
        `AccountName`); duckdb-azure's own pre-check
        (`ConnectionStringMatchStorageAccountName` in
        `azure_storage_account_client.cpp`) requires it and throws before ever
        reaching the real Azure SDK parser if it is missing. Fixed; still not
        independently confirmed that it authenticates successfully end to end, only
        that it clears DuckDB's own format check.
        idea is wrong; report it rather than assuming it should already have been
        caught.

        Every failure here, including a vending refusal on top of the protocol
        error, ends up at the same explanation, but with a specific reason attached
        rather than a second, unrelated-looking error: `credentials()`'s own
        "refused to vend" message escaping instead would bury the actual cause (the
        table's protocol, not a missing grant).
        """
        try:
            creds = self.credentials()
        except Exception as exc:
            raise self._translate_protocol_error(
                original_exc,
                "vending credentials for the fallback itself failed: %s" % exc,
            )

        secret = self._duckdb_secret_for(creds)
        if secret is None:
            raise self._translate_protocol_error(
                original_exc,
                "the vended credentials were shaped %s, and only "
                "aws_temp_credentials or azure_user_delegation_sas (with a "
                "parseable abfss:// account) have a matching DuckDB secret"
                % sorted(creds),
            )
        extension, secret_sql, secret_params = secret

        try:
            import duckdb
        except ImportError:
            raise self._translate_protocol_error(
                original_exc, "duckdb is not installed"
            )

        con = duckdb.connect()
        try:
            con.install_extension(extension)
            con.load_extension(extension)
            con.install_extension("delta")
            con.load_extension("delta")
            con.execute(secret_sql, secret_params)
            try:
                # `.fetch_arrow_table()` rather than the lazy `.arrow()` reader:
                # the connection closes in `finally` below, and a lazy
                # RecordBatchReader consumed after that point silently reads as
                # empty rather than raising, which is a worse bug than a slower one.
                return con.sql(
                    "SELECT * FROM delta_scan(?)", params=[self.storage_location]
                ).fetch_arrow_table()
            except Exception as exc:
                raise self._translate_protocol_error(
                    original_exc, "DuckDB's own reader also failed: %s" % exc
                ) from exc
        finally:
            con.execute("DROP SECRET IF EXISTS metaflow_uc_credential")
            con.close()

    def _duckdb_secret_for(self, creds):
        """Build the `(extension, CREATE SECRET sql, params)` for these credentials.

        Returns None when the credential shape has no DuckDB secret that accepts it
        (AAD tokens, GCP), so the caller falls back to the Spark-pointer error rather
        than attempting a connection that cannot possibly authenticate.
        """
        aws = creds.get("aws_temp_credentials")
        if aws:
            return (
                "httpfs",
                "CREATE OR REPLACE SECRET metaflow_uc_credential (TYPE s3, "
                "PROVIDER config, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION ?, "
                "SCOPE ?)",
                [
                    aws.get("access_key_id"),
                    aws.get("secret_access_key"),
                    aws.get("session_token"),
                    self._resolve_region(aws),
                    self.storage_location,
                ],
            )

        sas = creds.get("azure_user_delegation_sas")
        if sas and sas.get("sas_token"):
            account = _azure_storage_account(self.storage_location)
            if account:
                # AccountName= has to be present and has to match the account name
                # DuckDB itself parses out of the abfss:// URL: duckdb-azure's own
                # ConnectionStringMatchStorageAccountName check requires it and
                # throws "A invalid connection string has been provided" (its exact
                # wording) if it is missing, before the connection string ever
                # reaches the real Azure SDK parser. The general Azure
                # BlobEndpoint+SAS format Microsoft documents does not need
                # AccountName at all; DuckDB's own extension does, regardless.
                connection_string = (
                    "AccountName=%s;BlobEndpoint=https://%s.blob.core.windows.net;"
                    "SharedAccessSignature=%s" % (account, account, sas["sas_token"])
                )
                return (
                    "azure",
                    "CREATE OR REPLACE SECRET metaflow_uc_credential "
                    "(TYPE azure, CONNECTION_STRING ?)",
                    [connection_string],
                )

        return None

    def to_pandas(self, columns=None, filters=None):
        return self.to_arrow(columns=columns, filters=filters).to_pandas()

    def to_polars(self, columns=None, filters=None):
        import polars

        return polars.from_arrow(self.to_arrow(columns=columns, filters=filters))

    def to_duckdb(self, connection=None, view_name=None):
        """Register the table as a DuckDB view for SQL without a cluster."""
        import duckdb

        connection = connection or duckdb.connect()
        arrow_table = self.to_arrow()
        connection.register(view_name or self.table, arrow_table)
        return connection

    def to_spark(self, session):
        """Read through Spark, honouring the pinned version.

        This is the path that keeps every read inside Unity Catalog's enforcement, and
        the right choice for large tables or anything join-heavy.
        """
        reader = session.read
        if self.version is not None:
            reader = reader.option("versionAsOf", self.version)
        elif self.timestamp is not None:
            reader = reader.option("timestampAsOf", self.timestamp)
        return reader.table(self.full_name)

    def materialize(self, output_format):
        """Materialize according to an @spark output_format."""
        if output_format in ("url", "table", "none"):
            return self if output_format == "table" else None
        if output_format == "arrow":
            return self.to_arrow()
        if output_format == "pandas":
            return self.to_pandas()
        if output_format == "polars":
            return self.to_polars()
        raise UnityCatalogError(
            "Cannot materialize a Unity Catalog table as %r." % output_format
        )

    # ------------------------------------------------------------------
    def history(self, limit=20):
        """Delta history entries, most recent first."""
        delta = self._delta_table()
        return delta.history(limit)

    def schema_fields(self):
        try:
            info = self.client.api("GET", "%s/tables/%s" % (UC_API, self.full_name))
        except Exception:
            return []
        return [
            {"name": c.get("name"), "type": c.get("type_text")}
            for c in info.get("columns") or []
        ]

    def lineage_tags(self, ctx):
        """Tags to stamp on a table this flow produced, for the customer's lineage."""
        return {
            "metaflow_pathspec": ctx.pathspec,
            "metaflow_flow": ctx.flow_name,
            "metaflow_run_id": ctx.run_id,
        }


def read_table(name, version=None, columns=None, spark=None, config=None):
    """Convenience reader.

    With `spark`, reads through the session. Without it, reads via credential vending
    and returns an Arrow table.
    """
    ref = UnityCatalogTable(name, version=version, config=config)
    if spark is not None:
        return ref.to_spark(spark)
    return ref.to_arrow(columns=columns)
