"""Chapter 7b: turn the tags into a cost breakdown.

    python query_costs.py --warehouse-id abc123
    python query_costs.py --run-id 1724500000000000 --group-by step
    python query_costs.py --source query-history --group-by step

Groups the Databricks system billing tables by the tags the submitted jobs carried, through
a SQL warehouse. This is a parameterized version of `plugins/cost.DATABRICKS_USAGE_QUERY`,
which is the copy-paste form for a workspace SQL editor; `--print-query` prints the SQL if
you would rather run it there. Nothing here is Metaflow-specific machinery: it is the
customer's own billing data, grouped by tags they can also group by themselves.

`--source billing` (the default) covers submitted jobs, whose compute carries the tags as
`custom_tags`. `--source query-history` covers `query()` statements, which run on a shared
warehouse and carry the tags as `query_tags` instead, so the breakdown is in statements and
warehouse time rather than DBUs.

Requirements: `system.billing` (or `system.query`) enabled on the metastore, USE on the
schema, and a SQL warehouse. Rows land within minutes to hours, so a run submitted a minute
ago may not appear yet.
"""

import argparse
import os
import sys
import time

STATEMENTS = "/api/2.0/sql/statements"

GROUPINGS = {
    "run": ["flow", "run_id"],
    "step": ["flow", "run_id", "step"],
    "flow": ["flow"],
    "user": ["user"],
}

QUERY = """
SELECT
    {group_columns},
    SUM(u.usage_quantity) AS dbus,
    ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0)), 2)
        AS est_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
   AND u.usage_end_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.custom_tags['metaflow_flow'] IS NOT NULL
  AND u.usage_date >= DATEADD(day, -{days}, CURRENT_DATE())
  {run_filter}
GROUP BY ALL
ORDER BY est_cost_usd DESC
LIMIT {limit}
"""

# Warehouse statements share compute, so there is no per-statement DBU figure to sum;
# statement count and execution time are what is attributable.
HISTORY_QUERY = """
SELECT
    {group_columns},
    COUNT(*) AS statements,
    ROUND(SUM(q.total_duration_ms) / 1000.0, 1) AS seconds
FROM system.query.history q
WHERE q.query_tags['metaflow_flow'] IS NOT NULL
  AND q.start_time >= DATEADD(day, -{days}, CURRENT_DATE())
  {run_filter}
GROUP BY ALL
ORDER BY seconds DESC
LIMIT {limit}
"""

SOURCES = {
    "billing": (QUERY, "u.custom_tags"),
    "query-history": (HISTORY_QUERY, "q.query_tags"),
}


def build_query(group_by, days, run_id, limit, source="billing"):
    template, tags = SOURCES[source]
    columns = ",\n    ".join(
        "%s['metaflow_%s'] AS %s" % (tags, name, name) for name in GROUPINGS[group_by]
    )
    # run_id is validated as digits by the CLI, so formatting it in is safe.
    run_filter = "AND %s['metaflow_run_id'] = '%s'" % (tags, run_id) if run_id else ""
    return template.format(
        group_columns=columns, days=days, run_filter=run_filter, limit=limit
    )


def execute(client, warehouse_id, statement):
    response = client.api(
        "POST",
        STATEMENTS,
        body={
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
        },
    )
    while response.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        response = client.api("GET", "%s/%s" % (STATEMENTS, response["statement_id"]))

    state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        sys.exit(
            "query %s: %s"
            % (state, response.get("status", {}).get("error", {}).get("message", ""))
        )
    return response


def print_table(response):
    schema = response.get("manifest", {}).get("schema", {}).get("columns") or []
    names = [c["name"] for c in schema]
    rows = response.get("result", {}).get("data_array") or []
    if not rows:
        print(
            "Nothing tagged found. System table rows take a while to appear; billing "
            "rows only exist for jobs submitted with mode='job', and query history rows "
            "only for query() statements."
        )
        return

    widths = [
        max(len(name), *(len(str(row[i] or "")) for row in rows))
        for i, name in enumerate(names)
    ]
    line = "  ".join("%-*s" % (width, name) for width, name in zip(widths, names))
    print(line)
    print("-" * len(line))
    for row in rows:
        print(
            "  ".join(
                "%-*s" % (width, "" if value is None else value)
                for width, value in zip(widths, row)
            )
        )


def _run_id(value):
    if not value.isdigit():
        raise argparse.ArgumentTypeError("a Metaflow run id is all digits")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse to run the query on (or set DATABRICKS_WAREHOUSE_ID)",
    )
    parser.add_argument("--run-id", type=_run_id, help="restrict to one Metaflow run")
    parser.add_argument(
        "--source",
        choices=sorted(SOURCES),
        default="billing",
        help="billing for submitted jobs, query-history for query() statements",
    )
    parser.add_argument("--group-by", choices=sorted(GROUPINGS), default="step")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--print-query", action="store_true", help="print the SQL and exit"
    )
    args = parser.parse_args()

    statement = build_query(
        args.group_by, args.days, args.run_id, args.limit, source=args.source
    )
    if args.print_query:
        print(statement)
        return
    if not args.warehouse_id:
        sys.exit("--warehouse-id is required (or set DATABRICKS_WAREHOUSE_ID)")

    from metaflow_extensions.spark.plugins.backends.databricks.client import (
        DatabricksClient,
    )

    print_table(execute(DatabricksClient(), args.warehouse_id, statement))


if __name__ == "__main__":
    main()
