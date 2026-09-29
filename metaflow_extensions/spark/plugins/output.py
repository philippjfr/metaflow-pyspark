"""Turning an Arrow result into the format a step asked for."""

from .exceptions import SparkException


def _require(module, output_format):
    try:
        return __import__(module)
    except ImportError:
        raise SparkException(
            "output_format=%r requires '%s'. Install it, or add it to the step with "
            "@pypi(packages={'%s': ''})." % (output_format, module, module)
        )


def from_arrow_table(table, output_format):
    """Convert an already-collected Arrow table to `pandas`, `arrow`, or `polars`."""
    if output_format == "arrow":
        return table
    if output_format == "pandas":
        _require("pandas", output_format)
        return table.to_pandas()
    if output_format == "polars":
        polars = _require("polars", output_format)
        return polars.from_arrow(table)
    return table
