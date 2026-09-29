"""Metaflow extension for reading governed Databricks data.

Registers no step decorators yet: `UnityCatalogTable` and `query()` are plain objects
and functions usable from any step. Nothing here imports databricks-sdk, deltalake, or
pyarrow until a read actually happens.
"""
