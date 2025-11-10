from sql_ai_agent2.api_handler import SqlAgent2
import ibis

base_url="http://model-runner.docker.internal/engines/v1"
api_key="docker"
temperature=0
model = "ai/llama3.2:lates"
tbl_name = "air_traffic"
max_token = 10000

con = ibis.postgres.connect(
    user="postgres",
    password="password",
    host="postgres",
    port=5432,
    database="my_db",
)

con.get_schema("air_traffic")

# air_traffic = con.sql("SELECT * FROM air_traffic LIMIT 10").execute()

# print(air_traffic.head())


import duckdb
import pandas as pd
import ibis
from dataclasses import dataclass


@dataclass
class TableAttributes:
    col_names: list[str]
    col_types: list[str]
    tbl_schema: str


def get_tbl_attr(tbl, tbl_name: str = None, con=None) -> TableAttributes:
    """
    Extract table schema (column names, types, and SQL CREATE TABLE string)
    from DuckDB, PostgreSQL (via Ibis), or pandas DataFrame.
    """

    # --- Case 1: ibis table (e.g., from postgres or duckdb) ---
    if isinstance(tbl, ibis.Table):
        schema = tbl.schema()
        col_names = list(schema.names)
        col_types = [str(t) for t in schema.types]
        tbl_schema = ", ".join(f"{n} {t}" for n, t in zip(col_names, col_types))
        tbl_name = tbl_name or tbl.get_name() or "table"
        return TableAttributes(col_names, col_types, tbl_schema)

    # --- Case 2: pandas DataFrame ---
    elif isinstance(tbl, pd.DataFrame):
        # Use duckdb to infer SQL types from pandas types
        con = con or duckdb.connect()
        tbl_name = tbl_name or "table"
        con.register(tbl_name, tbl)
        info = con.sql(f"DESCRIBE SELECT * FROM {tbl_name};").df()
        col_names = info["column_name"].tolist()
        col_types = info["column_type"].tolist()
        tbl_schema = ", ".join(f"{n} {t}" for n, t in zip(col_names, col_types))
        return TableAttributes(col_names, col_types, tbl_schema)

    # --- Case 3: string table name, connected DB (DuckDB or Postgres) ---
    elif isinstance(tbl, str):
        tbl_name = tbl_name or tbl
        # Use DuckDB if provided or global connection
        con = con or duckdb.connect()
        try:
            info = con.sql(f"DESCRIBE SELECT * FROM {tbl_name};").df()
            col_names = info["column_name"].tolist()
            col_types = info["column_type"].tolist()
            tbl_schema = ", ".join(f"{n} {t}" for n, t in zip(col_names, col_types))
            return TableAttributes(col_names, col_types, tbl_schema)
        except duckdb.CatalogException:
            raise ValueError(
                f"Table '{tbl_name}' not found in DuckDB connection. "
                "Provide a registered DataFrame, Ibis table, or valid connection."
            )

    else:
        raise TypeError(
            f"Unsupported type {type(tbl)}. "
            "Provide a pandas DataFrame, Ibis table, or table name string."
        )


get_tbl_attr(tbl = tbl_name, tbl_name = tbl_name, con = con)
# agent = SqlAgent2(api_key, 
#                   base_url, 
#                   model=model, 
#                   temperature = temperature,
#                   max_token= max_token,
#                   tbl_name = tbl_name)

