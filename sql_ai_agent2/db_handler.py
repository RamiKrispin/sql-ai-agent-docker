import pandas as pd
import duckdb as db
import ibis

def get_postgres_schema(con, tbl_name: str):
    """
    Retrieve the schema information for a given PostgreSQL table.

    This function queries the `information_schema.columns` view to 
    return column-level metadata, including column names, data types,
    character lengths, and numeric precision details.

    Parameters
    ----------
    con : ibis.Connection
        An active Ibis connection to a PostgreSQL database.
    table_name : str
        Name of the table to retrieve schema information for.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing one row per column, with the following fields:
        - column_name
        - data_type
        - character_maximum_length
        - numeric_precision
        - numeric_scale

    Notes
    -----
    - The function assumes `con.con` exposes a compatible DB-API connection
      that supports `pd.read_sql()`.
    - Use parameterized queries if `table_name` comes from user input to
      prevent SQL injection.
    """

    query = f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_name = '{tbl_name}'
        ORDER BY ordinal_position
    """
    df = pd.read_sql(query, con.con) 
    df = df.rename(columns={"data_type": "column_type"})
    df = df[["column_name", "column_type"]]

    return df

def get_duckdb_schema(con, tbl_name: str):
    table_schema = con.sql(f"DESCRIBE SELECT * FROM {tbl_name};").df()
    col_info = table_schema[["column_name", "column_type"]]

    schema = ", ".join(
        f"{name} {dtype}"
        for name, dtype in zip(col_info["column_name"], col_info["column_type"])
    )
    return schema



def get_schema(con, tbl_name: str):
    if isinstance(con, ibis.backends.postgres.Backend):
        schema = get_postgres_schema(con = con, tbl_name = tbl_name)
    elif isinstance(con, db.duckdb.DuckDBPyConnection):
        schema = get_duckdb_schema(con = con, tbl_name = tbl_name)
    
    return schema



