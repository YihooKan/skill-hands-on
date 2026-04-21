import pandas as pd

from dagster_duckdb import DuckDBResource
import dagster as dg

sample_data_file = "dagster_hands_on/defs/data/sample_data.csv"
processed_data_file = "dagster_hands_on/defs/data/processed_data.csv"

@dg.asset
def hello_world(context: dg.AssetExecutionContext) -> str:
    """A simple asset that returns a greeting message"""
    context.log.info("Executing hello_world asset")
    return "Hello, Dagster!"



@dg.asset
def processed_data(context: dg.AssetExecutionContext) -> str:
    context.log.info("Executing processed_data asset")
    context.log.info(f"Reading sample data from {sample_data_file}")
    df = pd.read_csv(sample_data_file)
    ## Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    ## Save processed data
    context.log.info(f"Saving processed data to {processed_data_file}")
    df.to_csv(processed_data_file, index=False)
    return "Data loaded successfully"

@dg.asset
def customers(duckdb: DuckDBResource) -> str:
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv"
    table_name = "customers"
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_csv_auto('{url}')
            """
        )
    return f"Loaded customers data from {url} into DuckDB table {table_name}"


@dg.asset
def orders(duckdb: DuckDBResource) -> str:
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_orders.csv"
    table_name = "orders"
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_csv_auto('{url}')
            """
        )
    return f"Loaded orders data from {url} into DuckDB table {table_name}"


@dg.asset
def payments(duckdb: DuckDBResource) -> str:
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_payments.csv"
    table_name = "payments"
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_csv_auto('{url}')
            """
        )
    return f"Loaded payments data from {url} into DuckDB table {table_name}"
