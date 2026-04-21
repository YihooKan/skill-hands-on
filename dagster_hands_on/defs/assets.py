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


@dg.asset(
    deps=["customers", "orders", "payments"],
)
def orders_aggregation(duckdb: DuckDBResource):
    table_name = "orders_aggregation"

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {table_name} as (
                select
                    c.id as customer_id,
                    c.first_name,
                    c.last_name,
                    count(distinct o.id) as total_orders,
                    count(distinct p.id) as total_payments,
                    coalesce(sum(p.amount), 0) as total_amount_spent
                from customers c
                left join orders o
                    on c.id = o.user_id
                left join payments p
                    on o.id = p.order_id
                group by 1, 2, 3
            );
            """
        )
        
@dg.asset_check(asset=orders_aggregation)
def orders_aggregation_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    table_name = "orders_aggregation"
    with duckdb.get_connection() as conn:
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
    if row_count > 0:
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "row_count": row_count,
                "message": f"orders_aggregation has {row_count} rows, check passed!"
            }
        )

    return dg.AssetCheckResult(
        passed=False,
        metadata={
            "row_count": row_count, 
            "message": "No rows found in orders_aggregation"
        }
    )
