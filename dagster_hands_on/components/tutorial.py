import dagster as dg
from dagster_duckdb import DuckDBResource

class ETL(dg.Model):
    url_path: str
    table_name: str

class Tutorial(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """

    # added fields here will define params when instantiated in Python, and yaml schema via Resolvable
    
    duckdb_database: str
    etl_steps: list[ETL]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _etl_steps = []
        for etl in self.etl_steps:
            @dg.asset(name=etl.table_name)
            def _table(duckdb: DuckDBResource):
                with duckdb.get_connection() as conn:
                    conn.execute(
                        f"""
                        CREATE OR REPLACE TABLE {etl.table_name} AS
                        SELECT * 
                        FROM read_csv_auto('{etl.url_path}')
                        """
                    )
            _etl_steps.append(_table)
        
        return dg.Definitions(
            assets=_etl_steps,
            resources={"duckdb": DuckDBResource(database=self.duckdb_database)},
        )
