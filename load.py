import duckdb, logging
from pathlib import Path


DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/raw")
DIMENSION_TABLES = ["channels", "products", "customers", "times"]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def load_dimension(conn, table_name: str):
    sql = f"CREATE OR REPLACE TABLE staging.{table_name} AS SELECT * FROM read_parquet('{PARQUET_DIR}/{table_name}_*.parquet')"
    logging.info(f"Loading {table_name} table into staging table...")
    conn.execute(sql)
    sql = f"SELECT COUNT(*) FROM staging.{table_name}"
    result = conn.execute(sql).fetchone()
    logging.info(f"Loaded {result[0]} rows into staging table.")


def main(conn):
    sql = "CREATE SCHEMA IF NOT EXISTS staging"
    conn.execute(sql)
    for table in DIMENSION_TABLES:
        try:
           load_dimension(conn, table)
        except Exception as e:
           logging.error(f"Error occurred while loading {table}: {e}")



if __name__ == "__main__":
    with duckdb.connect(DB_PATH) as conn:
        main(conn)
