import duckdb, logging
from pathlib import Path


DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/raw")
DIMENSION_TABLES = ["channels", "products", "customers", "times"]
FACT_TABLES = ["sales"]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def find_latest_parquet_file(table_name: str) -> Path:
    parquet_files = list(PARQUET_DIR.glob(f"{table_name}_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found for table {table_name}")
    latest_file = max(parquet_files, key=lambda file: file.stat().st_mtime)
    return latest_file

def load_staging_table(conn, table_name: str, is_fact: bool = False):
    if is_fact:
        parquet_file = find_latest_parquet_file(table_name)
    else:
        parquet_file = PARQUET_DIR / f"{table_name}_*.parquet"  
    sql = f"CREATE OR REPLACE TABLE staging.{table_name} AS SELECT * FROM read_parquet('{parquet_file}')"
    logging.info(f"Loading {table_name} table into staging table...")
    conn.execute(sql)
    sql = f"SELECT COUNT(*) FROM staging.{table_name}"
    result = conn.execute(sql).fetchone()
    logging.info(f"Loaded {result[0]} rows into staging table.")


def load_core_table(conn, target_table_name: str, source_table_name: str):
    sql = f"CREATE OR REPLACE TABLE main.{target_table_name} AS SELECT * FROM staging.{source_table_name}"
    logging.info(f"Loading {source_table_name} table into core table...")
    conn.execute(sql)
    sql = f"SELECT COUNT(*) FROM main.{target_table_name}"
    result = conn.execute(sql).fetchone()
    logging.info(f"Loaded {result[0]} rows into core table.")


def load_staging(conn):
    sql = "CREATE SCHEMA IF NOT EXISTS staging"
    conn.execute(sql)
    for table in DIMENSION_TABLES:
        try:
            load_staging_table(conn, table, is_fact=False)
        except Exception as e:
            logging.error(f"Error occurred while loading {table}: {e}")

    for table in FACT_TABLES:
        try:
            load_staging_table(conn, table, is_fact=True)
        except Exception as e:
            logging.error(f"Error occurred while loading {table}: {e}")


def load_core(conn):
    sql = "CREATE SCHEMA IF NOT EXISTS main"
    conn.execute(sql)
    for table in DIMENSION_TABLES:
        try:
            load_core_table(conn, f"dim_{table}", table)
        except Exception as e:
            logging.error(f"Error occurred while loading {table} into core: {e}")
            
    for table in FACT_TABLES:
        try:
           load_core_table(conn, f"fct_{table}", table)
        except Exception as e:
            logging.error(f"Error occurred while loading {table}: {e}")

    sql = "SHOW ALL TABLES"
    result = conn.execute(sql).fetchall()
    for row in result:
        logging.info(f" {row[1]}.{row[2]}")


def main(conn):
    load_staging(conn)
    load_core(conn)


if __name__ == "__main__":
    with duckdb.connect(DB_PATH) as conn:
        main(conn)
