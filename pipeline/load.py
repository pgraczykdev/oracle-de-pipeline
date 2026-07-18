import os
import duckdb, logging
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()


SOURCE = os.environ.get("EXTRACTOR", "oracle")
DB_PATH = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/raw")

ORACLE_DIMENSION_TABLES = ["channels", "products", "customers", "times"]
ORACLE_FACT_TABLES = ["sales"]

DUMMY_JSON_DIMENSION_TABLES = ["products", "users", "dates"]
DUMMY_JSON_FACT_TABLES = ["orders"]

DIMENSION_TABLES = ORACLE_DIMENSION_TABLES if SOURCE == 'oracle' else DUMMY_JSON_DIMENSION_TABLES
FACT_TABLES = ORACLE_FACT_TABLES if SOURCE == 'oracle' else DUMMY_JSON_FACT_TABLES

WATERMARK_COL = "time_id" if SOURCE == "oracle" else "order_date"


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
    log_how_many_loaded_rows(conn, "staging", table_name)



def log_how_many_loaded_rows(conn, schema_name: str, table_name: str) -> int:
    result = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}").fetchone()
    logging.info(f"Loaded {result[0]} rows into {schema_name}.{table_name}.")
    return result[0]



def has_rows(conn, table_name: str) -> bool:
    sql = f"SELECT 1 FROM main.{table_name} LIMIT 1"
    try:
        result = conn.execute(sql).fetchone()
    except Exception as e:
        logging.info(f"Error occurred while checking rows in {table_name}: {e}")
        return False
    return result is not None



def load_core_fact_table(conn, target_table_name: str, source_table_name: str, watermark_col: str):
    trg_has_rows = has_rows(conn, target_table_name)
    if not trg_has_rows:
        sql = f"CREATE OR REPLACE TABLE main.{target_table_name} AS SELECT * FROM staging.{source_table_name}"
        logging.info(f"Loading {source_table_name} table into core table...")
        conn.execute(sql)
        log_how_many_loaded_rows(conn, "main", target_table_name)
    else:
        logging.info(f"Fact table {target_table_name} already has rows.")
        sql = f"""INSERT INTO main.{target_table_name}
                  SELECT * FROM staging.{source_table_name} src 
                  WHERE src.{watermark_col} > (SELECT MAX({watermark_col}) FROM main.{target_table_name})"""
        count_sql = f"""SELECT COUNT(*) FROM staging.{source_table_name}
                        WHERE {watermark_col} > (SELECT MAX({watermark_col}) FROM main.{target_table_name})"""
        new_rows = conn.execute(count_sql).fetchone()[0]
        logging.info(f"Loading {source_table_name} table into core table...")
        conn.execute(sql)
        logging.info(f"Inserted {new_rows} new rows into main.{target_table_name}.")



def load_core_dimension_table(conn, target_table_name: str, source_table_name: str):
    sql = f"CREATE OR REPLACE TABLE main.{target_table_name} AS SELECT * FROM staging.{source_table_name}"
    logging.info(f"Loading {source_table_name} table into core table...")
    conn.execute(sql)

    log_how_many_loaded_rows(conn, "main", target_table_name)



def load_core_table(conn, target_table_name: str, source_table_name: str, is_fact: bool = False):
    if is_fact:
        load_core_fact_table(conn, target_table_name, source_table_name, WATERMARK_COL)
    else:
        load_core_dimension_table(conn, target_table_name, source_table_name)
       
   

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
            load_core_table(conn, f"dim_{table}", table, is_fact=False)
        except Exception as e:
            logging.error(f"Error occurred while loading {table} into core: {e}")
            
    for table in FACT_TABLES:
        try:
           load_core_table(conn, f"fct_{table}", table, is_fact=True)
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
