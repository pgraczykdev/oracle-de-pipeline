import os
from datetime import datetime, timezone
from pathlib import Path

import oracledb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

oracledb.defaults.fetch_decimals = True

OUTPUT_DIR = Path("data/raw")
DIMENSION_TABLES = ["channels", "products", "customers", "times"]
SALES_BATCH_SIZE = 50000


def get_connection():
    return oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=os.environ["ORACLE_DSN"],
        config_dir=os.environ["WALLET_DIR"],
        wallet_location=os.environ["WALLET_DIR"],
        wallet_password=os.environ["WALLET_PASSWORD"],
    )

def write_parquet(df: pl.DataFrame, name: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = OUTPUT_DIR / f"{name}_{stamp}.parquet"
    df.write_parquet(path)
    return path


def extract_table(conn, table: str) -> Path:
    df = pl.read_database(f"SELECT * FROM sh.{table}", connection=conn)
    path = write_parquet(df, table)
    print(f"{table}: {df.height} rows -> {path}")
    return path

def extract_sales_full(conn) -> Path:
    df_chunks = []
    for df_chunk in pl.read_database("SELECT * FROM sh.sales", connection=conn, iter_batches=True, batch_size=SALES_BATCH_SIZE):
        df_chunks.append(df_chunk)
        print(f"Sales chunk {len(df_chunks)}: {df_chunk.height} rows")

    df = pl.concat(df_chunks)
    path = write_parquet(df, "sales")
    print(f"sales: {df.height} rows -> {path}")
    return path


if __name__ == "__main__":
    with get_connection() as conn:
        for table in DIMENSION_TABLES:
            extract_table(conn, table)

        extract_sales_full(conn=conn)    
    