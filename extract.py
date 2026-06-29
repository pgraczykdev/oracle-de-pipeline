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


if __name__ == "__main__":
    with get_connection() as conn:
        for table in DIMENSION_TABLES:
            extract_table(conn, table)

        salesQuery = """
                SELECT
                    prod_id,
                    cust_id,
                    time_id,
                    channel_id,
                    amount_sold
                FROM sh.sales
                FETCH FIRST 1000 ROWS ONLY
            """

        sales_sample = pl.read_database(salesQuery, conn)
        path = write_parquet(sales_sample, "sales_sample")
        print(f"sales_sample: {sales_sample.height} rows -> {path}")