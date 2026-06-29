import os
from datetime import datetime, timezone
from pathlib import Path

import oracledb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

oracledb.defaults.fetch_decimals = True

OUTPUT_DIR = Path("data/raw")


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


if __name__ == "__main__":
    with get_connection() as conn:
        channels = pl.read_database("SELECT * FROM sh.channels", connection=conn)
        p1 = write_parquet(channels, "channels")
        print(f"channels: {channels.height} rows -> {p1}")

        sales_query = """
            SELECT
                prod_id,
                cust_id,
                time_id,
                channel_id,
                amount_sold
            FROM sh.sales
            FETCH FIRST 1000 ROWS ONLY
        """

        sales = pl.read_database(sales_query, connection=conn)
        print("--- schema sales ---")
        print(sales.schema)        # amount_sold powinno być Decimal, nie Float64
        p2 = write_parquet(sales, "sales_sample")
        print(f"sales_sample: {sales.height} rows -> {p2}")