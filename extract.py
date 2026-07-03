import os
import logging
import json
from datetime import datetime, timezone
from pathlib import Path

import oracledb
import polars as pl

from dotenv import load_dotenv



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)

load_dotenv()

oracledb.defaults.fetch_decimals = True

OUTPUT_DIR = Path("data/raw")
WATERMARK_FILE = Path("data/watermark.json")
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


def read_watermark() -> datetime | None:
    if not WATERMARK_FILE.exists():
        return None

    with open(WATERMARK_FILE, "r") as f:
        data = json.load(f)
        return datetime.fromisoformat(data["last_extracted"])
    
    
def save_watermark(timestamp: datetime) -> None:
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_extracted": timestamp.isoformat()}, f)


def write_parquet(df: pl.DataFrame, name: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = OUTPUT_DIR / f"{name}_{stamp}.parquet"
    df.write_parquet(path)
    return path


def extract_table(conn, table: str) -> Path:
    df = pl.read_database(f"SELECT * FROM sh.{table}", connection=conn)
    path = write_parquet(df, table)
    logger.info(f"{table}: {df.height} rows -> {path}")
    return path


def extract_sales_full(conn) -> Path:
    df_chunks = []
    for df_chunk in pl.read_database("SELECT * FROM sh.sales", connection=conn, iter_batches=True, batch_size=SALES_BATCH_SIZE):
        df_chunks.append(df_chunk)
        logger.info(f"Sales chunk {len(df_chunks)}: {df_chunk.height} rows")

    df = pl.concat(df_chunks)
    save_watermark(df["TIME_ID"].max())
    path = write_parquet(df, "sales")
    logger.info(f"sales: {df.height} rows -> {path}")
    return path


def extract_sales_incremental(conn, watermark: datetime) -> Path | None:
    query = f"""
        SELECT *
        FROM sh.sales
        WHERE time_id > TO_DATE('{watermark.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
    """
    df_chunks = []
    for df_chunk in pl.read_database(query, connection=conn, iter_batches=True, batch_size=SALES_BATCH_SIZE):
        df_chunks.append(df_chunk)
        logger.info(f"Sales incremental chunk {len(df_chunks)}: {df_chunk.height} rows")
    
    if not df_chunks:
        logger.info("No new rows since last watermark.")
        return None

    df = pl.concat(df_chunks)
    save_watermark(df["TIME_ID"].max())
    path = write_parquet(df, "sales_incremental")
    logger.info(f"sales_incremental: {df.height} rows -> {path}")
    return path


def extract_sales(conn, watermark: datetime | None) -> Path | None:
    if watermark is None:
        logger.info("No watermark found, extracting full sales table.")
        return extract_sales_full(conn)

    return extract_sales_incremental(conn, watermark)           


if __name__ == "__main__":
    with get_connection() as conn:
        for table in DIMENSION_TABLES:
            try:
                extract_table(conn, table)
            except Exception as e:
                logger.error(f"{table}: failed — {e}")

        watermark = read_watermark()
        logger.info(f"Using watermark: {watermark}")
        try:
            extract_sales(conn, watermark)
        except Exception as e:
            logger.error(f"Failed to extract sales: {e}")
   