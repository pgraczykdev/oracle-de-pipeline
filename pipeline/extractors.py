
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Protocol
import logging
import requests
import polars as pl

from .io import write_parquet, save_watermark

logger = logging.getLogger(__name__)

SALES_BATCH_SIZE = 50000


class SourceExtractor(Protocol):
    def extract_dimensions(self, tables: list[str]) -> list[Path]: ...
    def extract_facts(self, watermark: datetime | None) -> Path | None: ...


class DummyJsonExtractor():
    BASE_URL = "https://dummyjson.com"


    def _fetch_all(self, endpoint) -> list[dict]:
        collected = []
        skip = 0
        limit = 100
        key = endpoint.strip('/')

        while True:
            response = requests.get(f"{self.BASE_URL}{endpoint}?limit={limit}&skip={skip}")
            response.raise_for_status()
            data = response.json()
            collected.extend(data[key])
            skip += limit
            if skip >= data['total']:
                break

        return collected
    
  
    def extract_dimensions(self, tables: list[str]) -> list[Path]:
        paths = []
        for table in tables:
            if table == "products":
                data = self._fetch_all("/products")
                df = pl.DataFrame([{
                    "product_id": p["id"],
                    "product_name": p["title"],
                    "category": p["category"],
                    "price": p["price"],
                    "brand": p.get("brand"),
                    "rating": p["rating"],
                    "stock": p["stock"],
                } for p in data])

            elif table == "users":
                data = self._fetch_all("/users")
                df = pl.DataFrame([{
                   "user_id": u["id"],
                   "first_name": u["firstName"],
                   "last_name": u["lastName"],
                   "age": u["age"],
                   "gender": u["gender"],
                   "city": u["address"]["city"],
                   "state": u["address"]["state"],
                   "country": u["address"]["country"],
                } for u in data])
            else:
                days = [date(2024, 1, 1) + timedelta(days=i) for i in range(366)]
                df = pl.DataFrame([{
                "date_id": int(d.strftime("%Y%m%d")),
                "date": d,
                "day": d.day,
                "month": d.month,
                "year": d.year,
                "quarter": ((d.month - 1) // 3) + 1,
                } for d in days])
            path = write_parquet(df, table)
            logger.info(f"{table}: {df.height} rows -> {path}")
            paths.append(path)
        return paths
    

    def extract_facts(self, watermark: datetime | None) -> Path | None:
        rows = []
        carts = self._fetch_all('/carts')
        for cart in carts:
            order_date = date(2024, 1, 1) + timedelta(days=cart["id"] - 1)
            for product in cart["products"]:
                rows.append({
                    "cart_id":          cart["id"],
                    "user_id":          cart["userId"],
                    "product_id":       product["id"],
                    "order_date":       order_date,
                    "quantity":         product["quantity"],
                    "price":            product["price"],
                    "total":            product["total"],
                    "discounted_total": product["discountedTotal"],    
                })
        if not rows:
            logger.info("No orders fetched.")
            return None
        df = pl.DataFrame(rows)
        path = write_parquet(df, "orders")
        logger.info(f"orders: {df.height} rows -> {path}")
        return path

            

class OracleExtractor:
    def __init__(self, conn):
        self.conn = conn


    def extract_dimensions(self, tables: list[str]) -> list[Path]:
        paths = []
        for table in tables:
            df = pl.read_database(f"SELECT * FROM sh.{table}", connection=self.conn)
            path = write_parquet(df, table)
            logger.info(f"{table}: {df.height} rows -> {path}")
            paths.append(path)
        return paths


    def extract_facts(self, watermark: datetime | None) -> Path | None:
        if watermark is None:
            logger.info("No watermark found, extracting full sales table.")
            return self._extract_full()
        return self._extract_incremental(watermark)

    def _extract_full(self) -> Path:
        df_chunks = []
        for df_chunk in pl.read_database("SELECT * FROM sh.sales", connection=self.conn, iter_batches=True, batch_size=SALES_BATCH_SIZE):
            df_chunks.append(df_chunk)
            logger.info(f"Sales chunk {len(df_chunks)}: {df_chunk.height} rows")

        df = pl.concat(df_chunks)
        save_watermark(df["TIME_ID"].max())
        path = write_parquet(df, "sales")
        logger.info(f"sales: {df.height} rows -> {path}")
        return path

    def _extract_incremental(self, watermark: datetime) -> Path | None:
        query = f"""
            SELECT *
            FROM sh.sales
            WHERE time_id > TO_DATE('{watermark.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
        """
        df_chunks = []
        for df_chunk in pl.read_database(query, connection=self.conn, iter_batches=True, batch_size=SALES_BATCH_SIZE):
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
