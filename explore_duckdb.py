import duckdb

TABLES = ["channels", "products", "customers", "times", "sales"]
DB_PATH = "data/warehouse.duckdb"
PARQUET_DIR = "data/raw"

if __name__ == "__main__":
    with duckdb.connect(DB_PATH) as conn:
        for table in TABLES:
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{PARQUET_DIR}/{table}_*.parquet')").fetchall()
            print(f"{table}: {count[0][0]} rows")
            info = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{PARQUET_DIR}/{table}*.parquet')").fetchall()
            print(f"{table} schema:")
            for row in info:
                print(f"  {row[0]} {row[1]}")