import os
import oracledb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

connection = oracledb.connect(
    user=os.environ["ORACLE_USER"],
    password=os.environ["ORACLE_PASSWORD"],
    dsn=os.environ["ORACLE_DSN"],
    config_dir=os.environ["WALLET_DIR"],
    wallet_location=os.environ["WALLET_DIR"],
    wallet_password=os.environ["WALLET_PASSWORD"]
)

df = pl.read_database(
    query="SELECT amount_sold FROM sh.sales FETCH FIRST 1000 ROWS ONLY ",
    connection=connection,
)


sum_float = df["AMOUNT_SOLD"].sum()
sum_decimal = df.with_columns(
    pl.col("AMOUNT_SOLD").cast(pl.Decimal(scale=2))
)["AMOUNT_SOLD"].sum()

print("float  :", sum_float)
print("decimal:", sum_decimal)