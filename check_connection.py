import os
import oracledb
from dotenv import load_dotenv

load_dotenv()

connection = oracledb.connect(
    user=os.environ["ORACLE_USER"],
    password=os.environ["ORACLE_PASSWORD"],
    dsn=os.environ["ORACLE_DSN"],
    config_dir=os.environ["WALLET_DIR"],     
    wallet_location=os.environ["WALLET_DIR"], 
    wallet_password=os.environ["WALLET_PASSWORD"],
)

with connection.cursor() as cur:
    cur.execute("SELECT 'connected!' FROM dual")
    print(cur.fetchone()[0])

connection.close()

