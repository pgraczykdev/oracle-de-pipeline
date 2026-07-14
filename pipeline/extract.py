import os
import logging

from dotenv import load_dotenv

from .extractors import OracleExtractor, DummyJsonExtractor
from .io import read_watermark


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)

load_dotenv()

ORACLE_DIMENSION_TABLES = ["channels", "products", "customers", "times"]
DUMMY_JSON_DIMENSION_TABLES = ["products", "users", "dates"]


def get_connection():
    import oracledb
    oracledb.defaults.fetch_decimals = True
    return oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=os.environ["ORACLE_DSN"],
        config_dir=os.environ["WALLET_DIR"],
        wallet_location=os.environ["WALLET_DIR"],
        wallet_password=os.environ["WALLET_PASSWORD"],
    )


if __name__ == "__main__":
    source = os.environ.get("EXTRACTOR", "oracle")

    if source == "DUMMY_JSON":
        extractor = DummyJsonExtractor()
        tables = DUMMY_JSON_DIMENSION_TABLES
        try:
            extractor.extract_dimensions(tables)
        except Exception as e:
            logger.error(f"Dimensions failed: {e}")
        try:
            extractor.extract_facts(None)
        except Exception as e:
            logger.error(f"Facts failed: {e}")
    else:
        with get_connection() as conn:
            extractor = OracleExtractor(conn)
            try:
                extractor.extract_dimensions(ORACLE_DIMENSION_TABLES)
            except Exception as e:
                logger.error(f"Dimensions failed: {e}")
            watermark = read_watermark()
            logger.info(f"Using watermark: {watermark}")
            try:
                extractor.extract_facts(watermark)
            except Exception as e:
                logger.error(f"Failed to extract sales: {e}")
