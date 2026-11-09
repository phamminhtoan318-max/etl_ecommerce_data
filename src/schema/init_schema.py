from sqlalchemy import text
from src.config.config import STAGING_DDL_SQL_PATH, TGT_DB_SCHEMA
from src.logger import get_logger

logger = get_logger(__name__)


def init_bronze_schema(tgt_engine, ddl_path=STAGING_DDL_SQL_PATH, target_schema=TGT_DB_SCHEMA):
    if not ddl_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file DDL khởi tạo Bronze: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8").replace("{schema}", target_schema)
    with tgt_engine.begin() as conn:
        conn.execute(text(ddl_sql))
    logger.info(f"[INIT BRONZE] Đã sẵn sàng cấu trúc tầng Bronze '{target_schema}' (Append-Only mode).")

