from functools import lru_cache, wraps
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, OperationalError
from src.config.config import SRC_SQLALCHEMY_URI, TGT_SQLALCHEMY_URI
from src.logger import get_logger

logger = get_logger(__name__)


def retry_on_db_error(max_retries=3, base_delay=2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, DBAPIError) as exc:
                    if attempt == max_retries:
                        raise
                    wait_sec = base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"⚠️ [DB RETRY {attempt}/{max_retries}] Lỗi kết nối cơ sở dữ liệu tạm thời: {exc}. "
                        f"Thử lại sau {wait_sec:.1f}s..."
                    )
                    time.sleep(wait_sec)
        return wrapper
    return decorator


@lru_cache(maxsize=1)
def get_source_engine():
    return create_engine(
        SRC_SQLALCHEMY_URI,
        pool_pre_ping=True,
    )

@lru_cache(maxsize=1)
def get_target_engine():
    return create_engine(
        TGT_SQLALCHEMY_URI,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )

@retry_on_db_error(max_retries=3, base_delay=2.0)
def test_connections():
    try:
        with get_source_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Kết nối Source Database thành công!")

        with get_target_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Kết nối Target Database thành công!")
        
        return True
    except Exception as exc:
        logger.error(f"Lỗi kết nối cơ sở dữ liệu: {exc}")
        raise exc
