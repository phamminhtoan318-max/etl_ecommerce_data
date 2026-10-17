from pathlib import Path
import time
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import inspect, text
from src.config import BASE_DIR
from src.database import get_source_engine
from src.logger import get_logger

logger = get_logger(__name__)

QUERIES_DIR = BASE_DIR / "sql" / "source" / "queries"

OLIST_7_TABLES = {
    "customers": ["customers", "olist_customers_dataset"],
    "orders": ["orders", "olist_orders_dataset"],
    "order_items": ["order_items", "olist_order_items_dataset"],
    "order_payments": ["order_payments", "olist_order_payments_dataset"],
    "order_reviews": ["order_reviews", "olist_order_reviews_dataset"],
    "products": ["products", "olist_products_dataset"],
    "sellers": ["sellers", "olist_sellers_dataset"],
}


def resolve_table_name(db_tables, candidates):
    lookup = {t.lower(): t for t in db_tables}
    for name in candidates:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return None


def extract_table(logical_name, sql_table_name, engine, batch_time, batch_id, schema="dbo", chunksize = 50_000):
    start_time = time.time()
    logger.info(f"[EXTRACT] Đang đọc bảng [{schema}].[{sql_table_name:<28}] từ SQL Server...")

    try:
        sql_file = QUERIES_DIR / f"{logical_name}.sql"
        if sql_file.exists():
            logger.info(f"[EXTRACT] Sử dụng query từ file: sql/source/queries/{sql_file.name}")
            with open(sql_file, "r", encoding="utf-8") as f:
                query_str = f.read()

            query_str = query_str.replace(f"[dbo].[{logical_name}]", f"[{schema}].[{sql_table_name}]")
            query_str = query_str.replace(f"[dbo].[olist_{logical_name}_dataset]", f"[{schema}].[{sql_table_name}]")
            query = text(query_str)
        else:
            logger.warning(f"[EXTRACT] Không tìm thấy '{sql_file.name}', fallback về SELECT *")
            query = text(f"SELECT * FROM [{schema}].[{sql_table_name}] WITH (NOLOCK)")

        chunks = []
        with engine.connect() as conn:
            for chunk in pd.read_sql(query, conn, chunksize=chunksize):
                chunk["_extracted_at"] = batch_time
                chunk["_batch_id"] = batch_id
                chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

        elapsed = time.time() - start_time
        logger.info(
            f"[EXTRACT] Thành công '{logical_name:<14}': {len(df):>7,} dòng, "
            f"{len(df.columns):>2} cột ({elapsed:>5.2f}s)"
        )
        return df

    except Exception as e:
        logger.error(f"[EXTRACT] Lỗi khi đọc bảng '{sql_table_name}': {e}", exc_info=True)
        raise


def extract_from_sqlserver(schema="dbo", batch_time=None):
    total_start = time.time()

    if batch_time is None:
        batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")

    logger.info(f"=== BẮT ĐẦU BƯỚC E (EXTRACT TỪ SQL SERVER) - BATCH: {batch_id} ===")

    engine = get_source_engine()
    raw_tables: dict[str, pd.DataFrame] = {}

    try:
        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names(schema=schema))
        logger.info(f"Các bảng hiện có trong SQL Server: {sorted(existing_tables)}")

        for logical_name, candidates in OLIST_7_TABLES.items():
            actual_name = resolve_table_name(existing_tables, candidates)
            if not actual_name:
                raise ValueError(
                    f"Không tìm thấy bảng '{logical_name}' trong SQL Server. "
                    f"Các bảng hiện có: {sorted(existing_tables)}"
                )

            raw_tables[logical_name] = extract_table(
                logical_name=logical_name,
                sql_table_name=actual_name,
                engine=engine,
                batch_time=batch_time,
                batch_id=batch_id,
                schema=schema,
            )

        total_elapsed = time.time() - total_start
        logger.info(f"=== HOÀN TẤT EXTRACT 7 BẢNG BATCH {batch_id} TRONG {total_elapsed:.2f}s ===")
        return raw_tables

    finally:
        engine.dispose()

