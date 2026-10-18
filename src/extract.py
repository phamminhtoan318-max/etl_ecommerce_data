from pathlib import Path
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import inspect, text
from src.config.config import BASE_DIR, QUERIES_DIR, EXTRACT_PIPELINE_RULES, OLIST_7_TABLES
from src.database import get_source_engine
from src.logger import get_logger

logger = get_logger(__name__)


def resolve_table_name(db_tables, candidates):
    lookup = {t.lower(): t for t in db_tables}
    for name in candidates:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return None


def build_dynamic_query(logical_name, sql_table_name, rule, last_watermark=None, schema="dbo"):
    sql_file = QUERIES_DIR / f"{logical_name}.sql"
    if sql_file.exists():
        with open(sql_file, "r", encoding="utf-8") as f:
            base_query = f.read()
        base_query = base_query.replace(f"[dbo].[{logical_name}]", f"[{schema}].[{sql_table_name}]")
        base_query = base_query.replace(f"[dbo].[olist_{logical_name}_dataset]", f"[{schema}].[{sql_table_name}]")
    else:
        base_query = f"SELECT * FROM [{schema}].[{sql_table_name}] WITH (NOLOCK)"

    strategy = rule.get("strategy", "full").lower()
    col = rule.get("watermark_column")
    where_filter = ""

    if strategy == "full" or not col:
        logger.info(f"[{logical_name}] Chế độ trích xuất: FULL")
        return base_query

    if strategy == "watermark":
        mark = last_watermark or rule.get("default_start_watermark", "1900-01-01 00:00:00")
        where_filter = f"{col} > '{mark}'"
        logger.info(f"[{logical_name}] Chế độ trích xuất: WATERMARK ({col} > '{mark}')")

    elif strategy == "lookback_window":
        mark = last_watermark or rule.get("default_start_watermark", "1900-01-01 00:00:00")
        days = rule.get("lookback_days", 3)
        dt = datetime.fromisoformat(str(mark)[:19])
        adjusted_mark = (dt - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        where_filter = f"{col} > '{adjusted_mark}'"
        logger.info(f"[{logical_name}] Chế độ trích xuất: LOOKBACK ({days} ngày lùi: {col} > '{adjusted_mark}')")

    elif strategy == "date_range":
        start_d = rule.get("start_date")
        end_d = rule.get("end_date")
        where_filter = f"{col} >= '{start_d}' AND {col} < '{end_d}'"
        logger.info(f"[{logical_name}] Chế độ trích xuất: DATE_RANGE ({start_d} -> {end_d})")

    if where_filter:
        connector = "AND" if "WHERE" in base_query.upper() else "WHERE"
        base_query = f"{base_query} {connector} {where_filter}"

    return base_query


def extract_table(logical_name, sql_table_name, rule, engine, batch_time, batch_id, last_watermark=None, schema="dbo"):
    start_time = time.time()
    chunksize = rule.get("chunksize", 50_000)
    query_str = build_dynamic_query(logical_name, sql_table_name, rule, last_watermark, schema)
    query = text(query_str)

    total_rows = 0
    chunk_idx = 0
    col_count = 0

    with engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql(query, conn, chunksize=chunksize):
            if chunk.empty:
                continue

            chunk_idx += 1
            total_rows += len(chunk)
            col_count = len(chunk.columns)

            chunk = chunk.copy()
            chunk["_extracted_at"] = batch_time
            chunk["_batch_id"] = batch_id

            logger.debug(f"[EXTRACT] Chunk #{chunk_idx} '{logical_name}': {len(chunk):,} dòng")
            yield chunk

    elapsed = time.time() - start_time
    logger.info(
        f"[EXTRACT] Hoàn tất stream '{logical_name:<14}': {total_rows:>7,} dòng, "
        f"{col_count:>2} cột ({chunk_idx} chunks, {elapsed:>5.2f}s)"
    )


def extract_from_sqlserver(schema="dbo", batch_time=None, watermark_states=None):
    total_start = time.time()
    if batch_time is None:
        batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")
    watermark_states = watermark_states or {}

    logger.info(f"=== BẮT ĐẦU EXTRACT LINH HOẠT TỪ SQL SERVER - BATCH: {batch_id} ===")

    engine = get_source_engine()

    try:
        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names(schema=schema))

        for logical_name, rule in EXTRACT_PIPELINE_RULES.items():
            actual_name = resolve_table_name(existing_tables, rule["candidates"])
            if not actual_name:
                raise ValueError(
                    f"Không tìm thấy bảng '{logical_name}' trên SQL Server. "
                    f"Các bảng hiện có: {sorted(existing_tables)}"
                )

            last_wm = watermark_states.get(logical_name)
            table_stream = extract_table(
                logical_name=logical_name,
                sql_table_name=actual_name,
                rule=rule,
                engine=engine,
                batch_time=batch_time,
                batch_id=batch_id,
                last_watermark=last_wm,
                schema=schema,
            )

            yield logical_name, actual_name, rule, table_stream

        total_elapsed = time.time() - total_start
        logger.info(f"=== KẾT THÚC BƯỚC EXTRACT (TỔNG THỜI GIAN: {total_elapsed:.2f}s) ===")

    finally:
        engine.dispose()