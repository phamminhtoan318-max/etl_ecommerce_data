import gc
from pathlib import Path
import time
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import inspect, text
from src.config import BASE_DIR
from src.database import get_source_engine, get_target_engine
from src.extract import OLIST_7_TABLES, resolve_table_name
from src.logger import get_logger

logger = get_logger(__name__)

QUERIES_DIR = BASE_DIR / "sql" / "source" / "queries"


def stream_extract_and_load_table(
    logical_name,
    sql_table_name,
    src_engine,
    tgt_engine,
    batch_time,
    batch_id,
    src_schema="dbo",
    tgt_schema="stg",
    chunksize=25_000,
):
    start_time = time.time()
    logger.info(f"[STREAM LOAD] Bắt đầu stream [{src_schema}].[{sql_table_name:<28}] -> Postgres [{tgt_schema}].[{sql_table_name:<28}]...")

    sql_file = QUERIES_DIR / f"{logical_name}.sql"
    if sql_file.exists():
        with open(sql_file, "r", encoding="utf-8") as f:
            query_str = f.read()
        query_str = query_str.replace(f"[dbo].[{logical_name}]", f"[{src_schema}].[{sql_table_name}]")
        query_str = query_str.replace(f"[dbo].[olist_{logical_name}_dataset]", f"[{src_schema}].[{sql_table_name}]")
        query = text(query_str)
    else:
        query = text(f"SELECT * FROM [{src_schema}].[{sql_table_name}] WITH (NOLOCK)")

    with tgt_engine.begin() as tgt_conn:
        tgt_conn.execute(text(f"TRUNCATE TABLE {tgt_schema}.{sql_table_name};"))

    total_rows = 0
    col_count = 0

    with src_engine.connect() as src_conn, tgt_engine.connect() as tgt_conn:
        for chunk in pd.read_sql(query, src_conn, chunksize=chunksize):
            chunk["_extracted_at"] = batch_time
            chunk["_batch_id"] = batch_id

            col_count = len(chunk.columns)
            chunk_len = len(chunk)

            chunk.to_sql(
                name=sql_table_name,
                con=tgt_conn,
                schema=tgt_schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=2_000,
            )

            total_rows += chunk_len
            del chunk
            gc.collect()

    elapsed = time.time() - start_time
    logger.info(
        f"[STREAM LOAD] Hoàn tất nạp [{tgt_schema}].[{sql_table_name:<28}]: "
        f"{total_rows:>7,} dòng, {col_count:>2} cột ({elapsed:>5.2f}s) - RAM giải phóng 100%"
    )

    return {
        "logical_name": logical_name,
        "table_name": sql_table_name,
        "loaded_rows": total_rows,
        "loaded_cols": col_count,
        "elapsed": elapsed,
    }


def stream_load_all_to_staging(
    batch_time=None,
    src_schema="dbo",
    tgt_schema="stg",
    chunksize=25_000,
):
    total_start = time.time()

    if batch_time is None:
        batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")

    logger.info(f"=== BẮT ĐẦU STREAM EXTRACT & LOAD VÀO POSTGRES STAGING (BATCH: {batch_id}) ===")

    src_engine = get_source_engine()
    tgt_engine = get_target_engine()

    load_summary = {}

    try:
        inspector = inspect(src_engine)
        existing_tables = set(inspector.get_table_names(schema=src_schema))

        for logical_name, candidates in OLIST_7_TABLES.items():
            actual_name = resolve_table_name(existing_tables, candidates)
            if not actual_name:
                raise ValueError(f"Không tìm thấy bảng '{logical_name}' trên SQL Server nguồn!")

            stats = stream_extract_and_load_table(
                logical_name=logical_name,
                sql_table_name=actual_name,
                src_engine=src_engine,
                tgt_engine=tgt_engine,
                batch_time=batch_time,
                batch_id=batch_id,
                src_schema=src_schema,
                tgt_schema=tgt_schema,
                chunksize=chunksize,
            )
            load_summary[logical_name] = stats

        total_elapsed = time.time() - total_start
        logger.info(f"=== HOÀN TẤT NẠP 7 BẢNG VÀO STAGING POSTGRES TRONG {total_elapsed:.2f}s ===")
        return load_summary

    finally:
        src_engine.dispose()
        tgt_engine.dispose()

