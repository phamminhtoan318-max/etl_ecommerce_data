from pathlib import Path
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import inspect, text
from src.config.config import BASE_DIR, EXTRACT_PIPELINE_RULES, OLIST_7_TABLES
from src.database import get_source_engine
from src.logger import get_logger
from src.schema import (
    SchemaValidationError,
    parse_columns_config,
    resolve_and_validate_columns,
)

logger = get_logger(__name__)


def resolve_table_name(db_tables, candidates):
    lookup = {t.lower(): t for t in db_tables}
    for name in candidates:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return None


def build_dynamic_query(
    logical_name,
    sql_table_name,
    rule,
    last_watermark=None,
    schema="dbo",
    include_order_by=True,
    resolved_columns=None,
    batch_time=None,
):
    strategy = rule.get("strategy")
    if not strategy:
        logger.error(f"[{logical_name}] LỖI CẤU HÌNH: Thiếu 'strategy' trong YAML! -> HỦY trích xuất bảng này.")
        return None

    strategy = str(strategy).lower().strip()
    if strategy == "cancel":
        logger.warning(f"[{logical_name}] Chế độ trích xuất là 'CANCEL' -> Hủy trích xuất bảng này.")
        return None

    if resolved_columns:
        cols_str = ", ".join(f"[{c.strip()}]" for c in resolved_columns)
    else:
        mode, col_list = parse_columns_config(rule)
        if mode == "selected" and col_list:
            cols_str = ", ".join(f"[{c.strip()}]" for c in col_list)
        else:
            cols_str = "*"

    base_query = f"SELECT {cols_str} FROM [{schema}].[{sql_table_name}]"
    col = rule.get("watermark_column")
    where_filter = ""
    cutoff_filter = ""
    if batch_time and col:
        cutoff = batch_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(batch_time, datetime) else str(batch_time)[:19]
        cutoff_filter = f" AND [{col}] <= '{cutoff}'"

    if strategy == "full":
        logger.info(f"[{logical_name}] Chế độ trích xuất: FULL")
        return base_query

    elif strategy == "watermark":
        if not col:
            logger.error(
                f"[{logical_name}] LỖI WATERMARK: Bảng thiếu cấu hình 'watermark_column'! "
                f"-> HỦY (CANCEL) trích xuất bảng này để bảo vệ dữ liệu."
            )
            return None

        mark = last_watermark or rule.get("default_start_watermark")
        if not mark:
            logger.error(
                f"[{logical_name}] LỖI WATERMARK: Thiếu 'default_start_watermark' và không có 'last_watermark'! "
                f"-> HỦY (CANCEL) trích xuất bảng này."
            )
            return None

        where_filter = f"[{col}] > '{mark}'{cutoff_filter}"
        logger.info(f"[{logical_name}] Chế độ trích xuất: WATERMARK ({where_filter})")

    elif strategy == "lookback_window":
        if not col:
            logger.error(
                f"[{logical_name}] LỖI LOOKBACK: Bảng thiếu cấu hình 'watermark_column'! "
                f"-> HỦY (CANCEL) trích xuất bảng này để bảo vệ dữ liệu."
            )
            return None

        mark = last_watermark or rule.get("default_start_watermark")
        if not mark:
            logger.error(
                f"[{logical_name}] LỖI LOOKBACK: Thiếu 'default_start_watermark' và không có 'last_watermark'! "
                f"-> HỦY (CANCEL) trích xuất bảng này."
            )
            return None

        days = rule.get("lookback_days")
        if days is None:
            logger.error(
                f"[{logical_name}] LỖI LOOKBACK: Bảng thiếu cấu hình 'lookback_days'! "
                f"-> HỦY (CANCEL) trích xuất bảng này."
            )
            return None

        try:
            dt = datetime.fromisoformat(str(mark)[:19])
            adjusted_mark = (dt - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            where_filter = f"[{col}] > '{adjusted_mark}'{cutoff_filter}"
            logger.info(f"[{logical_name}] Chế độ trích xuất: LOOKBACK ({where_filter})")
        except Exception as e:
            logger.error(
                f"[{logical_name}] LỖI LOOKBACK: Không parse được mốc thời gian '{mark}' ({e})! "
                f"-> HỦY (CANCEL) trích xuất bảng này."
            )
            return None

    elif strategy == "date_range":
        start_d = rule.get("start_date")
        end_d = rule.get("end_date")
        if not col or not start_d or not end_d:
            logger.error(
                f"[{logical_name}] LỖI DATE_RANGE: Thiếu 'watermark_column', 'start_date' hoặc 'end_date'! "
                f"-> HỦY (CANCEL) trích xuất bảng này."
            )
            return None
        where_filter = f"{col} >= '{start_d}' AND {col} < '{end_d}'"
        logger.info(f"[{logical_name}] Chế độ trích xuất: DATE_RANGE ({start_d} -> {end_d})")

    else:
        logger.error(
            f"[{logical_name}] LỖI STRATEGY: Chiến lược '{strategy}' không được hỗ trợ! "
            f"-> HỦY (CANCEL) trích xuất bảng này."
        )
        return None

    if where_filter:
        base_query = f"{base_query} WHERE {where_filter}"

    if include_order_by and strategy in ("watermark", "lookback_window", "date_range") and col:
        base_query = f"{base_query} ORDER BY [{col}] ASC"

    return base_query


def extract_table(logical_name, sql_table_name, rule, engine, batch_time, batch_id, last_watermark=None, query_str=None, schema="dbo"):
    start_time = time.time()
    chunksize = rule.get("chunksize")
    if not chunksize:
        logger.error(f"[{logical_name}] LỖI CẤU HÌNH: Thiếu 'chunksize' trong YAML! -> HỦY trích xuất bảng này.")
        return

    if query_str is None:
        query_str = build_dynamic_query(logical_name, sql_table_name, rule, last_watermark, schema)

    if not query_str:
        logger.warning(f"[EXTRACT SKIP] Đã bỏ qua bảng '{logical_name}' (không trích xuất dữ liệu).")
        return

    query = text(query_str)
    total_rows = 0
    chunk_idx = 0
    col_count = 0
    extract_duration = 0.0

    try:
        with engine.connect().execution_options(stream_results=True) as conn:
            reader = pd.read_sql(query, conn, chunksize=chunksize, dtype_backend="numpy_nullable")
            t0 = time.time()
            for chunk in reader:
                extract_duration += time.time() - t0

                if chunk.empty:
                    t0 = time.time()
                    continue

                chunk_idx += 1
                total_rows += len(chunk)
                col_count = len(chunk.columns)

                chunk = chunk.copy()
                chunk["_extracted_at"] = batch_time
                chunk["_batch_id"] = batch_id

                logger.debug(f"[EXTRACT] Chunk #{chunk_idx} '{logical_name}': {len(chunk):,} dòng")
                yield chunk
                t0 = time.time()
    except Exception as e:
        logger.error(f"[LỖI TRÍCH XUẤT] Bảng '{logical_name}' gặp lỗi query: {e}. Rollback bảng này và đánh dấu FAILED!")
        raise

    speed = (total_rows / extract_duration) if extract_duration > 0 else 0
    logger.info(
        f"[EXTRACT] Hoàn tất trích xuất '{logical_name:<14}': {total_rows:>7,} dòng, "
        f"{col_count:>2} cột ({chunk_idx} chunks, {extract_duration:>5.2f}s, {speed:,.0f} dòng/giây)"
        if extract_duration > 0
        else f"[EXTRACT] Hoàn tất trích xuất '{logical_name:<14}': {total_rows:>7,} dòng"
    )


def extract_from_sqlserver(schema="dbo", batch_time=None, watermark_states=None):
    total_start = time.time()
    if batch_time is None:
        batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")
    watermark_states = watermark_states or {}

    logger.info(f"=== BẮT ĐẦU EXTRACT LINH HOẠT TỪ SOURCE - BATCH: {batch_id} ===")

    engine = get_source_engine()

    try:
        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names(schema=schema))

        for logical_name, rule in EXTRACT_PIPELINE_RULES.items():
            actual_name = resolve_table_name(existing_tables, rule["candidates"])
            if not actual_name:
                msg = (
                    f"[THIẾU BẢNG NGUỒN] Không tìm thấy bảng '{logical_name}' trên SQL Server. "
                    f"Các bảng hiện có: {sorted(existing_tables)} -> Dừng pipeline có kiểm soát."
                )
                logger.error(msg)
                raise SchemaValidationError(msg)

            source_columns = inspector.get_columns(actual_name, schema=schema)
            resolved_columns, mode = resolve_and_validate_columns(
                logical_name=logical_name,
                actual_table_name=actual_name,
                rule=rule,
                source_columns=source_columns,
            )
            rule["resolved_columns"] = resolved_columns
            rule["source_columns"] = source_columns

            last_wm = watermark_states.get(logical_name)

            query_str = build_dynamic_query(
                logical_name,
                actual_name,
                rule,
                last_watermark=last_wm,
                schema=schema,
                resolved_columns=resolved_columns,
                batch_time=batch_time,
            )
            if not query_str:
                logger.warning(f"⏩ [BỎ QUA BẢNG] '{logical_name}' bị hủy do lỗi cơ chế/cancel. Tiếp tục các bảng khác...")
                continue

            table_stream = extract_table(
                logical_name=logical_name,
                sql_table_name=actual_name,
                rule=rule,
                engine=engine,
                batch_time=batch_time,
                batch_id=batch_id,
                last_watermark=last_wm,
                query_str=query_str,
                schema=schema,
            )

            yield logical_name, actual_name, rule, table_stream

        total_elapsed = time.time() - total_start
        logger.info(f"=== KẾT THÚC BƯỚC EXTRACT (TỔNG THỜI GIAN: {total_elapsed:.2f}s) ===")

    finally:
        engine.dispose()