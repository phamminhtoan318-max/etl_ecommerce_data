from sqlalchemy import inspect, text
from src.logger import get_logger

logger = get_logger(__name__)


class SchemaValidationError(Exception):
    pass


def parse_columns_config(rule):
    cols = rule.get("columns")
    if not cols or cols == "*":
        return "all", []

    if isinstance(cols, list):
        return "selected", [str(c).strip() for c in cols if str(c).strip()]

    if isinstance(cols, dict):
        mode = str(cols.get("mode", "all")).lower().strip()
        col_list = cols.get("list") or cols.get("selected") or cols.get("exclude") or []
        if isinstance(col_list, list):
            col_list = [str(c).strip() for c in col_list if str(c).strip()]
        else:
            col_list = []
        return mode, col_list

    return "all", []


def resolve_and_validate_columns(logical_name, actual_table_name, rule, source_columns):
    source_col_map = {c["name"].lower(): c["name"] for c in source_columns}
    source_col_names = set(source_col_map.values())

    pks = rule.get("primary_keys", [])
    missing_pks = [pk for pk in pks if pk.lower() not in source_col_map]
    if missing_pks:
        msg = (
            f"[SCHEMA CONTRACT VIOLATION] Bảng '{logical_name}' ({actual_table_name}): "
            f"Khóa chính {missing_pks} KHÔNG TỒN TẠI trên nguồn SQL Server "
            f"(có thể cột đã bị xóa hoặc đổi tên)! Dừng pipeline có kiểm soát để bảo vệ dữ liệu."
        )
        logger.error(msg)
        raise SchemaValidationError(msg)

    wm_col = rule.get("watermark_column")
    strategy = str(rule.get("strategy", "")).lower().strip()
    if wm_col and strategy in ("watermark", "lookback_window", "date_range"):
        if wm_col.lower() not in source_col_map:
            msg = (
                f"[SCHEMA CONTRACT VIOLATION] Bảng '{logical_name}' ({actual_table_name}): "
                f"Cột watermark '{wm_col}' KHÔNG TỒN TẠI trên nguồn SQL Server! "
                f"Dừng pipeline có kiểm soát."
            )
            logger.error(msg)
            raise SchemaValidationError(msg)

    mode, col_list = parse_columns_config(rule)

    if mode == "selected":
        if not col_list:
            msg = (
                f"[SCHEMA CONFIG ERROR] Bảng '{logical_name}': "
                f"Chế độ 'selected' yêu cầu cung cấp danh sách cột trong 'list'!"
            )
            logger.error(msg)
            raise SchemaValidationError(msg)

        missing_cols = [c for c in col_list if c.lower() not in source_col_map]
        if missing_cols:
            msg = (
                f"[SCHEMA CONTRACT VIOLATION] Bảng '{logical_name}' ({actual_table_name}): "
                f"Các cột chỉ định {missing_cols} KHÔNG TỒN TẠI trên bảng nguồn SQL Server "
                f"(cột đã bị xóa hoặc đổi tên)! Dừng pipeline có kiểm soát."
            )
            logger.error(msg)
            raise SchemaValidationError(msg)

        selected_lower = {c.lower() for c in col_list}
        resolved_cols = [source_col_map[c.lower()] for c in col_list]
        for pk in pks:
            if pk.lower() not in selected_lower:
                logger.warning(
                    f"[SCHEMA CONTRACT WARNING] Bảng '{logical_name}': Tự động bổ sung khóa chính '{pk}' "
                    f"vào danh sách trích xuất để đảm bảo tính toàn vẹn dữ liệu!"
                )
                resolved_cols.append(source_col_map[pk.lower()])

    elif mode == "exclude":
        exclude_lower = {c.lower() for c in col_list}
        forbidden_pks = [pk for pk in pks if pk.lower() in exclude_lower]
        if forbidden_pks:
            msg = (
                f"[SCHEMA CONTRACT VIOLATION] Bảng '{logical_name}': "
                f"KHÔNG ĐƯỢC PHÉP loại trừ (exclude) các cột khóa chính {forbidden_pks}! Dừng pipeline."
            )
            logger.error(msg)
            raise SchemaValidationError(msg)

        if wm_col and strategy in ("watermark", "lookback_window", "date_range"):
            if wm_col.lower() in exclude_lower:
                msg = (
                    f"[SCHEMA CONTRACT VIOLATION] Bảng '{logical_name}': "
                    f"KHÔNG ĐƯỢC PHÉP loại trừ cột watermark '{wm_col}'!"
                )
                logger.error(msg)
                raise SchemaValidationError(msg)

        unmatched_excludes = [c for c in col_list if c.lower() not in source_col_map]
        if unmatched_excludes:
            logger.warning(
                f"[SCHEMA CONTRACT] Bảng '{logical_name}': "
                f"Các cột loại trừ {unmatched_excludes} không có trên nguồn (đã bỏ qua)."
            )

        resolved_cols = [c["name"] for c in source_columns if c["name"].lower() not in exclude_lower]

    elif mode == "all":
        resolved_cols = [c["name"] for c in source_columns]

    else:
        msg = (
            f"[SCHEMA CONFIG ERROR] Bảng '{logical_name}': "
            f"Chế độ lấy cột '{mode}' không hợp lệ (hỗ trợ: all, selected, exclude)!"
        )
        logger.error(msg)
        raise SchemaValidationError(msg)

    logger.info(
        f"[SCHEMA CHECK] Bảng '{logical_name}' ({actual_table_name}) [mode: {mode}]: "
        f"Đã kiểm tra hợp lệ {len(resolved_cols)}/{len(source_columns)} cột."
    )
    return resolved_cols, mode


def map_source_type_to_postgres(col_name, col_meta=None):
    type_str = str(col_meta.get("type", "")).upper() if col_meta else ""
    if "BIGINT" in type_str:
        return "BIGINT"
    if "INT" in type_str:
        return "INTEGER"
    if "VARCHAR" in type_str or "NVARCHAR" in type_str or "CHAR" in type_str or "NCHAR" in type_str:
        type_obj = col_meta.get("type") if col_meta else None
        length = getattr(type_obj, "length", None)
        return f"VARCHAR({length})" if length and isinstance(length, int) and length > 0 else "TEXT"
    if "DATETIME" in type_str or "TIMESTAMP" in type_str:
        return "TIMESTAMP"
    if "DATE" in type_str:
        return "DATE"
    if "TIME" in type_str:
        return "TIME"
    if "FLOAT" in type_str or "REAL" in type_str:
        return "DOUBLE PRECISION"
    if "NUMERIC" in type_str or "DECIMAL" in type_str or "MONEY" in type_str:
        return "NUMERIC"
    if "BIT" in type_str or "BOOL" in type_str:
        return "BOOLEAN"
    return "TEXT"


def sync_target_schema_evolution(tgt_engine, tgt_schema, target_table, chunk_columns, source_columns_meta=None):
    tgt_inspector = inspect(tgt_engine)
    try:
        existing_tgt_cols = {c["name"].lower() for c in tgt_inspector.get_columns(target_table, schema=tgt_schema)}
    except Exception as e:
        logger.debug(f"[SCHEMA EVOLUTION] Bảng '{tgt_schema}.{target_table}' chưa có hoặc không đọc được cột: {e}")
        return []

    added_columns = []
    source_meta_lookup = {c["name"].lower(): c for c in (source_columns_meta or [])}

    for col in chunk_columns:
        if col.lower() not in existing_tgt_cols:
            if col in ("_extracted_at", "_batch_id"):
                pg_type = "TIMESTAMP" if col == "_extracted_at" else "VARCHAR(30)"
            else:
                col_meta = source_meta_lookup.get(col.lower(), {})
                pg_type = map_source_type_to_postgres(col, col_meta)

            logger.info(
                f"⚡ [SCHEMA EVOLUTION] Phát hiện cột mới '{col}' ({pg_type}) từ nguồn! "
                f"Đang tự động cập nhật schema đích '{tgt_schema}.{target_table}'..."
            )
            with tgt_engine.begin() as conn:
                conn.execute(
                    text(f'ALTER TABLE "{tgt_schema}"."{target_table}" ADD COLUMN IF NOT EXISTS "{col}" {pg_type};')
                )
            logger.info(
                f"✅ [SCHEMA EVOLUTION] Đã bổ sung thành công cột '{col}' ({pg_type}) vào '{tgt_schema}.{target_table}'."
            )
            added_columns.append(col)
            existing_tgt_cols.add(col.lower())

    return added_columns

