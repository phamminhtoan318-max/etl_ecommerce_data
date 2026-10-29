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

