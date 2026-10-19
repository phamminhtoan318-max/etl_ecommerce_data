from pathlib import Path
import pandas as pd
from sqlalchemy import inspect, text
from src.config.config import BASE_DIR, EXTRACT_PIPELINE_RULES, VALIDATE_SQL_PATH_SOURCE, VALIDATE_SQL_PATH_STAGING
from src.database import get_source_engine, get_target_engine
from src.extract import OLIST_7_TABLES, resolve_table_name
from src.logger import get_logger

logger = get_logger(__name__)

REQUIRED_TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
]

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
}

DATE_COLUMNS = {
    "orders": "order_purchase_timestamp",
    "order_reviews": "review_creation_date",
}



def measure_source_baseline(schema="dbo", engine=None):
    logger.info("=== [CƠ CHẾ 1 - PRE-EXTRACT] BẮT ĐẦU ĐO ĐẠC THÔNG SỐ CHUẨN TỪ SOURCE ===")
    engine = engine or get_source_engine()

    if not VALIDATE_SQL_PATH_SOURCE.exists():
        raise FileNotFoundError(f"Không tìm thấy file SQL chuẩn: {VALIDATE_SQL_PATH_SOURCE}")

    content = VALIDATE_SQL_PATH_SOURCE.read_text(encoding="utf-8")
    lines = [line for line in content.splitlines() if not line.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

    source_baseline = {}
    with engine.connect() as conn:
        row_counts = {}
        if len(statements) >= 1:
            res_rows = conn.execute(text(statements[0])).fetchall()
            for r in res_rows:
                mapping = dict(r._mapping)
                row_counts[mapping["table_name"]] = mapping["total_rows"]

        pk_issues = []
        if len(statements) >= 2:
            res_pk = conn.execute(text(statements[1])).fetchall()
            for r in res_pk:
                mapping = dict(r._mapping)
                if mapping["issue_count"] > 0:
                    pk_issues.append(mapping)

        if pk_issues:
            logger.warning(f"[BASELINE WARNING] Phát hiện {len(pk_issues)} cảnh báo khóa chính tại nguồn: {pk_issues}")
        else:
            logger.info("[BASELINE] Source: 0 lỗi Khóa chính (NULL & Duplicate đều bằng 0)")

        date_ranges = {}
        if len(statements) >= 3:
            res_dates = conn.execute(text(statements[2])).fetchall()
            for r in res_dates:
                mapping = dict(r._mapping)
                tbl = mapping["table_name"]
                date_ranges[tbl] = {
                    "column": mapping.get("watermark_column"),
                    "min": str(mapping.get("min_date"))[:19] if mapping.get("min_date") else None,
                    "max": str(mapping.get("max_date"))[:19] if mapping.get("max_date") else None,
                }

        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names(schema=schema))

        for logical_name, candidates in OLIST_7_TABLES.items():
            actual_name = resolve_table_name(existing_tables, candidates)
            if not actual_name:
                raise ValueError(f"Không tìm thấy bảng '{logical_name}' trên SQL Server!")

            source_cols = [c["name"] for c in inspector.get_columns(actual_name, schema=schema)]
            expected_row_count = row_counts.get(actual_name, 0)

            rule = EXTRACT_PIPELINE_RULES.get(logical_name, {})
            configured_cols = rule.get("columns", "*")
            if configured_cols == "*" or not configured_cols:
                expected_columns = list(source_cols)
            elif isinstance(configured_cols, list):
                expected_columns = list(configured_cols)
            else:
                expected_columns = list(source_cols)

            expected_col_count = len(expected_columns) + 2

            source_baseline[logical_name] = {
                "actual_table_name": actual_name,
                "source_row_count": expected_row_count,
                "source_columns": source_cols,
                "expected_columns": expected_columns,
                "source_col_count": len(source_cols),
                "expected_col_count": expected_col_count,
                "date_range": date_ranges.get(actual_name),
            }

            logger.info(
                f"[BASELINE] Bảng '{logical_name:<14}' ({actual_name:<28}): "
                f"{expected_row_count:>7,} dòng | {len(expected_columns):>2} cột cấu hình -> kỳ vọng {expected_col_count:>2} cột DataFrame"
            )

    logger.info("=== [CƠ CHẾ 1 - PRE-EXTRACT] ĐÃ LƯU THÀNH CÔNG THÔNG SỐ BASELINE VÀO BỘ NHỚ ===")
    return source_baseline


def check_table_completeness(raw_tables, errors):
    missing = [t for t in REQUIRED_TABLES if t not in raw_tables]
    if missing:
        err = f"[THIẾU BẢNG] Không tìm thấy các bảng sau trong kết quả trích xuất: {missing}"
        errors.append(err)
        logger.error(err)
        return False

    logger.info(f"[VALIDATE PYTHON] Đủ 7/7 bảng: {REQUIRED_TABLES}")
    return True


def check_empty_tables(raw_tables, errors):
    is_valid = True
    for table_name, df in raw_tables.items():
        if df is None or len(df) == 0:
            err = f"[BẢNG RỖNG] Bảng '{table_name}' không có dòng nào (0 dòng)!"
            errors.append(err)
            logger.error(err)
            is_valid = False
    return is_valid


def reconcile_dimensions_with_baseline(raw_tables, source_baseline, errors):
    logger.info("[VALIDATE PYTHON] Đối chiếu kích thước DataFrame (dòng, cột) trực tiếp với Baseline SQL...")
    is_valid = True

    for logical_name, df in raw_tables.items():
        baseline_info = source_baseline.get(logical_name)
        if not baseline_info:
            err = f"[LỖI BASELINE] Không có thông số baseline cho bảng '{logical_name}'!"
            errors.append(err)
            logger.error(err)
            is_valid = False
            continue

        actual_rows = len(df)
        expected_rows = baseline_info["source_row_count"]
        actual_cols = len(df.columns)
        expected_cols = baseline_info["expected_col_count"]

        if actual_rows != expected_rows:
            diff = abs(actual_rows - expected_rows)
            err = (
                f"[LỆCH SỐ DÒNG] Bảng '{logical_name}': SQL Server có {expected_rows:,} dòng "
                f"nhưng extract được {actual_rows:,} dòng (lệch {diff:,} dòng)!"
            )
            errors.append(err)
            logger.error(err)
            is_valid = False
        else:
            logger.info(f"[VALIDATE PYTHON] Bảng '{logical_name:<14}': Khớp 100% số dòng ({actual_rows:>7,} dòng)")

        if actual_cols != expected_cols:
            err = (
                f"[LỆCH SỐ CỘT] Bảng '{logical_name}': Kỳ vọng {expected_cols} cột "
                f"nhưng DataFrame có {actual_cols} cột!"
            )
            errors.append(err)
            logger.error(err)
            is_valid = False
        else:
            logger.info(f"[VALIDATE PYTHON] Bảng '{logical_name:<14}': Đủ {actual_cols:>2}/{expected_cols:>2} cột")

        df_col_set = set(df.columns)
        expected_columns = baseline_info.get("expected_columns", baseline_info["source_columns"])
        missing_configured_cols = [c for c in expected_columns if c not in df_col_set]
        if missing_configured_cols:
            err = f"[THIẾU CỘT CẤU HÌNH] Bảng '{logical_name}' bị thiếu các cột theo cấu hình: {missing_configured_cols}"
            errors.append(err)
            logger.error(err)
            is_valid = False

        missing_audit_cols = [c for c in ["_extracted_at", "_batch_id"] if c not in df_col_set]
        if missing_audit_cols:
            err = f"[THIẾU CỘT AUDIT] Bảng '{logical_name}' thiếu cột metadata: {missing_audit_cols}"
            errors.append(err)
            logger.error(err)
            is_valid = False

    return is_valid


def check_primary_keys_in_memory(raw_tables, errors):
    logger.info("[VALIDATE PYTHON] Kiểm tra tính toàn vẹn Khóa chính trên DataFrame (NULL & Trùng lặp)...")
    is_valid = True

    for table_name, pk_cols in PRIMARY_KEYS.items():
        df = raw_tables.get(table_name)
        if df is None or df.empty:
            continue

        missing_pks = [pk for pk in pk_cols if pk not in df.columns]
        if missing_pks:
            err = f"[LỖI THIẾU PK] Bảng '{table_name}': Cột khóa chính {missing_pks} không có trong DataFrame trích xuất!"
            errors.append(err)
            logger.error(err)
            is_valid = False
            continue

        null_count = df[pk_cols].isnull().any(axis=1).sum()
        if null_count > 0:
            err = f"[LỖI PK NULL] Bảng '{table_name}': Có {null_count:,} dòng bị NULL ở khóa chính {pk_cols}!"
            errors.append(err)
            logger.error(err)
            is_valid = False
        else:
            logger.info(f"[VALIDATE PYTHON] Bảng '{table_name:<14}': 0 dòng NULL ở khóa chính {pk_cols}")

        dup_count = df.duplicated(subset=pk_cols).sum()
        if dup_count > 0:
            err = f"[LỖI PK DUPLICATE] Bảng '{table_name}': Có {dup_count:,} dòng bị trùng lặp ở khóa chính {pk_cols}!"
            errors.append(err)
            logger.error(err)
            is_valid = False
        else:
            logger.info(f"[VALIDATE PYTHON] Bảng '{table_name:<14}': Duy nhất 100% (0 trùng lặp ở {pk_cols})")

    return is_valid


def check_date_ranges_in_memory(raw_tables, source_baseline, errors):
    logger.info("[VALIDATE PYTHON] Đối chiếu mốc thời gian MIN / MAX trên DataFrame với Baseline...")
    is_valid = True

    for logical_name, df in raw_tables.items():
        baseline_info = source_baseline.get(logical_name)
        if not baseline_info or not baseline_info.get("date_range"):
            continue

        date_meta = baseline_info["date_range"]
        col = date_meta["column"]
        if not col or col not in df.columns:
            continue

        df_date_series = pd.to_datetime(df[col], errors="coerce")
        df_min = str(df_date_series.min())[:19]
        df_max = str(df_date_series.max())[:19]

        expected_min = date_meta["min"]
        expected_max = date_meta["max"]

        if df_min != expected_min or df_max != expected_max:
            err = (
                f"[LỆCH THỜI GIAN] Bảng '{logical_name}' ({col}): "
                f"DataFrame [{df_min} -> {df_max}] khác Baseline [{expected_min} -> {expected_max}]!"
            )
            errors.append(err)
            logger.error(err)
            is_valid = False
        else:
            logger.info(
                f"[VALIDATE PYTHON] Bảng '{logical_name:<14}' ({col:<26}): "
                f"Khớp 100% mốc thời gian [{df_min} -> {df_max}]"
            )

    return is_valid


def validate_extracted_data(raw_tables, source_baseline=None, schema="dbo", engine=None):
    logger.info("=== [CƠ CHẾ 2 - POST-EXTRACT] BẮT ĐẦU ĐỐI CHIẾU DỮ LIỆU DATAFRAME VỚI BASELINE ===")

    if source_baseline is None:
        source_baseline = measure_source_baseline(schema=schema, engine=engine)

    errors = []

    if not check_table_completeness(raw_tables, errors):
        raise ValueError(f"Validation thất bại: {errors}")
    v_empty = check_empty_tables(raw_tables, errors)

    v_dims = reconcile_dimensions_with_baseline(raw_tables, source_baseline, errors)
    v_pks = check_primary_keys_in_memory(raw_tables, errors)
    v_dates = check_date_ranges_in_memory(raw_tables, source_baseline, errors)

    all_passed = v_empty and v_dims and v_pks and v_dates

    if not all_passed:
        summary_err = (
            f"\n❌ [DỪNG PIPELINE NGAY LẬP TỨC] Phát hiện {len(errors)} sai lệch giữa Source và Extract:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
        logger.error(summary_err)
        raise ValueError(summary_err)

    logger.info(
        "=== ✅ [VALIDATE THÀNH CÔNG 100%] DỮ LIỆU KHỚP TUYỆT ĐỐI VỚI SOURCE! SẴN SÀNG CHO BƯỚC TIẾP THEO ==="
    )
    return True


def validate_staging_against_baseline(source_baseline, tgt_schema="stg", tgt_engine=None):
    logger.info("=== [VALIDATE STAGING] BẮT ĐẦU ĐỐI CHIẾU DỮ LIỆU TẦNG STAGING VỚI BASELINE ===")
    tgt_engine = tgt_engine or get_target_engine()
    errors = []

    if not VALIDATE_SQL_PATH_STAGING.exists():
        raise FileNotFoundError(f"Không tìm thấy file SQL kiểm định Staging: {VALIDATE_SQL_PATH_STAGING}")

    stg_row_counts = {}
    with tgt_engine.connect() as conn:
        content = VALIDATE_SQL_PATH_STAGING.read_text(encoding="utf-8")
        lines = [line for line in content.splitlines() if not line.strip().startswith("--")]
        clean_statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

        for stmt in clean_statements:
            if not stmt:
                continue
            res = conn.execute(text(stmt)).fetchall()
            for r in res:
                mapping = dict(r._mapping)
                tbl = mapping.get("table_name", "")
                clean_tbl = tbl.split(".")[-1]
                stg_row_counts[clean_tbl] = mapping.get("total_rows", 0)

        for logical_name, base_info in source_baseline.items():
            tbl_name = base_info["actual_table_name"]
            expected_rows = base_info["source_row_count"]
            expected_cols = base_info["expected_col_count"]

            if tbl_name in stg_row_counts:
                stg_rows = stg_row_counts[tbl_name]
            else:
                stg_rows = conn.execute(text(f'SELECT COUNT(*) FROM "{tgt_schema}"."{tbl_name}";')).scalar()

            if stg_rows != expected_rows:
                diff = abs(stg_rows - expected_rows)
                err = (
                    f"[LỆCH SỐ DÒNG STAGING] Bảng '{logical_name}': Source nguồn có {expected_rows:,} dòng "
                    f"nhưng Staging Target có {stg_rows:,} dòng (lệch {diff:,} dòng)!"
                )
                errors.append(err)
                logger.error(err)
            else:
                logger.info(f"[ĐỐI CHIẾU STAGING] Bảng '{logical_name:<14}': Khớp 100% ({stg_rows:>7,} dòng)")

    if errors:
        summary_err = (
            f"\n❌ [DỪNG PIPELINE] Phát hiện {len(errors)} sai lệch giữa Source và Staging:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
        logger.error(summary_err)
        raise ValueError(summary_err)

    logger.info("=== ✅ [ĐỐI CHIẾU STAGING THÀNH CÔNG 100%] DỮ LIỆU ĐÃ NẠP AN TOÀN VÀO TARGET! ===")
    return True

