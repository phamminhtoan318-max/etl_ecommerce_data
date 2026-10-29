from sqlalchemy import inspect, text
from src.config.config import (
    TGT_DB_SCHEMA,
    VALIDATE_SQL_PATH_STAGING,
)
from src.database import get_target_engine
from src.logger import get_logger

logger = get_logger(__name__)


def _parse_sql_statements(sql_text):
    lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def validate_staging_against_baseline(
    source_baseline,
    pipeline_stats=None,
    tgt_schema=TGT_DB_SCHEMA,
    tgt_engine=None,
):
    logger.info("=== [CƠ CHẾ 2 - POST-LOAD] BẮT ĐẦU ĐỐI CHIẾU TOÀN DIỆN TẦNG STAGING VỚI BASELINE ===")
    tgt_engine = tgt_engine or get_target_engine()
    pipeline_stats = pipeline_stats or {}
    errors = []

    if not VALIDATE_SQL_PATH_STAGING.exists():
        raise FileNotFoundError(f"Không tìm thấy file SQL kiểm định Staging: {VALIDATE_SQL_PATH_STAGING}")

    content = VALIDATE_SQL_PATH_STAGING.read_text(encoding="utf-8").replace("{schema}", tgt_schema)
    statements = _parse_sql_statements(content)

    stg_row_counts = {}
    stg_pk_issues = []
    stg_date_ranges = {}

    with tgt_engine.connect() as conn:
        if len(statements) >= 1:
            res_rows = conn.execute(text(statements[0])).fetchall()
            for r in res_rows:
                mapping = dict(r._mapping)
                tbl = mapping.get("table_name", "")
                clean_tbl = tbl.split(".")[-1]
                stg_row_counts[clean_tbl] = mapping.get("total_rows", 0)

        if len(statements) >= 2:
            res_pk = conn.execute(text(statements[1])).fetchall()
            for r in res_pk:
                mapping = dict(r._mapping)
                if mapping.get("issue_count", 0) > 0:
                    stg_pk_issues.append(mapping)

        if stg_pk_issues:
            for issue in stg_pk_issues:
                err = f"[LỖI PK STAGING] {issue['check_type']}: Phát hiện {issue['issue_count']:,} dòng vi phạm!"
                errors.append(err)
                logger.error(err)
        else:
            logger.info("[VALIDATE STAGING] Khóa chính tầng Staging: 0 NULL & 0 Trùng lặp trên toàn bộ 7 bảng.")

        if len(statements) >= 3:
            res_dates = conn.execute(text(statements[2])).fetchall()
            for r in res_dates:
                mapping = dict(r._mapping)
                tbl = mapping["table_name"]
                stg_date_ranges[tbl] = {
                    "column": mapping.get("watermark_column"),
                    "min": str(mapping.get("min_date"))[:19] if mapping.get("min_date") else None,
                    "max": str(mapping.get("max_date"))[:19] if mapping.get("max_date") else None,
                }

        inspector = inspect(tgt_engine)

        for logical_name, base_info in source_baseline.items():
            strategy = base_info.get("strategy", "full")
            if strategy == "cancel":
                continue

            tbl_name = base_info.get("target_table_name") or base_info["actual_table_name"]
            expected_source_rows = base_info["source_row_count"]
            expected_extract_rows = base_info.get("expected_extract_rows", expected_source_rows)

            if tbl_name in stg_row_counts:
                stg_rows = stg_row_counts[tbl_name]
            else:
                stg_rows = conn.execute(text(f'SELECT COUNT(*) FROM "{tgt_schema}"."{tbl_name}";')).scalar()

            if logical_name in pipeline_stats:
                loaded_rows = pipeline_stats[logical_name].get("loaded_rows", 0)
                if loaded_rows != expected_extract_rows:
                    diff = abs(loaded_rows - expected_extract_rows)
                    err = (
                        f"[LỆCH SỐ DÒNG NẠP] Bảng '{logical_name}': Kỳ vọng trích xuất {expected_extract_rows:,} dòng "
                        f"nhưng nạp vào Staging {loaded_rows:,} dòng (lệch {diff:,} dòng)!"
                    )
                    errors.append(err)
                    logger.error(err)

            if strategy == "full" and stg_rows != expected_source_rows:
                diff = abs(stg_rows - expected_source_rows)
                err = (
                    f"[LỆCH SỐ DÒNG STAGING] Bảng '{logical_name}': Source nguồn có {expected_source_rows:,} dòng "
                    f"nhưng Staging Target có {stg_rows:,} dòng (lệch {diff:,} dòng)!"
                )
                errors.append(err)
                logger.error(err)
            elif strategy != "full" and stg_rows < expected_extract_rows:
                err = (
                    f"[THIẾU DÒNG INCREMENTAL] Bảng '{logical_name}': Staging có {stg_rows:,} dòng, "
                    f"nhỏ hơn số dòng kỳ vọng của batch ({expected_extract_rows:,} dòng)!"
                )
                errors.append(err)
                logger.error(err)

            stg_cols = {c["name"] for c in inspector.get_columns(tbl_name, schema=tgt_schema)}
            expected_cols = set(base_info.get("expected_columns", [])) | {"_extracted_at", "_batch_id"}
            missing_cols = sorted(expected_cols - stg_cols)
            if missing_cols:
                err = f"[THIẾU CỘT STAGING] Bảng '{tgt_schema}.{tbl_name}' thiếu các cột: {missing_cols}"
                errors.append(err)
                logger.error(err)

            src_date = base_info.get("date_range")
            stg_date = stg_date_ranges.get(tbl_name)
            if strategy == "full" and src_date and stg_date:
                if src_date["min"] != stg_date["min"] or src_date["max"] != stg_date["max"]:
                    err = (
                        f"[LỆCH MỐC THỜI GIAN] Bảng '{logical_name}' ({src_date['column']}): "
                        f"Staging [{stg_date['min']} -> {stg_date['max']}] khác Source [{src_date['min']} -> {src_date['max']}]!"
                    )
                    errors.append(err)
                    logger.error(err)
                else:
                    logger.info(
                        f"[VALIDATE STAGING] Bảng '{logical_name:<14}': Khớp 100% mốc thời gian "
                        f"[{stg_date['min']} -> {stg_date['max']}]"
                    )

            logger.info(
                f"[ĐỐI CHIẾU] Bảng '{logical_name:<14}' ({tgt_schema}.{tbl_name:<28}): "
                f"{stg_rows:>7,} dòng | {len(stg_cols):>2} cột"
            )

    if errors:
        summary_err = (
            f"\n❌ [DỪNG PIPELINE] Phát hiện {len(errors)} sai lệch giữa Source và Staging:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
        logger.error(summary_err)
        raise ValueError(summary_err)

    logger.info("=== ✅ [ĐỐI CHIẾU THÀNH CÔNG] DỮ LIỆU ĐÃ NẠP AN TOÀN VÀ KHỚP 100% VỚI BASELINE! ===")
    return True
