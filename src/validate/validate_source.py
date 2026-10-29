from sqlalchemy import inspect, text
from src.config.config import (
    EXTRACT_PIPELINE_RULES,
    VALIDATE_SQL_PATH_SOURCE,
)
from src.database import get_source_engine
from src.extract import OLIST_7_TABLES, build_dynamic_query, resolve_table_name
from src.logger import get_logger
from src.schema_contract import resolve_and_validate_columns

logger = get_logger(__name__)


def _parse_sql_statements(sql_text):
    lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def measure_source_baseline(schema="dbo", engine=None, watermark_states=None):
    logger.info("=== [CƠ CHẾ 1 - PRE-EXTRACT] BẮT ĐẦU ĐO ĐẠC THÔNG SỐ CHUẨN TỪ SOURCE ===")
    engine = engine or get_source_engine()
    watermark_states = watermark_states or {}

    if not VALIDATE_SQL_PATH_SOURCE.exists():
        raise FileNotFoundError(f"Không tìm thấy file SQL chuẩn: {VALIDATE_SQL_PATH_SOURCE}")

    statements = _parse_sql_statements(VALIDATE_SQL_PATH_SOURCE.read_text(encoding="utf-8"))

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
            rule = EXTRACT_PIPELINE_RULES.get(logical_name, {})
            strategy = str(rule.get("strategy", "full")).lower().strip()
            if strategy == "cancel":
                logger.warning(f"[BASELINE SKIP] Bảng '{logical_name}' có strategy='cancel' -> Bỏ qua đo baseline.")
                continue

            actual_name = resolve_table_name(existing_tables, candidates)
            if not actual_name:
                raise ValueError(f"Không tìm thấy bảng '{logical_name}' trên SQL Server!")

            source_columns_meta = inspector.get_columns(actual_name, schema=schema)
            source_cols = [c["name"] for c in source_columns_meta]
            resolved_cols, col_mode = resolve_and_validate_columns(
                logical_name=logical_name,
                actual_table_name=actual_name,
                rule=rule,
                source_columns=source_columns_meta,
            )
            expected_columns = list(resolved_cols)

            expected_row_count = row_counts.get(actual_name)
            if expected_row_count is None:
                expected_row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM [{schema}].[{actual_name}] WITH (NOLOCK)")
                ).scalar()

            expected_extract_rows = expected_row_count
            if strategy != "full":
                last_wm = watermark_states.get(logical_name)
                dyn_query = build_dynamic_query(
                    logical_name,
                    actual_name,
                    rule,
                    last_watermark=last_wm,
                    schema=schema,
                    include_order_by=False,
                    resolved_columns=resolved_cols,
                )
                if dyn_query:
                    expected_extract_rows = conn.execute(
                        text(f"SELECT COUNT(*) FROM ({dyn_query}) AS _sub")
                    ).scalar()

            expected_col_count = len(expected_columns) + 2

            source_baseline[logical_name] = {
                "actual_table_name": actual_name,
                "target_table_name": rule.get("target_table") or actual_name,
                "strategy": strategy,
                "source_row_count": expected_row_count,
                "expected_extract_rows": expected_extract_rows,
                "source_columns": source_cols,
                "expected_columns": expected_columns,
                "source_col_count": len(source_cols),
                "expected_col_count": expected_col_count,
                "date_range": date_ranges.get(actual_name),
            }

            logger.info(
                f"[BASELINE] Bảng '{logical_name:<14}' ({actual_name:<28}): "
                f"Tổng nguồn {expected_row_count:>7,} dòng | Kỳ vọng trích xuất {expected_extract_rows:>7,} dòng | "
                f"{len(expected_columns):>2} cột nguồn (+2 cột audit)"
            )

    logger.info("=== [CƠ CHẾ 1 - PRE-EXTRACT] ĐÃ LƯU THÀNH CÔNG THÔNG SỐ BASELINE VÀO BỘ NHỚ ===")
    return source_baseline
