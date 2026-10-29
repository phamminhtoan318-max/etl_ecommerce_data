from sqlalchemy import inspect, text
from src.config.config import EXTRACT_PIPELINE_RULES, TGT_DB_SCHEMA
from src.logger import get_logger

logger = get_logger(__name__)


def map_source_type_to_postgres(col_name, col_meta):
    """Ánh xạ kiểu dữ liệu từ SQL Server sang kiểu dữ liệu tương thích nhất trong PostgreSQL."""
    if not col_meta:
        return "TEXT"

    type_str = str(col_meta.get("type", "")).upper()

    if "BIGINT" in type_str:
        return "BIGINT"
    if "SMALLINT" in type_str:
        return "SMALLINT"
    if "TINYINT" in type_str:
        return "SMALLINT"
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
    """Tự động đồng bộ các cột mới phát sinh từ nguồn vào bảng đích trên PostgreSQL (Schema Evolution)."""
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


def ensure_staging_primary_keys(tgt_engine, tgt_schema=TGT_DB_SCHEMA):
    """Kiểm tra và tạo ràng buộc PRIMARY KEY (kèm Unique B-Tree Index) cho các bảng Staging nếu dữ liệu đạt chuẩn."""
    try:
        inspector = inspect(tgt_engine)
        existing_tables = set(inspector.get_table_names(schema=tgt_schema))

        for logical_name, rule in EXTRACT_PIPELINE_RULES.items():
            tbl = rule.get("target_table")
            primary_keys = rule.get("primary_keys") or []
            if not tbl or not primary_keys or tbl not in existing_tables:
                continue

            pk_constraint = inspector.get_pk_constraint(tbl, schema=tgt_schema)
            existing_pk_cols = pk_constraint.get("constrained_columns") or []
            if set(existing_pk_cols) == set(primary_keys):
                continue

            null_conditions = " OR ".join(f'"{col}" IS NULL' for col in primary_keys)
            with tgt_engine.connect() as conn:
                null_count = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{tgt_schema}"."{tbl}" WHERE {null_conditions};')
                ).scalar()

            if null_count > 0:
                logger.warning(
                    f"[STAGING PK CHECK] Bảng '{tgt_schema}.{tbl}' có {null_count:,} dòng mang PK NULL! "
                    f"Tạm thời bỏ qua việc gắn PRIMARY KEY để tránh lỗi."
                )
                continue

            cols_str = ", ".join(f'"{col}"' for col in primary_keys)
            with tgt_engine.connect() as conn:
                dup_count = conn.execute(
                    text(
                        f'SELECT COUNT(*) FROM ('
                        f'SELECT {cols_str} FROM "{tgt_schema}"."{tbl}" '
                        f'GROUP BY {cols_str} HAVING COUNT(*) > 1'
                        f') AS _dup;'
                    )
                ).scalar()

            if dup_count > 0:
                logger.warning(
                    f"[STAGING PK CHECK] Bảng '{tgt_schema}.{tbl}' có {dup_count:,} nhóm khóa chính bị trùng lặp! "
                    f"Tạm thời bỏ qua việc gắn PRIMARY KEY để tránh lỗi."
                )
                continue

            try:
                with tgt_engine.begin() as conn:
                    for col in primary_keys:
                        conn.execute(text(f'ALTER TABLE "{tgt_schema}"."{tbl}" ALTER COLUMN "{col}" SET NOT NULL;'))
                    pk_name = f"pk_{tbl}"
                    conn.execute(
                        text(f'ALTER TABLE "{tgt_schema}"."{tbl}" ADD CONSTRAINT "{pk_name}" PRIMARY KEY ({cols_str});')
                    )
                logger.info(f"🔑 [STAGING PK] Đã tạo thành công Khóa chính & Index cho '{tgt_schema}.{tbl}' ({cols_str}).")
            except Exception as e:
                logger.warning(f"[STAGING PK ERROR] Chưa thể tạo PK cho '{tgt_schema}.{tbl}': {e}")
    except Exception as e:
        logger.debug(f"[STAGING PK] Lỗi khi kiểm tra PK trong schema '{tgt_schema}': {e}")

