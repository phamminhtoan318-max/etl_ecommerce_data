from sqlalchemy import text
from src.config.config import EXTRACT_PIPELINE_RULES, STAGING_DDL_SQL_PATH, TGT_DB_SCHEMA
from src.logger import get_logger
from src.schema.evolution import ensure_staging_primary_keys

logger = get_logger(__name__)


def init_staging_tables(tgt_engine, ddl_path=STAGING_DDL_SQL_PATH, tgt_schema: str = TGT_DB_SCHEMA):
    """Khởi tạo cấu trúc bảng Staging từ file DDL và gắn khóa chính kèm Index."""
    if not ddl_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file DDL khởi tạo Staging: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8").replace("{schema}", tgt_schema)
    with tgt_engine.begin() as conn:
        conn.execute(text(ddl_sql))
    logger.info(f"[INIT STAGING] Đã kiểm tra và khởi tạo cấu trúc bảng Staging '{tgt_schema}' từ '{ddl_path.name}'.")
    ensure_staging_primary_keys(tgt_engine, tgt_schema=tgt_schema)


def prepare_staging_schema(
    tgt_engine,
    tmp_schema: str = "raw_staging_tmp",
    target_schema: str = TGT_DB_SCHEMA,
    ddl_path=STAGING_DDL_SQL_PATH,
):
    """Chuẩn bị schema nháp Blue-Green Staging: DDL bảng, sao chép watermark và bảng incremental cơ sở."""
    with tgt_engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{tmp_schema}" CASCADE;'))
        conn.execute(text(f'CREATE SCHEMA "{tmp_schema}";'))

    init_staging_tables(tgt_engine, ddl_path=ddl_path, tgt_schema=tmp_schema)

    has_target = False
    try:
        with tgt_engine.connect() as conn:
            has_target = conn.execute(
                text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"),
                {"s": target_schema},
            ).scalar() is not None
    except Exception:
        has_target = False

    if has_target:
        try:
            with tgt_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO "{tmp_schema}"."_etl_watermarks"
                    SELECT * FROM "{target_schema}"."_etl_watermarks"
                    ON CONFLICT (logical_name) DO NOTHING;
                """))
        except Exception as e:
            logger.debug(f"[SCHEMA SWAP] Chưa thể sao chép watermark từ '{target_schema}': {e}")

        for logical_name, rule in EXTRACT_PIPELINE_RULES.items():
            if str(rule.get("strategy", "")).lower().strip() != "full":
                tbl = rule.get("target_table")
                if tbl:
                    try:
                        with tgt_engine.begin() as conn:
                            conn.execute(text(f"""
                                INSERT INTO "{tmp_schema}"."{tbl}"
                                SELECT * FROM "{target_schema}"."{tbl}"
                                ON CONFLICT DO NOTHING;
                            """))
                    except Exception as e:
                        logger.debug(f"[SCHEMA SWAP] Chưa thể sao chép dữ liệu cơ sở bảng '{tbl}': {e}")


def execute_atomic_swap(
    tgt_engine,
    tmp_schema: str = "raw_staging_tmp",
    target_schema: str = TGT_DB_SCHEMA,
    backup_schema: str = "raw_old_backup",
):
    """Thực thi hoán đổi tên schema nguyên tử (Atomic Schema Swap) trong 0.05s."""
    logger.info(f"🔄 Đang thực hiện Atomic Schema Swap: '{tmp_schema}' -> '{target_schema}'...")
    with tgt_engine.begin() as conn:
        has_old = conn.execute(
            text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"),
            {"s": target_schema},
        ).scalar() is not None

        if has_old:
            conn.execute(text(f'ALTER SCHEMA "{target_schema}" RENAME TO "{backup_schema}";'))
            conn.execute(text(f'ALTER SCHEMA "{tmp_schema}" RENAME TO "{target_schema}";'))
            conn.execute(text(f'DROP SCHEMA "{backup_schema}" CASCADE;'))
        else:
            conn.execute(text(f'ALTER SCHEMA "{tmp_schema}" RENAME TO "{target_schema}";'))

    logger.info(f"✨ Atomic Schema Swap thành công! Schema '{target_schema}' đã sẵn sàng 100%.")


def cleanup_temp_schemas(
    tgt_engine,
    tmp_schema: str = "raw_staging_tmp",
    backup_schema: str = "raw_old_backup",
):
    """Dọn dẹp schema tạm và schema backup khi pipeline hoàn tất hoặc gặp sự cố."""
    if tgt_engine is not None:
        try:
            with tgt_engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{tmp_schema}" CASCADE;'))
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{backup_schema}" CASCADE;'))
        except Exception:
            pass

