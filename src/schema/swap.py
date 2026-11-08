from sqlalchemy import text
from src.checkpoint_manager import has_in_progress_checkpoints
from src.config.config import EXTRACT_PIPELINE_RULES, STAGING_DDL_SQL_PATH, TGT_DB_SCHEMA
from src.logger import get_logger

logger = get_logger(__name__)


def init_staging_tables(tgt_engine, ddl_path=STAGING_DDL_SQL_PATH, tgt_schema=TGT_DB_SCHEMA):
    if not ddl_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file DDL khởi tạo Bronze: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8").replace("{schema}", tgt_schema)
    with tgt_engine.begin() as conn:
        conn.execute(text(ddl_sql))
    logger.info(f"[INIT BRONZE] Đã khởi tạo cấu trúc 7 bảng raw Bronze '{tgt_schema}' từ '{ddl_path.name}'.")


def prepare_staging_schema(
    tgt_engine,
    tmp_schema="bronze_staging_tmp",
    target_schema=TGT_DB_SCHEMA,
    ddl_path=STAGING_DDL_SQL_PATH,
    resume=False,
):
    if not resume and has_in_progress_checkpoints(tgt_engine):
        logger.info("🔍 [CHECKPOINT MANAGER] Phát hiện có bảng đang ở trạng thái IN_PROGRESS từ lần trước -> Tự động chuyển sang chế độ RESUME!")
        resume = True

    if resume:
        try:
            with tgt_engine.connect() as conn:
                has_tmp = conn.execute(
                    text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"),
                    {"s": tmp_schema},
                ).scalar() is not None
            if has_tmp:
                logger.info(
                    f"🔄 [RESUME MODE] Bảo toàn '{tmp_schema}' và các chunk đã commit trước đó để tiếp tục nạp!"
                )
                init_staging_tables(tgt_engine, ddl_path=ddl_path, tgt_schema=tmp_schema)
                return
        except Exception as e:
            logger.debug(f"[RESUME MODE] Không thể kiểm tra '{tmp_schema}', sẽ khởi tạo mới: {e}")

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

        try:
            with tgt_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO "{tmp_schema}"."_etl_run_history"
                    SELECT * FROM "{target_schema}"."_etl_run_history"
                    ON CONFLICT (batch_id, logical_name) DO NOTHING;
                """))
        except Exception as e:
            logger.debug(f"[SCHEMA SWAP] Chưa thể sao chép lịch sử chạy từ '{target_schema}': {e}")

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
    tmp_schema="bronze_staging_tmp",
    target_schema=TGT_DB_SCHEMA,
    backup_schema="bronze_old_backup",
):
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
    tmp_schema="bronze_staging_tmp",
    backup_schema="bronze_old_backup",
):
    if tgt_engine is not None:
        try:
            with tgt_engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{tmp_schema}" CASCADE;'))
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{backup_schema}" CASCADE;'))
        except Exception:
            pass
