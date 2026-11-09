from sqlalchemy import text
from src.config.settings import (
    CHECKPOINT_SCHEMA,
    CHECKPOINT_TABLE,
    CHUNK_LOG_TABLE,
    EXTRACT_PIPELINE_RULES,
)
from src.core.logger import get_logger

logger = get_logger(__name__)


def init_checkpoint_manager(tgt_engine):
    create_sql = text(
        f"""
        CREATE SCHEMA IF NOT EXISTS "{CHECKPOINT_SCHEMA}";

        CREATE TABLE IF NOT EXISTS "{CHECKPOINT_SCHEMA}"."{CHECKPOINT_TABLE}" (
            logical_name     VARCHAR(50) PRIMARY KEY,
            target_table     VARCHAR(100) NOT NULL,
            watermark_column VARCHAR(100),
            last_checkpoint  VARCHAR(50) NOT NULL,
            last_batch_id    VARCHAR(64),
            committed_rows   BIGINT DEFAULT 0,
            status           VARCHAR(20) DEFAULT 'IN_PROGRESS',
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS "{CHECKPOINT_SCHEMA}"."{CHUNK_LOG_TABLE}" (
            batch_id         VARCHAR(64) NOT NULL,
            logical_name     VARCHAR(50) NOT NULL,
            chunk_index      INTEGER NOT NULL,
            chunk_rows       BIGINT NOT NULL,
            chunk_max_wm     VARCHAR(50),
            status           VARCHAR(20) DEFAULT 'COMPLETED',
            committed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (batch_id, logical_name, chunk_index)
        );
        """
    )
    with tgt_engine.begin() as conn:
        conn.execute(create_sql)
    logger.debug(f"[CHECKPOINT MANAGER] Đã sẵn sàng bảng quản trị '{CHECKPOINT_SCHEMA}.{CHECKPOINT_TABLE}'.")


def get_active_checkpoints(tgt_engine):
    init_checkpoint_manager(tgt_engine)
    checkpoints = {}
    try:
        with tgt_engine.connect() as conn:
            query = text(
                f"""
                SELECT logical_name, target_table, watermark_column, 
                       last_checkpoint, last_batch_id, committed_rows, status
                FROM "{CHECKPOINT_SCHEMA}"."{CHECKPOINT_TABLE}";
                """
            )
            rows = conn.execute(query).fetchall()
            for row in rows:
                m = dict(row._mapping)
                checkpoints[m["logical_name"]] = {
                    "last_checkpoint": str(m["last_checkpoint"])[:19] if m.get("last_checkpoint") else None,
                    "target_table": m.get("target_table"),
                    "watermark_column": m.get("watermark_column"),
                    "committed_rows": int(m.get("committed_rows") or 0),
                    "status": m.get("status", "COMPLETED"),
                    "last_batch_id": m.get("last_batch_id"),
                }
    except Exception as e:
        logger.warning(f"[CHECKPOINT MANAGER] Không thể đọc checkpoint từ '{CHECKPOINT_SCHEMA}': {e}")

    return checkpoints


def record_chunk_checkpoint_in_tx(
    conn,
    logical_name,
    target_table,
    watermark_column,
    chunk_max_wm,
    batch_id,
    chunk_rows,
    chunk_index=1,
):
    checkpoint_val = str(chunk_max_wm)[:19] if chunk_max_wm is not None else "CHUNK_COMMITTED"
    upsert_sql = text(
        f"""
        INSERT INTO "{CHECKPOINT_SCHEMA}"."{CHECKPOINT_TABLE}"
            (logical_name, target_table, watermark_column, last_checkpoint, 
             last_batch_id, committed_rows, status, updated_at)
        VALUES
            (:logical_name, :target_table, :watermark_column, :last_checkpoint, 
             :last_batch_id, :committed_rows, 'IN_PROGRESS', CURRENT_TIMESTAMP)
        ON CONFLICT (logical_name) DO UPDATE SET
            target_table = EXCLUDED.target_table,
            watermark_column = EXCLUDED.watermark_column,
            last_checkpoint = EXCLUDED.last_checkpoint,
            last_batch_id = EXCLUDED.last_batch_id,
            committed_rows = "{CHECKPOINT_SCHEMA}"."{CHECKPOINT_TABLE}".committed_rows + EXCLUDED.committed_rows,
            status = 'IN_PROGRESS',
            updated_at = CURRENT_TIMESTAMP;
        """
    )
    conn.execute(
        upsert_sql,
        {
            "logical_name": logical_name,
            "target_table": target_table,
            "watermark_column": watermark_column,
            "last_checkpoint": checkpoint_val,
            "last_batch_id": batch_id,
            "committed_rows": chunk_rows,
        },
    )

    chunk_log_sql = text(
        f"""
        INSERT INTO "{CHECKPOINT_SCHEMA}"."{CHUNK_LOG_TABLE}"
            (batch_id, logical_name, chunk_index, chunk_rows, chunk_max_wm, status, committed_at)
        VALUES
            (:batch_id, :logical_name, :chunk_index, :chunk_rows, :chunk_max_wm, 'COMPLETED', CURRENT_TIMESTAMP)
        ON CONFLICT (batch_id, logical_name, chunk_index) DO UPDATE SET
            chunk_rows = EXCLUDED.chunk_rows,
            chunk_max_wm = EXCLUDED.chunk_max_wm,
            status = 'COMPLETED',
            committed_at = CURRENT_TIMESTAMP;
        """
    )
    conn.execute(
        chunk_log_sql,
        {
            "batch_id": batch_id,
            "logical_name": logical_name,
            "chunk_index": chunk_index or 1,
            "chunk_rows": chunk_rows,
            "chunk_max_wm": checkpoint_val,
        },
    )


def reset_table_checkpoint_for_new_batch(tgt_engine, logical_name, target_table, watermark_col, batch_id):
    init_checkpoint_manager(tgt_engine)
    with tgt_engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO "{CHECKPOINT_SCHEMA}"."{CHECKPOINT_TABLE}"
                    (logical_name, target_table, watermark_column, last_checkpoint, last_batch_id, committed_rows, status, updated_at)
                VALUES
                    (:logical_name, :target_table, :wm_col, 'INIT', :batch_id, 0, 'IN_PROGRESS', CURRENT_TIMESTAMP)
                ON CONFLICT (logical_name) DO UPDATE SET
                    status = 'IN_PROGRESS',
                    last_batch_id = :batch_id,
                    committed_rows = 0,
                    updated_at = CURRENT_TIMESTAMP;
                """
            ),
            {
                "logical_name": logical_name,
                "target_table": target_table,
                "wm_col": watermark_col,
                "batch_id": batch_id,
            },
        )


def mark_all_checkpoints_completed(tgt_engine, pipeline_stats, batch_id):
    init_checkpoint_manager(tgt_engine)
    with tgt_engine.begin() as conn:
        for logical_name, stat in pipeline_stats.items():
            if stat.get("status") == "SUCCESS":
                conn.execute(
                    text(
                        f"""
                        UPDATE "{CHECKPOINT_SCHEMA}"."{CHECKPOINT_TABLE}"
                        SET status = 'COMPLETED',
                            last_batch_id = :batch_id,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE logical_name = :logical_name;
                        """
                    ),
                    {"batch_id": batch_id, "logical_name": logical_name},
                )
    logger.info(f"✅ [CHECKPOINT MANAGER] Đã đánh dấu COMPLETED thành công cho batch '{batch_id}'.")


def has_in_progress_checkpoints(tgt_engine):
    init_checkpoint_manager(tgt_engine)
    try:
        with tgt_engine.connect() as conn:
            cnt = conn.execute(
                text(f"SELECT COUNT(*) FROM \"{CHECKPOINT_SCHEMA}\".\"{CHECKPOINT_TABLE}\" WHERE status = 'IN_PROGRESS';")
            ).scalar()
            return (cnt or 0) > 0
    except Exception:
        return False
