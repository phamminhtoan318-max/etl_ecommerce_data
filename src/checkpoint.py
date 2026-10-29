from sqlalchemy import text
from src.config.config import EXTRACT_PIPELINE_RULES, TGT_DB_SCHEMA
from src.logger import get_logger

logger = get_logger(__name__)


def get_watermark_states(tgt_engine, tgt_schema: str = TGT_DB_SCHEMA) -> dict:
    """Đọc các mốc watermark thời gian gần nhất của từng bảng từ cơ sở dữ liệu đích."""
    watermark_states = {}
    try:
        with tgt_engine.connect() as conn:
            rows = conn.execute(
                text(f'SELECT logical_name, last_watermark FROM "{tgt_schema}"."_etl_watermarks";')
            ).fetchall()
            for row in rows:
                mapping = dict(row._mapping)
                if mapping.get("last_watermark") is not None:
                    watermark_states[mapping["logical_name"]] = str(mapping["last_watermark"])[:19]

            for logical_name, rule in EXTRACT_PIPELINE_RULES.items():
                wm_col = rule.get("watermark_column")
                target_table = rule.get("target_table")
                if not wm_col or not target_table or logical_name in watermark_states:
                    continue

                max_val = conn.execute(
                    text(f'SELECT MAX("{wm_col}") FROM "{tgt_schema}"."{target_table}";')
                ).scalar()
                if max_val is not None:
                    watermark_states[logical_name] = str(max_val)[:19]
    except Exception as e:
        logger.debug(f"[WATERMARK STATE] Chưa thể đọc mốc watermark từ '{tgt_schema}' ({e}) -> Sẽ dùng mốc mặc định.")

    if watermark_states:
        logger.info(f"[WATERMARK STATE] Đã đọc mốc watermark từ Target DB: {watermark_states}")
    else:
        logger.info("[WATERMARK STATE] Chưa có mốc watermark trước đó trên Target DB (sẽ dùng default_start_watermark nếu chạy incremental).")
    return watermark_states


def upsert_watermark_state(
    conn,
    tgt_schema: str,
    logical_name: str,
    target_table: str,
    watermark_column: str,
    max_watermark,
    batch_id: str = None,
    overwrite: bool = False,
):
    """Cập nhật mốc watermark đã xử lý thành công (Idempotent Checkpoint) vào bảng _etl_watermarks."""
    if overwrite:
        watermark_update_clause = "last_watermark = EXCLUDED.last_watermark,"
    else:
        watermark_update_clause = (
            f'last_watermark = GREATEST("{tgt_schema}"."_etl_watermarks".last_watermark, EXCLUDED.last_watermark),'
        )

    upsert_sql = text(
        f"""
        INSERT INTO "{tgt_schema}"."_etl_watermarks"
            (logical_name, target_table, watermark_column, last_watermark, last_batch_id, updated_at)
        VALUES
            (:logical_name, :target_table, :watermark_column, :last_watermark, :last_batch_id, CURRENT_TIMESTAMP)
        ON CONFLICT (logical_name) DO UPDATE SET
            target_table = EXCLUDED.target_table,
            watermark_column = EXCLUDED.watermark_column,
            {watermark_update_clause}
            last_batch_id = EXCLUDED.last_batch_id,
            updated_at = CURRENT_TIMESTAMP;
        """
    )
    conn.execute(
        upsert_sql,
        {
            "logical_name": logical_name,
            "target_table": target_table,
            "watermark_column": watermark_column,
            "last_watermark": str(max_watermark)[:19],
            "last_batch_id": batch_id,
        },
    )

