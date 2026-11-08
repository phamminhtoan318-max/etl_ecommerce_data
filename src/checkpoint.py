import json
import urllib.request
from sqlalchemy import text
from src.config.config import ALERT_WEBHOOK_URL, EXTRACT_PIPELINE_RULES, TGT_DB_SCHEMA
from src.logger import get_logger

logger = get_logger(__name__)


def get_watermark_states(tgt_engine, tgt_schema=TGT_DB_SCHEMA):
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
    tgt_schema,
    logical_name,
    target_table,
    watermark_column,
    max_watermark,
    batch_id=None,
    overwrite=False,
):
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


def save_pipeline_run_history(tgt_engine, tgt_schema, batch_id, pipeline_stats):
    if not tgt_engine or not pipeline_stats:
        return
    try:
        insert_sql = text(
            f"""
            INSERT INTO "{tgt_schema}"."_etl_run_history"
                (batch_id, logical_name, source_table, target_table, strategy, loaded_rows, elapsed_sec, status, recorded_at)
            VALUES
                (:batch_id, :logical_name, :source_table, :target_table, :strategy, :loaded_rows, :elapsed_sec, :status, CURRENT_TIMESTAMP)
            ON CONFLICT (batch_id, logical_name) DO UPDATE SET
                source_table = EXCLUDED.source_table,
                target_table = EXCLUDED.target_table,
                strategy = EXCLUDED.strategy,
                loaded_rows = EXCLUDED.loaded_rows,
                elapsed_sec = EXCLUDED.elapsed_sec,
                status = EXCLUDED.status,
                recorded_at = CURRENT_TIMESTAMP;
            """
        )
        with tgt_engine.begin() as conn:
            for logical_name, stat in pipeline_stats.items():
                conn.execute(
                    insert_sql,
                    {
                        "batch_id": batch_id,
                        "logical_name": logical_name,
                        "source_table": stat.get("source_table"),
                        "target_table": stat.get("target_table"),
                        "strategy": str(stat.get("strategy", "")),
                        "loaded_rows": int(stat.get("loaded_rows", 0)),
                        "elapsed_sec": float(stat.get("elapsed_sec", 0.0)),
                        "status": str(stat.get("status", "UNKNOWN")),
                    },
                )
        logger.info(f"[RUN HISTORY] Đã lưu thống kê vận hành batch '{batch_id}' vào '{tgt_schema}._etl_run_history'.")
    except Exception as e:
        logger.debug(f"[RUN HISTORY] Chưa thể lưu lịch sử chạy vào '{tgt_schema}._etl_run_history': {e}")


def send_alert_webhook(message):
    if not ALERT_WEBHOOK_URL:
        return
    try:
        payload = json.dumps({"content": message, "text": message}).encode("utf-8")
        req = urllib.request.Request(
            ALERT_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
        logger.info("[ALERT WEBHOOK] Đã gửi thông báo cảnh báo qua Webhook thành công.")
    except Exception as e:
        logger.warning(f"[ALERT WEBHOOK] Không thể gửi thông báo qua Webhook: {e}")
