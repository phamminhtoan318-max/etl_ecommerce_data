from sqlalchemy import text
from src.core.logger import get_logger

logger = get_logger(__name__)


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

