from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import time
from datetime import datetime, timezone
from src.config.settings import ETL_MAX_WORKERS, EXTRACT_PIPELINE_RULES, TGT_DB_SCHEMA
from src.core.alert import send_alert_webhook
from src.core.connection import get_source_engine, get_target_engine, test_connections
from src.core.logger import get_logger
from src.ingestion.extract import build_dynamic_query, extract_from_sqlserver, extract_table
from src.ingestion.load import load_single_table
from src.schema import (
    SchemaValidationError,
    init_bronze_schema,
)
from src.state.history import save_pipeline_run_history
from src.state.state_manager import (
    get_active_checkpoints,
    init_checkpoint_manager,
    mark_all_checkpoints_completed,
)
from src.state.watermark import get_watermark_states
from src.transformation.dbt_runner import run_dbt_all
from src.validate import measure_source_baseline, validate_staging_against_baseline

logger = get_logger(__name__)

REQUIRED_TABLES = [
    name
    for name, rule in EXTRACT_PIPELINE_RULES.items()
    if str(rule.get("strategy", "")).lower().strip() != "cancel"
]


def ensure_all_tables_loaded(pipeline_stats):
    failed = {n: s["status"] for n, s in pipeline_stats.items() if s["status"] != "SUCCESS"}
    skipped = [n for n in REQUIRED_TABLES if n not in pipeline_stats]

    if failed or skipped:
        details = [f"  - {n}: {status}" for n, status in failed.items()]
        details += [f"  - {n}: SKIPPED (không được extract/load)" for n in skipped]
        raise RuntimeError(
            f"Có {len(failed) + len(skipped)} bảng không nạp thành công -> DỪNG PIPELINE:\n" + "\n".join(details)
        )


def _process_single_table_worker(
    logical_name,
    base_info,
    rule_template,
    watermark_states,
    batch_time,
    batch_id,
    src_engine,
    tgt_engine,
    target_schema,
):
    rule = dict(rule_template)
    actual_name = base_info["actual_table_name"]
    resolved_columns = base_info["expected_columns"]
    rule["resolved_columns"] = resolved_columns
    rule["source_columns"] = base_info.get("source_columns_meta", [])
    last_wm = watermark_states.get(logical_name)

    query_str = build_dynamic_query(
        logical_name=logical_name,
        sql_table_name=actual_name,
        rule=rule,
        last_watermark=last_wm,
        resolved_columns=resolved_columns,
        batch_time=batch_time,
    )
    if not query_str:
        logger.warning(f"⏩ [BỎ QUA BẢNG] '{logical_name}' bị hủy do lỗi cơ chế/cancel.")
        return logical_name, None

    chunk_stream = extract_table(
        logical_name=logical_name,
        sql_table_name=actual_name,
        rule=rule,
        engine=src_engine,
        batch_time=batch_time,
        batch_id=batch_id,
        last_watermark=last_wm,
        query_str=query_str,
    )
    stat = load_single_table(
        logical_name=logical_name,
        actual_name=actual_name,
        rule=rule,
        chunk_stream=chunk_stream,
        tgt_engine=tgt_engine,
        tgt_schema=target_schema,
    )
    return logical_name, stat


def main():
    pipeline_start = time.time()
    batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")
    pipeline_stats = {}

    target_schema = TGT_DB_SCHEMA
    tgt_engine = None
    sequential_mode = "--sequential" in sys.argv or ETL_MAX_WORKERS <= 1

    logger.info(f"🚀 BẮT ĐẦU CHẠY PIPELINE (BATCH ID: {batch_id})")

    try:
        test_connections()
        tgt_engine = get_target_engine()
        src_engine = get_source_engine()

        init_bronze_schema(tgt_engine, target_schema=target_schema)

        watermark_states = get_watermark_states(tgt_engine, tgt_schema=target_schema)

        source_baseline = measure_source_baseline(watermark_states=watermark_states, batch_time=batch_time)

        if sequential_mode:
            for logical_name, actual_name, rule, chunk_stream in extract_from_sqlserver(
                batch_time=batch_time,
                watermark_states=watermark_states,
                source_baseline=source_baseline,
            ):
                pipeline_stats[logical_name] = load_single_table(
                    logical_name, actual_name, rule, chunk_stream, tgt_engine, tgt_schema=target_schema
                )
        else:
            logger.info(f"⚡ [CONCURRENT EL] Kích hoạt trích xuất & nạp song song ({ETL_MAX_WORKERS} workers)...")
            with ThreadPoolExecutor(max_workers=ETL_MAX_WORKERS) as executor:
                futures = [
                    executor.submit(
                        _process_single_table_worker,
                        logical_name,
                        base_info,
                        EXTRACT_PIPELINE_RULES.get(logical_name, {}),
                        watermark_states,
                        batch_time,
                        batch_id,
                        src_engine,
                        tgt_engine,
                        target_schema,
                    )
                    for logical_name, base_info in source_baseline.items()
                ]
                for future in as_completed(futures):
                    logical_name, stat = future.result()
                    if stat is not None:
                        pipeline_stats[logical_name] = stat

        ensure_all_tables_loaded(pipeline_stats)

        validate_staging_against_baseline(
            source_baseline=source_baseline,
            pipeline_stats=pipeline_stats,
            tgt_schema=target_schema,
            tgt_engine=tgt_engine,
        )

        mark_all_checkpoints_completed(tgt_engine, pipeline_stats=pipeline_stats, batch_id=batch_id)

        save_pipeline_run_history(
            tgt_engine=tgt_engine,
            tgt_schema=target_schema,
            batch_id=batch_id,
            pipeline_stats=pipeline_stats,
        )

        for name, stat in pipeline_stats.items():
            logger.debug(
                f"{name:<16} | {stat['target_table']:<32} | {str(stat['strategy']):<8} | "
                f"{stat['loaded_rows']:>12,} | {stat['elapsed_sec']:>8.2f}s | {stat['status']}"
            )

        total_rows = sum(s["loaded_rows"] for s in pipeline_stats.values())
        total_elapsed = time.time() - pipeline_start
        logger.info(
            f"📊 TỔNG CỘNG BRONZE: {len(pipeline_stats)} bảng | {total_rows:,} dòng | Thời gian nạp: {total_elapsed:.2f}s"
        )
        logger.info("🎉 NẠP TẦNG BRONZE THÀNH CÔNG 100%!")

        if "--transform" in sys.argv or "--all" in sys.argv:
            run_dbt_all()

    except SchemaValidationError as sve:
        total_elapsed = time.time() - pipeline_start
        err_msg = (
            f"🛑 [SCHEMA CONTRACT ERROR] PIPELINE DỪNG CÓ KIỂM SOÁT DO THAY ĐỔI CẤU TRÚC NGUỒN KHÔNG HỢP LỆ "
            f"(sau {total_elapsed:.2f}s):\n{sve}"
        )
        logger.error(err_msg)
        send_alert_webhook(err_msg)
        sys.exit(1)
    except Exception as exc:
        total_elapsed = time.time() - pipeline_start
        err_msg = f"💥 PIPELINE THẤT BẠI SAU {total_elapsed:.2f}s (Batch {batch_id}): {exc}"
        logger.error(err_msg)
        send_alert_webhook(err_msg)

        logger.warning(
            f"💾 [FAULT TOLERANCE] Dữ liệu các chunk đã commit được bảo toàn trong '{target_schema}'. "
            f"Checkpoint được lưu trong metadata, hệ thống sẽ tự động khôi phục ở lần chạy sau!"
        )
        sys.exit(1)
    finally:
        try:
            get_source_engine().dispose()
        except Exception:
            pass
        if tgt_engine is not None:
            try:
                tgt_engine.dispose()
            except Exception:
                pass


if __name__ == "__main__":
    main()
