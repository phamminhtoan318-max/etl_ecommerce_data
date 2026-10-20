import sys
import time
from datetime import datetime, timezone
from src.database import get_source_engine, get_target_engine, test_connections
from src.extract import extract_from_sqlserver
from src.load import init_staging_tables, stream_load_table_to_postgres
from src.logger import get_logger
from src.validate import measure_source_baseline, validate_staging_against_baseline

logger = get_logger(__name__)


def print_pipeline_summary(pipeline_stats, total_elapsed):
    total_rows = 0
    for logical_name, stat in pipeline_stats.items():
        rows = stat["loaded_rows"]
        total_rows += rows
        logger.info(
            f"{logical_name:<16} | {stat['target_table']:<32} | {str(stat['strategy']):<8} | "
            f"{rows:>12,} | {stat['elapsed_sec']:>8.2f}s | {stat['status']}"
        )

    logger.info(
        f"TỔNG CỘNG: {len(pipeline_stats)} bảng | {total_rows:,} dòng | Tổng thời gian toàn pipeline: {total_elapsed:.2f}s"
    )


def main():
    pipeline_start = time.time()
    batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")
    watermark_states = {}
    pipeline_stats = {}

    logger.info(f"🚀 BẮT ĐẦU CHẠY PIPELINE OLIST E-COMMERCE (BATCH ID: {batch_id})")

    try:
        test_connections()

        source_baseline = measure_source_baseline()

    
        tgt_engine = get_target_engine()
        init_staging_tables(tgt_engine)

        for logical_name, actual_name, rule, chunk_stream in extract_from_sqlserver(
            batch_time=batch_time, watermark_states=watermark_states
        ):
            target_table = rule.get("target_table") or actual_name
            strategy = rule.get("strategy", "full")
            t0 = time.time()
            logger.info(f"[PIPELINE] Bắt đầu nạp {logical_name} (nguồn: {actual_name}) -> stg.{target_table}")

            try:
                loaded_rows = stream_load_table_to_postgres(
                    chunk_stream=chunk_stream,
                    table_name=target_table,
                    strategy=strategy,
                    tgt_engine=tgt_engine,
                    tgt_schema="stg",
                )
                elapsed = time.time() - t0
                pipeline_stats[logical_name] = {
                    "source_table": actual_name,
                    "target_table": f"stg.{target_table}",
                    "strategy": strategy,
                    "loaded_rows": loaded_rows,
                    "elapsed_sec": round(elapsed, 2),
                    "status": "SUCCESS",
                }
                logger.info(
                    f"[PIPELINE] Đã nạp thành công {loaded_rows:,} dòng vào stg.{target_table} ({elapsed:.2f}s)"
                )
            except Exception as e:
                elapsed = time.time() - t0
                pipeline_stats[logical_name] = {
                    "source_table": actual_name,
                    "target_table": f"stg.{target_table}",
                    "strategy": strategy,
                    "loaded_rows": 0,
                    "elapsed_sec": round(elapsed, 2),
                    "status": f"FAILED ({e})",
                }
                logger.error(
                    f"[PIPELINE ERROR] Lỗi khi nạp bảng '{logical_name}' -> Đã rollback bảng stg.{target_table}: {e}"
                )

        validate_staging_against_baseline(source_baseline=source_baseline, tgt_schema="stg")

        total_elapsed = time.time() - pipeline_start
        print_pipeline_summary(pipeline_stats, total_elapsed)
        logger.info("🎉 PIPELINE HOÀN TẤT")

    except Exception as exc:
        total_elapsed = time.time() - pipeline_start
        logger.error(f"💥 PIPELINE THẤT BẠI SAU {total_elapsed:.2f}s: {exc}")
        sys.exit(1)
    finally:
        get_source_engine().dispose()
        get_target_engine().dispose()


if __name__ == "__main__":
    main()

