import sys
import time
from datetime import datetime, timezone
from src.config.config import EXTRACT_PIPELINE_RULES, TGT_DB_SCHEMA
from src.database import get_source_engine, get_target_engine, test_connections
from src.extract import extract_from_sqlserver
from src.load import get_watermark_states, init_staging_tables, load_single_table
from src.logger import get_logger
from src.validate.validate_pipeline import measure_source_baseline, validate_staging_against_baseline

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


def main():
    pipeline_start = time.time()
    batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")
    pipeline_stats = {}

    logger.info(f"🚀 BẮT ĐẦU CHẠY PIPELINE (BATCH ID: {batch_id})")

    try:
        test_connections()

        tgt_engine = get_target_engine()
        init_staging_tables(tgt_engine, tgt_schema=TGT_DB_SCHEMA)
        watermark_states = get_watermark_states(tgt_engine, tgt_schema=TGT_DB_SCHEMA)

        source_baseline = measure_source_baseline(watermark_states=watermark_states)

        for logical_name, actual_name, rule, chunk_stream in extract_from_sqlserver(
            batch_time=batch_time, watermark_states=watermark_states
        ):
            pipeline_stats[logical_name] = load_single_table(
                logical_name, actual_name, rule, chunk_stream, tgt_engine, tgt_schema=TGT_DB_SCHEMA
            )

        ensure_all_tables_loaded(pipeline_stats)

        validate_staging_against_baseline(
            source_baseline=source_baseline,
            pipeline_stats=pipeline_stats,
            tgt_schema=TGT_DB_SCHEMA,
            tgt_engine=tgt_engine,
        )

        for name, stat in pipeline_stats.items():
            logger.debug(
                f"{name:<16} | {stat['target_table']:<32} | {str(stat['strategy']):<8} | "
                f"{stat['loaded_rows']:>12,} | {stat['elapsed_sec']:>8.2f}s | {stat['status']}"
            )

        total_rows = sum(s["loaded_rows"] for s in pipeline_stats.values())
        total_elapsed = time.time() - pipeline_start
        logger.info(
            f"📊 TỔNG CỘNG: {len(pipeline_stats)} bảng | {total_rows:,} dòng | Tổng thời gian toàn pipeline: {total_elapsed:.2f}s"
        )
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
