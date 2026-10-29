import sys
import time
from datetime import datetime, timezone
from src.checkpoint import get_watermark_states
from src.config.config import EXTRACT_PIPELINE_RULES, TGT_DB_SCHEMA
from src.database import get_source_engine, get_target_engine, test_connections
from src.extract import extract_from_sqlserver
from src.load import load_single_table
from src.logger import get_logger
from src.schema import (
    SchemaValidationError,
    cleanup_temp_schemas,
    execute_atomic_swap,
    prepare_staging_schema,
)
from src.validate.validate_pipeline import measure_source_baseline, validate_staging_against_baseline

logger = get_logger(__name__)

REQUIRED_TABLES = [
    name
    for name, rule in EXTRACT_PIPELINE_RULES.items()
    if str(rule.get("strategy", "")).lower().strip() != "cancel"
]


def ensure_all_tables_loaded(pipeline_stats):
    """Xác nhận tất cả các bảng bắt buộc đều đã được nạp thành công trước khi hoán đổi schema."""
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

    target_schema = TGT_DB_SCHEMA
    tmp_schema = f"{target_schema}_staging_tmp"
    backup_schema = f"{target_schema}_old"
    tgt_engine = None

    logger.info(f"🚀 BẮT ĐẦU CHẠY PIPELINE (BATCH ID: {batch_id})")

    try:
        # 1. Kiểm tra kết nối tới cả 2 cơ sở dữ liệu
        test_connections()
        tgt_engine = get_target_engine()

        # 2. Đọc mốc checkpoint watermark từ schema chính thức
        watermark_states = get_watermark_states(tgt_engine, tgt_schema=target_schema)

        # 3. Chuẩn bị schema nháp Blue-Green Staging (DDL, watermark, dữ liệu base)
        prepare_staging_schema(
            tgt_engine=tgt_engine,
            tmp_schema=tmp_schema,
            target_schema=target_schema,
        )

        # 4. Đo baseline số dòng trên nguồn SQL Server
        source_baseline = measure_source_baseline(watermark_states=watermark_states, batch_time=batch_time)

        # 5. Vòng lặp Extract & Load streaming tuần tự từng bảng vào schema nháp
        for logical_name, actual_name, rule, chunk_stream in extract_from_sqlserver(
            batch_time=batch_time, watermark_states=watermark_states
        ):
            pipeline_stats[logical_name] = load_single_table(
                logical_name, actual_name, rule, chunk_stream, tgt_engine, tgt_schema=tmp_schema
            )

        # 6. Kiểm định tính toàn vẹn nạp của tất cả bảng
        ensure_all_tables_loaded(pipeline_stats)

        # 7. Đối soát số dòng 2 tầng giữa Staging và Source Baseline
        validate_staging_against_baseline(
            source_baseline=source_baseline,
            pipeline_stats=pipeline_stats,
            tgt_schema=tmp_schema,
            tgt_engine=tgt_engine,
        )

        # 8. Hoán đổi schema nguyên tử (Atomic Schema Swap trong 0.05s)
        execute_atomic_swap(
            tgt_engine=tgt_engine,
            tmp_schema=tmp_schema,
            target_schema=target_schema,
            backup_schema=backup_schema,
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

    except SchemaValidationError as sve:
        total_elapsed = time.time() - pipeline_start
        logger.error(
            f"🛑 [SCHEMA CONTRACT ERROR] PIPELINE DỪNG CÓ KIỂM SOÁT DO THAY ĐỔI CẤU TRÚC NGUỒN KHÔNG HỢP LỆ "
            f"(sau {total_elapsed:.2f}s):\n{sve}"
        )
        cleanup_temp_schemas(tgt_engine, tmp_schema, backup_schema)
        sys.exit(1)
    except Exception as exc:
        total_elapsed = time.time() - pipeline_start
        logger.error(f"💥 PIPELINE THẤT BẠI SAU {total_elapsed:.2f}s: {exc}")
        cleanup_temp_schemas(tgt_engine, tmp_schema, backup_schema)
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
