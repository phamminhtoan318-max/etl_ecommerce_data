from src.config.config import TGT_DB_SCHEMA
from src.logger import get_logger
from src.validate.validate_source import measure_source_baseline
from src.validate.validate_target import validate_staging_against_baseline

logger = get_logger(__name__)


def run_source_validation(schema="dbo", engine=None, watermark_states=None, batch_time=None):
    logger.info("[VALIDATE ORCHESTRATOR] Bắt đầu bước kiểm định và đo baseline nguồn...")
    return measure_source_baseline(schema=schema, engine=engine, watermark_states=watermark_states, batch_time=batch_time)


def run_staging_validation(
    source_baseline,
    pipeline_stats=None,
    tgt_schema=TGT_DB_SCHEMA,
    tgt_engine=None,
):
    logger.info("[VALIDATE ORCHESTRATOR] Bắt đầu bước đối chiếu chất lượng dữ liệu Staging với Baseline...")
    return validate_staging_against_baseline(
        source_baseline=source_baseline,
        pipeline_stats=pipeline_stats,
        tgt_schema=tgt_schema,
        tgt_engine=tgt_engine,
    )


__all__ = [
    "measure_source_baseline",
    "validate_staging_against_baseline",
    "run_source_validation",
    "run_staging_validation",
]
