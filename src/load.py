import csv
import io
import time
import pandas as pd
from sqlalchemy import text
from src.checkpoint import get_watermark_states, upsert_watermark_state
from src.cleaner.data_cleaner import clean_and_dedup_chunk
from src.config.config import STAGING_DDL_SQL_PATH, TGT_DB_SCHEMA
from src.logger import get_logger
from src.schema import (
    ensure_staging_primary_keys,
    init_staging_tables,
    sync_target_schema_evolution,
)

logger = get_logger(__name__)

# Re-exports để đảm bảo tính tương thích ngược hoàn toàn
_clean_and_dedup_chunk = clean_and_dedup_chunk
_upsert_watermark_state = upsert_watermark_state


class ChunkLoadError(Exception):
    """Báo hiệu lỗi xảy ra tại một chunk cụ thể trong quá trình nạp dữ liệu theo batch."""

    def __init__(self, original_error, batch_idx, committed_rows, last_checkpoint_wm):
        super().__init__(str(original_error))
        self.original_error = original_error
        self.batch_idx = batch_idx
        self.committed_rows = committed_rows
        self.last_checkpoint_wm = last_checkpoint_wm


def _psql_copy_method(table, conn, keys, data_iter):
    """Sử dụng PostgreSQL COPY qua StringIO để nạp dữ liệu đạt tốc độ tối đa."""
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(data_iter)
        s_buf.seek(0)

        tbl = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
        cols = ", ".join(f'"{k}"' for k in keys)
        copy_sql = f"COPY {tbl} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')"
        cur.copy_expert(sql=copy_sql, file=s_buf)
        s_buf.close()


def stream_load_table_to_postgres(
    chunk_stream,
    table_name: str,
    strategy: str,
    tgt_engine,
    tgt_schema: str = TGT_DB_SCHEMA,
    logical_name: str = None,
    rule: dict = None,
) -> int:
    """Nạp stream các DataFrame chunks vào bảng đích PostgreSQL (hỗ trợ Full Truncate & Incremental Checkpoint)."""
    total_rows = 0
    norm_strategy = str(strategy).lower().strip()
    rule = rule or {}
    primary_keys = rule.get("primary_keys") or []
    watermark_column = rule.get("watermark_column")
    use_temp_upsert = norm_strategy != "full" and len(primary_keys) > 0
    temp_table_name = f"_tmp_{table_name}"

    with tgt_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{tgt_schema}";'))

    if norm_strategy == "full":
        last_batch_id = None
        with tgt_engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{tgt_schema}"."{table_name}";'))
            logger.info(f'[TRUNCATE] Đã làm sạch bảng "{tgt_schema}"."{table_name}" (giữ nguyên cấu trúc DDL).')

            for chunk in chunk_stream:
                chunk = clean_and_dedup_chunk(chunk, primary_keys, table_name)
                if chunk.empty:
                    continue
                if "_batch_id" in chunk.columns and len(chunk) > 0:
                    last_batch_id = str(chunk["_batch_id"].iloc[0])
                chunk.to_sql(
                    name=table_name,
                    con=conn,
                    schema=tgt_schema,
                    if_exists="append",
                    index=False,
                    method=_psql_copy_method,
                )
                total_rows += len(chunk)

            if logical_name and watermark_column:
                max_val = conn.execute(
                    text(f'SELECT MAX("{watermark_column}") FROM "{tgt_schema}"."{table_name}";')
                ).scalar()
                if max_val is not None:
                    upsert_watermark_state(
                        conn=conn,
                        tgt_schema=tgt_schema,
                        logical_name=logical_name,
                        target_table=table_name,
                        watermark_column=watermark_column,
                        max_watermark=max_val,
                        batch_id=last_batch_id,
                        overwrite=True,
                    )
                    logger.info(
                        f"[WATERMARK] Đã cập nhật mốc watermark ban đầu cho '{logical_name}' "
                        f"({watermark_column} = {str(max_val)[:19]}) sau khi nạp Full Refresh."
                    )
    else:
        batch_idx = 0
        last_checkpoint_wm = None
        try:
            for chunk in chunk_stream:
                batch_idx += 1
                chunk = clean_and_dedup_chunk(chunk, primary_keys, table_name, f"chunk #{batch_idx}")
                chunk_rows = len(chunk)
                if chunk_rows == 0:
                    continue

                chunk_max_wm = None
                if watermark_column and watermark_column in chunk.columns:
                    c_max = pd.to_datetime(chunk[watermark_column], errors="coerce").max()
                    if pd.notna(c_max):
                        chunk_max_wm = c_max

                chunk_batch_id = (
                    str(chunk["_batch_id"].iloc[0])
                    if "_batch_id" in chunk.columns and len(chunk) > 0
                    else None
                )

                with tgt_engine.begin() as conn:
                    conn.execute(
                        text(
                            f'CREATE TEMP TABLE "{temp_table_name}" '
                            f'(LIKE "{tgt_schema}"."{table_name}" INCLUDING DEFAULTS) ON COMMIT DROP;'
                        )
                    )

                    chunk.to_sql(
                        name=temp_table_name,
                        con=conn,
                        schema=None,
                        if_exists="append",
                        index=False,
                        method=_psql_copy_method,
                    )

                    if use_temp_upsert:
                        pk_join_cond = " AND ".join(f't."{pk}" = s."{pk}"' for pk in primary_keys)
                        conn.execute(
                            text(
                                f'DELETE FROM "{tgt_schema}"."{table_name}" AS t '
                                f'USING "{temp_table_name}" AS s '
                                f"WHERE {pk_join_cond};"
                            )
                        )
                        conn.execute(
                            text(
                                f'INSERT INTO "{tgt_schema}"."{table_name}" '
                                f'SELECT * FROM "{temp_table_name}";'
                            )
                        )
                    else:
                        conn.execute(
                            text(
                                f'INSERT INTO "{tgt_schema}"."{table_name}" '
                                f'SELECT * FROM "{temp_table_name}";'
                            )
                        )

                    if logical_name and watermark_column and chunk_max_wm is not None:
                        upsert_watermark_state(
                            conn=conn,
                            tgt_schema=tgt_schema,
                            logical_name=logical_name,
                            target_table=table_name,
                            watermark_column=watermark_column,
                            max_watermark=chunk_max_wm,
                            batch_id=chunk_batch_id,
                            overwrite=False,
                        )
                        last_checkpoint_wm = chunk_max_wm

                total_rows += chunk_rows
                logger.debug(
                    f"[BATCH #{batch_idx} COMMIT] Bảng '{logical_name}': Đã commit {chunk_rows:,} dòng "
                    f"(checkpoint: {str(chunk_max_wm)[:19] if chunk_max_wm else 'N/A'}, lũy kế: {total_rows:,} dòng)"
                )
        except Exception as e:
            logger.error(
                f"[CHECKPOINT ROLLBACK] Bảng '{logical_name}' gặp lỗi tại chunk #{batch_idx}: {e}. "
                f"-> Đã rollback riêng chunk #{batch_idx}. Tiến độ đã checkpoint thành công ({total_rows:,} dòng, "
                f"checkpoint: {str(last_checkpoint_wm)[:19] if last_checkpoint_wm else 'N/A'}) vẫn được bảo toàn trong DB!"
            )
            raise ChunkLoadError(
                original_error=e,
                batch_idx=batch_idx,
                committed_rows=total_rows,
                last_checkpoint_wm=last_checkpoint_wm,
            ) from e

    return total_rows


def load_single_table(logical_name: str, actual_name: str, rule: dict, chunk_stream, tgt_engine, tgt_schema: str = TGT_DB_SCHEMA) -> dict:
    """Nạp dữ liệu cho một bảng đơn lẻ: đồng bộ schema evolution, nạp chunks và bắt lỗi checkpoint."""
    target_table = rule.get("target_table") or actual_name
    strategy = rule.get("strategy", "cancel")
    t0 = time.time()

    logger.info(f"[PIPELINE] Bắt đầu nạp {logical_name} (nguồn: {actual_name}) -> {tgt_schema}.{target_table}")
    try:
        rule = rule or {}
        if rule.get("resolved_columns"):
            cols_to_sync = list(rule["resolved_columns"]) + ["_extracted_at", "_batch_id"]
            sync_target_schema_evolution(
                tgt_engine=tgt_engine,
                tgt_schema=tgt_schema,
                target_table=target_table,
                chunk_columns=cols_to_sync,
                source_columns_meta=rule.get("source_columns"),
            )

        loaded_rows = stream_load_table_to_postgres(
            chunk_stream=chunk_stream,
            table_name=target_table,
            strategy=strategy,
            tgt_engine=tgt_engine,
            tgt_schema=tgt_schema,
            logical_name=logical_name,
            rule=rule,
        )
        elapsed = time.time() - t0
        logger.info(
            f"[PIPELINE] Đã nạp thành công {loaded_rows:,} dòng vào {tgt_schema}.{target_table} ({elapsed:.2f}s)"
        )
        return {
            "source_table": actual_name,
            "target_table": f"{tgt_schema}.{target_table}",
            "strategy": strategy,
            "loaded_rows": loaded_rows,
            "elapsed_sec": round(elapsed, 2),
            "status": "SUCCESS",
        }
    except ChunkLoadError as cle:
        elapsed = time.time() - t0
        logger.warning(
            f"[PIPELINE PARTIAL] Bảng '{logical_name}' dừng ở chunk #{cle.batch_idx}. "
            f"Đã lưu checkpoint {cle.committed_rows:,} dòng vào {tgt_schema}.{target_table} "
            f"(mốc watermark: {str(cle.last_checkpoint_wm)[:19] if cle.last_checkpoint_wm else 'N/A'})."
        )
        return {
            "source_table": actual_name,
            "target_table": f"{tgt_schema}.{target_table}",
            "strategy": strategy,
            "loaded_rows": cle.committed_rows,
            "elapsed_sec": round(elapsed, 2),
            "status": f"FAILED_AT_CHUNK_{cle.batch_idx} ({cle.original_error})",
            "last_checkpoint": str(cle.last_checkpoint_wm)[:19] if cle.last_checkpoint_wm else None,
        }
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(
            f"[PIPELINE ERROR] Lỗi khi nạp bảng '{logical_name}' -> Đã rollback: {e}"
        )
        return {
            "source_table": actual_name,
            "target_table": f"{tgt_schema}.{target_table}",
            "strategy": strategy,
            "loaded_rows": 0,
            "elapsed_sec": round(elapsed, 2),
            "status": f"FAILED ({e})",
        }
