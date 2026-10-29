import csv
import io
import time
import pandas as pd
from sqlalchemy import text
from src.config.config import EXTRACT_PIPELINE_RULES, STAGING_DDL_SQL_PATH, TGT_DB_SCHEMA
from src.logger import get_logger

logger = get_logger(__name__)


class ChunkLoadError(Exception):
    """Báo hiệu lỗi xảy ra tại một chunk cụ thể trong quá trình nạp dữ liệu theo batch."""

    def __init__(self, original_error, batch_idx, committed_rows, last_checkpoint_wm):
        super().__init__(str(original_error))
        self.original_error = original_error
        self.batch_idx = batch_idx
        self.committed_rows = committed_rows
        self.last_checkpoint_wm = last_checkpoint_wm


def _psql_copy_method(table, conn, keys, data_iter):
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


def init_staging_tables(tgt_engine, ddl_path=STAGING_DDL_SQL_PATH, tgt_schema=TGT_DB_SCHEMA):
    if not ddl_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file DDL khởi tạo Staging: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8").replace("{schema}", tgt_schema)
    with tgt_engine.begin() as conn:
        conn.execute(text(ddl_sql))
    logger.info(f"[INIT STAGING] Đã kiểm tra và khởi tạo cấu trúc bảng Staging '{tgt_schema}' từ '{ddl_path.name}'.")


def get_watermark_states(tgt_engine, tgt_schema=TGT_DB_SCHEMA) -> dict:
    watermark_states = {}
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

    if watermark_states:
        logger.info(f"[WATERMARK STATE] Đã đọc mốc watermark từ Target DB: {watermark_states}")
    else:
        logger.info("[WATERMARK STATE] Chưa có mốc watermark trước đó trên Target DB (sẽ dùng default_start_watermark nếu chạy incremental).")
    return watermark_states


def _upsert_watermark_state(
    conn, tgt_schema, logical_name, target_table, watermark_column, max_watermark, batch_id, overwrite: bool = False
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


def stream_load_table_to_postgres(
    chunk_stream,
    table_name,
    strategy,
    tgt_engine,
    tgt_schema=TGT_DB_SCHEMA,
    logical_name=None,
    rule=None,
):
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

            # Cập nhật mốc watermark lớn nhất sau khi nạp Full Refresh (ghi đè trực tiếp với overwrite=True)
            if logical_name and watermark_column:
                max_val = conn.execute(
                    text(f'SELECT MAX("{watermark_column}") FROM "{tgt_schema}"."{table_name}";')
                ).scalar()
                if max_val is not None:
                    _upsert_watermark_state(
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
        # Cơ chế nạp Incremental: Lưu lại trạng thái tiến độ từng Chunk (Checkpoint / Idempotent Load)
        batch_idx = 0
        last_checkpoint_wm = None
        try:
            for chunk in chunk_stream:
                batch_idx += 1
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

                # Mỗi batch có transaction riêng: lỗi thì rollback riêng batch này
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

                    # Dùng UPSERT chống trùng lặp bản ghi
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

                    # Lưu checkpoint watermark ngay khi batch này thành công trong cùng transaction
                    if logical_name and watermark_column and chunk_max_wm is not None:
                        _upsert_watermark_state(
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


def load_single_table(logical_name, actual_name, rule, chunk_stream, tgt_engine, tgt_schema=TGT_DB_SCHEMA) -> dict:
    target_table = rule.get("target_table") or actual_name
    strategy = rule.get("strategy", "cancel")
    t0 = time.time()

    logger.info(f"[PIPELINE] Bắt đầu nạp {logical_name} (nguồn: {actual_name}) -> {tgt_schema}.{target_table}")
    try:
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
