import csv
import io
import time
import pandas as pd
from sqlalchemy import inspect, text
from src.config.config import EXTRACT_PIPELINE_RULES, STAGING_DDL_SQL_PATH, TGT_DB_SCHEMA
from src.logger import get_logger
from src.schema_contract import sync_target_schema_evolution

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


def ensure_staging_primary_keys(tgt_engine, tgt_schema=TGT_DB_SCHEMA):
    try:
        inspector = inspect(tgt_engine)
        existing_tables = set(inspector.get_table_names(schema=tgt_schema))

        for logical_name, rule in EXTRACT_PIPELINE_RULES.items():
            tbl = rule.get("target_table")
            primary_keys = rule.get("primary_keys") or []
            if not tbl or not primary_keys or tbl not in existing_tables:
                continue

            pk_constraint = inspector.get_pk_constraint(tbl, schema=tgt_schema)
            existing_pk_cols = pk_constraint.get("constrained_columns") or []
            if set(existing_pk_cols) == set(primary_keys):
                continue

            null_conditions = " OR ".join(f'"{col}" IS NULL' for col in primary_keys)
            with tgt_engine.connect() as conn:
                null_count = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{tgt_schema}"."{tbl}" WHERE {null_conditions};')
                ).scalar()

            if null_count > 0:
                logger.warning(
                    f"[STAGING PK CHECK] Bảng '{tgt_schema}.{tbl}' có {null_count:,} dòng mang PK NULL! "
                    f"Tạm thời bỏ qua việc gắn PRIMARY KEY để tránh lỗi."
                )
                continue

            cols_str = ", ".join(f'"{col}"' for col in primary_keys)
            with tgt_engine.connect() as conn:
                dup_count = conn.execute(
                    text(
                        f'SELECT COUNT(*) FROM ('
                        f'SELECT {cols_str} FROM "{tgt_schema}"."{tbl}" '
                        f'GROUP BY {cols_str} HAVING COUNT(*) > 1'
                        f') AS _dup;'
                    )
                ).scalar()

            if dup_count > 0:
                logger.warning(
                    f"[STAGING PK CHECK] Bảng '{tgt_schema}.{tbl}' có {dup_count:,} nhóm khóa chính bị trùng lặp! "
                    f"Tạm thời bỏ qua việc gắn PRIMARY KEY để tránh lỗi."
                )
                continue

            try:
                with tgt_engine.begin() as conn:
                    for col in primary_keys:
                        conn.execute(text(f'ALTER TABLE "{tgt_schema}"."{tbl}" ALTER COLUMN "{col}" SET NOT NULL;'))
                    pk_name = f"pk_{tbl}"
                    conn.execute(
                        text(f'ALTER TABLE "{tgt_schema}"."{tbl}" ADD CONSTRAINT "{pk_name}" PRIMARY KEY ({cols_str});')
                    )
                logger.info(f"🔑 [STAGING PK] Đã tạo thành công Khóa chính & Index cho '{tgt_schema}.{tbl}' ({cols_str}).")
            except Exception as e:
                logger.warning(f"[STAGING PK ERROR] Chưa thể tạo PK cho '{tgt_schema}.{tbl}': {e}")
    except Exception as e:
        logger.debug(f"[STAGING PK] Lỗi khi kiểm tra PK trong schema '{tgt_schema}': {e}")


def init_staging_tables(tgt_engine, ddl_path=STAGING_DDL_SQL_PATH, tgt_schema=TGT_DB_SCHEMA):
    if not ddl_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file DDL khởi tạo Staging: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8").replace("{schema}", tgt_schema)
    with tgt_engine.begin() as conn:
        conn.execute(text(ddl_sql))
    logger.info(f"[INIT STAGING] Đã kiểm tra và khởi tạo cấu trúc bảng Staging '{tgt_schema}' từ '{ddl_path.name}'.")
    ensure_staging_primary_keys(tgt_engine, tgt_schema=tgt_schema)


def get_watermark_states(tgt_engine, tgt_schema=TGT_DB_SCHEMA) -> dict:
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


def _clean_and_dedup_chunk(chunk, primary_keys, table_name, batch_label=""):
    if not primary_keys or chunk.empty:
        return chunk

    valid_pks = [pk for pk in primary_keys if pk in chunk.columns]
    if not valid_pks:
        return chunk

    null_mask = chunk[valid_pks].isna().any(axis=1)
    if null_mask.any():
        null_rows = int(null_mask.sum())
        logger.warning(
            f"[DATA INTEGRITY] Phát hiện {null_rows:,} dòng mang PK NULL trong {batch_label} bảng '{table_name}'. "
            f"Tự động lọc bỏ các dòng này để bảo vệ toàn vẹn khóa chính."
        )
        chunk = chunk[~null_mask]

    dup_count = int(chunk.duplicated(subset=valid_pks).sum())
    if dup_count > 0:
        logger.warning(
            f"[DEDUPLICATION] Phát hiện {dup_count:,} dòng trùng lặp khóa chính trong {batch_label} bảng '{table_name}'. "
            f"Tự động giữ bản ghi mới nhất."
        )
        chunk = chunk.drop_duplicates(subset=valid_pks, keep="last")

    return chunk


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
                chunk = _clean_and_dedup_chunk(chunk, primary_keys, table_name)
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
                chunk = _clean_and_dedup_chunk(chunk, primary_keys, table_name, f"chunk #{batch_idx}")
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
