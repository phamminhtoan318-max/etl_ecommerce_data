import io
import csv
import time
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import text
from src.database import get_target_engine
from src.extract import extract_from_sqlserver
from src.logger import get_logger

logger = get_logger(__name__)


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


def stream_load_table_to_postgres(chunk_stream, table_name, strategy, tgt_engine, tgt_schema="stg"):
    total_rows = 0
    batch_idx = 0

    with tgt_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{tgt_schema}";'))

    with tgt_engine.begin() as conn:
        for chunk in chunk_stream:
            batch_idx += 1
            if batch_idx == 1:
                if strategy == "full":
                    chunk.head(0).to_sql(
                        name=table_name,
                        con=conn,
                        schema=tgt_schema,
                        if_exists="replace",
                        index=False,
                    )

            chunk.to_sql(
                name=table_name,
                con=conn,
                schema=tgt_schema,
                if_exists="append",
                index=False,
                method=_psql_copy_method,
            )
            total_rows += len(chunk)

    return total_rows


def run_pipeline():
    batch_time = datetime.now(timezone.utc)
    tgt_engine = get_target_engine()
    watermark_states = {}

    for logical_name, actual_name, rule, chunk_stream in extract_from_sqlserver(
        batch_time=batch_time, watermark_states=watermark_states
    ):
        logger.info(f"[PIPELINE] Bắt đầu nạp {logical_name} -> stg.{actual_name}")
        loaded_rows = stream_load_table_to_postgres(
            chunk_stream=chunk_stream,
            table_name=actual_name,
            strategy=rule.get("strategy", "full"),
            tgt_engine=tgt_engine,
            tgt_schema="stg",
        )
        logger.info(f"[PIPELINE] Đã nạp thành công {loaded_rows:,} dòng vào stg.{actual_name}")

    tgt_engine.dispose()