import io
import csv
import pandas as pd
from sqlalchemy import text
from src.config.config import STAGING_DDL_SQL_PATH
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


def init_staging_tables(tgt_engine, ddl_path=STAGING_DDL_SQL_PATH):
    if not ddl_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file DDL khởi tạo Staging: {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8")
    with tgt_engine.begin() as conn:
        conn.execute(text(ddl_sql))
    logger.info(f"[INIT STAGING] Đã kiểm tra và khởi tạo cấu trúc bảng Staging từ '{ddl_path.name}'.")


def stream_load_table_to_postgres(chunk_stream, table_name, strategy, tgt_engine, tgt_schema="stg"):
    total_rows = 0
    norm_strategy = str(strategy).lower().strip()

    with tgt_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{tgt_schema}";'))

        if norm_strategy == "full":
            conn.execute(text(f'TRUNCATE TABLE "{tgt_schema}"."{table_name}";'))
            logger.info(f'[TRUNCATE] Đã làm sạch bảng "{tgt_schema}"."{table_name}" (giữ nguyên cấu trúc DDL).')

        for chunk in chunk_stream:
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
