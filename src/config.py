import os
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parent.parent

# Đường dẫn các thư mục
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

for directory in [RAW_DIR, PROCESSED_DIR, OUTPUT_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / ".env")

# Cấu hình log
LOG_LEVEL_CONSOLE = os.getenv("LOG_LEVEL_CONSOLE", "INFO").upper()
LOG_LEVEL_FILE = os.getenv("LOG_LEVEL_FILE", "DEBUG").upper()


SRC_DB_DRIVER = os.getenv("SRC_DB_DRIVER", "ODBC Driver 18 for SQL Server")
SRC_DB_SERVER = os.getenv("SRC_DB_SERVER", "localhost")
SRC_DB_PORT = os.getenv("SRC_DB_PORT", "1433")
SRC_DB_NAME = os.getenv("SRC_DB_NAME", "ecommerce_db")
SRC_DB_TRUSTED_CONNECTION = os.getenv("SRC_DB_TRUSTED_CONNECTION", "yes").lower() in ["yes", "true", "1"]
SRC_DB_TRUST_SERVER_CERTIFICATE = os.getenv("SRC_DB_TRUST_SERVER_CERTIFICATE", "yes")
SRC_DB_USER = os.getenv("SRC_DB_USER", "")
SRC_DB_PASSWORD = os.getenv("SRC_DB_PASSWORD", "")

src_auth = (
    "Trusted_Connection=yes;"
    if SRC_DB_TRUSTED_CONNECTION
    else f"UID={SRC_DB_USER};PWD={SRC_DB_PASSWORD};"
)

SRC_DB_CONNECTION_STRING = (
    f"DRIVER={{{SRC_DB_DRIVER}}};"
    f"SERVER={SRC_DB_SERVER},{SRC_DB_PORT};"
    f"DATABASE={SRC_DB_NAME};"
    f"{src_auth}"
    f"TrustServerCertificate={SRC_DB_TRUST_SERVER_CERTIFICATE};"
)

src_params = quote_plus(SRC_DB_CONNECTION_STRING)
SRC_SQLALCHEMY_URI = f"mssql+pyodbc:///?odbc_connect={src_params}"

def get_source_engine():
    """Tạo engine kết nối tới SQL Server (Source OLTP)"""
    return create_engine(SRC_SQLALCHEMY_URI)



TGT_DB_HOST = os.getenv("TGT_DB_HOST", "localhost")
TGT_DB_PORT = os.getenv("TGT_DB_PORT", "5432")
TGT_DB_NAME = os.getenv("TGT_DB_NAME", "olist_dwh")
TGT_DB_USER = os.getenv("TGT_DB_USER", "postgres")
TGT_DB_PASSWORD = os.getenv("TGT_DB_PASSWORD", "")

TGT_SQLALCHEMY_URI = (
    f"postgresql+psycopg2://{TGT_DB_USER}:{TGT_DB_PASSWORD}@{TGT_DB_HOST}:{TGT_DB_PORT}/{TGT_DB_NAME}"
)

def get_target_engine():
    """Tạo engine kết nối tới PostgreSQL (Target DWH)"""
    return create_engine(TGT_SQLALCHEMY_URI)
