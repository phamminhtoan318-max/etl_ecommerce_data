import os
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parent.parent

OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

for directory in [OUTPUT_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / ".env")

LOG_LEVEL_CONSOLE = os.getenv("LOG_LEVEL_CONSOLE", "INFO").upper()
LOG_LEVEL_FILE = os.getenv("LOG_LEVEL_FILE", "DEBUG").upper()

SRC_DB_DRIVER = os.getenv("SRC_DB_DRIVER", "ODBC Driver 18 for SQL Server")
SRC_DB_SERVER = os.getenv("SRC_DB_SERVER", "localhost")
SRC_DB_PORT = os.getenv("SRC_DB_PORT", "1433")
SRC_DB_NAME = os.getenv("SRC_DB_NAME", "E_commerce")
SRC_DB_TRUSTED_CONNECTION = os.getenv("SRC_DB_TRUSTED_CONNECTION", "yes").lower() in ["yes", "true", "1"]
SRC_DB_TRUST_SERVER_CERTIFICATE = os.getenv("SRC_DB_TRUST_SERVER_CERTIFICATE", "yes")
SRC_DB_USER = os.getenv("SRC_DB_USER")
SRC_DB_PASSWORD = os.getenv("SRC_DB_PASSWORD")

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


TGT_DB_HOST = os.getenv("TGT_DB_HOST", "localhost")
TGT_DB_PORT = os.getenv("TGT_DB_PORT", "5432")
TGT_DB_NAME = os.getenv("TGT_DB_NAME", "olist_dwh")
TGT_DB_USER = os.getenv("TGT_DB_USER")
TGT_DB_PASSWORD = os.getenv("TGT_DB_PASSWORD")

TGT_SQLALCHEMY_URI = (
    f"postgresql+psycopg2://{TGT_DB_USER}:{TGT_DB_PASSWORD}@{TGT_DB_HOST}:{TGT_DB_PORT}/{TGT_DB_NAME}"
)

