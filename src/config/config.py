import os
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
import yaml
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[2]

LOGS_DIR = BASE_DIR / "logs"
VALIDATE_SQL_PATH_SOURCE = BASE_DIR / "sql" / "source" / "validate_source.sql"
VALIDATE_SQL_PATH_STAGING = BASE_DIR / "sql" / "raw" / "validate_staging.sql"
STAGING_DDL_SQL_PATH = BASE_DIR / "sql" / "raw" / "raw_table.sql"

for directory in [BASE_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
 
load_dotenv(BASE_DIR / ".env")

LOG_LEVEL_CONSOLE = os.getenv("LOG_LEVEL_CONSOLE", "INFO").upper()
LOG_LEVEL_FILE = os.getenv("LOG_LEVEL_FILE", "DEBUG").upper()


SRC_DB_DRIVER = os.getenv("SRC_DB_DRIVER")
SRC_DB_SERVER = os.getenv("SRC_DB_SERVER")
SRC_DB_PORT = os.getenv("SRC_DB_PORT")
SRC_DB_NAME = os.getenv("SRC_DB_NAME")
SRC_DB_TRUSTED_CONNECTION = str(os.getenv("SRC_DB_TRUSTED_CONNECTION", "")).lower() in ["yes", "true", "1"]
SRC_DB_TRUST_SERVER_CERTIFICATE = os.getenv("SRC_DB_TRUST_SERVER_CERTIFICATE")
SRC_DB_USER = os.getenv("SRC_DB_USER")
SRC_DB_PASSWORD = os.getenv("SRC_DB_PASSWORD") 

src_auth = (
    "Trusted_Connection=yes;"
    if SRC_DB_TRUSTED_CONNECTION
    else f"UID={SRC_DB_USER};PWD={SRC_DB_PASSWORD};"
)

src_server_target = (
    f"{SRC_DB_SERVER},{str(SRC_DB_PORT).strip()}"
    if SRC_DB_PORT and str(SRC_DB_PORT).strip()
    else SRC_DB_SERVER
)

SRC_DB_CONNECTION_STRING = (
    f"DRIVER={{{SRC_DB_DRIVER}}};"
    f"SERVER={src_server_target};"
    f"DATABASE={SRC_DB_NAME};"
    f"{src_auth}"
    f"TrustServerCertificate={SRC_DB_TRUST_SERVER_CERTIFICATE};"
)

src_params = quote_plus(SRC_DB_CONNECTION_STRING)
SRC_SQLALCHEMY_URI = f"mssql+pyodbc:///?odbc_connect={src_params}"


TGT_DB_HOST = os.getenv("TGT_DB_HOST")
TGT_DB_PORT = os.getenv("TGT_DB_PORT")
TGT_DB_NAME = os.getenv("TGT_DB_NAME")
TGT_DB_USER = os.getenv("TGT_DB_USER")
TGT_DB_PASSWORD = os.getenv("TGT_DB_PASSWORD")
TGT_DB_SCHEMA = os.getenv("TGT_DB_SCHEMA", "raw")

TGT_SQLALCHEMY_URI = (
    f"postgresql+psycopg2://{TGT_DB_USER}:{quote_plus(TGT_DB_PASSWORD or '')}@{TGT_DB_HOST}:{TGT_DB_PORT}/{TGT_DB_NAME}"
)

EXTRACT_RULES_FILE = Path(__file__).resolve().parent / "extract_rules.yaml"
with open(EXTRACT_RULES_FILE, "r", encoding="utf-8") as f:
    EXTRACT_PIPELINE_RULES = yaml.safe_load(f)

OLIST_7_TABLES = {k: v["candidates"] for k, v in EXTRACT_PIPELINE_RULES.items()}

