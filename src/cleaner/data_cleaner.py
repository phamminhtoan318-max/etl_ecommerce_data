import pandas as pd
from src.logger import get_logger

logger = get_logger(__name__)


def clean_and_dedup_chunk(chunk: pd.DataFrame, primary_keys: list, table_name: str, batch_label: str = "") -> pd.DataFrame:
    """Lọc các dòng có Primary Key bị NULL và khử trùng lặp (Dedup) khóa chính ngay trên RAM."""
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

