from pathlib import Path
import numpy as np
import pandas as pd
from src.config.config import REJECTED_DIR
from src.logger import get_logger

logger = get_logger(__name__)


def save_error_records(invalid_df, table_name, reason, output_dir=REJECTED_DIR):
    if invalid_df is None or invalid_df.empty:
        return
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    export_df = invalid_df.copy()
    export_df["error_reason"] = reason
    file_path = output_path / f"{table_name}_errors.csv"
    if file_path.exists():
        export_df.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8")
    else:
        export_df.to_csv(file_path, mode="w", header=True, index=False, encoding="utf-8")
    logger.warning(f"[QUALITY EXPORT] Đã lưu {len(export_df):,} dòng lỗi vào '{file_path}'.")


def check_positive_values(df, columns, allow_zero=False, table_name="DataFrame", output_dir=REJECTED_DIR):
    if df is None or df.empty:
        logger.warning(f"[{table_name}] DataFrame rỗng, bỏ qua kiểm tra check_positive_values.")
        return True, pd.DataFrame()

    if isinstance(columns, str):
        columns = [columns]

    invalid_indices = set()
    is_valid = True

    for col in columns:
        if col not in df.columns:
            logger.warning(f"[{table_name}] Cột '{col}' không tồn tại trong DataFrame để kiểm tra.")
            continue

        numeric_series = pd.to_numeric(df[col], errors="coerce")

        if allow_zero:
            violation_mask = (numeric_series < 0) | numeric_series.isna()
            cond_str = "< 0 hoặc NaN"
        else:
            violation_mask = (numeric_series <= 0) | numeric_series.isna()
            cond_str = "<= 0 hoặc NaN"

        violation_count = violation_mask.sum()
        if violation_count > 0:
            is_valid = False
            invalid_indices.update(df[violation_mask].index)
            sample_vals = df.loc[violation_mask, col].head(3).tolist()
            logger.warning(
                f"[QUALITY WARNING] Bảng '{table_name}' cột '{col}': Phát hiện {violation_count:,} dòng "
                f"vi phạm ({cond_str})! Mẫu giá trị lỗi: {sample_vals}"
            )
            col_invalid = df[violation_mask].copy()
            save_error_records(
                col_invalid,
                table_name,
                f"Cột {col} {cond_str}",
                output_dir=output_dir,
            )
        else:
            logger.info(f"[QUALITY PASS] Bảng '{table_name}' cột '{col}': 100% giá trị hợp lệ (> 0).")

    invalid_df = df.loc[sorted(invalid_indices)].copy()
    return is_valid, invalid_df


def check_chronological_dates(df, start_col, end_col, allow_equal=True, table_name="DataFrame", output_dir=REJECTED_DIR):
    if df is None or df.empty:
        logger.warning(f"[{table_name}] DataFrame rỗng, bỏ qua kiểm tra check_chronological_dates.")
        return True, pd.DataFrame()

    for c in [start_col, end_col]:
        if c not in df.columns:
            logger.warning(f"[{table_name}] Cột '{c}' không tồn tại trong DataFrame.")
            return True, pd.DataFrame()

    start_dt = pd.to_datetime(df[start_col], errors="coerce")
    end_dt = pd.to_datetime(df[end_col], errors="coerce")

    valid_pair_mask = start_dt.notna() & end_dt.notna()

    if allow_equal:
        time_violation_mask = valid_pair_mask & (end_dt < start_dt)
        compare_str = f"{end_col} < {start_col}"
    else:
        time_violation_mask = valid_pair_mask & (end_dt <= start_dt)
        compare_str = f"{end_col} <= {start_col}"

    violation_count = time_violation_mask.sum()
    if violation_count > 0:
        invalid_df = df[time_violation_mask].copy()
        logger.warning(
            f"[QUALITY WARNING] Bảng '{table_name}': Phát hiện {violation_count:,} dòng bị nghịch lý thời gian "
            f"({compare_str})!"
        )
        save_error_records(
            invalid_df,
            table_name,
            f"Nghịch lý thời gian: {compare_str}",
            output_dir=output_dir,
        )
        return False, invalid_df

    logger.info(
        f"[QUALITY PASS] Bảng '{table_name}': Logic thời gian [{start_col} -> {end_col}] hoàn toàn chính xác."
    )
    return True, pd.DataFrame()


def check_valid_categories(df, col, valid_list, case_sensitive=False, allow_null=True, table_name="DataFrame", output_dir=REJECTED_DIR):
    if df is None or df.empty or col not in df.columns:
        return True, pd.DataFrame()

    series = df[col]

    if not case_sensitive:
        valid_set = {str(v).strip().lower() for v in valid_list}
        check_series = series.astype(str).str.strip().str.lower()
    else:
        valid_set = {str(v).strip() for v in valid_list}
        check_series = series.astype(str).str.strip()

    is_null_mask = series.isna()
    in_valid_mask = check_series.isin(valid_set)

    if allow_null:
        violation_mask = (~is_null_mask) & (~in_valid_mask)
    else:
        violation_mask = (~in_valid_mask) | is_null_mask

    violation_count = violation_mask.sum()
    if violation_count > 0:
        invalid_df = df[violation_mask].copy()
        unknown_values = df.loc[violation_mask, col].dropna().unique().tolist()
        logger.warning(
            f"[QUALITY WARNING] Bảng '{table_name}' cột '{col}': Phát hiện {violation_count:,} dòng có giá trị "
            f"không nằm trong danh mục hợp lệ! Các giá trị lạ: {unknown_values[:10]}"
        )
        save_error_records(
            invalid_df,
            table_name,
            f"Giá trị cột {col} không thuộc danh mục hợp lệ",
            output_dir=output_dir,
        )
        return False, invalid_df

    logger.info(f"[QUALITY PASS] Bảng '{table_name}' cột '{col}': 100% giá trị nằm trong danh mục hợp lệ.")
    return True, pd.DataFrame()


def check_range_values(df, col, min_val, max_val, allow_null=True, table_name="DataFrame", output_dir=REJECTED_DIR):
    if df is None or df.empty or col not in df.columns:
        return True, pd.DataFrame()

    num_series = pd.to_numeric(df[col], errors="coerce")
    is_null = df[col].isna()

    if allow_null:
        violation_mask = (~is_null) & (num_series.isna() | (num_series < min_val) | (num_series > max_val))
    else:
        violation_mask = is_null | num_series.isna() | (num_series < min_val) | (num_series > max_val)

    violation_count = violation_mask.sum()
    if violation_count > 0:
        invalid_df = df[violation_mask].copy()
        sample_vals = df.loc[violation_mask, col].head(3).tolist()
        logger.warning(
            f"[QUALITY WARNING] Bảng '{table_name}' cột '{col}': Phát hiện {violation_count:,} dòng "
            f"ngoài miền giá trị [{min_val}, {max_val}]! Mẫu giá trị lỗi: {sample_vals}"
        )
        save_error_records(
            invalid_df,
            table_name,
            f"Điểm {col} nằm ngoài thang điểm [{min_val}, {max_val}]",
            output_dir=output_dir,
        )
        return False, invalid_df

    logger.info(f"[QUALITY PASS] Bảng '{table_name}' cột '{col}': 100% giá trị nằm trong miền [{min_val}, {max_val}].")
    return True, pd.DataFrame()


def check_conditional_not_null(df, condition_col, condition_val, target_col, table_name="DataFrame", output_dir=REJECTED_DIR):
    if df is None or df.empty:
        return True, pd.DataFrame()

    for c in [condition_col, target_col]:
        if c not in df.columns:
            logger.warning(f"[{table_name}] Cột '{c}' không tồn tại trong DataFrame.")
            return True, pd.DataFrame()

    cond_matches = (
        df[condition_col].astype(str).str.strip().str.lower()
        == str(condition_val).strip().lower()
    )
    violation_mask = cond_matches & (df[target_col].isna() | (df[target_col].astype(str).str.strip() == ""))

    violation_count = violation_mask.sum()
    if violation_count > 0:
        invalid_df = df[violation_mask].copy()
        logger.warning(
            f"[QUALITY WARNING] Bảng '{table_name}': Phát hiện {violation_count:,} dòng có "
            f"{condition_col}='{condition_val}' nhưng {target_col} bị NULL!"
        )
        save_error_records(
            invalid_df,
            table_name,
            f"{condition_col}='{condition_val}' nhưng {target_col} bị NULL",
            output_dir=output_dir,
        )
        return False, invalid_df

    logger.info(
        f"[QUALITY PASS] Bảng '{table_name}': Điều kiện {condition_col}='{condition_val}' -> {target_col} NOT NULL đạt 100%."
    )
    return True, pd.DataFrame()


def clean_string_columns(df, columns=None, remove_special_chars=False, table_name="DataFrame"):
    if df is None or df.empty:
        return df

    cleaned_df = df.copy()

    if columns is None:
        columns = cleaned_df.select_dtypes(include=["object", "string"]).columns.tolist()

    cleaned_col_count = 0
    for col in columns:
        if col not in cleaned_df.columns:
            continue

        s = cleaned_df[col]
        if col.startswith("_"):
            continue

        is_not_na = s.notna()
        if is_not_na.any():
            cleaned_s = s.loc[is_not_na].astype(str)
            cleaned_s = cleaned_s.str.strip()
            cleaned_s = cleaned_s.str.replace(r"\s+", " ", regex=True)

            if remove_special_chars:
                cleaned_s = cleaned_s.str.replace(r"[^\w\s\-\.,/]", "", regex=True)

            cleaned_df.loc[is_not_na, col] = cleaned_s

        cleaned_df[col] = cleaned_df[col].replace(r"^\s*$", np.nan, regex=True)
        cleaned_df[col] = cleaned_df[col].replace(["nan", "None", "null", "NULL"], np.nan)
        cleaned_col_count += 1

    logger.info(f"[QUALITY CLEAN] Bảng '{table_name}': Đã làm sạch thành công {cleaned_col_count} cột văn bản.")
    return cleaned_df


VALID_ORDER_STATUSES = [
    "delivered",
    "shipped",
    "canceled",
    "invoiced",
    "processing",
    "unavailable",
    "approved",
    "created",
]

VALID_PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]


def run_table_quality_checks(df, tbl_name, output_dir=REJECTED_DIR, is_first_chunk=True):
    if df is None or df.empty:
        return df

    if is_first_chunk:
        err_file = Path(output_dir) / f"{tbl_name}_errors.csv"
        if err_file.exists():
            err_file.unlink()

    clean_df = clean_string_columns(df, table_name=tbl_name)

    if tbl_name in ["order_items", "olist_order_items_dataset"]:
        check_positive_values(clean_df, columns=["price"], allow_zero=False, table_name=tbl_name, output_dir=output_dir)
        check_positive_values(clean_df, columns=["freight_value"], allow_zero=True, table_name=tbl_name, output_dir=output_dir)

    elif tbl_name in ["order_payments", "olist_order_payments_dataset"]:
        check_positive_values(clean_df, columns=["payment_value"], allow_zero=False, table_name=tbl_name, output_dir=output_dir)
        check_valid_categories(clean_df, col="payment_type", valid_list=VALID_PAYMENT_TYPES, table_name=tbl_name, output_dir=output_dir)

    elif tbl_name in ["order_reviews", "olist_order_reviews_dataset"]:
        check_range_values(clean_df, col="review_score", min_val=1, max_val=5, table_name=tbl_name, output_dir=output_dir)
        check_chronological_dates(
            clean_df,
            start_col="review_creation_date",
            end_col="review_answer_timestamp",
            allow_equal=False,
            table_name=tbl_name,
            output_dir=output_dir,
        )

    elif tbl_name in ["orders", "olist_orders_dataset"]:
        check_valid_categories(
            clean_df,
            col="order_status",
            valid_list=VALID_ORDER_STATUSES,
            table_name=tbl_name,
            output_dir=output_dir,
        )

        delivered_col = (
            "order_delivered_customer_date"
            if "order_delivered_customer_date" in clean_df.columns
            else ("delivered_date" if "delivered_date" in clean_df.columns else None)
        )
        purchase_col = (
            "order_purchase_timestamp"
            if "order_purchase_timestamp" in clean_df.columns
            else ("order_date" if "order_date" in clean_df.columns else None)
        )
        estimated_col = (
            "order_estimated_delivery_date"
            if "order_estimated_delivery_date" in clean_df.columns
            else ("estimated_delivery_date" if "estimated_delivery_date" in clean_df.columns else None)
        )

        if delivered_col:
            check_conditional_not_null(
                clean_df,
                condition_col="order_status",
                condition_val="delivered",
                target_col=delivered_col,
                table_name=tbl_name,
                output_dir=output_dir,
            )
            if purchase_col:
                check_chronological_dates(
                    clean_df,
                    start_col=purchase_col,
                    end_col=delivered_col,
                    allow_equal=True,
                    table_name=tbl_name,
                    output_dir=output_dir,
                )

        if estimated_col and purchase_col:
            check_chronological_dates(
                clean_df,
                start_col=purchase_col,
                end_col=estimated_col,
                allow_equal=True,
                table_name=tbl_name,
                output_dir=output_dir,
            )

    return clean_df


def run_ecommerce_quality_suite(raw_tables, output_dir=REJECTED_DIR):
    logger.info("=== [DATA QUALITY SUITE] BẮT ĐẦU QUY TRÌNH KIỂM ĐỊNH & LÀM SẠCH NGHIỆP VỤ ===")
    cleaned_tables = {}

    for tbl_name, df in raw_tables.items():
        cleaned_tables[tbl_name] = run_table_quality_checks(
            df, tbl_name=tbl_name, output_dir=output_dir, is_first_chunk=True
        )

    logger.info("=== [DATA QUALITY SUITE] HOÀN TẤT KIỂM ĐỊNH & LÀM SẠCH NGHIỆP VỤ 100% ===")
    return cleaned_tables
