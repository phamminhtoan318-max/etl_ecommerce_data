import os
import re
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from src.config.settings import BASE_DIR
from src.core.alert import send_alert_webhook
from src.core.logger import get_logger

logger = get_logger("dbt_runner")

DBT_PROJECT_DIR = BASE_DIR / "dbt_transform"
DBT_COMMAND_TIMEOUT_SEC = int(os.getenv("DBT_COMMAND_TIMEOUT_SEC", "600"))
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
TIMESTAMP_PREFIX_RE = re.compile(r"^\d{2}:\d{2}:\d{2}\s+")


def strip_ansi(text):
    if not text:
        return ""
    return ANSI_ESCAPE_RE.sub("", text)


def _format_and_log_dbt_line(line, stage_prefix, error_details=None):
    cleaned = strip_ansi(line).strip()
    if not cleaned:
        return

    cleaned_no_ts = TIMESTAMP_PREFIX_RE.sub("", cleaned).strip()

    if any(
        k in cleaned_no_ts
        for k in [
            "Running with dbt=",
            "Registered adapter:",
            "Performance info:",
            "dbt v2 is available",
            "New project -",
            "Existing v1",
        ]
    ):
        return

    start_match = re.search(
        r"(\d+ of \d+)\s+START\s+(?:sql\s+)?(?:table|view|incremental|snapshot)(?:\s+model)?\s+([\w\.]+)",
        cleaned_no_ts,
    )
    if start_match:
        progress = start_match.group(1)
        model_name = start_match.group(2)
        logger.info(f"[{stage_prefix}] ⏳ Đang xây dựng ({progress}): {model_name}...")
        return

    ok_match = re.search(
        r"(\d+ of \d+)\s+OK\s+(?:created\s+(?:sql\s+)?(?:table|view|incremental|snapshot)\s+model|snapshotted)\s+([\w\.]+)\s+\.*?\s*\[(.*?)\]",
        cleaned_no_ts,
    )
    if ok_match:
        progress = ok_match.group(1)
        model_name = ok_match.group(2)
        perf = ok_match.group(3).strip()
        logger.info(f"[{stage_prefix}] ✅ Hoàn tất ({progress}): {model_name} [{perf}]")
        return

    err_match = re.search(
        r"(\d+ of \d+)?\s*ERROR\s+(?:creating.*?model|snapshotting)\s+([\w\.]+)\s+\.*?\s*\[(.*?)\]",
        cleaned_no_ts,
    )
    if err_match:
        model_name = err_match.group(2)
        perf = err_match.group(3).strip() if len(err_match.groups()) >= 3 and err_match.group(3) else "ERROR"
        msg = f"[{stage_prefix}] ❌ THẤT BẠI: {model_name} [{perf}]"
        logger.error(msg)
        if error_details is not None:
            error_details.append(msg)
        return

    test_pass = re.search(r"(\d+ of \d+)\s+PASS\s+([\w_]+)\s+\.*?\s*\[(.*?)\]", cleaned_no_ts)
    if test_pass:
        progress = test_pass.group(1)
        test_name = test_pass.group(2)
        perf = test_pass.group(3).strip()
        logger.info(f"[{stage_prefix}] ✅ PASS ({progress}): {test_name} [{perf}]")
        return

    test_fail = re.search(r"(\d+ of \d+)\s+FAIL\s+(\d+)\s+([\w_]+)\s+\.*?\s*\[(.*?)\]", cleaned_no_ts)
    if test_fail:
        progress = test_fail.group(1)
        fail_count = test_fail.group(2)
        test_name = test_fail.group(3)
        perf = test_fail.group(4).strip()
        msg = (
            f"[{stage_prefix}] ❌ FAIL ({progress}): {test_name} - "
            f"Phát hiện {fail_count} bản ghi vi phạm (Đã lưu vào schema test_failures) [{perf}]"
        )
        logger.error(msg)
        if error_details is not None:
            error_details.append(msg)
        return

    test_warn = re.search(r"(\d+ of \d+)\s+WARN\s+(\d+)\s+([\w_]+)\s+\.*?\s*\[(.*?)\]", cleaned_no_ts)
    if test_warn:
        progress = test_warn.group(1)
        warn_count = test_warn.group(2)
        test_name = test_warn.group(3)
        logger.warning(f"[{stage_prefix}] ⚠️ CẢNH BÁO ({progress}): {test_name} ({warn_count} dòng vi phạm)")
        return

    if "Done. PASS=" in cleaned_no_ts:
        logger.info(f"[{stage_prefix}] 📋 TỔNG KẾT: {cleaned_no_ts}")
        return

    if "Completed successfully" in cleaned_no_ts:
        logger.info(f"[{stage_prefix}] ✨ Thực thi hoàn tất thành công 100%!")
        return

    if any(k in cleaned_no_ts for k in ["Concurrency:", "Found ", "Finished running"]):
        logger.debug(f"[{stage_prefix}] {cleaned_no_ts}")
        return

    if any(
        k in cleaned_no_ts
        for k in [
            "Database Error",
            "Compilation Error",
            "Runtime Error",
            "Failure in ",
            "LINE ",
            "DETAIL:",
            "HINT:",
            "compiled code at",
            "See test failures:",
        ]
    ):
        logger.error(f"[{stage_prefix}] {cleaned_no_ts}")
        if error_details is not None:
            error_details.append(cleaned_no_ts)


def run_dbt_command(command_args, stage_prefix, stage_label, timeout_sec=DBT_COMMAND_TIMEOUT_SEC):
    start_time = time.time()
    logger.info(f"🚀 [{stage_prefix}] Bắt đầu thực thi: {stage_label}...")

    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "--no-use-colors",
        "--log-path",
        "../logs",
        "--log-level-file",
        "none",
    ] + command_args

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    process = subprocess.Popen(
        cmd,
        cwd=str(DBT_PROJECT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    error_details = []
    recent_lines = deque(maxlen=30)

    try:
        for line in iter(process.stdout.readline, ""):
            cleaned_line = strip_ansi(line).strip()
            if cleaned_line:
                recent_lines.append(TIMESTAMP_PREFIX_RE.sub("", cleaned_line).strip())
            _format_and_log_dbt_line(line, stage_prefix, error_details=error_details)
            if timeout_sec and (time.time() - start_time) > timeout_sec:
                process.kill()
                raise TimeoutError(
                    f"[{stage_prefix}] Quá thời gian thực thi cho phép ({timeout_sec}s) -> Đã hủy tiến trình dbt để tránh treo lock!"
                )
    finally:
        if process.stdout:
            process.stdout.close()

    return_code = process.wait(timeout=30)
    elapsed = time.time() - start_time

    if return_code != 0:
        if not error_details and recent_lines:
            error_details = list(recent_lines)[-10:]
            for tail_line in error_details:
                logger.error(f"[{stage_prefix}] [CHI TIẾT LỖI DBT] {tail_line}")

        detail_summary = "\n".join(f"  • {d}" for d in error_details[:10]) if error_details else "Không có chi tiết bổ sung."
        logger.error(f"💥 [{stage_prefix}] Thất bại sau {elapsed:.2f}s (Exit code: {return_code})!")
        raise RuntimeError(
            f"Bước '{stage_prefix}' (dbt {' '.join(command_args)}) thất bại sau {elapsed:.2f}s (mã lỗi {return_code}):\n{detail_summary}"
        )

    logger.info(f"🎉 [{stage_prefix}] Hoàn tất {stage_label} thành công ({elapsed:.2f}s)!")
    return True


def run_dbt_silver(full_refresh=False):
    args = ["run", "--select", "staging"]
    if full_refresh:
        args.append("--full-refresh")
    return run_dbt_command(
        args,
        stage_prefix="DBT SILVER",
        stage_label="Làm sạch & Khử trùng lặp 7 bảng tầng Silver",
    )


def run_dbt_test_silver():
    return run_dbt_command(
        ["test", "--select", "staging", "source:*"],
        stage_prefix="DBT SILVER GATE",
        stage_label="Chốt chặn kiểm định chất lượng tầng Silver & Source trước khi đẩy lên Gold",
    )


def run_dbt_snapshot():
    return run_dbt_command(
        ["snapshot"],
        stage_prefix="DBT SNAPSHOT",
        stage_label="Theo dõi lịch sử thay đổi SCD Type 2",
    )


def run_dbt_gold(full_refresh=False):
    args = ["run", "--select", "marts"]
    if full_refresh:
        args.append("--full-refresh")
    return run_dbt_command(
        args,
        stage_prefix="DBT GOLD",
        stage_label="Xây dựng mô hình Unified Star Schema tầng Gold (5 Dims + 2 Facts)",
    )


def run_dbt_test_gold():
    return run_dbt_command(
        ["test", "--exclude", "staging", "source:*"],
        stage_prefix="DBT GOLD GATE",
        stage_label="Chốt chặn kiểm định khóa ngoại & đối soát nghiệp vụ tầng Gold",
    )


def run_dbt_test():
    return run_dbt_command(
        ["test"],
        stage_prefix="DBT TEST",
        stage_label="Kiểm định chất lượng toàn diện (100+ Data Tests)",
    )


def run_dbt_all(full_refresh=False):
    logger.info("================================================================================")
    logger.info("=== BẮT ĐẦU CHUỖI BIẾN ĐỔI DỮ LIỆU & KIỂM ĐỊNH CHẤT LƯỢNG (DBT TRANSFORMATION) ===")
    logger.info("================================================================================")
    run_dbt_silver(full_refresh=full_refresh)
    run_dbt_test_silver()
    run_dbt_snapshot()
    run_dbt_gold(full_refresh=full_refresh)
    run_dbt_test_gold()
    logger.info("================================================================================")
    logger.info("=== ✅ TOÀN BỘ CÁC TẦNG SILVER VÀ GOLD ĐÃ ĐƯỢC XÂY DỰNG & KIỂM ĐỊNH AN TOÀN! ===")
    logger.info("================================================================================")


if __name__ == "__main__":
    cli_args = [a for a in sys.argv[1:] if not a.startswith("--")]
    full_refresh_flag = "--full-refresh" in sys.argv
    action = cli_args[0].lower() if cli_args else "all"

    t0 = time.time()
    try:
        if action == "snapshot":
            run_dbt_snapshot()
        elif action == "silver":
            run_dbt_silver(full_refresh=full_refresh_flag)
        elif action == "test-silver":
            run_dbt_test_silver()
        elif action == "gold":
            run_dbt_gold(full_refresh=full_refresh_flag)
        elif action == "test-gold":
            run_dbt_test_gold()
        elif action == "test":
            run_dbt_test()
        else:
            run_dbt_all(full_refresh=full_refresh_flag)
    except Exception as exc:
        elapsed = time.time() - t0
        err_msg = f"💥 [DBT TRANSFORMATION FAILED] Tiến trình biến đổi dbt ('{action}') thất bại sau {elapsed:.2f}s:\n{exc}"
        logger.error(err_msg)
        send_alert_webhook(err_msg)
        sys.exit(1)

