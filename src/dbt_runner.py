import os
import re
import subprocess
import sys
import time
from pathlib import Path
from src.config.config import BASE_DIR
from src.logger import get_logger

logger = get_logger("dbt_runner")

DBT_PROJECT_DIR = BASE_DIR / "dbt_transform"


def _format_and_log_dbt_line(line, stage_prefix):
    cleaned = line.strip()
    if not cleaned:
        return

    if any(k in cleaned for k in ["Running with dbt=", "Registered adapter:", "Performance info:", "dbt v2 is available", "New project -", "Existing v1"]):
        return

    start_match = re.search(r"(\d+ of \d+)\s+START\s+(\w+)\s+(table|view|incremental)\s+model\s+([\w\.]+)", cleaned)
    if start_match:
        progress = start_match.group(1)
        model_name = start_match.group(4)
        logger.info(f"[{stage_prefix}] ⏳ Đang xây dựng ({progress}): {model_name}...")
        return

    ok_match = re.search(r"(\d+ of \d+)\s+OK created\s+(\w+)\s+(table|view|incremental)\s+model\s+([\w\.]+)\s+\.*?\s*\[(.*?)\]", cleaned)
    if ok_match:
        progress = ok_match.group(1)
        model_name = ok_match.group(4)
        perf = ok_match.group(5)
        logger.info(f"[{stage_prefix}] ✅ Hoàn tất ({progress}): {model_name} [{perf}]")
        return

    err_match = re.search(r"(\d+ of \d+)?\s*ERROR creating.*?model\s+([\w\.]+)\s+\.*?\s*\[(.*?)\]", cleaned)
    if err_match:
        model_name = err_match.group(2)
        perf = err_match.group(3)
        logger.error(f"[{stage_prefix}] ❌ THẤT BẠI: {model_name} [{perf}]")
        return

    test_pass = re.search(r"(\d+ of \d+)\s+PASS\s+([\w_]+)\s+\.*?\s*\[(.*?)\]", cleaned)
    if test_pass:
        progress = test_pass.group(1)
        test_name = test_pass.group(2)
        perf = test_pass.group(3)
        logger.info(f"[{stage_prefix}] ✅ PASS ({progress}): {test_name} [{perf}]")
        return

    test_fail = re.search(r"(\d+ of \d+)\s+FAIL\s+(\d+)\s+([\w_]+)\s+\.*?\s*\[(.*?)\]", cleaned)
    if test_fail:
        progress = test_fail.group(1)
        fail_count = test_fail.group(2)
        test_name = test_fail.group(3)
        perf = test_fail.group(4)
        logger.error(f"[{stage_prefix}] ❌ FAIL ({progress}): {test_name} - Phát hiện {fail_count} bản ghi vi phạm (Đã lưu vào schema test_failures) [{perf}]")
        return

    test_warn = re.search(r"(\d+ of \d+)\s+WARN\s+(\d+)\s+([\w_]+)\s+\.*?\s*\[(.*?)\]", cleaned)
    if test_warn:
        progress = test_warn.group(1)
        warn_count = test_warn.group(2)
        test_name = test_warn.group(3)
        logger.warning(f"[{stage_prefix}] ⚠️ CẢNH BÁO ({progress}): {test_name} ({warn_count} dòng vi phạm)")
        return

    if "Done. PASS=" in cleaned:
        logger.info(f"[{stage_prefix}] 📋 TỔNG KẾT: {cleaned}")
        return

    if "Completed successfully" in cleaned:
        logger.info(f"[{stage_prefix}] ✨ Thực thi hoàn tất thành công 100%!")
        return

    if any(k in cleaned for k in ["Concurrency:", "Found ", "Finished running"]):
        logger.debug(f"[{stage_prefix}] {cleaned}")
    elif "Database Error" in cleaned or "Compilation Error" in cleaned or "Runtime Error" in cleaned:
        logger.error(f"[{stage_prefix}] {cleaned}")


def run_dbt_command(command_args, stage_prefix, stage_label):
    start_time = time.time()
    logger.info(f"🚀 [{stage_prefix}] Bắt đầu thực thi: {stage_label}...")

    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
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

    for line in iter(process.stdout.readline, ""):
        _format_and_log_dbt_line(line, stage_prefix)

    process.stdout.close()
    return_code = process.wait()
    elapsed = time.time() - start_time

    if return_code != 0:
        logger.error(f"💥 [{stage_prefix}] Thất bại sau {elapsed:.2f}s (Exit code: {return_code})!")
        raise RuntimeError(f"dbt command '{' '.join(command_args)}' thất bại với mã lỗi {return_code}")

    logger.info(f"🎉 [{stage_prefix}] Hoàn tất {stage_label} thành công ({elapsed:.2f}s)!")
    return True


def run_dbt_silver():
    return run_dbt_command(
        ["run", "--select", "staging"],
        stage_prefix="DBT SILVER",
        stage_label="Làm sạch & Khử trùng lặp 7 bảng tầng Silver",
    )


def run_dbt_gold():
    return run_dbt_command(
        ["run", "--select", "marts"],
        stage_prefix="DBT GOLD",
        stage_label="Xây dựng mô hình Unified Star Schema tầng Gold (5 Dims + 2 Facts)",
    )


def run_dbt_test():
    return run_dbt_command(
        ["test"],
        stage_prefix="DBT TEST",
        stage_label="Kiểm định chất lượng toàn diện (100+ Data Tests)",
    )


def run_dbt_all():
    logger.info("================================================================================")
    logger.info("=== BẮT ĐẦU CHUỖI BIẾN ĐỔI DỮ LIỆU & KIỂM ĐỊNH CHẤT LƯỢNG (DBT TRANSFORMATION) ===")
    logger.info("================================================================================")
    run_dbt_silver()
    run_dbt_gold()
    run_dbt_test()
    logger.info("================================================================================")
    logger.info("=== ✅ TOÀN BỘ CÁC TẦNG SILVER VÀ GOLD ĐÃ ĐƯỢC XÂY DỰNG & KIỂM ĐỊNH AN TOÀN! ===")
    logger.info("================================================================================")


if __name__ == "__main__":
    action = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    if action == "silver":
        run_dbt_silver()
    elif action == "gold":
        run_dbt_gold()
    elif action == "test":
        run_dbt_test()
    else:
        run_dbt_all()

