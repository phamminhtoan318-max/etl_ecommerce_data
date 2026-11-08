#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"

# Nạp biến từ .env (bỏ CRLF của Windows). Không ghi đè biến đã có sẵn
# trong môi trường (vd: docker-compose set TGT_DB_HOST=postgres_dwh).
if [ -f ".env" ]; then
    while IFS='=' read -r key value || [ -n "$key" ]; do
        key="${key%$'\r'}"
        value="${value%$'\r'}"
        case "$key" in ''|\#*) continue ;; esac
        if [ -z "${!key+x}" ]; then
            export "$key=$value"
        fi
    done < .env
fi

export PIPELINE_LOG_PATH="../logs"

if [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON="$PWD/.venv/Scripts/python.exe"
    DBT="$PWD/.venv/Scripts/dbt.exe"
elif [ -f ".venv/bin/python" ]; then
    PYTHON="$PWD/.venv/bin/python"
    DBT="$PWD/.venv/bin/dbt"
else
    PYTHON="python"
    DBT="dbt"
fi

extract_load() {
    "$PYTHON" main.py
}

resume_load() {
    "$PYTHON" main.py --resume
}

transform_silver() {
    "$PYTHON" -m src.dbt_runner silver
}

transform_snapshot() {
    "$PYTHON" -m src.dbt_runner snapshot
}

transform_gold() {
    "$PYTHON" -m src.dbt_runner gold
}

test_models() {
    "$PYTHON" -m src.dbt_runner test
}

transform() {
    "$PYTHON" -m src.dbt_runner all
}

unit_test() {
    "$PYTHON" -m unittest discover -s tests -v
}

pipeline() {
    extract_load
    transform
    echo "Toan bo Pipeline da thuc thi hoan tat!"
}

clean() {
    (cd dbt_transform && "$DBT" clean)
}

docker_up() {
    docker compose --env-file .env -f Docker/docker-compose.yml up --build
}

show_help() {
    echo "Cac lenh kha dung:"
    echo "  ./run.sh extract-load - Trich xuat du lieu tu nguon va nap vao Bronze"
    echo "  ./run.sh resume       - Tiep tuc nap Bronze tu moc Checkpoint neu bi gian doan"
    echo "  ./run.sh transform    - Chay dbt Silver, Snapshot, Gold va Test voi log dong bo"
    echo "  ./run.sh snapshot     - Chay dbt snapshot theo doi lich su SCD Type 2"
    echo "  ./run.sh test         - Chay dbt test kiem tra chat luong du lieu (luu loi vao DB)"
    echo "  ./run.sh unit-test    - Chay Unit Tests kiem thu logic Python"
    echo "  ./run.sh pipeline     - Chay toan bo pipeline tu E&L den Transform & Test"
    echo "  ./run.sh clean        - Don dep thu muc build cua dbt"
    echo "  ./run.sh docker       - Build va chay Postgres + pipeline bang Docker Compose"
}

case "$1" in
    extract-load)
        extract_load
        ;;
    resume)
        resume_load
        ;;
    transform)
        transform
        ;;
    transform-silver)
        transform_silver
        ;;
    snapshot)
        transform_snapshot
        ;;
    transform-gold)
        transform_gold
        ;;
    test)
        test_models
        ;;
    unit-test)
        unit_test
        ;;
    pipeline|pineline)
        pipeline
        ;;
    clean)
        clean
        ;;
    docker)
        docker_up
        ;;
    *)
        show_help
        ;;
esac
