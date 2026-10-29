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

export DBT_LOG_PATH="../logs"

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

transform() {
    (cd dbt_transform && "$DBT" run)
}

test_models() {
    (cd dbt_transform && "$DBT" test)
}

pipeline() {
    extract_load
    transform
    test_models
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
    echo "  ./run.sh extract-load - Trich xuat du lieu tu nguon va nap vao Staging"
    echo "  ./run.sh transform    - Chay dbt run chuyen doi mo hinh Marts"
    echo "  ./run.sh test         - Chay dbt test kiem tra chat luong du lieu (luu loi vao DB)"
    echo "  ./run.sh pipeline     - Chay toan bo pipeline tu E&L den Transform & Test"
    echo "  ./run.sh clean        - Don dep thu muc build cua dbt"
    echo "  ./run.sh docker       - Build va chay Postgres + pipeline bang Docker Compose"
}

case "$1" in
    extract-load)
        extract_load
        ;;
    transform)
        transform
        ;;
    test)
        test_models
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
