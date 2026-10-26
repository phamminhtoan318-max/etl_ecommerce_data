#!/usr/bin/env bash

set -e

if [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON=".venv/Scripts/python.exe"
    DBT="../.venv/Scripts/dbt.exe"
else
    PYTHON=".venv/bin/python"
    DBT="../.venv/bin/dbt"
fi

extract_load() {
    $PYTHON main.py
}

transform() {
    (cd dbt_transform && $DBT run)
}

test_models() {
    (cd dbt_transform && $DBT test)
}

pipeline() {
    extract_load
    transform
    test_models
    echo "Toan bo Pipeline da thuc thi hoan tat!"
}

clean() {
    (cd dbt_transform && $DBT clean)
}

show_help() {
    echo "Cac lenh kha dung:"
    echo "  ./run.sh extract-load - Trich xuat du lieu tu nguon va nap vao Staging"
    echo "  ./run.sh transform    - Chay dbt run chuyen doi mo hinh Marts"
    echo "  ./run.sh test         - Chay dbt test kiem tra chat luong du lieu (luu loi vao DB)"
    echo "  ./run.sh pipeline     - Chay toan bo pipeline tu E&L den Transform & Test"
    echo "  ./run.sh clean        - Don dep thu muc build cua dbt"
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
    *)
        show_help
        ;;
esac
