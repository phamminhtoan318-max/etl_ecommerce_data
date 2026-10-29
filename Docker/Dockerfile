FROM python:3.11-slim

# 1. Cài đặt các thư viện hệ thống và Microsoft ODBC Driver 18 cho SQL Server trên Linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Cài đặt Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy toàn bộ mã nguồn
COPY . .

# 4. Cấu hình môi trường mặc định
ENV PYTHONUNBUFFERED=1 \
    SRC_DB_TRUSTED_CONNECTION=no

# 5. Lệnh chạy toàn bộ pipeline (Extract-Load -> dbt run -> dbt test)
CMD ["bash", "run.sh", "pipeline"]
