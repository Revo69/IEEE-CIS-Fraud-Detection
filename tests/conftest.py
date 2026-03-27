# conftest.py
# ===========
# Общие fixtures для всех тестов.
#
# Совместимость:
# - Локально в Docker: /opt/airflow/src/...
# - GitHub Actions:    ./src/... (PYTHONPATH=.)
#
# Решение: добавляем оба пути — один из них всегда сработает.

import sys
import os
import pytest
import duckdb

# Добавляем корень проекта в sys.path
# Это позволяет импортировать src.* в любом окружении:
# - Docker контейнер: /opt/airflow уже в PATH через docker-compose volume
# - GitHub Actions: PYTHONPATH=. задан в workflow, но на всякий случай добавляем явно
# - Локальный запуск: текущая директория

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Для Docker контейнера
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")


# ============================================================
# Fixture: соединение с DuckDB в памяти
# ============================================================

@pytest.fixture
def duckdb_con():
    """
    Создаёт чистое DuckDB соединение в памяти для каждого теста.

    ':memory:' = база живёт только в RAM.
    Каждый тест начинает с чистого листа — изоляция гарантирована.
    """
    con = duckdb.connect(":memory:")
    yield con
    con.close()


# ============================================================
# Fixture: тестовые транзакции
# ============================================================

@pytest.fixture
def sample_transactions(duckdb_con):
    """
    Минимальная тестовая таблица транзакций.
    Имитирует структуру IEEE-CIS датасета.
    """
    duckdb_con.execute("""
        CREATE TABLE raw_transactions AS
        SELECT * FROM (VALUES
            (1, 0, 86400,   100.0,  'W', 1001, 'visa',       'credit', 100, 'gmail.com'),
            (2, 1, 90000,   999.0,  'H', 1001, 'visa',       'credit', 100, 'yahoo.com'),
            (3, 0, 7200,     50.0,  'W', 1002, 'mastercard', 'debit',  200, 'gmail.com'),
            (4, 1, 3600,   2500.0,  'C', 1003, 'visa',       'credit', NULL, NULL),
            (5, 0, 172800,   15.99, 'W', 1002, 'mastercard', 'debit',  200, 'gmail.com'),
            (6, 0, 43200,   200.0,  'H', 1004, 'amex',       'credit', 300, 'hotmail.com')
        ) AS t(
            TransactionID, isFraud, TransactionDT, TransactionAmt,
            ProductCD, card1, card4, card6, addr1, P_emaildomain
        )
    """)

    # Дополнительные колонки которые ожидает transformer
    duckdb_con.execute("""
        ALTER TABLE raw_transactions ADD COLUMN card2 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN card3 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN card5 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN addr2 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN dist1 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN dist2 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN R_emaildomain VARCHAR DEFAULT 'unknown';
        ALTER TABLE raw_transactions ADD COLUMN C1  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C2  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C3  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C4  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C5  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C6  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C7  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C8  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C9  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C10 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C11 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C12 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C13 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN C14 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D1  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D2  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D3  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D4  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D5  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D6  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D7  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D8  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D9  DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D10 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D11 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D12 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D13 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D14 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN D15 DOUBLE DEFAULT -999;
        ALTER TABLE raw_transactions ADD COLUMN M1 BOOLEAN DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M2 BOOLEAN DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M3 BOOLEAN DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M4 VARCHAR DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M5 BOOLEAN DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M6 BOOLEAN DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M7 BOOLEAN DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M8 BOOLEAN DEFAULT NULL;
        ALTER TABLE raw_transactions ADD COLUMN M9 BOOLEAN DEFAULT NULL;
    """)

    return duckdb_con


@pytest.fixture
def sample_identity(sample_transactions):
    """Добавляет тестовую таблицу identity."""
    sample_transactions.execute("""
        CREATE TABLE raw_identity AS
        SELECT * FROM (VALUES
            (1, 'desktop', 'Windows'),
            (2, 'mobile',  'iOS'),
            (3, 'desktop', 'MacOS')
        ) AS t(TransactionID, DeviceType, DeviceInfo)
    """)
    return sample_transactions