"""
Extract: Загрузка CSV данных в DuckDB
======================================
Этот модуль отвечает за первый шаг ETL — Extract.

Что делает:
1. Проверяет наличие исходных CSV файлов
2. Загружает train_transaction.csv и train_identity.csv в DuckDB
3. Выводит базовую статистику по загруженным данным
4. Возвращает метаданные для следующего шага пайплайна

Почему DuckDB, а не Pandas:
- Читает CSV напрямую без загрузки в память Python
- SQL-интерфейс (наша сильная сторона)
- В 5-10x быстрее Pandas для аналитических запросов
- Легко переключиться на Parquet позже
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from src.config.settings import settings
from src.utils.logger import logger


# ============================================================
# Вспомогательные функции
# ============================================================

def _ensure_directories() -> None:
    """Создаёт нужные директории, если их нет."""
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug(f"DuckDB directory ready: {settings.duckdb_path.parent}")


def _check_source_files() -> None:
    """
    Проверяет, что исходные CSV файлы существуют.
    Если нет — сразу сообщаем об ошибке (fail fast).
    """
    files_to_check = {
        "transactions": settings.raw_transaction_file,
        "identity":     settings.raw_identity_file,
    }

    for name, path in files_to_check.items():
        if not path.exists():
            raise FileNotFoundError(
                f"Файл '{name}' не найден: {path}\n"
                f"Убедитесь, что данные лежат в {settings.raw_transaction_file.parent}"
            )
        logger.info(f"✓ Файл найден: {path.name} ({path.stat().st_size / 1_048_576:.1f} MB)")


def _get_csv_info(con: duckdb.DuckDBPyConnection, file_path: Path) -> dict:
    """
    Получает базовую информацию о CSV файле без полной загрузки.
    DuckDB умеет читать метаданные быстро.
    """
    result = con.execute(f"""
        SELECT COUNT(*) AS row_count
        FROM read_csv_auto('{file_path}', ignore_errors=true)
    """).fetchone()

    return {"row_count": result[0], "file": file_path.name}


# ============================================================
# Основные функции загрузки
# ============================================================

def load_transactions(con: duckdb.DuckDBPyConnection) -> int:
    """
    Загружает train_transaction.csv в таблицу raw_transactions.

    DuckDB читает CSV напрямую через read_csv_auto —
    автоматически определяет типы колонок.

    Returns:
        Количество загруженных строк.
    """
    table  = settings.raw_transactions_table
    source = settings.raw_transaction_file

    logger.info(f"Загружаем транзакции: {source.name} → таблица '{table}'")

    # Если таблица уже есть — пересоздаём (для идемпотентности)
    con.execute(f"DROP TABLE IF EXISTS {table}")

    # LIMIT применяем только если задан sample_size
    limit_clause = ""
    if settings.sample_size:
        limit_clause = f"LIMIT {settings.sample_size}"
        logger.info(f"  Режим разработки: загружаем {settings.sample_size:,} строк")

    con.execute(f"""
        CREATE TABLE {table} AS
        SELECT *
        FROM read_csv_auto(
            '{source}',
            ignore_errors = true,
            header        = true
        )
        {limit_clause}
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info(f"  ✓ Загружено строк: {row_count:,}")

    return row_count


def load_identity(con: duckdb.DuckDBPyConnection) -> int:
    """
    Загружает train_identity.csv в таблицу raw_identity.

    Returns:
        Количество загруженных строк.
    """
    table  = settings.raw_identity_table
    source = settings.raw_identity_file

    logger.info(f"Загружаем идентификаторы: {source.name} → таблица '{table}'")

    con.execute(f"DROP TABLE IF EXISTS {table}")

    limit_clause = ""
    if settings.sample_size:
        limit_clause = f"LIMIT {settings.sample_size}"

    con.execute(f"""
        CREATE TABLE {table} AS
        SELECT *
        FROM read_csv_auto(
            '{source}',
            ignore_errors = true,
            header        = true
        )
        {limit_clause}
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info(f"  ✓ Загружено строк: {row_count:,}")

    return row_count


def print_data_summary(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Выводит краткую статистику по загруженным данным.
    Полезно для быстрой проверки что данные выглядят правильно.

    Returns:
        Словарь с метаданными для передачи в следующий шаг.
    """
    logger.info("📊 Статистика загруженных данных:")

    # --- Транзакции ---
    tx_stats = con.execute(f"""
        SELECT
            COUNT(*)                                  AS total_rows,
            COUNT(DISTINCT TransactionID)             AS unique_tx,
            ROUND(MIN(TransactionAmt), 2)             AS min_amount,
            ROUND(MAX(TransactionAmt), 2)             AS max_amount,
            ROUND(AVG(TransactionAmt), 2)             AS avg_amount,
            SUM(isFraud)                              AS fraud_count,
            ROUND(AVG(isFraud) * 100, 2)             AS fraud_rate_pct,
            COUNT(*) - COUNT(TransactionAmt)          AS missing_amounts
        FROM {settings.raw_transactions_table}
    """).fetchone()

    logger.info(
        f"\n  Транзакции ({settings.raw_transactions_table}):\n"
        f"    Всего строк      : {tx_stats[0]:>10,}\n"
        f"    Уникальных TX    : {tx_stats[1]:>10,}\n"
        f"    Сумма (min/max)  : ${tx_stats[2]:>9.2f} / ${tx_stats[3]:,.2f}\n"
        f"    Средняя сумма    : ${tx_stats[4]:>9.2f}\n"
        f"    Мошеннических    : {tx_stats[5]:>10,}\n"
        f"    Доля фрода       : {tx_stats[6]:>9.2f}%\n"
        f"    Пропуски (Amount): {tx_stats[7]:>10,}"
    )

    # --- Identity ---
    id_stats = con.execute(f"""
        SELECT
            COUNT(*)                      AS total_rows,
            COUNT(DISTINCT TransactionID) AS unique_tx
        FROM {settings.raw_identity_table}
    """).fetchone()

    logger.info(
        f"\n  Идентификаторы ({settings.raw_identity_table}):\n"
        f"    Всего строк      : {id_stats[0]:>10,}\n"
        f"    Уникальных TX    : {id_stats[1]:>10,}"
    )

    # Сводка для передачи в следующий шаг через XCom
    return {
        "transactions_rows": tx_stats[0],
        "fraud_count":       int(tx_stats[5]),
        "fraud_rate_pct":    float(tx_stats[6]),
        "identity_rows":     id_stats[0],
        "duckdb_path":       str(settings.duckdb_path),
    }


# ============================================================
# Главная функция — вызывается из Airflow DAG
# ============================================================

def run_extract() -> dict:
    """
    Точка входа для Airflow task.
    Выполняет полный цикл извлечения данных.

    Returns:
        Метаданные о загруженных данных (для XCom).
    """
    logger.info("=" * 55)
    logger.info("  EXTRACT: Начинаем загрузку данных")
    logger.info("=" * 55)

    # 1. Подготовка
    _ensure_directories()
    _check_source_files()

    # 2. Подключаемся к DuckDB (создаёт файл если его нет)
    logger.info(f"Подключаемся к DuckDB: {settings.duckdb_path}")
    con = duckdb.connect(str(settings.duckdb_path))

    try:
        # 3. Загружаем данные
        tx_rows = load_transactions(con)
        id_rows = load_identity(con)

        # 4. Статистика
        summary = print_data_summary(con)

        logger.info("=" * 55)
        logger.info("  EXTRACT: Завершено успешно ✓")
        logger.info(f"  Транзакций: {tx_rows:,} | Identity: {id_rows:,}")
        logger.info("=" * 55)

        return summary

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise

    finally:
        # Всегда закрываем соединение
        con.close()


# ============================================================
# Запуск напрямую (для тестирования без Airflow)
# ============================================================

if __name__ == "__main__":
    result = run_extract()
    print("\nРезультат:", result)