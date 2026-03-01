"""
DAG: Extract — загрузка данных IEEE-CIS в DuckDB
=================================================
Первый реальный DAG нашего пайплайна.

Шаги:
  1. validate_files  — проверяем, что CSV файлы существуют
  2. load_data       — загружаем transaction + identity в DuckDB
  3. show_summary    — выводим статистику в логи

Запуск:
  - Вручную через UI: Trigger DAG ▶
  - Или из командной строки:
    docker-compose exec airflow-apiserver airflow dags trigger extract_raw_data
"""

from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta

# Добавляем /opt/airflow в sys.path, чтобы Python видел папку src/
# Это нужно потому что Airflow 3.x запускает таски в изолированном процессе
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

# Airflow 3.x: новые импорты через airflow.sdk
from airflow.sdk import dag, task


default_args = {
    "owner":       "fraud-pipeline",
    "retries":     1,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="extract_raw_data",
    description="Загрузка сырых CSV данных IEEE-CIS в DuckDB",
    schedule=None,                # Запускаем вручную
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["phase-1", "extract"],
)
def extract_raw_data_dag():

    # --------------------------------------------------------
    # Task 1: Проверка файлов
    # --------------------------------------------------------
    @task
    def validate_files() -> dict:
        """
        Проверяем наличие и размер CSV файлов.
        Если файлов нет — DAG упадёт здесь с понятной ошибкой,
        а не где-то в середине загрузки.
        """
        from pathlib import Path
        from src.config.settings import settings
        from src.utils.logger import logger

        logger.info("Проверяем исходные файлы...")

        files = {
            "transactions": settings.raw_transaction_file,
            "identity":     settings.raw_identity_file,
        }

        file_info = {}
        for name, path in files.items():
            if not Path(path).exists():
                raise FileNotFoundError(
                    f"Файл не найден: {path}\n"
                    f"Убедитесь, что данные лежат в data/raw/"
                )
            size_mb = Path(path).stat().st_size / 1_048_576
            file_info[name] = {"path": str(path), "size_mb": round(size_mb, 1)}
            logger.info(f"  ✓ {name}: {size_mb:.1f} MB")

        return file_info

    # --------------------------------------------------------
    # Task 2: Загрузка данных
    # --------------------------------------------------------
    @task
    def load_data(file_info: dict) -> dict:
        """
        Загружаем CSV в DuckDB.
        Получаем file_info от предыдущей задачи (через XCom).
        """
        from src.extract.csv_loader import run_extract
        from src.utils.logger import logger

        logger.info(f"Начинаем загрузку: {list(file_info.keys())}")
        summary = run_extract()

        return summary

    # --------------------------------------------------------
    # Task 3: Итоговый отчёт
    # --------------------------------------------------------
    @task
    def show_summary(summary: dict) -> None:
        """
        Красиво выводим итоги в лог.
        Этот лог виден в Airflow UI → Task Logs.
        """
        from src.utils.logger import logger

        logger.info("\n" + "=" * 55)
        logger.info("  ИТОГИ ЗАГРУЗКИ")
        logger.info("=" * 55)
        logger.info(f"  Транзакций загружено : {summary['transactions_rows']:>10,}")
        logger.info(f"  Identity загружено   : {summary['identity_rows']:>10,}")
        logger.info(f"  Мошеннических        : {summary['fraud_count']:>10,}")
        logger.info(f"  Доля фрода           : {summary['fraud_rate_pct']:>9.2f}%")
        logger.info(f"  DuckDB файл          : {summary['duckdb_path']}")
        logger.info("=" * 55)
        logger.info("  Следующий шаг: transform_data DAG")
        logger.info("=" * 55)

    # --------------------------------------------------------
    # Порядок выполнения
    # --------------------------------------------------------
    file_info = validate_files()
    summary   = load_data(file_info)
    show_summary(summary)


extract_raw_data_dag()