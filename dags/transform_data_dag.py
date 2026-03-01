"""
DAG: Transform — трансформация данных IEEE-CIS
===============================================
Второй реальный DAG пайплайна.

Предусловие: extract_raw_data DAG должен быть выполнен успешно
(данные должны быть в DuckDB).

Шаги:
  1. check_source   — проверяем что данные из Extract шага есть
  2. run_transform  — merge + clean + features + save to parquet
  3. show_summary   — статистика трансформации
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

from airflow.sdk import dag, task


default_args = {
    "owner":       "fraud-pipeline",
    "retries":     1,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="transform_data",
    description="Трансформация: merge + clean + feature engineering → Parquet",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["phase-1", "transform"],
)
def transform_data_dag():

    # --------------------------------------------------------
    # Task 1: Проверяем что Extract уже был выполнен
    # --------------------------------------------------------
    @task
    def check_source() -> dict:
        """
        Проверяем наличие DuckDB файла и таблиц из Extract шага.
        Если Extract не запускался — сразу говорим об этом.
        """
        import duckdb
        from src.config.settings import settings
        from src.utils.logger import logger

        if not settings.duckdb_path.exists():
            raise FileNotFoundError(
                f"DuckDB файл не найден: {settings.duckdb_path}\n"
                f"Сначала запустите DAG: extract_raw_data"
            )

        con = duckdb.connect(str(settings.duckdb_path))
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]

            required = [
                settings.raw_transactions_table,
                settings.raw_identity_table,
            ]
            for table in required:
                if table not in table_names:
                    raise ValueError(
                        f"Таблица '{table}' не найдена в DuckDB.\n"
                        f"Сначала запустите DAG: extract_raw_data"
                    )

            tx_count = con.execute(
                f"SELECT COUNT(*) FROM {settings.raw_transactions_table}"
            ).fetchone()[0]

            logger.info(f"  ✓ DuckDB найден      : {settings.duckdb_path}")
            logger.info(f"  ✓ Таблицы найдены    : {required}")
            logger.info(f"  ✓ Транзакций в БД    : {tx_count:,}")

            return {"tx_count": tx_count, "tables": table_names}

        finally:
            con.close()

    # --------------------------------------------------------
    # Task 2: Запускаем трансформацию
    # --------------------------------------------------------
    @task
    def run_transform(source_info: dict) -> dict:
        """
        Выполняем полный цикл трансформации:
        merge → clean → feature engineering → save parquet
        """
        from src.transform.transformer import run_transform as transform
        from src.utils.logger import logger

        logger.info(f"Трансформируем {source_info['tx_count']:,} транзакций...")
        summary = transform()

        return summary

    # --------------------------------------------------------
    # Task 3: Итоговый отчёт
    # --------------------------------------------------------
    @task
    def show_summary(summary: dict) -> None:
        """Выводим итоги трансформации."""
        from src.utils.logger import logger

        logger.info("\n" + "=" * 55)
        logger.info("  ИТОГИ ТРАНСФОРМАЦИИ")
        logger.info("=" * 55)
        logger.info(f"  После merge          : {summary['merged_rows']:>10,}")
        logger.info(f"  После cleaning       : {summary['cleaned_rows']:>10,}")
        logger.info(f"  С новыми фичами      : {summary['feature_rows']:>10,}")
        logger.info(f"  Parquet файл         : {summary['parquet_path']}")
        logger.info("=" * 55)
        logger.info("  Следующий шаг: load_to_bigquery DAG")
        logger.info("=" * 55)

    # --------------------------------------------------------
    # Порядок выполнения
    # --------------------------------------------------------
    source_info = check_source()
    summary     = run_transform(source_info)
    show_summary(summary)


transform_data_dag()