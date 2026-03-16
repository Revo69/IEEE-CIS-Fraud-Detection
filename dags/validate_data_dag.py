"""
DAG: Validate — проверка качества данных
=========================================
Запускается после transform_data DAG.

Шаги:
  1. check_staging  — проверяем что staging таблица существует
  2. run_validation — запускаем Great Expectations проверки
  3. show_summary   — итоговый отчёт

Место в пайплайне:
  extract → transform → [validate] → load
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

from airflow.sdk import dag, task


default_args = {
    "owner":       "fraud-pipeline",
    "retries":     0,       # валидацию не ретраим — данные не изменятся
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="validate_data",
    description="Great Expectations валидация staging_fraud данных",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["phase-2", "validate", "great-expectations"],
)
def validate_data_dag():

    # --------------------------------------------------------
    # Task 1: Проверяем что staging таблица существует
    # --------------------------------------------------------
    @task
    def check_staging() -> dict:
        """Проверяем наличие данных перед валидацией."""
        import duckdb
        from src.config.settings import settings
        from src.utils.logger import logger

        if not settings.duckdb_path.exists():
            raise FileNotFoundError(
                f"DuckDB не найден: {settings.duckdb_path}\n"
                f"Сначала запустите: extract_raw_data → transform_data"
            )

        con = duckdb.connect(str(settings.duckdb_path), read_only=True)
        try:
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {settings.staging_table}"
            ).fetchone()[0]

            logger.info(f"  ✓ Таблица найдена: {settings.staging_table}")
            logger.info(f"  ✓ Строк в таблице: {row_count:,}")

            return {"row_count": row_count}
        finally:
            con.close()

    # --------------------------------------------------------
    # Task 2: Запуск Great Expectations
    # --------------------------------------------------------
    @task
    def run_validation(staging_info: dict) -> dict:
        """
        Запускаем все Expectations из staging_fraud_suite.
        Если проверки упали — таск завершится с ошибкой.
        """
        from src.validate.ge_validator import run_validation as do_validate
        from src.utils.logger import logger

        logger.info(
            f"Валидируем {staging_info['row_count']:,} строк "
            f"через Great Expectations..."
        )
        summary = do_validate()

        return summary

    # --------------------------------------------------------
    # Task 3: Итоговый отчёт
    # --------------------------------------------------------
    @task
    def show_summary(summary: dict) -> None:
        """Выводим итоги валидации."""
        from src.utils.logger import logger

        status = "✅ PASSED" if summary["validation_success"] else "❌ FAILED"

        logger.info("\n" + "=" * 55)
        logger.info(f"  ИТОГИ ВАЛИДАЦИИ: {status}")
        logger.info("=" * 55)
        logger.info(f"  Всего проверок  : {summary['total_expectations']}")
        logger.info(f"  Прошло          : {summary['successful_expectations']}")
        logger.info(f"  Упало           : {summary['failed_expectations']}")
        logger.info(f"  Успешность      : {summary['success_percent']:.1f}%")
        logger.info("=" * 55)

    # --------------------------------------------------------
    # Порядок выполнения
    # --------------------------------------------------------
    staging_info = check_staging()
    summary      = run_validation(staging_info)
    show_summary(summary)


validate_data_dag()