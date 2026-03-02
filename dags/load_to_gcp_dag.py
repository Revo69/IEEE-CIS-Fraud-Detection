"""
DAG: Load — загрузка данных в GCS и BigQuery
=============================================
Третий реальный DAG пайплайна.

Предусловие: transform_data DAG должен быть выполнен успешно
(Parquet файл должен лежать в data/processed/parquet/).

Шаги:
  1. check_parquet   — проверяем что Parquet файл существует
  2. upload_gcs      — загружаем в Google Cloud Storage
  3. load_bigquery   — загружаем из GCS в BigQuery
  4. validate        — проверяем данные в BigQuery
  5. show_summary    — итоговый отчёт
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
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="load_to_gcp",
    description="Загрузка Parquet → GCS → BigQuery с валидацией",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["phase-2", "load", "gcp"],
)
def load_to_gcp_dag():

    # --------------------------------------------------------
    # Task 1: Проверяем Parquet файл
    # --------------------------------------------------------
    @task
    def check_parquet() -> dict:
        """Проверяем что Parquet из Transform шага существует."""
        from src.config.settings import settings
        from src.utils.logger import logger

        parquet_path = settings.parquet_dir / "staging_fraud.parquet"

        if not parquet_path.exists():
            raise FileNotFoundError(
                f"Parquet файл не найден: {parquet_path}\n"
                f"Сначала запустите DAG: transform_data"
            )

        size_mb = parquet_path.stat().st_size / 1_048_576
        logger.info(f"  ✓ Parquet найден: {parquet_path.name} ({size_mb:.1f} MB)")

        return {"parquet_path": str(parquet_path), "size_mb": size_mb}

    # --------------------------------------------------------
    # Task 2: Загрузка в GCS
    # --------------------------------------------------------
    @task
    def upload_to_gcs(parquet_info: dict) -> str:
        """Загружаем Parquet файл в Google Cloud Storage."""
        from src.load.gcp_loader import upload_to_gcs as do_upload
        from src.utils.logger import logger

        logger.info(f"Загружаем {parquet_info['size_mb']:.1f} MB в GCS...")
        gcs_uri = do_upload()

        return gcs_uri

    # --------------------------------------------------------
    # Task 3: Загрузка в BigQuery
    # --------------------------------------------------------
    @task
    def load_bigquery(gcs_uri: str) -> int:
        """Загружаем данные из GCS в BigQuery таблицу."""
        from src.load.gcp_loader import load_to_bigquery as do_load
        from src.utils.logger import logger

        logger.info(f"Загружаем в BigQuery из: {gcs_uri}")
        row_count = do_load(gcs_uri)

        return row_count

    # --------------------------------------------------------
    # Task 4: Валидация в BigQuery
    # --------------------------------------------------------
    @task
    def validate_data(row_count: int) -> dict:
        """Проверяем что данные в BigQuery выглядят правильно."""
        from src.load.gcp_loader import validate_bigquery
        from src.utils.logger import logger

        logger.info(f"Валидируем {row_count:,} строк в BigQuery...")
        validation = validate_bigquery()

        return validation

    # --------------------------------------------------------
    # Task 5: Итоговый отчёт
    # --------------------------------------------------------
    @task
    def show_summary(parquet_info: dict, validation: dict) -> None:
        """Финальный отчёт по всему пайплайну."""
        from src.config.settings import settings
        from src.utils.logger import logger

        logger.info("\n" + "=" * 55)
        logger.info("  ИТОГИ ЗАГРУЗКИ В GCP")
        logger.info("=" * 55)
        logger.info(f"  Parquet размер   : {parquet_info['size_mb']:.1f} MB")
        logger.info(f"  GCS bucket       : {settings.gcs_bucket}")
        logger.info(f"  BigQuery таблица : {settings.gcp_project}.{settings.bigquery_dataset}.{settings.bigquery_table}")
        logger.info(f"  Строк загружено  : {validation['total_rows']:>10,}")
        logger.info(f"  Мошеннических    : {validation['fraud_count']:>10,}")
        logger.info(f"  Доля фрода       : {validation['fraud_rate_pct']:>9.2f}%")
        logger.info(f"  Уникальных карт  : {validation['unique_cards']:>10,}")
        logger.info("=" * 55)
        logger.info("  🎉 ETL пайплайн Phase 1 завершён!")
        logger.info("  Extract → Transform → Load — всё работает!")
        logger.info("=" * 55)

    # --------------------------------------------------------
    # Порядок выполнения
    # --------------------------------------------------------
    parquet_info = check_parquet()
    gcs_uri      = upload_to_gcs(parquet_info)
    row_count    = load_bigquery(gcs_uri)
    validation   = validate_data(row_count)
    show_summary(parquet_info, validation)


load_to_gcp_dag()