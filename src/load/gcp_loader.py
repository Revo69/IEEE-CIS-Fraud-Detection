"""
Load: Загрузка данных в Google Cloud
=====================================
Третий шаг ETL пайплайна.

Что делает этот модуль:
1. GCS UPLOAD   — загружаем Parquet файл в Google Cloud Storage
2. BQ LOAD      — загружаем данные из GCS в BigQuery
3. BQ VALIDATE  — проверяем что данные попали правильно

Архитектура:
  Parquet (локально) → GCS (data lake) → BigQuery (data warehouse)

Почему через GCS, а не напрямую в BigQuery?
- GCS — промежуточное хранилище (data lake), стандартная практика
- BigQuery умеет быстро загружать из GCS
- Файлы в GCS остаются как backup
- Можно загружать в несколько таблиц из одного файла
"""

from __future__ import annotations

from pathlib import Path

from google.cloud import bigquery, storage

from src.config.settings import settings
from src.utils.logger import logger


# ============================================================
# Шаг 1: Загрузка Parquet в GCS
# ============================================================

def upload_to_gcs() -> str:
    """
    Загружает Parquet файл из локальной папки в GCS bucket.

    Returns:
        GCS URI загруженного файла (gs://bucket/path/file.parquet)
    """
    logger.info("Шаг 1/3: Загружаем Parquet → GCS...")

    local_path = settings.parquet_dir / "staging_fraud.parquet"

    if not local_path.exists():
        raise FileNotFoundError(
            f"Parquet файл не найден: {local_path}\n"
            f"Сначала запустите DAG: transform_data"
        )

    size_mb = local_path.stat().st_size / 1_048_576
    logger.info(f"  Файл    : {local_path.name} ({size_mb:.1f} MB)")
    logger.info(f"  Bucket  : {settings.gcs_bucket}")

    # Подключаемся к GCS
    client = storage.Client(project=settings.gcp_project)
    bucket = client.bucket(settings.gcs_bucket)

    # Путь внутри bucket
    blob_name = "parquet/staging_fraud.parquet"
    blob = bucket.blob(blob_name)

    # Загружаем файл
    blob.upload_from_filename(str(local_path))

    gcs_uri = f"gs://{settings.gcs_bucket}/{blob_name}"
    logger.info(f"  ✓ Загружено в : {gcs_uri}")

    return gcs_uri


# ============================================================
# Шаг 2: Загрузка из GCS в BigQuery
# ============================================================

def load_to_bigquery(gcs_uri: str) -> int:
    """
    Загружает данные из GCS в таблицу BigQuery.

    Используем WRITE_TRUNCATE — каждый раз пересоздаём таблицу.
    Это идемпотентно: можно запускать сколько угодно раз,
    результат всегда будет одинаковым.

    Returns:
        Количество строк в BigQuery таблице после загрузки.
    """
    logger.info("Шаг 2/3: Загружаем GCS → BigQuery...")

    client = bigquery.Client(project=settings.gcp_project)

    # Полный ID таблицы: project.dataset.table
    table_id = (
        f"{settings.gcp_project}"
        f".{settings.bigquery_dataset}"
        f".{settings.bigquery_table}"
    )
    logger.info(f"  Таблица : {table_id}")
    logger.info(f"  Источник: {gcs_uri}")

    # Настройка загрузки
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,

        # WRITE_TRUNCATE = удалить таблицу и создать заново
        # Альтернативы: WRITE_APPEND (добавить), WRITE_EMPTY (только если пусто)
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,

        # Автоматически определить схему из Parquet
        autodetect=True,
    )

    # Запускаем job загрузки
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    logger.info("  Ожидаем завершения BigQuery job...")
    load_job.result()  # Ждём завершения (синхронно)

    # Проверяем сколько строк загрузилось
    table = client.get_table(table_id)
    row_count = table.num_rows

    logger.info(f"  ✓ Job завершён   : {load_job.job_id}")
    logger.info(f"  ✓ Строк в таблице: {row_count:,}")
    logger.info(f"  ✓ Колонок        : {len(table.schema)}")

    return row_count


# ============================================================
# Шаг 3: Валидация данных в BigQuery
# ============================================================

def validate_bigquery() -> dict:
    """
    Проверяем что данные в BigQuery выглядят правильно.
    Простые проверки: количество строк, доля фрода, диапазон сумм.
    """
    logger.info("Шаг 3/3: Валидация данных в BigQuery...")

    client = bigquery.Client(project=settings.gcp_project)

    table_id = (
        f"{settings.gcp_project}"
        f".{settings.bigquery_dataset}"
        f".{settings.bigquery_table}"
    )

    # Проверочный запрос
    query = f"""
        SELECT
            COUNT(*)                            AS total_rows,
            SUM(isFraud)                        AS fraud_count,
            ROUND(AVG(isFraud) * 100, 2)        AS fraud_rate_pct,
            ROUND(MIN(TransactionAmt), 2)        AS min_amount,
            ROUND(MAX(TransactionAmt), 2)        AS max_amount,
            ROUND(AVG(TransactionAmt), 2)        AS avg_amount,
            COUNTIF(TransactionAmt = -999)       AS missing_amounts,
            COUNT(DISTINCT card1)               AS unique_cards
        FROM `{table_id}`
    """

    results = client.query(query).result()
    row = list(results)[0]

    validation = {
        "total_rows":      row.total_rows,
        "fraud_count":     row.fraud_count,
        "fraud_rate_pct":  float(row.fraud_rate_pct),
        "min_amount":      float(row.min_amount),
        "max_amount":      float(row.max_amount),
        "avg_amount":      float(row.avg_amount),
        "missing_amounts": row.missing_amounts,
        "unique_cards":    row.unique_cards,
    }

    # Простые checks — если что-то не так, логируем WARNING
    checks_passed = True

    if validation["total_rows"] < 500_000:
        logger.warning(f"  ⚠ Мало строк: {validation['total_rows']:,} (ожидалось ~590k)")
        checks_passed = False

    if not (3.0 <= validation["fraud_rate_pct"] <= 4.0):
        logger.warning(f"  ⚠ Необычная доля фрода: {validation['fraud_rate_pct']}%")
        checks_passed = False

    if validation["missing_amounts"] > 0:
        logger.warning(f"  ⚠ Есть пропуски в суммах: {validation['missing_amounts']:,}")

    if checks_passed:
        logger.info("  ✓ Все проверки пройдены")

    logger.info(
        f"\n  Итоги валидации BigQuery:\n"
        f"    Строк всего      : {validation['total_rows']:,}\n"
        f"    Мошеннических    : {validation['fraud_count']:,}\n"
        f"    Доля фрода       : {validation['fraud_rate_pct']}%\n"
        f"    Сумма (min/max)  : ${validation['min_amount']} / "
        f"${validation['max_amount']:,.2f}\n"
        f"    Средняя сумма    : ${validation['avg_amount']}\n"
        f"    Уникальных карт  : {validation['unique_cards']:,}"
    )

    return validation


# ============================================================
# Главная функция — вызывается из Airflow DAG
# ============================================================

def run_load() -> dict:
    """
    Точка входа для Airflow task.
    Выполняет полный цикл загрузки в GCP.

    Returns:
        Метаданные о загруженных данных (для XCom).
    """
    logger.info("=" * 55)
    logger.info("  LOAD: Начинаем загрузку в GCP")
    logger.info("=" * 55)

    try:
        gcs_uri    = upload_to_gcs()
        row_count  = load_to_bigquery(gcs_uri)
        validation = validate_bigquery()

        summary = {
            "gcs_uri":        gcs_uri,
            "bq_rows":        row_count,
            "fraud_rate_pct": validation["fraud_rate_pct"],
            "unique_cards":   validation["unique_cards"],
        }

        logger.info("=" * 55)
        logger.info("  LOAD: Завершено успешно ✓")
        logger.info("=" * 55)

        return summary

    except Exception as e:
        logger.error(f"Ошибка при загрузке в GCP: {e}")
        raise


# ============================================================
# Запуск напрямую (для тестирования без Airflow)
# ============================================================

if __name__ == "__main__":
    result = run_load()
    print("\nРезультат:", result)