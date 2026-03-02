"""
Конфигурация проекта
====================
Все пути и параметры в одном месте.
Используем Pydantic Settings — читает из .env автоматически.
"""

from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field


# ============================================================
# Базовые пути проекта
# ============================================================

BASE_DIR = Path("/opt/airflow")

RAW_DATA_DIR       = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
PARQUET_DIR        = PROCESSED_DATA_DIR / "parquet"
DUCKDB_DIR         = BASE_DIR / "data" / "duckdb"


# ============================================================
# Настройки проекта через Pydantic
# ============================================================

class ProjectSettings(BaseSettings):
    """
    Настройки читаются из переменных окружения или .env файла.
    Pydantic автоматически подхватывает их по имени.
    """

    # --- Пути к исходным файлам ---
    raw_transaction_file: Path = Field(
        default=RAW_DATA_DIR / "train_transaction.csv",
        description="Путь к файлу транзакций",
    )
    raw_identity_file: Path = Field(
        default=RAW_DATA_DIR / "train_identity.csv",
        description="Путь к файлу идентификаторов",
    )

    # --- Пути к обработанным данным ---
    parquet_dir: Path = Field(
        default=PARQUET_DIR,
        description="Директория для Parquet файлов",
    )

    # --- DuckDB ---
    duckdb_path: Path = Field(
        default=DUCKDB_DIR / "fraud_detection.duckdb",
        description="Путь к файлу базы данных DuckDB",
    )

    # --- Параметры загрузки ---
    sample_size: int | None = Field(
        default=None,
        description="Кол-во строк для загрузки (None = все)",
    )

    # --- Названия таблиц в DuckDB ---
    raw_transactions_table: str = "raw_transactions"
    raw_identity_table: str     = "raw_identity"
    staging_table: str          = "staging_fraud"

    # --- Google Cloud Platform ---
    gcp_project: str = Field(
        default="notional-life-489018-q1",
        description="GCP Project ID",
    )
    gcs_bucket: str = Field(
        default="notional-life-489018-q1-fraud-pipeline",
        description="GCS bucket для хранения Parquet файлов",
    )
    bigquery_dataset: str = Field(
        default="fraud_detection",
        description="BigQuery dataset",
    )
    bigquery_table: str = Field(
        default="staging_fraud",
        description="BigQuery таблица с трансформированными данными",
    )

    # GCS путь куда загружаем Parquet
    @property
    def gcs_parquet_path(self) -> str:
        return f"gs://{self.gcs_bucket}/parquet/staging_fraud.parquet"

    class Config:
        env_prefix = "FRAUD_"
        env_file = ".env"
        extra = "ignore"


# Глобальный экземпляр настроек
settings = ProjectSettings()