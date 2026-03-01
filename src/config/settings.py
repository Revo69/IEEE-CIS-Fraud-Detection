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

# Корень проекта (папка, где лежит docker-compose.yml)
# Внутри Docker-контейнера всё монтируется в /opt/airflow
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
    # None = загружать весь файл
    # Число = загружать только N строк (для разработки/тестирования)
    sample_size: int | None = Field(
        default=None,
        description="Кол-во строк для загрузки (None = все)",
    )

    # --- Названия таблиц в DuckDB ---
    raw_transactions_table: str = "raw_transactions"
    raw_identity_table: str     = "raw_identity"
    staging_table: str          = "staging_fraud"

    class Config:
        # Читаем переменные окружения с префиксом FRAUD_
        # Например: FRAUD_SAMPLE_SIZE=100000
        env_prefix = "FRAUD_"
        env_file = ".env"
        extra = "ignore"


# Создаём глобальный экземпляр настроек
# Импортируйте его в других модулях: from src.config.settings import settings
settings = ProjectSettings()