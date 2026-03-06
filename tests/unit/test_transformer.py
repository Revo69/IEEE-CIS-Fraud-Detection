# tests/unit/test_transformer.py
# ================================
# Unit тесты для src/transform/transformer.py
#
# Что такое unit тест?
# Проверяет ОДНУ функцию изолированно.
# Не использует реальные файлы, реальные БД, интернет.
# Должен работать быстро (миллисекунды).
#
# Структура каждого теста (AAA pattern):
#   Arrange — подготовить данные
#   Act     — вызвать функцию
#   Assert  — проверить результат

import sys
import pytest
import duckdb

sys.path.insert(0, "/opt/airflow")


# ============================================================
# Тесты для merge_tables()
# ============================================================

class TestMergeTables:
    """
    Группируем тесты по функции — так удобнее читать вывод pytest.
    Класс просто организует тесты, никакой магии.
    """

    def test_merge_preserves_all_transactions(self, sample_identity):
        """
        После LEFT JOIN количество строк = количество транзакций.
        Identity не должен фильтровать транзакции.
        """
        from src.transform.transformer import merge_tables

        # Act
        row_count = merge_tables(sample_identity)

        # Assert
        assert row_count == 6, (
            f"Ожидали 6 строк (все транзакции), получили {row_count}. "
            f"LEFT JOIN не должен фильтровать транзакции без identity."
        )

    def test_merge_adds_has_identity_flag(self, sample_identity):
        """
        Колонка has_identity должна быть 1 для транзакций с identity
        и 0 для транзакций без него.
        """
        from src.transform.transformer import merge_tables

        merge_tables(sample_identity)

        result = sample_identity.execute("""
            SELECT
                SUM(has_identity)           AS with_identity,
                SUM(1 - has_identity)       AS without_identity
            FROM merged_data
        """).fetchone()

        assert result[0] == 3, f"Ожидали 3 с identity, получили {result[0]}"
        assert result[1] == 3, f"Ожидали 3 без identity, получили {result[1]}"

    def test_merge_joins_device_info(self, sample_identity):
        """
        DeviceType из identity должен попасть в merged_data
        для транзакций у которых есть identity запись.
        """
        from src.transform.transformer import merge_tables

        merge_tables(sample_identity)

        # Транзакция 1 должна иметь DeviceType = 'desktop'
        result = sample_identity.execute("""
            SELECT id_DeviceType
            FROM merged_data
            WHERE TransactionID = 1
        """).fetchone()

        assert result[0] == "desktop"

    def test_merge_null_device_for_missing_identity(self, sample_identity):
        """
        Транзакции без identity должны иметь NULL в полях identity.
        """
        from src.transform.transformer import merge_tables

        merge_tables(sample_identity)

        result = sample_identity.execute("""
            SELECT id_DeviceType
            FROM merged_data
            WHERE TransactionID = 4  -- нет в identity таблице
        """).fetchone()

        assert result[0] is None


# ============================================================
# Тесты для clean_data()
# ============================================================

class TestCleanData:
    """Тесты для функции очистки данных."""

    def test_clean_fills_missing_email(self, sample_identity):
        """
        NULL в P_emaildomain должен заменяться на 'unknown'.
        Транзакция 4 имеет NULL email.
        """
        from src.transform.transformer import merge_tables, clean_data

        merge_tables(sample_identity)
        clean_data(sample_identity)

        result = sample_identity.execute("""
            SELECT P_emaildomain
            FROM cleaned_data
            WHERE TransactionID = 4
        """).fetchone()

        assert result[0] == "unknown", (
            f"NULL email должен стать 'unknown', получили: {result[0]}"
        )

    def test_clean_fills_missing_addr(self, sample_identity):
        """NULL в addr1 должен заменяться на -999."""
        from src.transform.transformer import merge_tables, clean_data

        merge_tables(sample_identity)
        clean_data(sample_identity)

        result = sample_identity.execute("""
            SELECT addr1
            FROM cleaned_data
            WHERE TransactionID = 4  -- addr1 был NULL
        """).fetchone()

        assert result[0] == -999.0

    def test_clean_preserves_existing_values(self, sample_identity):
        """
        Существующие значения не должны меняться.
        Транзакция 1 имеет email = 'gmail.com' — должен остаться.
        """
        from src.transform.transformer import merge_tables, clean_data

        merge_tables(sample_identity)
        clean_data(sample_identity)

        result = sample_identity.execute("""
            SELECT P_emaildomain
            FROM cleaned_data
            WHERE TransactionID = 1
        """).fetchone()

        assert result[0] == "gmail.com"

    def test_clean_row_count_unchanged(self, sample_identity):
        """
        Очистка не должна удалять строки — только заполнять пропуски.
        """
        from src.transform.transformer import merge_tables, clean_data

        merge_tables(sample_identity)
        clean_data(sample_identity)

        count = sample_identity.execute(
            "SELECT COUNT(*) FROM cleaned_data"
        ).fetchone()[0]

        assert count == 6


# ============================================================
# Тесты для engineer_features()
# ============================================================

class TestEngineerFeatures:
    """Тесты для feature engineering."""

    @pytest.fixture(autouse=True)
    def setup_cleaned_data(self, sample_identity):
        """
        autouse=True — этот fixture запускается автоматически
        перед каждым тестом в классе.
        Подготавливаем данные: merge → clean → готово к feature engineering.
        """
        from src.transform.transformer import merge_tables, clean_data

        merge_tables(sample_identity)
        clean_data(sample_identity)
        self.con = sample_identity  # сохраняем соединение для тестов

    def test_features_tx_hour_range(self):
        """
        tx_hour должен быть в диапазоне 0-23.
        TransactionDT=86400 → 86400/3600 % 24 = 0 (полночь).
        """
        from src.transform.transformer import engineer_features

        engineer_features(self.con)

        result = self.con.execute("""
            SELECT MIN(tx_hour), MAX(tx_hour)
            FROM staging_fraud
        """).fetchone()

        assert 0 <= result[0] <= 23, f"MIN tx_hour вне диапазона: {result[0]}"
        assert 0 <= result[1] <= 23, f"MAX tx_hour вне диапазона: {result[1]}"

    def test_features_is_night_tx(self):
        """
        TransactionDT=3600 → tx_hour=1 → должен быть ночной (is_night_tx=1).
        TransactionDT=43200 → tx_hour=12 → дневной (is_night_tx=0).
        """
        from src.transform.transformer import engineer_features

        engineer_features(self.con)

        # tx_hour = 1 (ночь)
        night = self.con.execute("""
            SELECT is_night_tx FROM staging_fraud
            WHERE TransactionID = 4  -- TransactionDT=3600, hour=1
        """).fetchone()[0]

        # tx_hour = 12 (день)
        day = self.con.execute("""
            SELECT is_night_tx FROM staging_fraud
            WHERE TransactionID = 6  -- TransactionDT=43200, hour=12
        """).fetchone()[0]

        assert night == 1, f"Час=1 должен быть ночным, получили is_night_tx={night}"
        assert day == 0,   f"Час=12 должен быть дневным, получили is_night_tx={day}"

    def test_features_log_amount_positive(self):
        """
        log_TransactionAmt = LN(1 + amount) должен быть > 0
        для любой положительной суммы.
        """
        from src.transform.transformer import engineer_features

        engineer_features(self.con)

        result = self.con.execute("""
            SELECT MIN(log_TransactionAmt)
            FROM staging_fraud
            WHERE TransactionAmt > 0
        """).fetchone()[0]

        assert result > 0, f"log(amount) должен быть > 0, получили {result}"

    def test_features_card_aggregates_exist(self):
        """
        Агрегации по карте (card1_tx_count, card1_avg_amt)
        должны быть посчитаны для всех строк.
        """
        from src.transform.transformer import engineer_features

        engineer_features(self.con)

        # card1=1001 встречается 2 раза → card1_tx_count должен быть 2
        result = self.con.execute("""
            SELECT card1_tx_count
            FROM staging_fraud
            WHERE TransactionID = 1  -- card1=1001
        """).fetchone()[0]

        assert result == 2, (
            f"card1=1001 встречается 2 раза, "
            f"card1_tx_count должен быть 2, получили {result}"
        )

    def test_features_is_large_tx(self):
        """
        is_large_tx=1 если TransactionAmt > 150.
        Транзакция 2: amount=999 → large.
        Транзакция 3: amount=50  → not large.
        """
        from src.transform.transformer import engineer_features

        engineer_features(self.con)

        large = self.con.execute("""
            SELECT is_large_tx FROM staging_fraud WHERE TransactionID = 2
        """).fetchone()[0]

        small = self.con.execute("""
            SELECT is_large_tx FROM staging_fraud WHERE TransactionID = 3
        """).fetchone()[0]

        assert large == 1, f"amount=999 должен быть large, получили {large}"
        assert small == 0, f"amount=50 не должен быть large, получили {small}"

    def test_features_row_count_preserved(self):
        """Feature engineering не должен терять строки."""
        from src.transform.transformer import engineer_features

        row_count = engineer_features(self.con)

        assert row_count == 6, (
            f"Ожидали 6 строк после feature engineering, получили {row_count}"
        )


# ============================================================
# Тесты для настроек (settings.py)
# ============================================================

class TestSettings:
    """Проверяем что конфигурация работает правильно."""

    def test_settings_loads(self):
        """Settings должен создаваться без ошибок."""
        from src.config.settings import settings
        assert settings is not None

    def test_default_paths_are_absolute(self):
        """Все пути должны быть абсолютными (начинаться с /)."""
        from src.config.settings import settings

        assert settings.duckdb_path.is_absolute(), \
            f"duckdb_path должен быть абсолютным: {settings.duckdb_path}"
        assert settings.parquet_dir.is_absolute(), \
            f"parquet_dir должен быть абсолютным: {settings.parquet_dir}"

    def test_table_names_not_empty(self):
        """Названия таблиц не должны быть пустыми."""
        from src.config.settings import settings

        assert settings.raw_transactions_table, "raw_transactions_table пустой"
        assert settings.raw_identity_table,     "raw_identity_table пустой"
        assert settings.staging_table,          "staging_table пустой"

    def test_gcp_project_set(self):
        """GCP project должен быть настроен."""
        from src.config.settings import settings

        assert settings.gcp_project, "gcp_project не настроен"
        assert settings.gcs_bucket,  "gcs_bucket не настроен"

    def test_gcs_parquet_path_format(self):
        """GCS путь должен начинаться с gs://"""
        from src.config.settings import settings

        assert settings.gcs_parquet_path.startswith("gs://"), \
            f"GCS путь должен начинаться с gs://, получили: {settings.gcs_parquet_path}"