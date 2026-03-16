"""
Great Expectations: Валидация данных
=====================================
Проверяем качество данных после Transform шага.

Используем GE 1.x fluent API с ephemeral context.
Прямой подход: batch → validate() без Checkpoint.
"""

from __future__ import annotations

import great_expectations as gx
import pandas as pd
import duckdb

from src.config.settings import settings
from src.utils.logger import logger


# ============================================================
# Загрузка данных из DuckDB в DataFrame
# ============================================================

def _load_staging_to_df() -> pd.DataFrame:
    """Читаем staging_fraud из DuckDB в pandas DataFrame."""
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        df = con.execute(f"""
            SELECT
                TransactionID,
                isFraud,
                TransactionAmt,
                ProductCD,
                card1,
                card4,
                card6,
                P_emaildomain,
                has_identity,
                tx_hour,
                tx_day_of_week,
                is_night_tx,
                is_weekend,
                is_large_tx,
                log_TransactionAmt,
                card1_tx_count,
                card1_avg_amt
            FROM {settings.staging_table}
        """).df()
        logger.info(f"  Загружено из DuckDB: {len(df):,} строк")
        return df
    finally:
        con.close()


# ============================================================
# Expectations — список всех проверок
# ============================================================

def _get_expectations() -> list:
    """
    Возвращает список всех Expectation объектов.

    В GE 1.x каждый Expectation — это объект из gx.expectations.*
    Создаём список и передаём в Suite — чисто и без side effects.
    """
    return [
        # --- Таблица целиком ---
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=500_000,
            max_value=700_000,
        ),
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "TransactionID", "isFraud", "TransactionAmt",
                "ProductCD", "card4", "card6", "P_emaildomain",
                "has_identity", "tx_hour", "tx_day_of_week",
                "is_night_tx", "is_weekend", "is_large_tx",
                "log_TransactionAmt", "card1_tx_count", "card1_avg_amt",
                "card1",
            ],
            exact_match=False,  # допускаем дополнительные колонки
        ),

        # --- isFraud ---
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="isFraud",
            value_set=[0, 1],
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="isFraud",
        ),
        gx.expectations.ExpectColumnMeanToBeBetween(
            column="isFraud",
            min_value=0.01,
            max_value=0.10,
        ),

        # --- TransactionAmt ---
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="TransactionAmt",
        ),

        # --- log_TransactionAmt ---
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="log_TransactionAmt",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="log_TransactionAmt",
            min_value=0,
            strict_min=False,
        ),

        # --- Категориальные ---
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="ProductCD",
            value_set=["W", "H", "C", "S", "R", "unknown"],
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="P_emaildomain",
        ),

        # --- Временные фичи ---
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="tx_hour",
            min_value=0,
            max_value=23,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="tx_day_of_week",
            min_value=0,
            max_value=6,
        ),

        # --- Флаги ---
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_night_tx",
            value_set=[0, 1],
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_large_tx",
            value_set=[0, 1],
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="has_identity",
            value_set=[0, 1],
        ),

        # --- Агрегаты по карте ---
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="card1_tx_count",
            min_value=0,
            strict_min=False,
        ),
    ]


# ============================================================
# Главная функция валидации
# ============================================================

def run_validation() -> dict:
    """
    Запускает полную валидацию staging_fraud данных через GE 1.x.

    Возвращает словарь с результатами для XCom.
    Выбрасывает исключение если проверки упали.
    """
    logger.info("=" * 55)
    logger.info("  GREAT EXPECTATIONS: Начинаем валидацию")
    logger.info("=" * 55)

    # 1. Загружаем данные
    df = _load_staging_to_df()

    # 2. Создаём GE контекст в памяти
    context = gx.get_context(mode="ephemeral")

    # 3. Подключаем DataFrame как источник данных
    data_source      = context.data_sources.add_pandas("fraud_datasource")
    data_asset       = data_source.add_dataframe_asset("staging_fraud")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "full_batch"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    # 4. Создаём Suite с нашими проверками
    logger.info("  Создаём Expectation Suite...")
    suite = gx.ExpectationSuite(name="staging_fraud_suite")
    for expectation in _get_expectations():
        suite.add_expectation(expectation)

    # 5. Запускаем валидацию напрямую через batch.validate()
    logger.info("  Запускаем валидацию...")
    validation_result = batch.validate(suite)

    # 6. Разбираем результаты
    total      = validation_result.statistics["evaluated_expectations"]
    successful = validation_result.statistics["successful_expectations"]
    failed     = validation_result.statistics["unsuccessful_expectations"]
    success_pct = validation_result.statistics["success_percent"]

    logger.info(f"\n  Результаты валидации:")
    logger.info(f"  {'─' * 40}")
    logger.info(f"  Всего проверок    : {total}")
    logger.info(f"  Прошло            : {successful} ✓")
    logger.info(f"  Упало             : {failed} {'✗' if failed else '✓'}")
    logger.info(f"  Успешность        : {success_pct:.1f}%")
    logger.info(f"  {'─' * 40}")

    # Детали по упавшим проверкам
    if failed > 0:
        logger.warning("  Упавшие проверки:")
        for exp_result in validation_result.results:
            if not exp_result.success:
                col = exp_result.expectation_config.kwargs.get(
                    "column", "table"
                )
                logger.warning(
                    f"    ✗ {exp_result.expectation_config.type}"
                    f"  |  колонка: {col}"
                )

    summary = {
        "total_expectations":      total,
        "successful_expectations": successful,
        "failed_expectations":     failed,
        "success_percent":         float(success_pct),
        "validation_success":      failed == 0,
    }

    # 7. Поднимаем исключение если есть упавшие проверки
    if failed > 0:
        raise ValueError(
            f"Валидация данных не прошла: "
            f"{failed}/{total} проверок упало. "
            f"Проверьте логи для деталей."
        )

    logger.info("  ✓ Все проверки пройдены!")
    logger.info("=" * 55)

    return summary


if __name__ == "__main__":
    result = run_validation()
    print("\nРезультат:", result)