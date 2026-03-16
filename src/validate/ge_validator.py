"""
Great Expectations: Валидация данных
=====================================
Проверяем качество данных после Transform шага.

Концепции которые используем:
- Expectation      — одна проверка ("колонка X > 0")
- ExpectationSuite — набор проверок для таблицы
- ValidationResult — результат запуска Suite

GE 1.x использует новый fluent API — проще и чище чем старый.
"""

from __future__ import annotations

import great_expectations as gx
import pandas as pd
import duckdb

from src.config.settings import settings
from src.utils.logger import logger


# ============================================================
# Вспомогательная функция: загрузка данных из DuckDB в DataFrame
# ============================================================

def _load_staging_to_df() -> pd.DataFrame:
    """
    Читаем staging_fraud из DuckDB в pandas DataFrame.

    GE 1.x работает с DataFrame через PandasDatasource —
    это самый простой способ для нашего случая.
    """
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
        """).df()  # .df() — DuckDB метод, возвращает pandas DataFrame
        logger.info(f"  Загружено из DuckDB: {len(df):,} строк")
        return df
    finally:
        con.close()


# ============================================================
# Expectation Suite: набор проверок для staging_fraud
# ============================================================

def build_staging_suite(validator) -> None:
    """
    Описываем все ожидания к данным staging_fraud.

    Разбиваем на группы для удобства чтения:
    1. Таблица целиком
    2. Целевая переменная (isFraud)
    3. Суммы транзакций
    4. Категориальные колонки
    5. Новые фичи (feature engineering)
    """

    # ----------------------------------------------------------
    # 1. Проверки на уровне таблицы
    # ----------------------------------------------------------

    # Количество строк должно быть в ожидаемом диапазоне
    validator.expect_table_row_count_to_be_between(
        min_value=500_000,
        max_value=700_000,
    )

    # Обязательные колонки должны существовать
    for col in ["TransactionID", "isFraud", "TransactionAmt",
                "tx_hour", "log_TransactionAmt", "has_identity"]:
        validator.expect_column_to_exist(col)

    # ----------------------------------------------------------
    # 2. Целевая переменная isFraud
    # ----------------------------------------------------------

    # Только значения 0 и 1
    validator.expect_column_values_to_be_in_set(
        column="isFraud",
        value_set=[0, 1],
    )

    # Нет пропусков — isFraud всегда известен
    validator.expect_column_values_to_not_be_null(
        column="isFraud",
    )

    # Доля фрода: реалистичный диапазон 1%-10%
    # mostly=0.99 означает "99% строк должны удовлетворять условию"
    validator.expect_column_mean_to_be_between(
        column="isFraud",
        min_value=0.01,
        max_value=0.10,
    )

    # ----------------------------------------------------------
    # 3. Суммы транзакций
    # ----------------------------------------------------------

    # TransactionAmt: нет пропусков (мы заполнили -999)
    validator.expect_column_values_to_not_be_null(
        column="TransactionAmt",
    )

    # log_TransactionAmt: должен быть >= 0
    validator.expect_column_values_to_be_between(
        column="log_TransactionAmt",
        min_value=0,
        strict_min=False,
    )

    # log_TransactionAmt: нет пропусков
    validator.expect_column_values_to_not_be_null(
        column="log_TransactionAmt",
    )

    # ----------------------------------------------------------
    # 4. Категориальные колонки
    # ----------------------------------------------------------

    # ProductCD: только известные типы продуктов
    validator.expect_column_values_to_be_in_set(
        column="ProductCD",
        value_set=["W", "H", "C", "S", "R", "unknown"],
    )

    # card4: известные платёжные системы
    validator.expect_column_values_to_be_in_set(
        column="card4",
        value_set=["visa", "mastercard", "american express",
                   "discover", "unknown", -999],
        mostly=0.95,  # 95% должны быть из этого набора
    )

    # P_emaildomain: нет пропусков (заполнили 'unknown')
    validator.expect_column_values_to_not_be_null(
        column="P_emaildomain",
    )

    # ----------------------------------------------------------
    # 5. Новые фичи (feature engineering)
    # ----------------------------------------------------------

    # tx_hour: 0-23
    validator.expect_column_values_to_be_between(
        column="tx_hour",
        min_value=0,
        max_value=23,
    )

    # tx_day_of_week: 0-6
    validator.expect_column_values_to_be_between(
        column="tx_day_of_week",
        min_value=0,
        max_value=6,
    )

    # is_night_tx: только 0 или 1
    validator.expect_column_values_to_be_in_set(
        column="is_night_tx",
        value_set=[0, 1],
    )

    # is_large_tx: только 0 или 1
    validator.expect_column_values_to_be_in_set(
        column="is_large_tx",
        value_set=[0, 1],
    )

    # has_identity: только 0 или 1
    validator.expect_column_values_to_be_in_set(
        column="has_identity",
        value_set=[0, 1],
    )

    # card1_tx_count: количество транзакций по карте >= 1
    validator.expect_column_values_to_be_between(
        column="card1_tx_count",
        min_value=0,
        strict_min=False,
    )


# ============================================================
# Главная функция валидации
# ============================================================

def run_validation() -> dict:
    """
    Запускает полную валидацию staging_fraud данных.

    Возвращает словарь с результатами для XCom.
    Выбрасывает исключение если критические проверки упали.
    """
    logger.info("=" * 55)
    logger.info("  GREAT EXPECTATIONS: Начинаем валидацию")
    logger.info("=" * 55)

    # 1. Загружаем данные
    df = _load_staging_to_df()

    # 2. Создаём GE Data Context (в памяти, без файлов на диске)
    context = gx.get_context(mode="ephemeral")

    # 3. Добавляем источник данных — pandas DataFrame
    datasource = context.data_sources.add_pandas("fraud_datasource")
    data_asset = datasource.add_dataframe_asset("staging_fraud")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "full_batch"
    )

    # 4. Создаём Suite (набор проверок)
    suite = context.suites.add(
        gx.ExpectationSuite(name="staging_fraud_suite")
    )

    # 5. Создаём Validator и добавляем проверки
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validator = context.get_validator(
        batch_definition=batch_definition,
        batch_parameters={"dataframe": df},
        expectation_suite=suite,
    )

    logger.info("  Добавляем проверки (expectations)...")
    build_staging_suite(validator)
    validator.save_expectation_suite()

    # 6. Запускаем валидацию
    logger.info("  Запускаем валидацию...")
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="staging_fraud_checkpoint",
            validation_definitions=[
                gx.ValidationDefinition(
                    name="staging_fraud_validation",
                    data=batch_definition,
                    suite=suite,
                )
            ],
        )
    )

    result = checkpoint.run(batch_parameters={"dataframe": df})

    # 7. Разбираем результаты
    validation_result = list(
        result.run_results.values()
    )[0]

    total       = validation_result.statistics["evaluated_expectations"]
    successful  = validation_result.statistics["successful_expectations"]
    failed      = validation_result.statistics["unsuccessful_expectations"]
    success_pct = validation_result.statistics["success_percent"]

    # 8. Логируем результаты
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
                logger.warning(
                    f"    ✗ {exp_result.expectation_config.type}"
                    f"  |  колонка: "
                    f"{exp_result.expectation_config.kwargs.get('column', 'table')}"
                )

    summary = {
        "total_expectations":      total,
        "successful_expectations": successful,
        "failed_expectations":     failed,
        "success_percent":         success_pct,
        "validation_success":      failed == 0,
    }

    # 9. Если упали критические проверки — поднимаем исключение
    #    Airflow пометит таск как FAILED
    if failed > 0:
        raise ValueError(
            f"Валидация данных не прошла: "
            f"{failed}/{total} проверок упало. "
            f"Проверьте логи для деталей."
        )

    logger.info("  ✓ Все проверки пройдены!")
    logger.info("=" * 55)

    return summary


# ============================================================
# Запуск напрямую (для тестирования без Airflow)
# ============================================================

if __name__ == "__main__":
    result = run_validation()
    print("\nРезультат:", result)