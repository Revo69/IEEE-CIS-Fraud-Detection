"""
Transform: Трансформация данных IEEE-CIS
=========================================
Второй шаг ETL пайплайна.

Что делает этот модуль:
1. MERGE    — объединяем transactions + identity по TransactionID
2. CLEAN    — обрабатываем пропуски, исправляем типы
3. FEATURES — создаём новые признаки для анализа
4. SAVE     — сохраняем результат в Parquet

Всё делаем через SQL в DuckDB — быстро и читаемо.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from src.config.settings import settings
from src.utils.logger import logger


# ============================================================
# Шаг 1: MERGE — объединяем две таблицы
# ============================================================

def merge_tables(con: duckdb.DuckDBPyConnection) -> int:
    """
    Объединяем raw_transactions и raw_identity по TransactionID.

    Используем LEFT JOIN — транзакции без identity тоже важны
    (их большинство: 590k транзакций, но только 144k с identity).

    Returns:
        Количество строк после merge.
    """
    logger.info("Шаг 1/4: MERGE transactions + identity...")

    con.execute("DROP TABLE IF EXISTS merged_data")

    con.execute(f"""
        CREATE TABLE merged_data AS
        SELECT
            -- Все колонки из транзакций
            t.*,

            -- Колонки из identity (с префиксом id_ чтобы не путаться)
            i.DeviceType                    AS id_DeviceType,
            i.DeviceInfo                    AS id_DeviceInfo,

            -- Сетевые данные
            i.id_01, i.id_02, i.id_03, i.id_04, i.id_05,
            i.id_06, i.id_09, i.id_10, i.id_11,

            -- Браузер / ОС
            i.id_12, i.id_13, i.id_14, i.id_15,
            i.id_16, i.id_17, i.id_18, i.id_19,
            i.id_20, i.id_21, i.id_22, i.id_23,
            i.id_24, i.id_25, i.id_26, i.id_27,
            i.id_28, i.id_29, i.id_30, i.id_31,
            i.id_32, i.id_33, i.id_34, i.id_35,
            i.id_36, i.id_37, i.id_38,

            -- Флаг: была ли запись в identity таблице
            CASE WHEN i.TransactionID IS NOT NULL
                 THEN 1 ELSE 0
            END AS has_identity

        FROM {settings.raw_transactions_table} t
        LEFT JOIN {settings.raw_identity_table} i
            ON t.TransactionID = i.TransactionID
    """)

    row_count = con.execute("SELECT COUNT(*) FROM merged_data").fetchone()[0]
    identity_count = con.execute(
        "SELECT COUNT(*) FROM merged_data WHERE has_identity = 1"
    ).fetchone()[0]

    logger.info(f"  ✓ Строк после merge    : {row_count:,}")
    logger.info(f"  ✓ С identity данными   : {identity_count:,} ({identity_count/row_count*100:.1f}%)")
    logger.info(f"  ✓ Без identity данных  : {row_count - identity_count:,}")

    return row_count


# ============================================================
# Шаг 2: CLEAN — обработка пропусков и типов
# ============================================================

def clean_data(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Обрабатываем пропуски и приводим типы к нужным.

    Стратегия по типам колонок:
    - Числовые (суммы, id)   → заполняем -999 (явный маркер пропуска)
    - Категориальные (card)  → заполняем 'unknown'
    - TransactionDT          → оставляем как есть (это секунды от базовой даты)

    Почему -999, а не 0 или среднее?
    - 0 может быть реальным значением
    - Среднее скрывает факт пропуска
    - -999 явно говорит "здесь данных не было" — модель это поймёт
    """
    logger.info("Шаг 2/4: CLEAN — обработка пропусков...")

    # Считаем пропуски до очистки
    missing_before = con.execute("""
        SELECT
            COUNT(*) - COUNT(TransactionAmt)  AS missing_amt,
            COUNT(*) - COUNT(card1)           AS missing_card1,
            COUNT(*) - COUNT(P_emaildomain)   AS missing_email,
            COUNT(*) - COUNT(addr1)           AS missing_addr1
        FROM merged_data
    """).fetchone()

    logger.info(
        f"  Пропуски до очистки:\n"
        f"    TransactionAmt : {missing_before[0]:,}\n"
        f"    card1          : {missing_before[1]:,}\n"
        f"    P_emaildomain  : {missing_before[2]:,}\n"
        f"    addr1          : {missing_before[3]:,}"
    )

    con.execute("DROP TABLE IF EXISTS cleaned_data")

    con.execute("""
        CREATE TABLE cleaned_data AS
        SELECT
            -- Идентификаторы (не меняем)
            TransactionID,
            isFraud,
            TransactionDT,

            -- Сумма транзакции: пропуски → -999
            COALESCE(TransactionAmt, -999)          AS TransactionAmt,

            -- Тип продукта: пропуски → 'unknown'
            COALESCE(ProductCD, 'unknown')          AS ProductCD,

            -- Данные карты
            COALESCE(card1, -999)                   AS card1,
            COALESCE(card2, -999)                   AS card2,
            COALESCE(card3, -999)                   AS card3,
            COALESCE(card4, 'unknown')              AS card4,
            COALESCE(card5, -999)                   AS card5,
            COALESCE(card6, 'unknown')              AS card6,

            -- Адрес
            COALESCE(addr1, -999)                   AS addr1,
            COALESCE(addr2, -999)                   AS addr2,

            -- Дистанция
            COALESCE(dist1, -999)                   AS dist1,
            COALESCE(dist2, -999)                   AS dist2,

            -- Email домены
            COALESCE(P_emaildomain, 'unknown')      AS P_emaildomain,
            COALESCE(R_emaildomain, 'unknown')      AS R_emaildomain,

            -- C-features (счётчики)
            COALESCE(C1,  -999) AS C1,
            COALESCE(C2,  -999) AS C2,
            COALESCE(C3,  -999) AS C3,
            COALESCE(C4,  -999) AS C4,
            COALESCE(C5,  -999) AS C5,
            COALESCE(C6,  -999) AS C6,
            COALESCE(C7,  -999) AS C7,
            COALESCE(C8,  -999) AS C8,
            COALESCE(C9,  -999) AS C9,
            COALESCE(C10, -999) AS C10,
            COALESCE(C11, -999) AS C11,
            COALESCE(C12, -999) AS C12,
            COALESCE(C13, -999) AS C13,
            COALESCE(C14, -999) AS C14,

            -- D-features (временные дельты)
            COALESCE(D1,  -999) AS D1,
            COALESCE(D2,  -999) AS D2,
            COALESCE(D3,  -999) AS D3,
            COALESCE(D4,  -999) AS D4,
            COALESCE(D5,  -999) AS D5,
            COALESCE(D6,  -999) AS D6,
            COALESCE(D7,  -999) AS D7,
            COALESCE(D8,  -999) AS D8,
            COALESCE(D9,  -999) AS D9,
            COALESCE(D10, -999) AS D10,
            COALESCE(D11, -999) AS D11,
            COALESCE(D12, -999) AS D12,
            COALESCE(D13, -999) AS D13,
            COALESCE(D14, -999) AS D14,
            COALESCE(D15, -999) AS D15,

            -- M-features (совпадения, тип BOOL в датасете)
            -- Для булевых колонок NULL оставляем как NULL —
            -- нельзя подставить 'unknown' в BOOL колонку
            M1, M2, M3, M4, M5, M6, M7, M8, M9,

            -- Identity данные
            has_identity,
            COALESCE(id_DeviceType, 'unknown')  AS id_DeviceType,
            COALESCE(id_DeviceInfo, 'unknown')  AS id_DeviceInfo

        FROM merged_data
    """)

    row_count = con.execute("SELECT COUNT(*) FROM cleaned_data").fetchone()[0]
    logger.info(f"  ✓ Очищено строк: {row_count:,}")

    return {"cleaned_rows": row_count}


# ============================================================
# Шаг 3: FEATURES — создаём новые признаки
# ============================================================

def engineer_features(con: duckdb.DuckDBPyConnection) -> int:
    """
    Feature Engineering — создаём новые признаки из существующих.

    Что создаём:
    1. Временные признаки  — час, день недели из TransactionDT
    2. Агрегации по карте  — средняя/макс сумма по card1
    3. Лог-трансформация   — для TransactionAmt (убираем скос)
    4. Флаги               — выходной день, ночная транзакция

    TransactionDT — это секунды от некоей базовой даты.
    Точная дата неизвестна, но паттерны (час, день) реальные.
    """
    logger.info("Шаг 3/4: FEATURES — создаём новые признаки...")

    # --- Агрегации по карте (window functions) ---
    # Считаем статистику по каждой карте по всем её транзакциям
    logger.info("  Считаем агрегации по card1...")
    con.execute("DROP TABLE IF EXISTS card_aggregates")
    con.execute("""
        CREATE TABLE card_aggregates AS
        SELECT
            card1,
            COUNT(*)                        AS card1_tx_count,
            ROUND(AVG(TransactionAmt), 2)   AS card1_avg_amt,
            ROUND(MAX(TransactionAmt), 2)   AS card1_max_amt,
            ROUND(STDDEV(TransactionAmt), 2) AS card1_std_amt,
            SUM(isFraud)                    AS card1_fraud_count
        FROM cleaned_data
        WHERE card1 != -999
        GROUP BY card1
    """)

    # --- Финальная таблица с фичами ---
    logger.info("  Создаём финальную таблицу с признаками...")
    con.execute(f"DROP TABLE IF EXISTS {settings.staging_table}")

    con.execute(f"""
        CREATE TABLE {settings.staging_table} AS
        SELECT
            -- === Исходные колонки ===
            c.*,

            -- === Временные признаки ===
            -- TransactionDT в секундах → переводим в часы/дни
            (c.TransactionDT / 3600) % 24           AS tx_hour,
            (c.TransactionDT / 86400) % 7           AS tx_day_of_week,
            (c.TransactionDT / 86400)               AS tx_day,

            -- === Флаги ===
            -- Ночная транзакция (22:00 - 06:00) — подозрительнее
            CASE WHEN (c.TransactionDT / 3600) % 24
                      NOT BETWEEN 6 AND 21
                 THEN 1 ELSE 0
            END                                     AS is_night_tx,

            -- Выходной день (суббота=5, воскресенье=6)
            CASE WHEN (c.TransactionDT / 86400) % 7
                      IN (5, 6)
                 THEN 1 ELSE 0
            END                                     AS is_weekend,

            -- Крупная транзакция (выше 75-го перцентиля ~$150)
            CASE WHEN c.TransactionAmt > 150
                 THEN 1 ELSE 0
            END                                     AS is_large_tx,

            -- === Лог-трансформация суммы ===
            -- Убираем правый скос распределения
            -- LN(1 + x) чтобы избежать LN(0)
            LN(1 + GREATEST(c.TransactionAmt, 0))   AS log_TransactionAmt,

            -- === Агрегации по карте ===
            COALESCE(a.card1_tx_count,    0)        AS card1_tx_count,
            COALESCE(a.card1_avg_amt,  -999)        AS card1_avg_amt,
            COALESCE(a.card1_max_amt,  -999)        AS card1_max_amt,
            COALESCE(a.card1_std_amt,  -999)        AS card1_std_amt,
            COALESCE(a.card1_fraud_count, 0)        AS card1_fraud_count,

            -- Отклонение текущей транзакции от среднего по карте
            -- Большое отклонение = подозрительно
            CASE WHEN a.card1_avg_amt IS NOT NULL
                      AND a.card1_avg_amt > 0
                 THEN ROUND(
                     (c.TransactionAmt - a.card1_avg_amt)
                     / NULLIF(a.card1_std_amt, 0),
                 2)
                 ELSE -999
            END                                     AS amt_deviation_from_card_mean

        FROM cleaned_data c
        LEFT JOIN card_aggregates a
            ON c.card1 = a.card1
    """)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM {settings.staging_table}"
    ).fetchone()[0]

    # Проверяем несколько новых фич
    sample = con.execute(f"""
        SELECT
            ROUND(AVG(tx_hour), 1)          AS avg_hour,
            SUM(is_night_tx)                AS night_tx_count,
            SUM(is_weekend)                 AS weekend_count,
            ROUND(AVG(log_TransactionAmt), 3) AS avg_log_amt
        FROM {settings.staging_table}
    """).fetchone()

    logger.info(
        f"  ✓ Строк с фичами      : {row_count:,}\n"
        f"  ✓ Средний час тр-ции  : {sample[0]}\n"
        f"  ✓ Ночных транзакций   : {sample[1]:,}\n"
        f"  ✓ В выходные дни      : {sample[2]:,}\n"
        f"  ✓ Avg log(Amount)     : {sample[3]}"
    )

    return row_count


# ============================================================
# Шаг 4: SAVE — сохраняем в Parquet
# ============================================================

def save_to_parquet(con: duckdb.DuckDBPyConnection) -> str:
    """
    Сохраняем staging таблицу в Parquet файл.

    Parquet вместо CSV потому что:
    - Колоночный формат: быстрые агрегации
    - Сжатие: ~5x меньше места
    - Схема: типы колонок сохранены
    - Стандарт для data lakes
    """
    logger.info("Шаг 4/4: SAVE → Parquet...")

    output_path = settings.parquet_dir / "staging_fraud.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        COPY {settings.staging_table}
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    size_mb = output_path.stat().st_size / 1_048_576
    logger.info(f"  ✓ Файл сохранён : {output_path}")
    logger.info(f"  ✓ Размер файла  : {size_mb:.1f} MB")

    return str(output_path)


# ============================================================
# Главная функция — вызывается из Airflow DAG
# ============================================================

def run_transform() -> dict:
    """
    Точка входа для Airflow task.
    Выполняет полный цикл трансформации.

    Returns:
        Метаданные о трансформированных данных (для XCom).
    """
    logger.info("=" * 55)
    logger.info("  TRANSFORM: Начинаем трансформацию")
    logger.info("=" * 55)

    con = duckdb.connect(str(settings.duckdb_path))

    try:
        merged_rows  = merge_tables(con)
        clean_result = clean_data(con)
        feature_rows = engineer_features(con)
        parquet_path = save_to_parquet(con)

        summary = {
            "merged_rows":   merged_rows,
            "cleaned_rows":  clean_result["cleaned_rows"],
            "feature_rows":  feature_rows,
            "parquet_path":  parquet_path,
        }

        logger.info("=" * 55)
        logger.info("  TRANSFORM: Завершено успешно ✓")
        logger.info("=" * 55)

        return summary

    except Exception as e:
        logger.error(f"Ошибка при трансформации: {e}")
        raise

    finally:
        con.close()


# ============================================================
# Запуск напрямую (для тестирования без Airflow)
# ============================================================

if __name__ == "__main__":
    result = run_transform()
    print("\nРезультат:", result)