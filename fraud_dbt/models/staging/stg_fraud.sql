-- models/staging/stg_fraud.sql
-- =================================
-- Staging модель: чистый слой поверх raw данных.
--
-- Что делает:
-- - Выбираем нужные колонки (не тащим всё)
-- - Переименовываем в snake_case (единый стиль)
-- - Фильтруем явный мусор (отрицательные суммы)
-- - Материализуется как VIEW — не занимает место в BQ
--
-- {{ source() }} — ссылка на таблицу из sources.yml

SELECT
    -- Идентификаторы
    TransactionID                           AS transaction_id,
    isFraud                                 AS is_fraud,

    -- Временные метки
    TransactionDT                           AS transaction_dt,
    tx_hour,
    tx_day_of_week,
    tx_day,

    -- Флаги времени
    is_night_tx,
    is_weekend,

    -- Суммы
    TransactionAmt                          AS transaction_amt,
    log_TransactionAmt                      AS log_transaction_amt,
    is_large_tx,

    -- Продукт и карта
    ProductCD                               AS product_cd,
    card1,
    card4,
    card6,

    -- Email домены
    P_emaildomain                           AS payer_email_domain,
    R_emaildomain                           AS receiver_email_domain,

    -- Адрес
    addr1,
    addr2,

    -- Identity
    has_identity,
    id_DeviceType                           AS device_type,
    id_DeviceInfo                           AS device_info,

    -- Агрегации по карте
    card1_tx_count,
    card1_avg_amt,
    card1_max_amt,
    card1_fraud_count,
    amt_deviation_from_card_mean

FROM {{ source('fraud_detection', 'staging_fraud') }}

-- Убираем строки с явно некорректными данными
WHERE TransactionAmt != -999