-- models/marts/fraud_hourly.sql
-- =================================
-- Mart модель: фрод по часам суток.
--
-- Отвечает на вопрос:
-- "В какое время суток чаще всего происходит мошенничество?"
--
-- Полезно для:
-- - Настройки пороговых значений по времени суток
-- - Визуализации в дашборде
-- - Понимания паттернов фрода

SELECT
    tx_hour,

    -- Флаг ночного времени для удобства фильтрации
    is_night_tx,

    -- Общая статистика по часу
    COUNT(*)                                        AS total_transactions,
    SUM(is_fraud)                                   AS fraud_transactions,
    ROUND(AVG(is_fraud) * 100, 2)                   AS fraud_rate_pct,

    -- Суммы
    ROUND(AVG(transaction_amt), 2)                  AS avg_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1
                   THEN transaction_amt ELSE 0 END), 2)
                                                    AS fraud_total_amount,

    -- Доля от всех транзакций за день
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    )                                               AS pct_of_daily_volume,

    -- Доля от всего фрода за день
    ROUND(
        SUM(is_fraud) * 100.0 / SUM(SUM(is_fraud)) OVER (),
        2
    )                                               AS pct_of_daily_fraud

FROM {{ ref('stg_fraud') }}

GROUP BY tx_hour, is_night_tx

ORDER BY tx_hour