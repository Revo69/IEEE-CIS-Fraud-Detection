-- models/marts/fraud_summary.sql
-- ==================================
-- Mart модель: агрегации по картам.
--
-- Что делает:
-- - Считает статистику по каждой карте
-- - Показывает какие карты чаще используются для фрода
-- - Материализуется как TABLE — быстрые запросы в BQ
--
-- {{ ref() }} — ссылка на нашу stg_fraud модель.
-- dbt автоматически запустит stg_fraud первым.

SELECT
    card1,
    card4                                           AS card_network,
    card6                                           AS card_type,

    -- Общая статистика
    COUNT(*)                                        AS total_transactions,
    SUM(is_fraud)                                   AS fraud_transactions,
    ROUND(AVG(is_fraud) * 100, 2)                   AS fraud_rate_pct,

    -- Суммы
    ROUND(MIN(transaction_amt), 2)                  AS min_amount,
    ROUND(MAX(transaction_amt), 2)                  AS max_amount,
    ROUND(AVG(transaction_amt), 2)                  AS avg_amount,
    ROUND(SUM(transaction_amt), 2)                  AS total_amount,

    -- Поведение
    SUM(is_night_tx)                                AS night_transactions,
    SUM(is_weekend)                                 AS weekend_transactions,
    SUM(is_large_tx)                                AS large_transactions,
    SUM(has_identity)                               AS transactions_with_identity,

    -- Доля ночных среди фродовых
    ROUND(
        SAFE_DIVIDE(
            SUM(CASE WHEN is_fraud = 1 AND is_night_tx = 1 THEN 1 ELSE 0 END),
            NULLIF(SUM(is_fraud), 0)
        ) * 100, 2
    )                                               AS fraud_night_pct

FROM {{ ref('stg_fraud') }}

GROUP BY card1, card4, card6

-- Только карты с минимальной активностью
HAVING COUNT(*) >= 5

ORDER BY fraud_rate_pct DESC