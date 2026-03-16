-- models/marts/fraud_hourly.sql
-- Fraud distribution by hour of day
-- Answers: at what time does fraud happen most often?

SELECT
    tx_hour,
    is_night_tx,

    COUNT(*)                                        AS total_transactions,
    SUM(is_fraud)                                   AS fraud_transactions,
    ROUND(AVG(is_fraud) * 100, 2)                   AS fraud_rate_pct,

    ROUND(AVG(transaction_amt), 2)                  AS avg_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1
                   THEN transaction_amt ELSE 0 END), 2)
                                                    AS fraud_total_amount,

    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    )                                               AS pct_of_daily_volume,

    ROUND(
        SUM(is_fraud) * 100.0 / SUM(SUM(is_fraud)) OVER (),
        2
    )                                               AS pct_of_daily_fraud

FROM {{ ref('stg_fraud') }}

GROUP BY tx_hour, is_night_tx
ORDER BY tx_hour