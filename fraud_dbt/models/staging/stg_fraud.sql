-- models/staging/stg_fraud.sql
-- Clean staging layer on top of raw ETL data
-- Materialized as VIEW (no storage cost)
-- source() links to sources.yml definition

SELECT
    TransactionID                           AS transaction_id,
    isFraud                                 AS is_fraud,

    TransactionDT                           AS transaction_dt,
    tx_hour,
    tx_day_of_week,
    tx_day,

    is_night_tx,
    is_weekend,

    TransactionAmt                          AS transaction_amt,
    log_TransactionAmt                      AS log_transaction_amt,
    is_large_tx,

    ProductCD                               AS product_cd,
    card1,
    card4,
    card6,

    P_emaildomain                           AS payer_email_domain,
    R_emaildomain                           AS receiver_email_domain,

    addr1,
    addr2,

    has_identity,
    id_DeviceType                           AS device_type,
    id_DeviceInfo                           AS device_info,

    card1_tx_count,
    card1_avg_amt,
    card1_max_amt,
    card1_fraud_count,
    amt_deviation_from_card_mean

FROM {{ source('fraud_detection', 'staging_fraud') }}

WHERE TransactionAmt != -999