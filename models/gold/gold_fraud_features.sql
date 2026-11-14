{{ config(materialized='table') }}

with base as (
    select
        transaction_id, transaction_ts, transaction_date, card_id, user_id,
        amount, abs_amount, is_refund,
        transaction_type, is_online, merchant_id, mcc, mcc_description,
        has_error, err_insufficient_balance, err_bad_pin, err_bad_card_number,
        err_bad_expiration, err_bad_cvv, err_bad_zipcode, err_technical_glitch,
        card_status_at_txn, card_risk_tier_at_txn, credit_limit_at_txn,
        has_chip, account_open_date, is_after_card_expiry, is_card_user_mismatch,
        credit_score, current_age,
        is_fraud, is_labeled,
        cast(transaction_ts as long) as ts_sec
    from {{ ref('gold_transactions_enriched') }}
),

history as (
    select
        *,
        count(*) over (partition by card_id order by ts_sec, transaction_id
                       rows between unbounded preceding and 1 preceding) as prior_txn_count,
        avg(case when not is_refund then abs_amount end)
            over (partition by card_id order by ts_sec, transaction_id
                  rows between unbounded preceding and 1 preceding) as prior_avg_amount,
        lag(ts_sec) over (partition by card_id order by ts_sec, transaction_id) as prev_ts_sec,
        count(*) over (partition by card_id order by ts_sec
                       range between 3600 preceding and 1 preceding)  as txns_prior_1h,
        count(*) over (partition by card_id order by ts_sec
                       range between 86400 preceding and 1 preceding) as txns_prior_24h,
        count(*) over (partition by card_id, mcc order by ts_sec, transaction_id
                       rows between unbounded preceding and 1 preceding) as prior_txns_same_mcc,
        count(*) over (partition by card_id, merchant_id order by ts_sec, transaction_id
                       rows between unbounded preceding and 1 preceding) as prior_txns_same_merchant,
        min(transaction_date) over (partition by card_id, merchant_id order by ts_sec, transaction_id
                       rows between unbounded preceding and current row) as first_merchant_date,
        (row_number() over (partition by card_id, merchant_id order by ts_sec, transaction_id) = 1) as is_first_visit
    from base
),

history2 as (
    select
        *,
        sum(cast(is_first_visit as int)) over (partition by card_id order by ts_sec
                       range between 604800 preceding and 1 preceding)  as new_merchants_prior_7d,
        sum(cast(is_first_visit as int)) over (partition by card_id order by ts_sec
                       range between 2592000 preceding and 1 preceding) as new_merchants_prior_30d
    from history
)

select
    transaction_id,
    transaction_ts,
    transaction_date,
    card_id,
    user_id,
    merchant_id,

    -- label and split (time-based; set the cutoff with --vars after checking label dates)
    is_fraud,
    case
        when not is_labeled then 'unlabeled'
        when transaction_date <= to_date('{{ var("train_cutoff_date", "2018-12-31") }}') then 'train'
        else 'test'
    end                                                     as dataset_split,

    -- the transaction itself
    amount, abs_amount, is_refund,
    transaction_type, is_online,
    mcc, mcc_description,
    hour(transaction_ts)                                    as hour_of_day,
    dayofweek(transaction_date)                             as day_of_week,
    (hour(transaction_ts) < 6)                              as is_night,

    -- behaviour compared with this card's own past
    prior_txn_count,
    (prior_txn_count = 0)                                   as is_first_txn_on_card,
    prior_avg_amount,
    case when prior_avg_amount > 0 then abs_amount / prior_avg_amount end as amount_vs_card_avg,
    (ts_sec - prev_ts_sec)                                  as secs_since_prev_txn,
    txns_prior_1h,
    txns_prior_24h,
    (prior_txns_same_mcc = 0)                               as is_new_mcc_for_card,
    (prior_txns_same_merchant = 0)                          as is_new_merchant_for_card,

    -- merchant familiarity (past and present only)
    datediff(transaction_date, first_merchant_date)         as days_since_first_visit_to_merchant,
    case
        when is_first_visit                                          then '0 first visit'
        when datediff(transaction_date, first_merchant_date) <= 7    then '1 within 7 days of first visit'
        when datediff(transaction_date, first_merchant_date) <= 30   then '2 within 30 days'
        else '3 over 30 days'
    end                                                     as merchant_familiarity,
    coalesce(new_merchants_prior_7d, 0)                     as new_merchants_prior_7d,
    coalesce(new_merchants_prior_30d, 0)                    as new_merchants_prior_30d,

    -- card and account context (point-in-time)
    months_between(transaction_date, account_open_date)     as card_age_months,
    card_status_at_txn,
    card_risk_tier_at_txn,
    (card_status_at_txn = 'dormant')                        as is_dormant_card,
    abs_amount / nullif(credit_limit_at_txn, 0)             as amount_to_credit_limit,
    has_chip,
    is_after_card_expiry,
    is_card_user_mismatch,

    -- errors on this transaction
    has_error, err_insufficient_balance, err_bad_pin, err_bad_card_number,
    err_bad_expiration, err_bad_cvv, err_bad_zipcode, err_technical_glitch,

    -- user
    credit_score,
    current_age
from history2