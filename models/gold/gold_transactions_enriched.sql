select
    t.transaction_id,
    t.transaction_ts,
    t.transaction_date,
    t.user_id,
    t.card_id,
    t.amount,
    (t.amount < 0)                                   as is_refund,
    abs(t.amount)                                    as abs_amount,
    t.transaction_type,
    t.is_online,
    t.merchant_id,
    t.merchant_city,
    t.merchant_state,
    t.zip,
    t.mcc,
    m.mcc_description,

    -- errors
    t.error_code,
    (t.error_code is not null)                       as has_error,
    coalesce(t.error_code like '%Insufficient Balance%', false) as err_insufficient_balance,
    coalesce(t.error_code like '%Bad PIN%', false)              as err_bad_pin,
    coalesce(t.error_code like '%Bad Card Number%', false)      as err_bad_card_number,
    coalesce(t.error_code like '%Bad Expiration%', false)       as err_bad_expiration,
    coalesce(t.error_code like '%Bad CVV%', false)              as err_bad_cvv,
    coalesce(t.error_code like '%Bad Zipcode%', false)          as err_bad_zipcode,
    coalesce(t.error_code like '%Technical Glitch%', false)     as err_technical_glitch,

    -- card attributes (static)
    c.card_brand,
    c.card_type,
    c.card_last4,
    c.has_chip,
    c.card_on_dark_web,   -- today's value, not point-in-time
    c.expires_date,
    c.account_open_date,
    (t.transaction_date > last_day(c.expires_date))  as is_after_card_expiry,
    (t.transaction_date < c.account_open_date)       as is_before_account_open,
    (c.user_id <> t.user_id)                         as is_card_user_mismatch,

    -- card profile AS IT WAS at the transaction date (SCD2 point-in-time join)
    d.card_profile_key,
    d.version_number                                 as card_profile_version,
    p.risk_tier                                      as card_risk_tier_at_txn,
    d.card_status                                    as card_status_at_txn,
    d.credit_limit                                   as credit_limit_at_txn,

    -- user attributes
    u.current_age,
    u.yearly_income,
    u.total_debt,
    u.credit_score,
    u.latitude  as user_latitude,
    u.longitude as user_longitude,

    -- fraud label (null = unlabeled)
    f.is_fraud,
    (f.transaction_id is not null)                   as is_labeled
from {{ ref('silver_transactions') }} t
left join {{ ref('silver_cards') }}        c on t.card_id = c.card_id
left join {{ ref('silver_users') }}        u on t.user_id = u.user_id
left join {{ ref('silver_mcc_codes') }}    m on t.mcc = m.mcc
left join {{ ref('silver_fraud_labels') }} f on t.transaction_id = f.transaction_id
left join {{ ref('dim_card_profile_scd2') }} d
       on t.card_id = d.card_id
      and t.transaction_date >= d.valid_from
      and t.transaction_date <  d.valid_to
left join {{ ref('int_card_monthly_profile') }} p
       on t.card_id = p.card_id
      and trunc(t.transaction_date, 'MM') = p.month_start