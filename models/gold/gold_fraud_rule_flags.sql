{{ config(materialized='table') }}

with flags as (
    select
        transaction_id,
        transaction_date,
        dataset_split,
        is_fraud,
        coalesce(is_online and is_new_mcc_for_card, false)          as r1_online_new_mcc,
        coalesce(is_online and amount_vs_card_avg >= 3, false)      as r2_online_large_amount,
        coalesce(err_bad_cvv or err_bad_card_number or err_bad_expiration, false) as r4_card_data_errors,
        coalesce(is_dormant_card, false)                            as r5_dormant_card,
        coalesce(transaction_type = 'Chip Transaction'
                 and days_since_first_visit_to_merchant <= 30, false) as r6_chip_new_merchant
    from {{ ref('gold_fraud_features') }}
)

select
    *,
    cast(r1_online_new_mcc as int) + cast(r2_online_large_amount as int)
  + cast(r4_card_data_errors as int) + cast(r5_dormant_card as int)
  + cast(r6_chip_new_merchant as int)                               as rules_hit
from flags