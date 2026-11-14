{{ config(materialized='table') }}

with scored as (
    select
        year(transaction_date) as yr,
        is_fraud,
        stack(7,
            'r1_online_new_mcc',      r1_online_new_mcc,
            'r2_online_large_amount', r2_online_large_amount,
            'r4_card_data_errors',    r4_card_data_errors,
            'r5_dormant_card',        r5_dormant_card,
            'r6_chip_new_merchant',   r6_chip_new_merchant,
            'any_rule',               rules_hit >= 1,
            'two_or_more_rules',      rules_hit >= 2
        ) as (rule, flagged)
    from {{ ref('gold_fraud_rule_flags') }}
    where dataset_split in ('train', 'test')
)

select
    yr,
    rule,
    count(*)                                   as total_txns,
    sum(cast(is_fraud as int))                 as total_frauds,
    sum(cast(flagged as int))                  as flagged_txns,
    sum(cast(flagged and is_fraud as int))     as true_positives,
    round(100.0 * sum(cast(flagged and is_fraud as int))
          / nullif(sum(cast(flagged as int)), 0), 3)  as precision_pct,
    round(100.0 * sum(cast(flagged and is_fraud as int))
          / nullif(sum(cast(is_fraud as int)), 0), 3) as recall_pct
from scored
group by yr, rule