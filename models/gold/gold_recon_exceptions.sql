{{ config(materialized='table') }}

with txns as (
    select
        transaction_id,
        is_after_card_expiry,
        is_before_account_open,
        unix_timestamp(transaction_ts) - unix_timestamp(lag(transaction_ts) over (
            partition by card_id, merchant_id, amount
            order by transaction_ts, transaction_id)) as secs_since_same
    from {{ ref('gold_transactions_enriched') }}
),

exceptions as (
    select transaction_id, 'card_used_after_expiry' as exception_type, 'high' as severity,
           'transaction dated after the card expiry month' as detail
    from txns
    where is_after_card_expiry

    union all
    select transaction_id, 'card_used_before_account_open', 'high',
           'transaction dated before the account open month'
    from txns
    where is_before_account_open

    union all
    select transaction_id, 'possible_duplicate', 'high',
           concat('same card, merchant and amount, ', cast(secs_since_same as string), ' seconds after the previous one')
    from txns
    where secs_since_same <= {{ var('dup_high_secs', 60) }}

    union all
    select transaction_id, 'possible_duplicate', 'medium',
           concat('same card, merchant and amount, ', cast(secs_since_same as string), ' seconds after the previous one')
    from txns
    where secs_since_same > {{ var('dup_high_secs', 60) }}
      and secs_since_same <= {{ var('dup_medium_secs', 600) }}

        union all
    select transaction_id, 'refund_without_prior_purchase',
           case when refund_ts < to_timestamp('{{ var("data_warmup_end", "2011-01-01") }}')
                then 'low' else 'medium' end,
           case when refund_ts < to_timestamp('{{ var("data_warmup_end", "2011-01-01") }}')
                then 'refund with no earlier purchase in the data; the purchase may predate the data window (starts 2010-01-01)'
                else 'refund with no earlier purchase at this merchant on this card' end
    from {{ ref('gold_recon_refund_matches') }}
    where match_status = 'no_prior_purchase'
)

select
    e.transaction_id,
    e.exception_type,
    e.severity,
    e.detail,
    t.card_id,
    t.merchant_id,
    t.mcc_description,
    t.amount,
    t.transaction_ts
from exceptions e
join {{ ref('gold_transactions_enriched') }} t
  on e.transaction_id = t.transaction_id