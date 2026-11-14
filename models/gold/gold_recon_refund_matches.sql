{{ config(materialized='table') }}

with txns as (
    select transaction_id, card_id, merchant_id, amount, abs_amount, is_refund, transaction_ts
    from {{ ref('gold_transactions_enriched') }}
),

refunds as (
    select * from txns where is_refund
),

purchases as (
    select * from txns where not is_refund
),

-- most recent earlier purchase with exactly the refunded amount
exact_match as (
    select refund_id, purchase_id, purchase_ts
    from (
        select
            r.transaction_id as refund_id,
            p.transaction_id as purchase_id,
            p.transaction_ts as purchase_ts,
            row_number() over (
                partition by r.transaction_id
                order by p.transaction_ts desc, p.transaction_id desc
            ) as rn
        from refunds r
        join purchases p
          on p.card_id = r.card_id
         and p.merchant_id = r.merchant_id
         and p.amount = r.abs_amount
         and p.transaction_ts < r.transaction_ts
    )
    where rn = 1
),

-- any earlier purchase at the same merchant on the same card
any_prior as (
    select r.transaction_id as refund_id, count(*) as prior_purchases_at_merchant
    from refunds r
    join purchases p
      on p.card_id = r.card_id
     and p.merchant_id = r.merchant_id
     and p.transaction_ts < r.transaction_ts
    group by r.transaction_id
)

select
    r.transaction_id,
    r.card_id,
    r.merchant_id,
    r.abs_amount                                              as refund_amount,
    r.transaction_ts                                          as refund_ts,
    e.purchase_id                                             as matched_purchase_id,
    datediff(cast(r.transaction_ts as date), cast(e.purchase_ts as date)) as days_since_purchase,
    coalesce(a.prior_purchases_at_merchant, 0)                as prior_purchases_at_merchant,
    case
        when e.refund_id is not null then 'exact_match'
        when a.refund_id is not null then 'partial_or_different_amount'
        else 'no_prior_purchase'
    end                                                       as match_status
from refunds r
left join exact_match e on r.transaction_id = e.refund_id
left join any_prior   a on r.transaction_id = a.refund_id