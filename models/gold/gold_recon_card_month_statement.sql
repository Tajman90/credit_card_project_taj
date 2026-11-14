{{ config(materialized='table') }}

select
    card_id,
    cast(date_trunc('month', transaction_date) as date)          as statement_month,
    count(*)                                                     as txn_count,
    sum(case when not is_refund then 1 else 0 end)               as purchase_count,
    sum(case when is_refund then 1 else 0 end)                   as refund_count,
    sum(case when has_error then 1 else 0 end)                   as error_txn_count,
    sum(case when not is_refund then amount else 0 end)          as purchases_amount,
    sum(case when is_refund then abs_amount else 0 end)          as refunds_amount,
    sum(amount)                                                  as net_amount,
    sum(case when not has_error then amount else 0 end)          as settled_net_amount,
    max(credit_limit_at_txn)                                     as credit_limit
from {{ ref('gold_transactions_enriched') }}
group by 1, 2