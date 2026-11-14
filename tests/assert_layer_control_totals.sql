with layers as (
    select 1 as step, 'bronze' as layer, count(*) as txn_count,
           sum(try_cast(regexp_replace(amount, '[$,]', '') as decimal(12,2))) as total_amount
    from {{ ref('bronze_transactions') }}
    union all
    select 2, 'silver', count(*), sum(amount)
    from {{ ref('silver_transactions') }}
    union all
    select 3, 'gold_enriched', count(*), sum(amount)
    from {{ ref('gold_transactions_enriched') }}
    union all
    select 4, 'gold_features', count(*), sum(amount)
    from {{ ref('gold_fraud_features') }}
    union all
    select 5, 'gold_statement', sum(txn_count), sum(net_amount)
    from {{ ref('gold_recon_card_month_statement') }}
)

select
    a.layer as from_layer,
    b.layer as to_layer,
    a.txn_count as from_count,
    b.txn_count as to_count,
    a.total_amount as from_amount,
    b.total_amount as to_amount
from layers a
join layers b on b.step = a.step + 1
where a.txn_count <> b.txn_count
   or a.total_amount <> b.total_amount