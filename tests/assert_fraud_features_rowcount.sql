select *
from (
    select
        (select count(*) from {{ ref('gold_fraud_features') }})        as features_rows,
        (select count(*) from {{ ref('gold_transactions_enriched') }}) as enriched_rows
)
where features_rows <> enriched_rows