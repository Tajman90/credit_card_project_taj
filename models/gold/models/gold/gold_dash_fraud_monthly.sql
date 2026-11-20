{{ config(materialized='table') }}

select
    date_trunc('month', transaction_date) as month,
    transaction_type,
    count(*) as txns,
    sum(case when is_fraud is not null then 1 else 0 end) as labeled_txns,
    sum(cast(is_fraud as int)) as frauds
from {{ ref('gold_fraud_features') }}
group by 1, 2