select
    cast(transaction_id as bigint) as transaction_id,
    (is_fraud = 'Yes')             as is_fraud
from {{ ref('bronze_train_fraud_labels') }}