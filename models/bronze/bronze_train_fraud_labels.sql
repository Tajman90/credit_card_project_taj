SELECT 
    * 
FROM 
    {{ source('source', 'train_fraud_labels') }}
