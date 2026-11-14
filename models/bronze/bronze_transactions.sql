SELECT 
    * 
FROM 
    {{ source('source', 'transactions') }}
