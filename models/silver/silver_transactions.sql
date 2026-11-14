select
    cast(id as bigint)                                            as transaction_id,
    try_cast(date as timestamp)                                   as transaction_ts,
    cast(try_cast(date as timestamp) as date)                     as transaction_date,
    cast(client_id as int)                                        as user_id,
    cast(card_id as int)                                          as card_id,
    try_cast(regexp_replace(amount, '[$,]', '') as decimal(12,2)) as amount,
    trim(use_chip)                                                as transaction_type,
    cast(merchant_id as bigint)                                   as merchant_id,
    trim(merchant_city)                                           as merchant_city,
    trim(merchant_state)                                          as merchant_state,
    lpad(regexp_replace(zip, '\\.0$', ''), 5, '0')                as zip,
    trim(mcc)                                                     as mcc,
    nullif(trim(errors), '')                                      as error_code,
    (trim(merchant_city) = 'ONLINE')                              as is_online
from {{ ref('bronze_transactions') }}