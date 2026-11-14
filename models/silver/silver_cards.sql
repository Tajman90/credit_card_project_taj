select
    cast(id as int)                                              as card_id,
    cast(client_id as int)                                       as user_id,
    trim(card_brand)                                             as card_brand,
    trim(card_type)                                              as card_type,
    right(regexp_replace(card_number, '[^0-9]', ''), 4)          as card_last4,
    try_to_date(concat('01/', expires), 'dd/MM/yyyy')            as expires_date,
    try_to_date(concat('01/', acct_open_date), 'dd/MM/yyyy')     as account_open_date,
    try_cast(year_pin_last_changed as int)                       as year_pin_last_changed,
    (upper(trim(has_chip)) = 'YES')                              as has_chip,
    (upper(trim(card_on_dark_web)) = 'YES')                      as card_on_dark_web,
    try_cast(num_cards_issued as int)                            as num_cards_issued,
    try_cast(regexp_replace(credit_limit, '[$,]', '') as decimal(12,2)) as credit_limit
from {{ ref('bronze_cards') }}