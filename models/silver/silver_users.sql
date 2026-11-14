select
    cast(id as int)                                                   as user_id,
    try_cast(current_age as int)                                      as current_age,
    try_cast(retirement_age as int)                                   as retirement_age,
    try_cast(birth_year as int)                                       as birth_year,
    try_cast(birth_month as int)                                      as birth_month,
    trim(gender)                                                      as gender,
    try_cast(latitude as double)                                      as latitude,
    try_cast(longitude as double)                                     as longitude,
    try_cast(regexp_replace(per_capita_income, '[$,]', '') as decimal(12,2)) as per_capita_income,
    try_cast(regexp_replace(yearly_income, '[$,]', '') as decimal(12,2))     as yearly_income,
    try_cast(regexp_replace(total_debt, '[$,]', '') as decimal(12,2))        as total_debt,
    try_cast(credit_score as int)                                     as credit_score,
    try_cast(num_credit_cards as int)                                 as num_credit_cards
from {{ ref('bronze_users') }}