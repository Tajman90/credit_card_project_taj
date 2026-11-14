{{ config(materialized='table') }}

select
    c.card_id,
    c.user_id,
    c.card_brand,
    c.card_type,
    c.account_open_date,
    c.expires_date,
    c.credit_limit,
    c.card_on_dark_web
from {{ ref('silver_cards') }} c
left join (select distinct card_id from {{ ref('silver_transactions') }}) t
  on c.card_id = t.card_id
where t.card_id is null