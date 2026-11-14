{{ config(materialized='table') }}

with profile as (
    select card_id, month_start, card_status, credit_limit
    from {{ ref('int_card_monthly_profile') }}
),

lagged as (
    select
        *,
        row_number() over (partition by card_id order by month_start) as rn,
        lag(card_status)  over (partition by card_id order by month_start) as prev_card_status,
        lag(credit_limit) over (partition by card_id order by month_start) as prev_credit_limit
    from profile
),

flagged as (
    select
        *,
        case
            when rn = 1 then 1
            when card_status <> prev_card_status then 1
            when not (credit_limit <=> prev_credit_limit) then 1
            else 0
        end as is_new_version
    from lagged
),

versioned as (
    select
        *,
        sum(is_new_version) over (
            partition by card_id order by month_start
            rows between unbounded preceding and current row
        ) as version_number
    from flagged
),

collapsed as (
    select
        card_id,
        version_number,
        card_status,
        credit_limit,
        min(month_start) as valid_from
    from versioned
    group by card_id, version_number, card_status, credit_limit
),

ranged as (
    select
        md5(concat_ws('|', cast(card_id as string), cast(valid_from as string))) as card_profile_key,
        card_id,
        version_number,
        card_status,
        credit_limit,
        valid_from,
        coalesce(
            lead(valid_from) over (partition by card_id order by valid_from),
            date'9999-12-31'
        ) as valid_to
    from collapsed
)

select
    *,
    (valid_to = date'9999-12-31') as is_current
from ranged

