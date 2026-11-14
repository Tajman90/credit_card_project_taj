{{ config(materialized='table') }}

with card_bounds as (
    select
        card_id,
        cast(date_trunc('month', min(transaction_date)) as date) as first_month
    from {{ ref('silver_transactions') }}
    group by card_id
),

global_end as (
    select cast(date_trunc('month', max(transaction_date)) as date) as last_month
    from {{ ref('silver_transactions') }}
),

-- one row per card per month, from the card's first active month to the end of the data
spine as (
    select
        b.card_id,
        explode(sequence(b.first_month, g.last_month, interval 1 month)) as month_start
    from card_bounds b
    cross join global_end g
),

monthly_activity as (
    select
        card_id,
        cast(date_trunc('month', transaction_date) as date) as month_start,
        count(*)                                            as txn_count,
        sum(case when error_code is not null then 1 else 0 end) as error_count,
        sum(case when error_code rlike 'Bad Card Number|Bad CVV|Bad Expiration'
                 then 1 else 0 end)                         as card_data_error_count,
        max(abs(amount))                                    as max_abs_amount
    from {{ ref('silver_transactions') }}
    group by 1, 2
),

joined as (
    select
        s.card_id,
        s.month_start,
        coalesce(a.txn_count, 0)             as txn_count,
        coalesce(a.error_count, 0)           as error_count,
        coalesce(a.card_data_error_count, 0) as card_data_error_count,
        a.max_abs_amount
    from spine s
    left join monthly_activity a
        on s.card_id = a.card_id
       and s.month_start = a.month_start
),

-- windows look only at PRIOR months (1 preceding), never the current month
windowed as (
    select
        card_id,
        month_start,
        row_number() over (partition by card_id order by month_start) - 1 as months_of_history,
        sum(txn_count) over (partition by card_id order by month_start
                             rows between 3 preceding and 1 preceding) as txn_3m,
        sum(error_count) over (partition by card_id order by month_start
                               rows between 3 preceding and 1 preceding) as error_3m,
        sum(card_data_error_count) over (partition by card_id order by month_start
                                         rows between 3 preceding and 1 preceding) as card_data_error_3m,
        max(max_abs_amount) over (partition by card_id order by month_start
                                  rows between 3 preceding and 1 preceding) as max_amount_3m,
        sum(txn_count) over (partition by card_id order by month_start
                             rows between 6 preceding and 1 preceding) as txn_6m
    from joined
),

scored as (
    select
        w.card_id,
        w.month_start,
        w.months_of_history,
        w.txn_3m,
        w.card_data_error_3m,
        w.max_amount_3m,
        c.credit_limit,
        c.expires_date,
        coalesce(w.error_3m / nullif(w.txn_3m, 0), 0) as error_rate_3m,
        w.txn_6m
    from windowed w
    left join {{ ref('silver_cards') }} c
        on w.card_id = c.card_id
),

points as (
    select
        *,
        (case when error_rate_3m >= 0.05  then 2
              when error_rate_3m >= 0.025 then 1 else 0 end)
      + (case when card_data_error_3m >= 3 then 2
              when card_data_error_3m >= 1 then 1 else 0 end)
      + (case when credit_limit > 0 and max_amount_3m >= 0.5 * credit_limit
              then 1 else 0 end)                         as risk_points
    from scored
)

select
    card_id,
    month_start,
    months_of_history,
    case
        when months_of_history < 3 then 'unscored'
        when risk_points >= 4      then 'high'
        when risk_points >= 2      then 'medium'
        else 'low'
    end                                                  as risk_tier,
    risk_points,
    case
        when month_start > last_day(expires_date)                  then 'expired'
        when months_of_history >= 6 and coalesce(txn_6m, 0) = 0    then 'dormant'
        else 'active'
    end                                                  as card_status,
    credit_limit,
    -- supporting features, kept here (not in the SCD2) because they change monthly
    error_rate_3m,
    card_data_error_3m,
    txn_3m
from points