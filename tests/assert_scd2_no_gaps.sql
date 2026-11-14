with covered as (
    select
        p.card_id,
        p.month_start,
        count(d.card_profile_key) as matching_versions
    from {{ ref('int_card_monthly_profile') }} p
    left join {{ ref('dim_card_profile_scd2') }} d
      on p.card_id = d.card_id
     and p.month_start >= d.valid_from
     and p.month_start <  d.valid_to
    group by p.card_id, p.month_start
)
select * from covered where matching_versions <> 1