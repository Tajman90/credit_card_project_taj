select card_id, count(*) as current_rows
from {{ ref('dim_card_profile_scd2') }}
where is_current
group by card_id
having count(*) <> 1