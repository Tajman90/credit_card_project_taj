select
    a.card_id,
    a.valid_from, a.valid_to,
    b.valid_from as other_valid_from, b.valid_to as other_valid_to
from {{ ref('dim_card_profile_scd2') }} a
join {{ ref('dim_card_profile_scd2') }} b
  on a.card_id = b.card_id
 and a.valid_from < b.valid_from
 and b.valid_from < a.valid_to