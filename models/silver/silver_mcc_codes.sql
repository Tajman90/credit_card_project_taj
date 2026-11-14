select
    trim(mcc)         as mcc,
    trim(description) as mcc_description
from {{ ref('bronze_mcc_codes') }}