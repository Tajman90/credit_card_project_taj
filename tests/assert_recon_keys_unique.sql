select 'exceptions' as model_name,
       cast(transaction_id as string) as key_value,
       count(*) as n
from {{ ref('gold_recon_exceptions') }}
group by transaction_id, exception_type
having count(*) > 1

union all

select 'statement',
       concat(cast(card_id as string), '|', cast(statement_month as string)),
       count(*)
from {{ ref('gold_recon_card_month_statement') }}
group by card_id, statement_month
having count(*) > 1