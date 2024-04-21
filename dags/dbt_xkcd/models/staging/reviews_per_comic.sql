{{ config(primary_key='num', foreign_key='num') }}
select
  num,
  round(CAST(random() AS numeric) * 9 + 1, 2) as customer_review
from {{source('xkcd', 'comic')}}
