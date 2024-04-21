{{ config(primary_key='num', foreign_key='num') }}

select
    num,
    length(title) * 5 AS cost
from {{source('xkcd', 'comic')}}