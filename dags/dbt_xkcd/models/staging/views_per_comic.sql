{{ config(primary_key='num', foreign_key='num') }}
select
    num,
    cast(random() * 10000 as int) as views 
from {{source('xkcd', 'comic')}}