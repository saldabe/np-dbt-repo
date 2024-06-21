{{ config(
    materialized='incremental',
    unique_key='ACCTID'
) }}

with acq_v360_account as (
    select *
    from FULGENT.ACQ.V360_ACCOUNT
)

select *
from acq_v360_account
{% if is_incremental() %}
where _MD_UPDATED_AT > (select max(_MD_UPDATED_AT) from {{ this }})
{% endif %}
