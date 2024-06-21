{{ config(
    materialized='incremental',
    unique_key=['SUBCASENUMBER','"ROW"']
) }}

with v360_billing as (
    select *
    from FULGENT.ACQ.V360_BILLING
)

select *
from v360_billing
{% if is_incremental() %}
where _MD_UPDATED_AT > (select max(_MD_UPDATED_AT) from {{ this }})
{% endif %}
