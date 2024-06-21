{{ config(
    materialized='incremental',
    unique_key='PROFILEID'
) }}

with tbl_source as (
    select *
    from FULGENT.ACQ.V360_BILLINGPROFILES
)

select *
from tbl_source
{% if is_incremental() %}
where _MD_UPDATED_AT > (select max(_MD_UPDATED_AT) from {{ this }})
{% endif %}
