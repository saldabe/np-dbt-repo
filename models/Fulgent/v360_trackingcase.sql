{{ config(
    materialized='incremental',
    unique_key=['IDCASE','CASENUMBER']
) }}

with tbl_source as (
    select *
    from FULGENT.ACQ.V360_TRACKINGCASE
)

select *
from tbl_source
{% if is_incremental() %}
where _MD_UPDATED_AT > (select max(_MD_UPDATED_AT) from {{ this }})
{% endif %}
