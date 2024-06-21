{{ config(
    materialized='incremental',
    unique_key=['CASENUMBER','TEXTTYPE']
) }}

with v360_additionaltext as (
    select *
    from FULGENT.ACQ.V360_ADDITIONALTEXT
)

select *
from v360_additionaltext
{% if is_incremental() %}
where _MD_UPDATED_AT > (select max(_MD_UPDATED_AT) from {{ this }})
{% endif %}
