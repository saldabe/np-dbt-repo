{{ config(
    materialized='incremental',
    unique_key='ASSID'
) }}

with acq_v360_association as (
    select *
    from FULGENT.ACQ.V360_ASSOCIATION
)

select *
from acq_v360_association
{% if is_incremental() %}
where _MD_UPDATED_AT > (select max(_MD_UPDATED_AT) from {{ this }})
{% endif %}
