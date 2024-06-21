{{ config(
    materialized='incremental',
    unique_key='IDBLOCK'
) }}

with acq_v360_block as (
    select *
    from FULGENT.ACQ.V360_BLOCK
)

-- En los modelos incrementales de dbt, puedes definir una condición para
-- seleccionar solo las filas que han cambiado.
-- Esto es opcional si estás seguro de que siempre quieres procesar todos los datos.
select *
from acq_v360_block
{% if is_incremental() %}
where _MD_UPDATED_AT > (select max(_MD_UPDATED_AT) from {{ this }})
{% endif %}
