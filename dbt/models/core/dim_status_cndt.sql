{{ config(materialized='table') }}

select
    codigo_status,
    descricao_status,
    permite_licitar,
    nivel_risco_trabalhista
from {{ ref('stg_tst_cndt_status_dominio') }}
