{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(assessores_investimento_ativos as bigint) as assessores_investimento_ativos,
    cast(sociedades_aai_registradas as bigint) as sociedades_aai_registradas,
    cast(analistas_consultores_cvm as bigint) as analistas_consultores_cvm,
    cast(alertas_sancionatorios_pasm_pct as double) as alertas_sancionatorios_pasm_pct
from {{ source('cvm_mercado_extra', 'cvm_agentes_autonomos_assessoria') }}
