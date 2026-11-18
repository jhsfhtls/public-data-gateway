{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(beneficios_inss_emitidos as integer) as beneficios_inss_emitidos,
    cast(valor_mensal_beneficios_mil_brl as double) as valor_mensal_beneficios_mil_brl,
    cast(ticket_medio_beneficio_brl as double) as ticket_medio_beneficio_brl,
    cast(participacao_bpc_loas_pct as double) as participacao_bpc_loas_pct
from {{ source('mps_inss_beneficios', 'inss_previdencia_resumo_municipal') }}
where id_municipio is not null
