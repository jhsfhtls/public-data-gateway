{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(familias_bolsa_familia as bigint) as familias_bolsa_familia,
    cast(beneficiarios_bpc_loas as bigint) as beneficiarios_bpc_loas,
    cast(volume_mensal_transferencias_mil_brl as double) as volume_mensal_transferencias_mil_brl,
    cast(valor_medio_beneficio_brl as double) as valor_medio_beneficio_brl,
    cast(cobertura_cadunico_pobreza_pct as double) as cobertura_cadunico_pobreza_pct
from {{ source('cgu_beneficios_cidadao', 'cgu_beneficios_resumo_municipal') }}
