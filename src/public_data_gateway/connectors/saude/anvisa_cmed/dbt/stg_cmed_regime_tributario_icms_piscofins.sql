{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(aliquota_icms_modal_uf_pct as double) as aliquota_icms_modal_uf_pct,
    cast(apresentacoes_lista_positiva_piscofins as integer) as apresentacoes_lista_positiva_piscofins,
    cast(apresentacoes_lista_negativa_piscofins as integer) as apresentacoes_lista_negativa_piscofins,
    cast(apresentacoes_lista_neutra_piscofins as integer) as apresentacoes_lista_neutra_piscofins,
    cast(participacao_lista_positiva_piscofins_pct as double) as participacao_lista_positiva_piscofins_pct,
    cast(apresentacoes_comercializacao_2024_ativa_pct as double) as apresentacoes_comercializacao_2024_ativa_pct,
    cast(spread_medio_pmc_sobre_pf_pct as double) as spread_medio_pmc_sobre_pf_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anvisa_cmed_silver', 'cmed_regime_tributario_icms_piscofins') }}
