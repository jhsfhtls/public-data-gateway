{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(apresentacoes_sujeitas_cap_obrigatorio as integer) as apresentacoes_sujeitas_cap_obrigatorio,
    cast(proporcao_apresentacoes_cap_pct as double) as proporcao_apresentacoes_cap_pct,
    cast(coeficiente_cap_vigente_pct as double) as coeficiente_cap_vigente_pct,
    cast(preco_fabrica_medio_ponderado_brl as double) as preco_fabrica_medio_ponderado_brl,
    cast(pmc_medio_ponderado_aliquota_uf_brl as double) as pmc_medio_ponderado_aliquota_uf_brl,
    cast(pmvg_medio_ponderado_brl as double) as pmvg_medio_ponderado_brl,
    cast(economia_potencial_pmvg_sobre_pmc_pct as double) as economia_potencial_pmvg_sobre_pmc_pct,
    cast(conformidade_teto_pmvg_compras_sus_pct as double) as conformidade_teto_pmvg_compras_sus_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anvisa_cmed_silver', 'cmed_compras_publicas_cap_pmvg') }}
