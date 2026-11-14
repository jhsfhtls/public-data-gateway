{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(classe_terapeutica_atc as varchar) as classe_terapeutica_atc,
    cast(apresentacoes_classe as integer) as apresentacoes_classe,
    cast(participacao_classe_pct as double) as participacao_classe_pct,
    cast(preco_fabrica_pf_medio_brl as double) as preco_fabrica_pf_medio_brl,
    cast(preco_maximo_consumidor_pmc_medio_brl as double) as preco_maximo_consumidor_pmc_medio_brl,
    cast(preco_maximo_venda_governo_pmvg_medio_brl as double) as preco_maximo_venda_governo_pmvg_medio_brl,
    cast(desconto_medio_pmvg_sobre_pf_pct as double) as desconto_medio_pmvg_sobre_pf_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anvisa_cmed_silver', 'cmed_precos_regulados_classe_terapeutica') }}
