{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_exercicio_fiscal as bigint) as ano_exercicio_fiscal,
    cast(preco_medio_m2_itbi_declarado_brl as double) as preco_medio_m2_itbi_declarado_brl,
    cast(variacao_anual_preco_m2_itbi_pct as double) as variacao_anual_preco_m2_itbi_pct,
    cast(razao_venal_pgv_sobre_mercado_itbi as double) as razao_venal_pgv_sobre_mercado_itbi,
    cast(participacao_lancamentos_novos_pct as double) as participacao_lancamentos_novos_pct,
    cast(concentracao_uso_residencial_pct as double) as concentracao_uso_residencial_pct,
    cast(indice_liquidez_imobiliaria_itbi_pct as double) as indice_liquidez_imobiliaria_itbi_pct,
    cast(taxa_vacancia_cadastral_estimada_pct as double) as taxa_vacancia_cadastral_estimada_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('capitais_iptu_itbi_silver', 'iptu_itbi_dinamica_precos_bairros') }}
