{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_exercicio_fiscal as bigint) as ano_exercicio_fiscal,
    cast(cobertura_cadastral_georreferenciada_ctm_pct as double) as cobertura_cadastral_georreferenciada_ctm_pct,
    cast(transacoes_itbi_emissao_digital_pct as double) as transacoes_itbi_emissao_digital_pct,
    cast(integracao_iptu_sinter_cno_onr_pct as double) as integracao_iptu_sinter_cno_onr_pct,
    cast(arrecadacao_itbi_milhoes_brl as double) as arrecadacao_itbi_milhoes_brl,
    cast(participacao_iptu_itbi_receita_propria_pct as double) as participacao_iptu_itbi_receita_propria_pct,
    cast(receita_imobiliaria_per_capita_brl as double) as receita_imobiliaria_per_capita_brl,
    cast(nivel_transparencia_dados_abertos_pgv as varchar) as nivel_transparencia_dados_abertos_pgv,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('capitais_iptu_itbi_silver', 'iptu_itbi_governanca_fiscal_geoespacial') }}
