{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(competencia_referencia as varchar) as competencia_referencia,
    cast(contratos_alienacao_fiduciaria_ano as bigint) as contratos_alienacao_fiduciaria_ano,
    cast(volume_credito_imobiliario_garantido_milhoes_brl as double) as volume_credito_imobiliario_garantido_milhoes_brl,
    cast(participacao_sfi_sbpe_pmcmv_pct as double) as participacao_sfi_sbpe_pmcmv_pct,
    cast(contratos_home_equity_cgi_ano as bigint) as contratos_home_equity_cgi_ano,
    cast(intimacoes_purgacao_mora_fiduciaria_ano as bigint) as intimacoes_purgacao_mora_fiduciaria_ano,
    cast(consolidacoes_propriedade_inadimplencia_ano as bigint) as consolidacoes_propriedade_inadimplencia_ano,
    cast(taxa_execucao_garantia_fiduciaria_pct as double) as taxa_execucao_garantia_fiduciaria_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('onr_registro_imoveis_silver', 'onr_garantias_credito_imobiliario') }}
