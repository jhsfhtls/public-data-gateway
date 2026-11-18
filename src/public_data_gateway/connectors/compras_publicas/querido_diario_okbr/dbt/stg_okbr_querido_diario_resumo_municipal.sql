{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(edicoes_indexadas_12m as bigint) as edicoes_indexadas_12m,
    cast(qtd_atos_licitatorios_12m as bigint) as qtd_atos_licitatorios_12m,
    cast(valor_extratos_citados_mil_brl as double) as valor_extratos_citados_mil_brl,
    cast(taxa_ocr_legivel_pct as double) as taxa_ocr_legivel_pct,
    cast(indice_transparencia_diario_pct as double) as indice_transparencia_diario_pct
from {{ source('querido_diario_okbr', 'okbr_querido_diario_resumo_municipal') }}
