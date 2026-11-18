{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(cobertura_historica_anos as integer) as cobertura_historica_anos,
    cast(formato_aberto_pdf_txt_pct as double) as formato_aberto_pdf_txt_pct,
    cast(regularidade_publicacao_pct as double) as regularidade_publicacao_pct,
    cast(indice_transparencia_diario_pct as double) as indice_transparencia_diario_pct
from {{ source('querido_diario_okbr', 'okbr_diarios_transparencia_ativa') }}
