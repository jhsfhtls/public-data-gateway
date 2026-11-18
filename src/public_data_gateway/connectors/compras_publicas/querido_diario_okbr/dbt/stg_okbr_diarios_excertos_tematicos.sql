{{ config(materialized='view') }}

select
    cast(cod_excerto_qd as varchar) as cod_excerto_qd,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(edicoes_indexadas_12m as bigint) as edicoes_indexadas_12m,
    cast(mencoes_educacao_saude_12m as bigint) as mencoes_educacao_saude_12m,
    cast(mencoes_esg_clima_12m as bigint) as mencoes_esg_clima_12m,
    cast(taxa_ocr_legivel_pct as double) as taxa_ocr_legivel_pct
from {{ source('querido_diario_okbr', 'okbr_diarios_excertos_tematicos') }}
