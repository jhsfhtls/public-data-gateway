{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(capitulo_cid10 as varchar) as capitulo_cid10,
    cast(internacoes_capitulo_aih as integer) as internacoes_capitulo_aih,
    cast(participacao_capitulo_pct as double) as participacao_capitulo_pct,
    cast(valor_total_capitulo_brl as double) as valor_total_capitulo_brl,
    cast(tempo_medio_permanencia_dias as double) as tempo_medio_permanencia_dias,
    cast(taxa_letalidade_capitulo_pct as double) as taxa_letalidade_capitulo_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sih_silver', 'sih_morbidade_capitulos_cid10') }}
