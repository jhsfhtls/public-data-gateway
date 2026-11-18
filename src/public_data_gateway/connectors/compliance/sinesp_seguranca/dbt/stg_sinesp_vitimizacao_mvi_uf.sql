{{ config(materialized='view') }}

select
    cast(cod_vitimizacao_mvi as varchar) as cod_vitimizacao_mvi,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(total_vitimas_mvi as bigint) as total_vitimas_mvi,
    cast(taxa_mvi_por_100k_hab as double) as taxa_mvi_por_100k_hab,
    cast(taxa_elucidacao_inqueritos_pct as double) as taxa_elucidacao_inqueritos_pct,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('sinesp_seguranca', 'sinesp_vitimizacao_mvi_uf') }}
where cod_vitimizacao_mvi is not null
  and id_municipio is not null
