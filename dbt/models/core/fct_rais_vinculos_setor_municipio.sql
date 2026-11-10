{{ config(materialized='table') }}

select
    ano,
    id_municipio,
    nome_municipio,
    sigla_uf,
    grande_setor_cnae,
    total_vinculos_ativos,
    round(total_vinculos_ativos * 100.0 / nullif(
        sum(total_vinculos_ativos) over (partition by ano, id_municipio), 0
    ), 2) as participacao_vinculos_municipio_pct,
    source_url,
    reference_date,
    extracted_at
from {{ ref('stg_rais_vinculos_ativos') }}
