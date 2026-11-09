{{ config(materialized='view') }}

select
    id_uf,
    sigla_uf,
    nome_uf,
    ano_trimestre,
    taxa_desocupacao_homens_pct,
    taxa_desocupacao_mulheres_pct,
    taxa_desocupacao_jovens_18_24_pct,
    taxa_desocupacao_ensino_superior_pct,
    razao_rendimento_mulher_homem,
    reference_date,
    extracted_at
from {{ source('ibge_pnad_continua_silver', 'pnad_demografia_educacao_trabalho') }}
