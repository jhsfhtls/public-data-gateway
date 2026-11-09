{{ config(materialized='view') }}

select
    id_uf,
    sigla_uf,
    nome_uf,
    ano_trimestre,
    taxa_composta_subutilizacao_pct,
    subocupados_por_insuficiencia_horas_mil,
    forca_trabalho_potencial_mil,
    populacao_desalentada_mil,
    taxa_desalento_pct,
    reference_date,
    extracted_at
from {{ source('ibge_pnad_continua_silver', 'pnad_subutilizacao_forca') }}
