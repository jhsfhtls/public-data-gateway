{{ config(materialized='view') }}

select
    id_uf,
    sigla_uf,
    nome_uf,
    ano_trimestre,
    populacao_idade_trabalhar_mil,
    forca_trabalho_total_mil,
    populacao_ocupada_mil,
    populacao_desocupada_mil,
    populacao_fora_forca_mil,
    taxa_desocupacao_pct,
    taxa_participacao_forca_pct,
    taxa_nivel_ocupacao_pct,
    reference_date,
    extracted_at
from {{ source('ibge_pnad_continua_silver', 'pnad_mercado_trabalho_trimestral') }}
