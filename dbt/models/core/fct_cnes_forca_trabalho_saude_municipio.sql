{{ config(materialized='table') }}

with prof as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        categoria_profissional,
        total_vinculos_ativos,
        vinculos_atendimento_sus,
        carga_horaria_semanal_media,
        densidade_por_10k_hab,
        reference_date,
        extracted_at
    from {{ ref('stg_cnes_profissionais_equipes_saude') }}
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    categoria_profissional,
    total_vinculos_ativos,
    vinculos_atendimento_sus,
    round(
        (cast(vinculos_atendimento_sus as double) / nullif(total_vinculos_ativos, 0)) * 100.0,
        2
    ) as proporcao_vinculos_sus_pct,
    carga_horaria_semanal_media,
    densidade_por_10k_hab,
    reference_date,
    extracted_at
from prof
