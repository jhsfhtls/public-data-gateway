{{ config(materialized='table') }}

with cine as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_censo,
        area_cine_rotulo,
        cursos_graduacao_ativos,
        matriculas_ativas_area,
        concluintes_anuais_area,
        participacao_matriculas_area_pct,
        proporcao_modalidade_ead_area_pct,
        conceito_enade_medio_area,
        reference_date,
        extracted_at
    from {{ ref('stg_censo_superior_cursos_areas_cine') }}
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    ano_censo,
    area_cine_rotulo,
    cursos_graduacao_ativos,
    matriculas_ativas_area,
    concluintes_anuais_area,
    participacao_matriculas_area_pct,
    proporcao_modalidade_ead_area_pct,
    conceito_enade_medio_area,
    reference_date,
    extracted_at
from cine
