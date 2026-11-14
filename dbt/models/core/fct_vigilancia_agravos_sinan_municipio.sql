{{ config(materialized='table') }}

with sinan as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        regiao,
        ano_epidemiologico,
        grupo_agravo_sinan,
        categoria_transmissao,
        casos_notificados_ano,
        casos_confirmados_ano,
        taxa_incidencia_confirmada_100k_hab,
        proporcao_encerramento_oportuno_sinan_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_vigilancia_agravos_notificacao_sinan') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    s.id_municipio,
    s.nome_municipio,
    s.sigla_uf,
    s.id_uf,
    coalesce(m.nome_uf, s.sigla_uf) as nome_uf,
    coalesce(m.regiao, s.regiao) as regiao,
    s.ano_epidemiologico,
    s.grupo_agravo_sinan,
    s.categoria_transmissao,
    s.casos_notificados_ano,
    s.casos_confirmados_ano,
    s.taxa_incidencia_confirmada_100k_hab,
    s.proporcao_encerramento_oportuno_sinan_pct,
    s.reference_date,
    s.extracted_at
from sinan s
left join municipios m on s.id_municipio = m.id_municipio
