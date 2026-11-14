{{ config(materialized='table') }}

with prenatal as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_referencia,
        faixa_assistencia_prenatal,
        nascidos_vivos_faixa,
        participacao_faixa_prenatal_pct,
        inicio_prenatal_primeiro_trimestre_pct,
        proporcao_cesarea_faixa_pct,
        idade_media_materna_faixa_anos,
        reference_date,
        extracted_at
    from {{ ref('stg_sinasc_assistencia_prenatal_parto') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    p.id_municipio,
    p.nome_municipio,
    p.sigla_uf,
    p.id_uf,
    coalesce(m.nome_uf, p.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    p.ano_referencia,
    p.faixa_assistencia_prenatal,
    p.nascidos_vivos_faixa,
    p.participacao_faixa_prenatal_pct,
    p.inicio_prenatal_primeiro_trimestre_pct,
    p.proporcao_cesarea_faixa_pct,
    p.idade_media_materna_faixa_anos,
    p.reference_date,
    p.extracted_at
from prenatal p
left join municipios m on p.id_municipio = m.id_municipio
