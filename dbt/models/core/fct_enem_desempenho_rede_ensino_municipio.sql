{{ config(materialized='table') }}

with redes as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        regiao,
        ano_edicao_enem,
        dependencia_administrativa_escola,
        categoria_escola_enem,
        concluintes_participantes_rede,
        nota_media_objetivas_rede,
        nota_media_matematica_rede,
        nota_media_redacao_rede,
        reference_date,
        extracted_at
    from {{ ref('stg_enem_desempenho_rede_ensino') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    r.id_municipio,
    r.nome_municipio,
    r.sigla_uf,
    r.id_uf,
    coalesce(m.nome_uf, r.sigla_uf) as nome_uf,
    coalesce(m.regiao, r.regiao) as regiao,
    r.ano_edicao_enem,
    r.dependencia_administrativa_escola,
    r.categoria_escola_enem,
    r.concluintes_participantes_rede,
    r.nota_media_objetivas_rede,
    r.nota_media_matematica_rede,
    r.nota_media_redacao_rede,
    r.reference_date,
    r.extracted_at
from redes r
left join municipios m on r.id_municipio = m.id_municipio
