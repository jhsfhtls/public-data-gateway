{{ config(materialized='table') }}

with produtividade as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        semestre_referencia,
        atribuicao_cartorial,
        serventias_atribuicao,
        atos_praticados_semestre,
        certidoes_expedidas_semestre,
        participacao_atos_municipio_pct,
        proporcao_atos_eletronicos_e_notariado_pct,
        prazo_medio_emissao_dias,
        reference_date,
        extracted_at
    from {{ ref('stg_justica_aberta_produtividade_atribuicao') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('stg_ibge_municipios') }}
)

select
    p.id_municipio,
    p.nome_municipio,
    p.sigla_uf,
    p.id_uf,
    coalesce(m.nome_uf, p.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    p.semestre_referencia,
    p.atribuicao_cartorial,
    p.serventias_atribuicao,
    p.atos_praticados_semestre,
    p.certidoes_expedidas_semestre,
    p.participacao_atos_municipio_pct,
    p.proporcao_atos_eletronicos_e_notariado_pct,
    p.prazo_medio_emissao_dias,
    p.reference_date,
    p.extracted_at
from produtividade p
left join municipios m on p.id_municipio = m.id_municipio
