{{ config(materialized='table') }}

with cov as (
    select * from {{ ref('stg_bcb_agencias_cobertura_municipal') }}
),

pa as (
    select * from {{ ref('stg_bcb_agencias_postos_atendimento') }}
),

inc as (
    select * from {{ ref('stg_bcb_agencias_inclusao_financeira') }}
),

cad_agg as (
    select
        id_municipio,
        count(*) as qtd_dependencias_amostra_cadastral
    from {{ ref('stg_bcb_agencias_cadastro_nacional') }}
    group by id_municipio
)

select
    c.id_municipio,
    c.sigla_uf,
    c.nome_municipio,
    c.total_agencias_fisicas,
    c.total_bancos_presentes,
    c.agencias_por_100k_hab,
    c.percentual_bancos_publicos_pct,
    c.percentual_bancos_privados_pct,
    p.qtd_postos_pa,
    p.qtd_postos_pae_eletronico,
    p.qtd_unidades_administrativas,
    p.cobertura_autoatendimento_24h_pct,
    i.possui_agencia_fisica,
    i.dependencia_correspondente_bancario_pct,
    i.distancia_media_agencia_km,
    i.status_inclusao_financeira,
    coalesce(ca.qtd_dependencias_amostra_cadastral, 0) as qtd_dependencias_amostra_cadastral
from cov c
left join pa p on c.id_municipio = p.id_municipio
left join inc i on c.id_municipio = i.id_municipio
left join cad_agg ca on c.id_municipio = ca.id_municipio
