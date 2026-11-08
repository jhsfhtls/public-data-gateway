{{ config(materialized='table') }}

with stg_orgaos as (
    select * from {{ ref('stg_pncp_orgaos') }}
),

municipios as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        regiao as nome_regiao
    from {{ ref('dim_municipio') }}
)

select
    o.cnpj_orgao,
    o.razao_social,
    o.esfera_id,
    case
        when o.esfera_id = 'F' then 'FEDERAL'
        when o.esfera_id = 'E' then 'ESTADUAL'
        when o.esfera_id = 'M' then 'MUNICIPAL'
        when o.esfera_id = 'D' then 'DISTRITAL'
        else 'OUTROS'
    end as esfera_descricao,
    o.poder_id,
    case
        when o.poder_id = 'E' then 'EXECUTIVO'
        when o.poder_id = 'L' then 'LEGISLATIVO'
        when o.poder_id = 'J' then 'JUDICIARIO'
        else 'MINISTERIO_PUBLICO_DEFENSORIA'
    end as poder_descricao,
    o.id_municipio,
    coalesce(m.nome_municipio, o.nome_municipio) as nome_municipio,
    coalesce(m.sigla_uf, o.sigla_uf) as sigla_uf,
    m.nome_regiao,
    o.total_compras_registradas,
    o.total_contratos_registrados
from stg_orgaos o
left join municipios m
    on o.id_municipio = m.id_municipio
