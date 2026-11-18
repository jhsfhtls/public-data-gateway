{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(corretoras_ativas_susep as bigint) as corretoras_ativas_susep,
    cast(agentes_corretor_pj as bigint) as agentes_corretor_pj,
    cast(indice_solvencia_regulatoria_pct as double) as indice_solvencia_regulatoria_pct,
    cast(indice_cobertura_provisoes_pct as double) as indice_cobertura_provisoes_pct
from {{ source('susep_seguros', 'susep_corretoras_solvencia_mercado') }}
where id_municipio is not null
