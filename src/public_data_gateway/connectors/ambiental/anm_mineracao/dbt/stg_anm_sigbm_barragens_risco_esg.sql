{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_barragens_sigbm as bigint) as qtd_barragens_sigbm,
    cast(barragens_paebm_emergencia as bigint) as barragens_paebm_emergencia,
    cast(indice_conformidade_sigbm_pct as double) as indice_conformidade_sigbm_pct,
    cast(nivel_dano_potencial_associado as varchar) as nivel_dano_potencial_associado
from {{ source('anm_mineracao', 'anm_sigbm_barragens_risco_esg') }}
where id_municipio is not null
