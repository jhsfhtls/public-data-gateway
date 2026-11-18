{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(rodovia_br_principal as varchar) as rodovia_br_principal,
    cast(total_sinistros_prf_ano as integer) as total_sinistros_prf_ano,
    cast(total_feridos_prf_ano as integer) as total_feridos_prf_ano,
    cast(total_obitos_prf_ano as integer) as total_obitos_prf_ano,
    cast(taxa_letalidade_sinistro_pct as double) as taxa_letalidade_sinistro_pct
from {{ source('prf_acidentes', 'prf_sinistralidade_resumo_municipal') }}
where id_municipio is not null
