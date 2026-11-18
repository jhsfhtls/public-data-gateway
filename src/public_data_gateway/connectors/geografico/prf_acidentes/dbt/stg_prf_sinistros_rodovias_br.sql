{{ config(materialized='view') }}

select
    cast(cod_sinistro_prf as varchar) as cod_sinistro_prf,
    cast(rodovia_br as varchar) as rodovia_br,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(causa_principal_sinistro as varchar) as causa_principal_sinistro,
    cast(classificacao_gravidade as varchar) as classificacao_gravidade,
    cast(qtd_ocorrencias_trecho as integer) as qtd_ocorrencias_trecho,
    cast(qtd_feridos_leves_graves as integer) as qtd_feridos_leves_graves,
    cast(qtd_obitos_rodovia as integer) as qtd_obitos_rodovia
from {{ source('prf_acidentes', 'prf_sinistros_rodovias_br') }}
where cod_sinistro_prf is not null
