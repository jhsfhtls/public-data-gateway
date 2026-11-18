{{ config(materialized='view') }}

select
    cast(cod_pesquisa_ibge as varchar) as cod_pesquisa_ibge,
    cast(eixo_pesquisa_ibge as varchar) as eixo_pesquisa_ibge,
    cast(capacidade_governanca_ti as varchar) as capacidade_governanca_ti,
    cast(situacao_legislacao_urbana as varchar) as situacao_legislacao_urbana,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(indice_capacidade_institucional_munic as double) as indice_capacidade_institucional_munic,
    cast(servidores_estatutarios_municipio as bigint) as servidores_estatutarios_municipio,
    cast(despesa_familiar_media_pof_brl as double) as despesa_familiar_media_pof_brl
from {{ source('ibge_institucional', 'ibge_munic_governanca_municipal') }}
