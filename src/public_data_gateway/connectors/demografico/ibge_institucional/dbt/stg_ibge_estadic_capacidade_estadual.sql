{{ config(materialized='view') }}

select
    cast(cod_estadic_ibge as varchar) as cod_estadic_ibge,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(efetivo_estatutario_estadual_uf as bigint) as efetivo_estatutario_estadual_uf,
    cast(politicas_habitacao_saneamento_ativas as bigint) as politicas_habitacao_saneamento_ativas,
    cast(indice_governanca_estadual_estadic as double) as indice_governanca_estadual_estadic,
    cast(cobertura_sistema_protecao_civil_pct as double) as cobertura_sistema_protecao_civil_pct
from {{ source('ibge_institucional', 'ibge_estadic_capacidade_estadual') }}
