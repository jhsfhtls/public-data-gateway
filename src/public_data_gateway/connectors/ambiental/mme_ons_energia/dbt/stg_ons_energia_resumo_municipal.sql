{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(subsistema_sin as varchar) as subsistema_sin,
    cast(potencia_instalada_sin_mw as double) as potencia_instalada_sin_mw,
    cast(geracao_renovavel_sin_pct as double) as geracao_renovavel_sin_pct,
    cast(carga_energia_mwmed as double) as carga_energia_mwmed,
    cast(fator_capacidade_renovavel_pct as double) as fator_capacidade_renovavel_pct
from {{ source('mme_ons_energia', 'ons_energia_resumo_municipal') }}
where id_municipio is not null
