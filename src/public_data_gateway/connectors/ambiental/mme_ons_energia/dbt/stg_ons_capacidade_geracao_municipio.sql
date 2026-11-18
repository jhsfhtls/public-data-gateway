{{ config(materialized='view') }}

select
    cast(cod_usina_ons as varchar) as cod_usina_ons,
    cast(ceg_aneel as varchar) as ceg_aneel,
    cast(nome_usina_ons as varchar) as nome_usina_ons,
    cast(fonte_geracao_sin as varchar) as fonte_geracao_sin,
    cast(subsistema_sin as varchar) as subsistema_sin,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(capacidade_instalada_mw as double) as capacidade_instalada_mw,
    cast(geracao_media_verificada_mwmed as double) as geracao_media_verificada_mwmed
from {{ source('mme_ons_energia', 'ons_capacidade_geracao_municipio') }}
where cod_usina_ons is not null
