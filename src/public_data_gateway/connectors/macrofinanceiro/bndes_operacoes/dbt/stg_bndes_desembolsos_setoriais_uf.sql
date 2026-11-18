{{ config(materialized='view') }}

select
    cast(cod_desembolso_bndes as varchar) as cod_desembolso_bndes,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(desembolso_anual_mil_brl as double) as desembolso_anual_mil_brl,
    cast(fomento_verde_esg_pct as double) as fomento_verde_esg_pct,
    cast(mpme_participacao_pct as double) as mpme_participacao_pct,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('bndes_operacoes', 'bndes_desembolsos_setoriais_uf') }}
