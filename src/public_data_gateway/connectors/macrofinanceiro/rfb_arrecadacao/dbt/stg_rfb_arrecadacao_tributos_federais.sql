{{ config(materialized='view') }}

select
    cast(cod_arrecadacao_rfb as varchar) as cod_arrecadacao_rfb,
    cast(grupo_tributo_rfb as varchar) as grupo_tributo_rfb,
    cast(regime_recolhimento as varchar) as regime_recolhimento,
    cast(situacao_regularidade_fiscal as varchar) as situacao_regularidade_fiscal,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(arrecadacao_federal_anual_mi_brl as double) as arrecadacao_federal_anual_mi_brl,
    cast(contribuintes_pj_ativos_municipio as bigint) as contribuintes_pj_ativos_municipio,
    cast(inscritos_divida_ativa_pgfn_mi_brl as double) as inscritos_divida_ativa_pgfn_mi_brl
from {{ source('rfb_arrecadacao', 'rfb_arrecadacao_tributos_federais') }}
