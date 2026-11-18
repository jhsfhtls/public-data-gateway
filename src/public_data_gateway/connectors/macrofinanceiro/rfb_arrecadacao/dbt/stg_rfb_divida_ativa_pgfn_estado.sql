{{ config(materialized='view') }}

select
    cast(cod_divida_pgfn as varchar) as cod_divida_pgfn,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(estoque_divida_ativa_uf_mi_brl as double) as estoque_divida_ativa_uf_mi_brl,
    cast(devedores_previdenciarios_pj as bigint) as devedores_previdenciarios_pj,
    cast(recuperacao_creditos_anual_mi_brl as double) as recuperacao_creditos_anual_mi_brl,
    cast(indice_recuperacao_pgfn_pct as double) as indice_recuperacao_pgfn_pct
from {{ source('rfb_arrecadacao', 'rfb_divida_ativa_pgfn_estado') }}
