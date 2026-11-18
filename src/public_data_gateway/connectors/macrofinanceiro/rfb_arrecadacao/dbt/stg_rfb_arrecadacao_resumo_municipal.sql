{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(arrecadacao_federal_total_mi_brl as double) as arrecadacao_federal_total_mi_brl,
    cast(contribuintes_pj_ativos_municipio as bigint) as contribuintes_pj_ativos_municipio,
    cast(inscritos_divida_ativa_pgfn_mi_brl as double) as inscritos_divida_ativa_pgfn_mi_brl,
    cast(repasses_fpm_anual_mi_brl as double) as repasses_fpm_anual_mi_brl,
    cast(crescimento_real_arrecadacao_pct as double) as crescimento_real_arrecadacao_pct
from {{ source('rfb_arrecadacao', 'rfb_arrecadacao_resumo_municipal') }}
