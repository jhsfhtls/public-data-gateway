{{ config(materialized='view') }}

select
    cast(cod_processo_anm as varchar) as cod_processo_anm,
    cast(substancia_mineral as varchar) as substancia_mineral,
    cast(fase_processo_sigmine as varchar) as fase_processo_sigmine,
    cast(situacao_barragem_sigbm as varchar) as situacao_barragem_sigbm,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(area_titulada_hectares as double) as area_titulada_hectares,
    cast(arrecadacao_cfem_anual_mil_brl as double) as arrecadacao_cfem_anual_mil_brl,
    cast(ano_referencia as bigint) as ano_referencia
from {{ source('anm_mineracao', 'anm_sigmine_processos_municipio') }}
where cod_processo_anm is not null
