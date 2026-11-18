{{ config(materialized='view') }}

select
    cast(cod_ato_diario as varchar) as cod_ato_diario,
    cast(tema_ato_normativo as varchar) as tema_ato_normativo,
    cast(poder_publico_emissor as varchar) as poder_publico_emissor,
    cast(situacao_extracao_nlp as varchar) as situacao_extracao_nlp,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(qtd_atos_publicados_mes as bigint) as qtd_atos_publicados_mes,
    cast(valor_extratos_citados_mil_brl as double) as valor_extratos_citados_mil_brl,
    cast(competencia_diario as varchar) as competencia_diario
from {{ source('querido_diario_okbr', 'okbr_diarios_licitacoes_dispensas') }}
