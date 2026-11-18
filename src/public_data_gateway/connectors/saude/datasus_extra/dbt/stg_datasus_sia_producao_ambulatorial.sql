{{ config(materialized='view') }}

select
    cast(cod_atendimento_sia as varchar) as cod_atendimento_sia,
    cast(grupo_procedimento_sia as varchar) as grupo_procedimento_sia,
    cast(agravo_notificacao_sinan as varchar) as agravo_notificacao_sinan,
    cast(situacao_vigilancia_sus as varchar) as situacao_vigilancia_sus,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(producao_ambulatorial_sia_12m as bigint) as producao_ambulatorial_sia_12m,
    cast(notificacoes_sinan_12m as bigint) as notificacoes_sinan_12m,
    cast(valor_aprovado_sia_mil_brl as double) as valor_aprovado_sia_mil_brl
from {{ source('datasus_extra', 'datasus_sia_producao_ambulatorial') }}
where cod_atendimento_sia is not null
