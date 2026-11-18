{{ config(materialized='view') }}

select
    cast(cod_registro_s2id as varchar) as cod_registro_s2id,
    cast(cobrade_codigo as varchar) as cobrade_codigo,
    cast(tipologia_desastre as varchar) as tipologia_desastre,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(status_reconhecimento as varchar) as status_reconhecimento,
    cast(populacao_afetada as integer) as populacao_afetada,
    cast(danos_materiais_mil_brl as double) as danos_materiais_mil_brl,
    cast(data_ocorrencia as varchar) as data_ocorrencia
from {{ source('sedec_desastres', 'sedec_ocorrencias_desastres') }}
where cod_registro_s2id is not null
