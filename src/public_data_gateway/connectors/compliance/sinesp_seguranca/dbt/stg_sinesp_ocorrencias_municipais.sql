{{ config(materialized='view') }}

select
    cast(cod_ocorrencia_sinesp as varchar) as cod_ocorrencia_sinesp,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(tipo_crime as varchar) as tipo_crime,
    cast(qtd_ocorrencias as bigint) as qtd_ocorrencias,
    cast(taxa_por_100k_hab as double) as taxa_por_100k_hab,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('sinesp_seguranca', 'sinesp_ocorrencias_municipais') }}
where cod_ocorrencia_sinesp is not null
  and id_municipio is not null
