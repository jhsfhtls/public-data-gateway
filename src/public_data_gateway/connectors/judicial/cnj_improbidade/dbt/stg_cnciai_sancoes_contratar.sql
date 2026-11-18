{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/cnj_improbidade/silver/cnciai_sancoes_contratar.parquet')
)

select
    cast(id_condenacao as varchar) as id_condenacao,
    cast(documento_requerido as varchar) as documento_requerido,
    cast(nome_requerido as varchar) as nome_requerido,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(tribunal_origem as varchar) as tribunal_origem,
    cast(prazo_proibicao_anos as bigint) as prazo_proibicao_anos,
    cast(data_fim_vigencia as varchar) as data_fim_vigencia
from source
