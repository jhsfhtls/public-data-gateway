{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cno/silver/cno_vinculos_responsaveis.parquet')
)

select
    cast(inscricao_cno as varchar) as inscricao_cno,
    cast(documento_responsavel as varchar) as documento_responsavel,
    cast(nome_responsavel as varchar) as nome_responsavel,
    cast(qualificacao_responsavel as varchar) as qualificacao_responsavel,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(cnae_7_construtora as varchar) as cnae_7_construtora,
    cast(data_inicio_vinculo as varchar) as data_inicio_vinculo
from source
