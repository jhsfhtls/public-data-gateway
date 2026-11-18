{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cno/silver/cno_obras_cadastradas.parquet')
)

select
    cast(inscricao_cno as varchar) as inscricao_cno,
    cast(documento_responsavel as varchar) as documento_responsavel,
    cast(qualificacao_responsavel as varchar) as qualificacao_responsavel,
    cast(nome_responsavel as varchar) as nome_responsavel,
    cast(nome_empresarial_obra as varchar) as nome_empresarial_obra,
    cast(situacao_cno as varchar) as situacao_cno,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(cep_8 as varchar) as cep_8,
    cast(cnae_7_obra as varchar) as cnae_7_obra,
    cast(area_total_m2 as double) as area_total_m2,
    cast(unidade_medida as varchar) as unidade_medida,
    cast(data_inicio_obra as varchar) as data_inicio_obra,
    cast(data_situacao as varchar) as data_situacao
from source
