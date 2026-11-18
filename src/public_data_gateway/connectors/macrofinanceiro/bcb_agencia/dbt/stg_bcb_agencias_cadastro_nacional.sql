{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/bcb_agencia/silver/agencias_cadastro_nacional.parquet')
)

select
    cast(cnpj_14_agencia as varchar) as cnpj_14_agencia,
    cast(cnpj_8_if as varchar) as cnpj_8_if,
    cast(nome_instituicao as varchar) as nome_instituicao,
    cast(segmento_if as varchar) as segmento_if,
    cast(nome_agencia as varchar) as nome_agencia,
    cast(tipo_instalacao as varchar) as tipo_instalacao,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bairro as varchar) as bairro,
    cast(cep_8 as varchar) as cep_8,
    cast(fone_ddd as varchar) as fone_ddd,
    cast(data_inicio_operacao as varchar) as data_inicio_operacao
from source
