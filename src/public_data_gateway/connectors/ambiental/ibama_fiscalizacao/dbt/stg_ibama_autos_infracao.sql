{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/ibama_fiscalizacao/silver/ibama_autos_infracao.parquet')
)

select
    cast(num_auto_infracao as varchar) as num_auto_infracao,
    cast(documento_infrator as varchar) as documento_infrator,
    cast(nome_infrator as varchar) as nome_infrator,
    cast(tipo_infracao as varchar) as tipo_infracao,
    cast(valor_multa_brl as double) as valor_multa_brl,
    cast(situacao_auto as varchar) as situacao_auto,
    cast(enquadramento_legal as varchar) as enquadramento_legal,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(data_hora_auto as varchar) as data_hora_auto,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude
from source
