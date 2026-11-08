with source as (
    select * from {{ source('cvm_dfp_itr', 'cvm_cia_aberta_cadastral') }}
)

select
    codigo_cvm,
    cnpj_14,
    cnpj_raiz,
    razao_social,
    nome_comercial,
    setor_atividade,
    categoria_registro,
    situacao_registro,
    cast(data_registro_cvm as date) as data_registro_cvm,
    cast(data_constituicao as date) as data_constituicao,
    tipo_mercado,
    sigla_uf,
    id_municipio_ibge,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp
from source
