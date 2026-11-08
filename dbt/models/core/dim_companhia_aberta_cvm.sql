with source as (
    select * from {{ ref('stg_cvm_cia_aberta') }}
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
    data_registro_cvm,
    data_constituicao,
    tipo_mercado,
    sigla_uf,
    id_municipio_ibge,
    case
        when situacao_registro = 'ATIVO' then true
        else false
    end as is_registro_ativo,
    ingestion_timestamp
from source
