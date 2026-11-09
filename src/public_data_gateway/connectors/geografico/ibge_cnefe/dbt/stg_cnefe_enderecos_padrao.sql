{{ config(materialized='view') }}

select
    id_municipio,
    sigla_uf,
    nome_municipio,
    codigo_distrito,
    codigo_subdistrito,
    codigo_setor_censitario,
    seq_endereco,
    cep_8,
    tipo_logradouro,
    titulo_patente,
    nome_logradouro,
    numero_predial,
    complemento,
    bairro,
    especie_edificacao,
    reference_date,
    extracted_at
from {{ source('ibge_cnefe_silver', 'cnefe_enderecos_padrao') }}
