{{ config(materialized='view') }}

with source as (
    select * from {{ source('pncp_contratacoes', 'pncp_contratacoes') }}
),

renamed as (
    select
        trim(numero_controle_pncp) as numero_controle_pncp,
        cast(ano_compra as integer) as ano_compra,
        cast(sequencial_compra as integer) as sequencial_compra,
        trim(numero_compra) as numero_compra,
        trim(processo) as numero_processo,
        lpad(trim(cnpj_orgao), 14, '0') as cnpj_orgao,
        trim(razao_social_orgao) as razao_social_orgao,
        upper(trim(poder_id)) as poder_id,
        upper(trim(esfera_id)) as esfera_id,
        trim(codigo_unidade_orgao) as codigo_unidade_orgao,
        trim(nome_unidade_orgao) as nome_unidade_orgao,
        lpad(trim(id_municipio), 7, '0') as id_municipio,
        trim(nome_municipio) as nome_municipio,
        upper(trim(sigla_uf)) as sigla_uf,
        cast(modalidade_id as integer) as modalidade_id,
        trim(modalidade_nome) as modalidade_nome,
        cast(modo_disputa_id as integer) as modo_disputa_id,
        trim(modo_disputa_nome) as modo_disputa_nome,
        cast(tipo_instrumento_convocatorio_codigo as integer) as tipo_instrumento_convocatorio_codigo,
        trim(tipo_instrumento_convocatorio_nome) as tipo_instrumento_convocatorio_nome,
        trim(objeto_compra) as objeto_compra,
        trim(informacao_complementar) as informacao_complementar,
        cast(srp_flag as boolean) as is_srp,
        cast(valor_total_estimado as double) as valor_total_estimado,
        cast(valor_total_homologado as double) as valor_total_homologado,
        cast(situacao_compra_id as integer) as situacao_compra_id,
        trim(situacao_compra_nome) as situacao_compra_nome,
        data_publicacao_pncp,
        data_abertura_proposta,
        data_encerramento_proposta,
        trim(link_sistema_origem) as link_sistema_origem
    from source
)

select * from renamed
