{{ config(materialized='view') }}

with source as (
    select * from {{ source('pncp_contratacoes', 'pncp_contratos') }}
),

renamed as (
    select
        trim(numero_controle_pncp) as numero_controle_pncp,
        trim(numero_controle_pncp_compra) as numero_controle_pncp_compra,
        cast(ano_contrato as integer) as ano_contrato,
        cast(sequencial_contrato as integer) as sequencial_contrato,
        trim(numero_contrato_empenho) as numero_contrato_empenho,
        trim(processo) as numero_processo,
        lpad(trim(cnpj_orgao), 14, '0') as cnpj_orgao,
        trim(razao_social_orgao) as razao_social_orgao,
        upper(trim(esfera_id)) as esfera_id,
        upper(trim(poder_id)) as poder_id,
        trim(codigo_unidade_orgao) as codigo_unidade_orgao,
        trim(nome_unidade_orgao) as nome_unidade_orgao,
        lpad(trim(id_municipio), 7, '0') as id_municipio,
        trim(nome_municipio) as nome_municipio,
        upper(trim(sigla_uf)) as sigla_uf,
        cast(tipo_contrato_id as integer) as tipo_contrato_id,
        trim(tipo_contrato_nome) as tipo_contrato_nome,
        cast(categoria_processo_id as integer) as categoria_processo_id,
        trim(categoria_processo_nome) as categoria_processo_nome,
        upper(trim(tipo_pessoa_fornecedor)) as tipo_pessoa_fornecedor,
        trim(ni_fornecedor) as ni_fornecedor,
        trim(nome_fornecedor) as nome_fornecedor,
        upper(trim(codigo_pais_fornecedor)) as codigo_pais_fornecedor,
        trim(objeto_contrato) as objeto_contrato,
        cast(valor_inicial as double) as valor_inicial,
        cast(valor_global as double) as valor_global,
        cast(valor_parcela as double) as valor_parcela,
        cast(valor_acumulado as double) as valor_acumulado,
        cast(numero_parcelas as integer) as numero_parcelas,
        data_assinatura,
        data_vigencia_inicio,
        data_vigencia_fim,
        data_publicacao_pncp
    from source
)

select * from renamed
