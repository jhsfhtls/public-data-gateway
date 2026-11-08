{{ config(materialized='table') }}

with contratos as (
    select * from {{ ref('stg_pncp_contratos') }}
)

select
    c.numero_controle_pncp,
    c.numero_controle_pncp_compra,
    c.ano_contrato,
    c.sequencial_contrato,
    c.numero_contrato_empenho,
    c.numero_processo,
    c.cnpj_orgao,
    c.razao_social_orgao,
    c.esfera_id,
    c.poder_id,
    c.codigo_unidade_orgao,
    c.nome_unidade_orgao,
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.tipo_contrato_id,
    c.tipo_contrato_nome,
    c.categoria_processo_id,
    c.categoria_processo_nome,
    c.tipo_pessoa_fornecedor,
    c.ni_fornecedor,
    case
        when length(c.ni_fornecedor) = 14 then substring(c.ni_fornecedor, 1, 8)
        else null
    end as cnpj_basico_fornecedor,
    c.nome_fornecedor,
    c.codigo_pais_fornecedor,
    c.objeto_contrato,
    c.valor_inicial,
    c.valor_global,
    c.valor_parcela,
    c.valor_acumulado,
    c.numero_parcelas,
    c.data_assinatura,
    c.data_vigencia_inicio,
    c.data_vigencia_fim,
    c.data_publicacao_pncp
from contratos c
