{{ config(materialized='table') }}

with bal as (
    select * from {{ ref('stg_estban_balancete_municipal') }}
),

cred as (
    select * from {{ ref('stg_estban_carteira_credito_setorial') }}
),

dep as (
    select * from {{ ref('stg_estban_captacoes_depositos') }}
),

inst_agg as (
    select
        id_municipio,
        count(*) as qtd_bancos_lideres_amostra,
        round(sum(share_credito_local_pct), 2) as top2_concentracao_credito_pct
    from {{ ref('stg_estban_instituicoes_municipio') }}
    group by id_municipio
)

select
    b.id_municipio,
    b.sigla_uf,
    b.nome_municipio,
    b.data_base_yyyymm,
    b.qtd_instituicoes_atuantes,
    b.qtd_agencias_ativas,
    b.ativo_total_agencias_brl,
    b.operacoes_credito_total_brl,
    b.depositos_totais_brl,
    b.relacao_credito_deposito_pct,
    b.indice_concentracao_hhi_local,
    c.emprestimos_titulos_descontados_brl,
    c.financiamentos_gerais_brl,
    c.financiamento_agropecuario_rural_brl,
    c.financiamento_imobiliario_habitacional_brl,
    c.provisao_credito_liquidacao_duvidosa_brl,
    c.taxa_provisao_inadimplencia_pct,
    d.depositos_vista_brl,
    d.depositos_poupanca_brl,
    d.depositos_prazo_cdb_rdb_brl,
    d.depositos_governo_publico_brl,
    d.liquidez_imediata_caixa_brl,
    coalesce(i.qtd_bancos_lideres_amostra, 0) as qtd_bancos_lideres_amostra,
    coalesce(i.top2_concentracao_credito_pct, 0.0) as top2_concentracao_credito_pct
from bal b
left join cred c on b.id_municipio = c.id_municipio
left join dep d on b.id_municipio = d.id_municipio
left join inst_agg i on b.id_municipio = i.id_municipio
