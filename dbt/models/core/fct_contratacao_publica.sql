{{ config(materialized='table') }}

with contratacoes as (
    select * from {{ ref('stg_pncp_contratacoes') }}
),

modalidades as (
    select
        modalidade_id,
        regime_competicao
    from {{ ref('dim_modalidade_licitacao') }}
)

select
    c.numero_controle_pncp,
    c.ano_compra,
    c.sequencial_compra,
    c.numero_compra,
    c.numero_processo,
    c.cnpj_orgao,
    c.razao_social_orgao,
    c.poder_id,
    c.esfera_id,
    c.codigo_unidade_orgao,
    c.nome_unidade_orgao,
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.modalidade_id,
    c.modalidade_nome,
    coalesce(m.regime_competicao, 'COMPETITIVA') as regime_competicao,
    c.modo_disputa_id,
    c.modo_disputa_nome,
    c.tipo_instrumento_convocatorio_codigo,
    c.tipo_instrumento_convocatorio_nome,
    c.objeto_compra,
    c.is_srp,
    c.valor_total_estimado,
    c.valor_total_homologado,
    coalesce(c.valor_total_estimado - c.valor_total_homologado, 0.0) as economia_estimada_homologada_absoluta,
    case
        when c.valor_total_estimado > 0 and c.valor_total_homologado is not null
        then round(((c.valor_total_estimado - c.valor_total_homologado) / c.valor_total_estimado) * 100.0, 2)
        else 0.0
    end as taxa_economia_licitatoria_pct,
    c.situacao_compra_id,
    c.situacao_compra_nome,
    c.data_publicacao_pncp,
    c.data_abertura_proposta,
    c.data_encerramento_proposta,
    c.link_sistema_origem
from contratacoes c
left join modalidades m
    on c.modalidade_id = m.modalidade_id
