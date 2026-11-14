{{ config(materialized='table') }}

WITH base AS (
    SELECT
        i.pack_id,
        i.pack_name,
        i.sigla_uf,
        i.regiao,
        i.total_municipios_uf,
        i.total_estabelecimentos_ativos,
        i.valor_homologado_compras_uf,
        i.total_sancoes_ativas_uf,
        i.score_dinamismo_b2b_territorial,
        i.score_oportunidade_pack_uf,
        COALESCE(m.valor_multas_ativas_uf, 0.0) AS valor_multas_ativas_uf,
        COALESCE(m.selic_meta_fim_mes_pct, 14.25) AS selic_meta_fim_mes_pct,
        COALESCE(m.ipca_acumulado_12m_pct, 5.06) AS ipca_acumulado_12m_pct,
        i.reference_period
    FROM {{ ref('mart_solution_packs_insights_brasil') }} i
    LEFT JOIN {{ ref('mart_cross_domain_territorio_b2b_macro_brasil') }} m
        ON i.sigla_uf = m.sigla_uf
)

SELECT
    pack_id,
    pack_name,
    sigla_uf,
    regiao,
    total_municipios_uf,
    score_oportunidade_pack_uf,
    CASE
        WHEN pack_id IN ('P04', 'P07') THEN 'Exposicao a sancoes ativas e risco cadastral na cadeia de fornecedores'
        WHEN pack_id = 'P05' THEN 'Compressao de margem por juros reais elevados (Selic/IPCA) sem deflacionamento'
        WHEN pack_id = 'P09' THEN 'Risco climatico, hidrico e conformidade fundiaria (CAR/IBAMA) no territorio'
        WHEN pack_id IN ('P01', 'P02') THEN 'Dados publicos e corporativos fragmentados sem camada analitica ou IA agêntica'
        ELSE 'Baixa rastreabilidade operacional e necessidade de padroes de confiabilidade'
    END AS dor_latente_principal,
    CASE
        WHEN pack_id IN ('P04', 'P07') THEN CAST(total_sancoes_ativas_uf AS VARCHAR) || ' sancoes ativas na UF'
        WHEN pack_id = 'P05' THEN 'Selic ' || CAST(selic_meta_fim_mes_pct AS VARCHAR) || '% / IPCA ' || CAST(ipca_acumulado_12m_pct AS VARCHAR) || '%'
        ELSE CAST(total_estabelecimentos_ativos AS VARCHAR) || ' estabelecimentos ativos mapeados'
    END AS metrica_dor_quantificada,
    ROUND(
        CASE
            WHEN pack_id IN ('P04', 'P07') THEN GREATEST(valor_multas_ativas_uf * 0.15, valor_homologado_compras_uf * 0.01)
            WHEN pack_id = 'P05' THEN valor_homologado_compras_uf * (ipca_acumulado_12m_pct / 100.0) * 0.20
            ELSE GREATEST(valor_homologado_compras_uf * 0.025, 150000.0)
        END,
        2
    ) AS impacto_financeiro_estimado_brl,
    CASE
        WHEN pack_id IN ('P01', 'P02') THEN 'PoC 14d: Ingestao Medallion + Agente MCP consultando Feature Store municipal'
        WHEN pack_id IN ('P04', 'P07') THEN 'PoC 14d: Triagem automatizada de CNPJ/QSA + radar CEIS/CNEP em lote'
        WHEN pack_id = 'P05' THEN 'PoC 14d: Painel de deflacionamento IPCA/IGPM com ASOF joins em contratos'
        WHEN pack_id = 'P09' THEN 'PoC 14d: Cruzamento geoespacial CAR/ANA/5G para carteira agroindustrial'
        ELSE 'PoC 14d: Auditoria de contratos de dados ODCS e scorecard de qualidade'
    END AS escopo_poc_14d_sugerido,
    reference_period
FROM base
ORDER BY pack_id ASC, impacto_financeiro_estimado_brl DESC, sigla_uf ASC
