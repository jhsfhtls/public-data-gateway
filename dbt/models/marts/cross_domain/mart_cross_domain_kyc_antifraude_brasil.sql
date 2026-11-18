{{ config(materialized='table') }}

WITH uf_base AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        COUNT(*) AS total_municipios_uf,
        ROUND(AVG(feat_score_propensao_b2b_composto), 2) AS avg_propensao_b2b_uf
    FROM {{ ref('feature_store_cross_domain_municipio') }}
    GROUP BY sigla_uf
),
cnpj_minha_receita AS (
    SELECT sigla_uf, COUNT(*) AS total_cnpjs_minha_receita
    FROM {{ ref('fct_minha_receita_cnpjs') }}
    GROUP BY sigla_uf
),
cnpj_cnpja AS (
    SELECT sigla_uf, COUNT(*) AS total_cnpjs_cnpja
    FROM {{ ref('fct_cnpja_open_offices') }}
    GROUP BY sigla_uf
),
cnpj_receitaws AS (
    SELECT sigla_uf, COUNT(*) AS total_cnpjs_receitaws
    FROM {{ ref('fct_receitaws_cnpjs') }}
    GROUP BY sigla_uf
),
cep_viacep AS (
    SELECT sigla_uf, COUNT(*) AS total_ceps_viacep
    FROM {{ ref('dim_viacep_enderecos') }}
    GROUP BY sigla_uf
),
cep_opencep AS (
    SELECT sigla_uf, COUNT(*) AS total_ceps_opencep
    FROM {{ ref('dim_opencep_enderecos') }}
    GROUP BY sigla_uf
),
geo_nominatim AS (
    SELECT sigla_uf, COUNT(*) AS total_geocodes_nominatim
    FROM {{ ref('dim_nominatim_geocodes') }}
    GROUP BY sigla_uf
),
rdap_summary AS (
    SELECT COUNT(*) AS total_rdap_dominios_br
    FROM {{ ref('fct_rdap_dominios_br') }}
),
gleif_summary AS (
    SELECT COUNT(*) AS total_gleif_entidades
    FROM {{ ref('dim_gleif_lei_entidades') }}
),
bgp_summary AS (
    SELECT COUNT(*) AS total_bgp_ips_monitorados
    FROM {{ ref('fct_bgpview_ip_asn_intel') }}
),
b2b_summary AS (
    SELECT COUNT(*) AS total_perfis_b2b
    FROM {{ ref('dim_company_enrich_perfis_b2b') }}
),
pgfn_uf AS (
    SELECT
        sigla_uf,
        SUM(total_inscricoes_ativas) AS total_inscricoes_pgfn_uf,
        ROUND(SUM(valor_total_consolidado_brl), 2) AS valor_divida_ativa_pgfn_uf_brl
    FROM {{ ref('mart_divida_ativa_pgfn_brasil') }}
    GROUP BY sigla_uf
),
cnciai_uf AS (
    SELECT
        sigla_uf,
        SUM(total_condenacoes_ativas) AS total_condenacoes_cnciai_uf,
        ROUND(SUM(valor_total_ressarcimento_multa_brl), 2) AS valor_ressarcimento_cnciai_uf_brl
    FROM {{ ref('mart_improbidade_administrativa_cnciai_brasil') }}
    GROUP BY sigla_uf
)
SELECT
    u.sigla_uf,
    u.regiao,
    u.total_municipios_uf,
    u.avg_propensao_b2b_uf,
    COALESCE(mr.total_cnpjs_minha_receita, 0) AS total_cnpjs_minha_receita,
    COALESCE(cn.total_cnpjs_cnpja, 0) AS total_cnpjs_cnpja,
    COALESCE(rw.total_cnpjs_receitaws, 0) AS total_cnpjs_receitaws,
    COALESCE(vc.total_ceps_viacep, 0) AS total_ceps_viacep,
    COALESCE(oc.total_ceps_opencep, 0) AS total_ceps_opencep,
    COALESCE(nm.total_geocodes_nominatim, 0) AS total_geocodes_nominatim,
    rd.total_rdap_dominios_br,
    gl.total_gleif_entidades,
    bg.total_bgp_ips_monitorados,
    b2.total_perfis_b2b,
    COALESCE(pg.total_inscricoes_pgfn_uf, 0) AS total_inscricoes_pgfn_uf,
    COALESCE(pg.valor_divida_ativa_pgfn_uf_brl, 0.0) AS valor_divida_ativa_pgfn_uf_brl,
    COALESCE(ci.total_condenacoes_cnciai_uf, 0) AS total_condenacoes_cnciai_uf,
    COALESCE(ci.valor_ressarcimento_cnciai_uf_brl, 0.0) AS valor_ressarcimento_cnciai_uf_brl
FROM uf_base u
LEFT JOIN cnpj_minha_receita mr ON u.sigla_uf = mr.sigla_uf
LEFT JOIN cnpj_cnpja cn ON u.sigla_uf = cn.sigla_uf
LEFT JOIN cnpj_receitaws rw ON u.sigla_uf = rw.sigla_uf
LEFT JOIN cep_viacep vc ON u.sigla_uf = vc.sigla_uf
LEFT JOIN cep_opencep oc ON u.sigla_uf = oc.sigla_uf
LEFT JOIN geo_nominatim nm ON u.sigla_uf = nm.sigla_uf
LEFT JOIN pgfn_uf pg ON u.sigla_uf = pg.sigla_uf
LEFT JOIN cnciai_uf ci ON u.sigla_uf = ci.sigla_uf
CROSS JOIN rdap_summary rd
CROSS JOIN gleif_summary gl
CROSS JOIN bgp_summary bg
CROSS JOIN b2b_summary b2
