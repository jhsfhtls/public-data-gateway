{{ config(materialized='table') }}

WITH terra AS (
    SELECT
        id_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        bioma_predominante,
        score_preservacao_conformidade_florestal,
        score_pressao_desmatamento_deter,
        score_composto_risco_esg_territorial,
        reference_date
    FROM {{ ref('mart_desmatamento_terrabrasilis_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

sicar AS (
    SELECT
        id_municipio,
        qtd_imoveis_car_ativos,
        imoveis_livres_embargo_ibama_pct,
        score_conformidade_sicar
    FROM {{ ref('mart_conformidade_car_sicar_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

zarc AS (
    SELECT
        id_municipio,
        qtd_culturas_zoneadas,
        adesao_zarc_credito_custeio_pct,
        score_aptidao_zarc
    FROM {{ ref('mart_aptidao_agroclimatica_zarc_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

hidro AS (
    SELECT
        id_municipio,
        vazao_media_global_m3s,
        precipitacao_acumulada_anual_mm,
        score_seguranca_hidrica_hidroweb
    FROM {{ ref('mart_seguranca_hidrica_hidroweb_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

telecom AS (
    SELECT
        id_municipio,
        populacao_coberta_5g_pct,
        participacao_fibra_ftth_pct,
        score_maturidade_5g,
        score_conectividade_digital
    FROM {{ ref('mart_conectividade_digital_telecom_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

ibama AS (
    SELECT
        id_municipio,
        qtd_autos_infracao_ibama,
        qtd_embargos_ativos_ibama,
        area_embargada_ibama_ha,
        valor_multas_ibama_brl,
        score_conformidade_ibama
    FROM {{ ref('mart_fiscalizacao_embargos_ibama_brasil') }}
),

sicor AS (
    SELECT
        id_municipio,
        qtd_contratos_sicor,
        valor_total_sicor_brl,
        cobertura_proagro_sicor_pct,
        participacao_abc_renovagro_pct,
        score_dinamismo_credito_rural_sicor
    FROM {{ ref('mart_credito_rural_proagro_sicor_brasil') }}
),

joined AS (
    SELECT
        t.id_municipio,
        t.nome_municipio,
        t.sigla_uf,
        t.regiao,
        t.bioma_predominante,
        t.score_preservacao_conformidade_florestal,
        t.score_pressao_desmatamento_deter,
        t.score_composto_risco_esg_territorial,
        COALESCE(s.qtd_imoveis_car_ativos, 0) AS qtd_imoveis_car_ativos,
        COALESCE(s.imoveis_livres_embargo_ibama_pct, 95.0) AS imoveis_livres_embargo_ibama_pct,
        COALESCE(s.score_conformidade_sicar, 65.0) AS score_conformidade_sicar,
        COALESCE(z.qtd_culturas_zoneadas, 0) AS qtd_culturas_zoneadas,
        COALESCE(z.adesao_zarc_credito_custeio_pct, 80.0) AS adesao_zarc_credito_custeio_pct,
        COALESCE(z.score_aptidao_zarc, 65.0) AS score_aptidao_zarc,
        COALESCE(h.vazao_media_global_m3s, 0.0) AS vazao_media_global_m3s,
        COALESCE(h.precipitacao_acumulada_anual_mm, 1200.0) AS precipitacao_acumulada_anual_mm,
        COALESCE(h.score_seguranca_hidrica_hidroweb, 65.0) AS score_seguranca_hidrica_hidroweb,
        COALESCE(tel.populacao_coberta_5g_pct, 0.0) AS populacao_coberta_5g_pct,
        COALESCE(tel.participacao_fibra_ftth_pct, 0.0) AS participacao_fibra_ftth_pct,
        COALESCE(tel.score_conectividade_digital, 60.0) AS score_conectividade_digital,
        COALESCE(ib.qtd_autos_infracao_ibama, 0) AS qtd_autos_infracao_ibama,
        COALESCE(ib.qtd_embargos_ativos_ibama, 0) AS qtd_embargos_ativos_ibama,
        COALESCE(ib.area_embargada_ibama_ha, 0.0) AS area_embargada_ibama_ha,
        COALESCE(ib.valor_multas_ibama_brl, 0.0) AS valor_multas_ibama_brl,
        COALESCE(ib.score_conformidade_ibama, 70.0) AS score_conformidade_ibama,
        COALESCE(sc.qtd_contratos_sicor, 0) AS qtd_contratos_sicor,
        COALESCE(sc.valor_total_sicor_brl, 0.0) AS valor_total_sicor_brl,
        COALESCE(sc.cobertura_proagro_sicor_pct, 0.0) AS cobertura_proagro_sicor_pct,
        COALESCE(sc.participacao_abc_renovagro_pct, 0.0) AS participacao_abc_renovagro_pct,
        COALESCE(sc.score_dinamismo_credito_rural_sicor, 50.0) AS score_dinamismo_credito_rural_sicor,
        t.reference_date
    FROM terra t
    LEFT JOIN sicar s ON t.id_municipio = s.id_municipio
    LEFT JOIN zarc z ON t.id_municipio = z.id_municipio
    LEFT JOIN hidro h ON t.id_municipio = h.id_municipio
    LEFT JOIN telecom tel ON t.id_municipio = tel.id_municipio
    LEFT JOIN ibama ib ON t.id_municipio = ib.id_municipio
    LEFT JOIN sicor sc ON t.id_municipio = sc.id_municipio
),

scored AS (
    SELECT
        *,
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    0.25 * score_composto_risco_esg_territorial
                    + 0.25 * score_conformidade_sicar
                    + 0.20 * score_aptidao_zarc
                    + 0.15 * score_seguranca_hidrica_hidroweb
                    + 0.15 * score_conectividade_digital
                )
            ),
            2
        ) AS score_sustentabilidade_agro_infra
    FROM joined
)

SELECT
    *,
    CASE
        WHEN score_sustentabilidade_agro_infra >= 78.0 THEN 'POLO_AGRO_ESG_CONECTADO_OURO'
        WHEN score_sustentabilidade_agro_infra >= 66.0 THEN 'TERRITORIO_SUSTENTAVEL_CONSOLIDADO'
        WHEN score_sustentabilidade_agro_infra >= 54.0 THEN 'ZONA_TRANSICAO_ESG_INFRAESTRUTURA'
        ELSE 'AREA_PRIORITARIA_MITIGACAO_ESG'
    END AS tier_prontidao_esg_agro
FROM scored
ORDER BY score_sustentabilidade_agro_infra DESC, id_municipio ASC
