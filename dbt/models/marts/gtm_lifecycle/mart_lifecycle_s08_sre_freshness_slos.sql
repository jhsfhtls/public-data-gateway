{{ config(materialized='table') }}

WITH packs AS (
    SELECT DISTINCT pack_id, pack_name
    FROM {{ ref('mart_solution_packs_insights_brasil') }}
)

SELECT
    pack_id,
    pack_name,
    CASE
        WHEN pack_id IN ('P04', 'P05', 'P07') THEN 'DIARIA_ALTA_CRITICIDADE'
        WHEN pack_id IN ('P01', 'P02', 'P09', 'P10') THEN 'SEMANAL_CONSOLIDADA'
        ELSE 'MENSAL_ESTRUTURAL'
    END AS cadencia_ingestao_recomendada,
    CASE
        WHEN pack_id IN ('P04', 'P05', 'P07') THEN 24
        WHEN pack_id IN ('P01', 'P02', 'P09', 'P10') THEN 168
        ELSE 720
    END AS freshness_sla_horas,
    CASE
        WHEN pack_id IN ('P02', 'P04', 'P05', 'P10') THEN 99.9
        ELSE 99.5
    END AS slo_disponibilidade_pct,
    CASE
        WHEN pack_id IN ('P04', 'P07') THEN 'Fallback em cache Parquet local + circuito aberto se API externa retornar 5xx/429'
        WHEN pack_id = 'P05' THEN 'Alerta imediato se serie Selic/IPCA/PTAX atrasar > 24h uteis'
        ELSE 'Regressao live com rotacao de IDs + validacao de schema ODCS antes da publicacao Gold'
    END AS politica_resiliencia_sre,
    '2025-03' AS reference_period
FROM packs
ORDER BY pack_id ASC
