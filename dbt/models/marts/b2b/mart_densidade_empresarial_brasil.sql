{{ config(materialized='table') }}

WITH estabelecimentos AS (
    SELECT
        cnpj_14,
        cnpj_basico,
        identificador_matriz_filial,
        is_ativo,
        sigla_uf
    FROM {{ ref('dim_estabelecimento') }}
    WHERE sigla_uf IS NOT NULL
),

empresas AS (
    SELECT
        cnpj_basico,
        capital_social,
        is_simples_nacional,
        is_mei
    FROM {{ ref('dim_empresa') }}
),

estab_empresa AS (
    SELECT
        est.cnpj_14,
        est.cnpj_basico,
        est.identificador_matriz_filial,
        est.is_ativo,
        est.sigla_uf,
        COALESCE(emp.capital_social, 0.0) AS capital_social,
        COALESCE(emp.is_simples_nacional, FALSE) AS is_simples_nacional,
        COALESCE(emp.is_mei, FALSE) AS is_mei
    FROM estabelecimentos est
    LEFT JOIN empresas emp ON est.cnpj_basico = emp.cnpj_basico
)

SELECT
    sigla_uf,
    COUNT(cnpj_14) AS total_estabelecimentos,
    COUNT(CASE WHEN is_ativo THEN 1 END) AS total_estabelecimentos_ativos,
    COUNT(CASE WHEN identificador_matriz_filial = '1' THEN 1 END) AS total_matrizes,
    COUNT(CASE WHEN identificador_matriz_filial = '2' THEN 1 END) AS total_filiais,
    COUNT(DISTINCT cnpj_basico) AS total_empresas_distintas,
    COUNT(CASE WHEN is_simples_nacional THEN 1 END) AS total_optantes_simples,
    COUNT(CASE WHEN is_mei THEN 1 END) AS total_enquadrados_mei,
    SUM(capital_social) AS capital_social_total_uf,
    ROUND(AVG(capital_social), 2) AS capital_social_medio_uf,
    ROUND(COUNT(CASE WHEN is_ativo THEN 1 END) * 100.0 / NULLIF(COUNT(cnpj_14), 0), 2) AS taxa_estabelecimentos_ativos_pct,
    ROUND(COUNT(CASE WHEN is_simples_nacional THEN 1 END) * 100.0 / NULLIF(COUNT(cnpj_14), 0), 2) AS taxa_optantes_simples_pct,
    CURRENT_DATE AS data_processamento
FROM estab_empresa
GROUP BY sigla_uf
ORDER BY total_estabelecimentos DESC, sigla_uf ASC
