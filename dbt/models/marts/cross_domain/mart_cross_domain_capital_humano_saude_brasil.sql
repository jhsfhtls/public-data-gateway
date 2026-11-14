{{ config(materialized='table') }}

WITH saude AS (
    SELECT
        id_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        total_estabelecimentos_ativos AS total_estabelecimentos_saude,
        total_leitos_hospitalares,
        total_leitos_uti,
        score_capacidade_hospitalar,
        reference_date
    FROM {{ ref('mart_capacidade_assistencial_saude_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

ideb AS (
    SELECT
        id_municipio,
        ideb_anos_iniciais_rede_publica,
        ideb_anos_finais_rede_publica,
        score_desempenho_ideb_municipal,
        score_proficiencia_equidade_saeb
    FROM {{ ref('mart_qualidade_educacao_basica_ideb_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY reference_date DESC NULLS LAST) = 1
),

emprego AS (
    SELECT
        id_municipio,
        saldo_movimentacao,
        salario_medio_admissao_reais,
        score_aquecimento_mercado_trabalho
    FROM {{ ref('mart_dinamica_emprego_formal_brasil') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio ORDER BY competencia_movimentacao DESC NULLS LAST) = 1
),

joined AS (
    SELECT
        s.id_municipio,
        s.nome_municipio,
        s.sigla_uf,
        s.regiao,
        s.total_estabelecimentos_saude,
        s.total_leitos_hospitalares,
        s.total_leitos_uti,
        s.score_capacidade_hospitalar,
        COALESCE(i.ideb_anos_iniciais_rede_publica, 5.5) AS ideb_anos_iniciais_rede_publica,
        COALESCE(i.ideb_anos_finais_rede_publica, 4.8) AS ideb_anos_finais_rede_publica,
        COALESCE(i.score_desempenho_ideb_municipal, 60.0) AS score_desempenho_ideb_municipal,
        COALESCE(i.score_proficiencia_equidade_saeb, 60.0) AS score_proficiencia_equidade_saeb,
        COALESCE(e.saldo_movimentacao, 0) AS saldo_emprego_formal,
        COALESCE(e.salario_medio_admissao_reais, 2400.0) AS salario_medio_admissao_reais,
        COALESCE(e.score_aquecimento_mercado_trabalho, 55.0) AS score_aquecimento_mercado_trabalho,
        s.reference_date
    FROM saude s
    LEFT JOIN ideb i ON s.id_municipio = i.id_municipio
    LEFT JOIN emprego e ON s.id_municipio = e.id_municipio
),

scored AS (
    SELECT
        *,
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    0.35 * score_capacidade_hospitalar
                    + 0.35 * score_desempenho_ideb_municipal
                    + 0.30 * score_aquecimento_mercado_trabalho
                )
            ),
            2
        ) AS score_capital_humano_saude
    FROM joined
)

SELECT
    *,
    CASE
        WHEN score_capital_humano_saude >= 75.0 THEN 'HUB_CAPITAL_HUMANO_AVANCADO'
        WHEN score_capital_humano_saude >= 60.0 THEN 'DESENVOLVIMENTO_HUMANO_CONSOLIDADO'
        WHEN score_capital_humano_saude >= 45.0 THEN 'EM_EXPANSAO_ASSISTENCIAL_EDUCACIONAL'
        ELSE 'PRIORIDADE_INVESTIMENTO_SOCIAL'
    END AS tier_desenvolvimento_humano_local
FROM scored
ORDER BY score_capital_humano_saude DESC, id_municipio ASC
