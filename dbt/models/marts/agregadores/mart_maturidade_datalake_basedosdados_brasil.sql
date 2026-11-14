with fct as (
    select * from {{ ref('fct_basedosdados_datalake_municipio') }}
),
scored as (
    select
        f.id_municipio,
        f.nome_municipio,
        f.sigla_uf,
        f.id_uf,
        f.regiao,
        f.qtd_datasets_tratados_bd,
        f.qtd_tabelas_bigquery_municipio,
        f.completude_chaves_ibge_pct,
        f.indice_cobertura_datalake_bd,
        f.indice_governanca_bd,
        f.indice_eficiencia_federada_bd,
        cast(
            round(
                cast(
                    (
                        0.35 * f.indice_cobertura_datalake_bd
                        + 0.35 * f.indice_governanca_bd
                        + 0.30 * f.indice_eficiencia_federada_bd
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_maturidade_bd,
        f.reference_date
    from fct as f
)
select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    qtd_datasets_tratados_bd,
    qtd_tabelas_bigquery_municipio,
    completude_chaves_ibge_pct,
    indice_cobertura_datalake_bd,
    indice_governanca_bd,
    indice_eficiencia_federada_bd,
    score_maturidade_bd,
    case
        when score_maturidade_bd >= 82.0 and completude_chaves_ibge_pct >= 96.0
            then 'BD_DATALAKE_OURO_FEDERADO'
        when score_maturidade_bd >= 68.0
            then 'BD_INTEGRACAO_CROSS_DOMAIN_ALTA'
        when score_maturidade_bd >= 54.0
            then 'BD_COBERTURA_PARCIAL_EXPANSAO'
        else 'BD_LACUNA_CATALOGACAO_PRIORITARIA'
    end as tier_maturidade_bd,
    reference_date
from scored
