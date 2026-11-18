{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        total_agencias_fisicas,
        total_bancos_presentes,
        agencias_por_100k_hab,
        percentual_bancos_publicos_pct,
        percentual_bancos_privados_pct,
        qtd_postos_pa,
        qtd_postos_pae_eletronico,
        qtd_unidades_administrativas,
        cobertura_autoatendimento_24h_pct,
        possui_agencia_fisica,
        dependencia_correspondente_bancario_pct,
        distancia_media_agencia_km,
        status_inclusao_financeira,
        qtd_dependencias_amostra_cadastral,
        round(
            least(
                99.0,
                greatest(
                    10.0,
                    0.45 * least(100.0, (ln(1.0 + total_agencias_fisicas) / 7.8) * 100.0)
                    + 0.30 * least(100.0, cobertura_autoatendimento_24h_pct)
                    + 0.25 * least(100.0, (total_bancos_presentes / 40.0) * 100.0)
                )
            ),
            2
        ) as score_capilaridade_bancaria_bcb
    from {{ ref('fct_capilaridade_agencias_bancarias_municipal') }}
)

select
    *,
    case
        when score_capilaridade_bancaria_bcb >= 75.0
            then 'ALTA_CAPILARIDADE_BANCARIA'
        when score_capilaridade_bancaria_bcb >= 50.0
            then 'CAPILARIDADE_ADEQUADA'
        else 'DESERTO_BANCARIO_ALERTA'
    end as classificacao_capilaridade_agencias
from scored
