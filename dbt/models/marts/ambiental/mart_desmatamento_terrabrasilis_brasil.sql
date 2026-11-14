with base as (
    select
        f.id_municipio,
        f.nome_municipio,
        f.sigla_uf,
        f.id_uf,
        f.regiao,
        f.bioma_predominante,
        f.ano_prodes,
        f.area_desmatada_prodes_km2,
        f.cobertura_florestal_remanescente_pct,
        f.variacao_anual_prodes_pct,
        f.total_alertas_deter_ano,
        f.area_total_alertas_deter_km2,
        f.conformidade_codigo_florestal_pct,
        f.embargos_ibama_ativos_ano,
        f.elegibilidade_credito_rural_esg_pct,
        f.risco_desconformidade_eudr_pct,
        f.indice_prontidao_fiscalizacao,
        cast(
            round(
                cast(
                    least(
                        100.0,
                        greatest(
                            0.0,
                            f.cobertura_florestal_remanescente_pct * 0.35
                            + f.conformidade_codigo_florestal_pct * 0.35
                            + f.elegibilidade_credito_rural_esg_pct * 0.30
                        )
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_preservacao_conformidade_florestal,
        cast(
            round(
                cast(
                    least(
                        100.0,
                        greatest(
                            0.0,
                            f.area_desmatada_prodes_km2 * 0.45
                            + f.area_total_alertas_deter_km2 * 0.55
                            + f.risco_desconformidade_eudr_pct * 0.40
                        )
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_pressao_desmatamento_deter,
        f.reference_date,
        f.extracted_at
    from {{ ref('fct_terrabrasilis_desmatamento_governanca_municipio') }} f
),

scored as (
    select
        *,
        cast(
            round(
                cast(
                    score_preservacao_conformidade_florestal * 0.60
                    + (100.0 - score_pressao_desmatamento_deter) * 0.40
                    as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_composto_risco_esg_territorial
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    bioma_predominante,
    ano_prodes,
    area_desmatada_prodes_km2,
    cobertura_florestal_remanescente_pct,
    variacao_anual_prodes_pct,
    total_alertas_deter_ano,
    area_total_alertas_deter_km2,
    conformidade_codigo_florestal_pct,
    embargos_ibama_ativos_ano,
    elegibilidade_credito_rural_esg_pct,
    risco_desconformidade_eudr_pct,
    indice_prontidao_fiscalizacao,
    score_preservacao_conformidade_florestal,
    score_pressao_desmatamento_deter,
    score_composto_risco_esg_territorial,
    case
        when score_composto_risco_esg_territorial >= 80.0
            and score_pressao_desmatamento_deter <= 5.0
            then 'MUNICIPIO_CONFORMIDADE_VERDE_EUDR'
        when score_composto_risco_esg_territorial >= 70.0
            then 'ZONA_CONSOLIDADA_BAIXA_PRESSAO_DETER'
        when score_composto_risco_esg_territorial >= 60.0
            then 'FRONTEIRA_ATENCAO_CONTROLE_AMBIENTAL'
        else 'ARCO_CRITICO_DESMATAMENTO_PRIORITARIO'
    end as perfil_risco_ambiental_terrabrasilis,
    reference_date,
    extracted_at
from scored
