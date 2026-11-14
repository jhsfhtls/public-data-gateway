{{ config(materialized='table') }}

with infra as (
    select
        *,
        least(100.0, greatest(0.0, (extensao_malha_federal_km / 320.0) * 100.0)) as ext_norm,
        least(100.0, greatest(0.0, (cast(volume_medio_diario_vmd_pesados as double) / 25000.0) * 100.0)) as vmd_norm,
        least(100.0, greatest(0.0, (cast(frota_veiculos_habilitados as double) / 120000.0) * 100.0)) as frota_norm,
        least(100.0, greatest(0.0, (cast(terminais_carga_intermodal as double) / 6.0) * 100.0)) as term_norm
    from {{ ref('fct_antt_infraestrutura_logistica_municipio') }}
),

modais_agg as (
    select
        id_municipio,
        max(case when modal_transporte = 'Rodoviario' then participacao_modal_cargas_pct else 0.0 end) as participacao_rodoviaria_pct,
        max(case when modal_transporte = 'Ferroviario' then participacao_modal_cargas_pct else 0.0 end) as participacao_ferroviaria_pct,
        max(case when modal_transporte = 'Hidroviario/Cabotagem' then participacao_modal_cargas_pct else 0.0 end) as participacao_hidroviaria_pct,
        round(
            sum(participacao_modal_cargas_pct * custo_tonelada_km_brl)
            / nullif(sum(participacao_modal_cargas_pct), 0.0),
            4
        ) as custo_medio_ponderado_tku_brl
    from {{ ref('fct_antt_matriz_modal_cargas_municipio') }}
    group by id_municipio
),

scored_intermodal as (
    select
        i.*,
        coalesce(m.participacao_rodoviaria_pct, 100.0) as participacao_rodoviaria_pct,
        coalesce(m.participacao_ferroviaria_pct, 0.0) as participacao_ferroviaria_pct,
        coalesce(m.participacao_hidroviaria_pct, 0.0) as participacao_hidroviaria_pct,
        coalesce(m.custo_medio_ponderado_tku_brl, 0.42) as custo_medio_ponderado_tku_brl,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.40 * least(
                        100.0,
                        greatest(
                            0.0,
                            ((coalesce(m.participacao_ferroviaria_pct, 0.0) + coalesce(m.participacao_hidroviaria_pct, 0.0)) / 45.0) * 100.0
                        )
                    )
                    + 0.40 * i.term_norm
                    + (case when i.possui_conexao_ferroviaria then 20.0 else 0.0 end)
                )
            ),
            2
        ) as score_intermodalidade_cargas
    from infra i
    left join modais_agg m on i.id_municipio = m.id_municipio
),

final_scores as (
    select
        *,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.25 * ext_norm
                    + 0.20 * proporcao_malha_concedida_pct
                    + 0.25 * vmd_norm
                    + 0.15 * frota_norm
                    + 0.15 * score_intermodalidade_cargas
                )
            ),
            2
        ) as score_infraestrutura_logistica
    from scored_intermodal
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_referencia,
    rodovias_federais_principais,
    extensao_malha_federal_km,
    trechos_concedidos_antt_km,
    proporcao_malha_concedida_pct,
    pracas_pedagio_ativas,
    volume_medio_diario_vmd_pesados,
    fluxo_anual_cargas_estimado_ton,
    total_transportadores_rntrc,
    transportadores_etc_empresas,
    transportadores_tac_autonomos,
    transportadores_ctc_cooperativas,
    proporcao_etc_empresas_pct,
    frota_veiculos_habilitados,
    capacidade_estatica_total_ton,
    capacidade_media_veiculo_ton,
    idade_media_frota_anos,
    possui_conexao_ferroviaria,
    concessionarias_ferroviarias,
    extensao_ferroviaria_km,
    terminais_carga_intermodal,
    movimentacao_ferroviaria_anual_tu,
    principais_mercadorias_ferrovia,
    participacao_rodoviaria_pct,
    participacao_ferroviaria_pct,
    participacao_hidroviaria_pct,
    custo_medio_ponderado_tku_brl,
    score_intermodalidade_cargas,
    score_infraestrutura_logistica,
    case
        when possui_conexao_ferroviaria and score_infraestrutura_logistica >= 75.0 and score_intermodalidade_cargas >= 65.0
            then 'HUB_LOGISTICO_INTERMODAL_NACIONAL'
        when possui_conexao_ferroviaria and score_infraestrutura_logistica >= 55.0
            then 'CORREDOR_RODOFERROVIARIO_CONSOLIDADO'
        when score_infraestrutura_logistica >= 40.0 or score_intermodalidade_cargas >= 40.0
            then 'POLO_RODOVIARIO_REGIONAL'
        else 'ACESSO_LOGISTICO_SECUNDARIO'
    end as classificacao_nodo_logistico,
    reference_date,
    extracted_at
from final_scores
