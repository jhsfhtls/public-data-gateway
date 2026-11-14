{{ config(materialized='table') }}

with base as (
    select * from {{ ref('fct_hidroweb_telemetria_seguranca_hidrica_municipio') }}
),

scored as (
    select
        *,
        cast(
            round(
                cast(
                    (
                        0.25 * indice_cobertura_rede_hidroweb
                        + 0.25 * indice_equilibrio_pluviometrico
                        + 0.30 * indice_resiliencia_hidrica
                        + 0.20 * (100.0 - taxa_eventos_criticos_media_pct)
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_seguranca_hidrica_hidroweb
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    regiao_hidrografica,
    ano_referencia,
    qtd_estacoes_telemetricas_ativas,
    disponibilidade_telemetria_pct,
    vazao_media_global_m3s,
    cota_media_global_cm,
    precipitacao_acumulada_anual_mm,
    comprometimento_balanco_hidrico_pct,
    indice_cobertura_rede_hidroweb,
    indice_equilibrio_pluviometrico,
    indice_resiliencia_hidrica,
    taxa_eventos_criticos_media_pct,
    score_seguranca_hidrica_hidroweb,
    case
        when score_seguranca_hidrica_hidroweb >= 78.0 and comprometimento_balanco_hidrico_pct <= 28.0
            then 'SEGURANCA_HIDRICA_RESILIENTE'
        when score_seguranca_hidrica_hidroweb >= 65.0
            then 'EQUILIBRIO_HIDROLOGICO_MODERADO'
        when score_seguranca_hidrica_hidroweb >= 52.0
            then 'ALERTA_ESTRESSE_HIDRICO'
        else 'CRITICO_ESCASSEZ_OU_CHEIA'
    end as tier_seguranca_hidrica_hidroweb,
    reference_date
from scored
