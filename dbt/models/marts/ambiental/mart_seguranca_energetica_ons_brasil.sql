{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        subsistema_sin,
        potencia_instalada_sin_mw,
        geracao_renovavel_sin_pct,
        carga_energia_mwmed,
        fator_capacidade_renovavel_pct,
        qtd_complexos_geracao_ons,
        geracao_verificada_total_mwmed,
        ear_energia_armazenada_pct,
        cmo_medio_brl_mwh,
        subestacoes_alta_tensao,
        linhas_transmissao_km,
        indice_continuidade_dec_horas,
        disponibilidade_rede_sin_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.40 * geracao_renovavel_sin_pct
                    + 0.35 * ear_energia_armazenada_pct
                    + 0.25 * greatest(0.0, 100.0 - indice_continuidade_dec_horas * 2.5)
                )
            ),
            2
        ) as score_seguranca_energetica_ons
    from {{ ref('fct_energia_eletrica_mme_ons_municipal') }}
)

select
    *,
    case
        when score_seguranca_energetica_ons >= 75.0
            then 'ALTA_SEGURANCA_ENERGETICA_SIN'
        when score_seguranca_energetica_ons >= 50.0
            then 'SISTEMA_INTERLIGADO_ESTAVEL'
        else 'VULNERABILIDADE_DESPACHO_ENERGETICO'
    end as tier_seguranca_energetica_sin
from scored
