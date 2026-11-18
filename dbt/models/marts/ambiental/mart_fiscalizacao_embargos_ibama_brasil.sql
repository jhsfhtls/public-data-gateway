{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        bioma_predominante,
        qtd_autos_infracao_ibama,
        qtd_embargos_ativos_ibama,
        area_embargada_ibama_ha,
        valor_multas_ibama_brl,
        taxa_infracoes_flora_pct,
        qtd_processos_julgados,
        valor_multas_arrecadadas_brl,
        percentual_divida_inscrita_pgfn_pct,
        indice_recuperacao_embargos_pct,
        qtd_autos_amostra_sifisc,
        ticket_medio_multa_amostra_brl,
        qtd_termos_embargo_amostra,
        qtd_embargos_ativos_amostra,
        round(
            least(
                98.0,
                greatest(
                    10.0,
                    0.40 * greatest(0.0, 100.0 - (ln(1.0 + area_embargada_ibama_ha) / 12.0) * 100.0)
                    + 0.30 * greatest(0.0, 100.0 - (ln(1.0 + qtd_embargos_ativos_ibama) / 6.2) * 100.0)
                    + 0.30 * least(100.0, indice_recuperacao_embargos_pct)
                )
            ),
            2
        ) as score_conformidade_ibama
    from {{ ref('fct_fiscalizacao_embargos_ibama_municipal') }}
)

select
    *,
    case
        when score_conformidade_ibama >= 75.0
            then 'BAIXA_PRESSAO_AMBIENTAL'
        when score_conformidade_ibama >= 50.0
            then 'ATENCAO_FISCALIZACAO_MODERADA'
        else 'CRITICO_EMBARGO_IBAMA_EUDR'
    end as classificacao_risco_ibama,
    case
        when qtd_embargos_ativos_ibama >= 40 or area_embargada_ibama_ha >= 2500.0
            then true
        else false
    end as flag_alerta_embargo_eudr
from scored
