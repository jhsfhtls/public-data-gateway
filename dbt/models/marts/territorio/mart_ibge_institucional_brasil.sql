{{ config(materialized='table') }}

with scored as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        regiao,
        indice_capacidade_institucional_munic,
        servidores_estatutarios_municipio,
        indice_governanca_estadual_estadic,
        despesa_familiar_media_pof_brl,
        comprometimento_habitacao_transporte_pct,
        qtd_eixos_pesquisa_ibge,
        media_indice_institucional_munic,
        efetivo_estatutario_estadual_uf,
        politicas_habitacao_saneamento_ativas,
        cobertura_sistema_protecao_civil_pct,
        despesa_mensal_familiar_pof_brl,
        renda_monetaria_media_domiciliar_brl,
        participacao_alimentacao_domicilio_pct,
        round(
            least(
                98.0,
                greatest(
                    25.0,
                    0.45 * indice_capacidade_institucional_munic
                    + 0.30 * indice_governanca_estadual_estadic
                    + 0.25 * least(100.0, (despesa_mensal_familiar_pof_brl / 8500.0) * 100.0)
                )
            ),
            2
        ) as score_capacidade_institucional_ibge
    from {{ ref('fct_ibge_institucional_municipal') }}
)

select
    *,
    case
        when score_capacidade_institucional_ibge >= 78.0
            then 'GOVERNANCA_INSTITUCIONAL_AVANCADA'
        when score_capacidade_institucional_ibge >= 60.0
            then 'CAPACIDADE_GESTORA_CONSOLIDADA'
        else 'ESTRUTURACAO_ADMINISTRATIVA_LOCAL'
    end as tier_governanca_publica_ibge
from scored
