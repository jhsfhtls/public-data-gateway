{{ config(materialized='table') }}

with tx_agg as (
    select
        id_municipio,
        sum(quantidade_transacoes_itbi_ano) as total_transacoes_itbi_ano,
        round(sum(valor_transacionado_itbi_milhoes_brl), 2) as volume_total_itbi_milhoes_brl
    from {{ ref('fct_itbi_transacoes_segmento_municipio') }}
    group by id_municipio
),

base as (
    select
        f.*,
        tx.total_transacoes_itbi_ano,
        tx.volume_total_itbi_milhoes_brl,
        least(
            100.0,
            greatest(
                0.0,
                ((cast(f.preco_medio_m2_itbi_declarado_brl as double) - 3500.0) / 8500.0) * 100.0
            )
        ) as norm_preco,
        least(
            100.0,
            greatest(
                0.0,
                ((cast(f.variacao_anual_preco_m2_itbi_pct as double) + 2.0) / 14.0) * 100.0
            )
        ) as norm_var,
        least(
            100.0,
            greatest(
                0.0,
                (cast(f.indice_liquidez_imobiliaria_itbi_pct as double) / 6.0) * 100.0
            )
        ) as norm_liq,
        least(
            100.0,
            greatest(
                0.0,
                (cast(f.razao_venal_pgv_sobre_mercado_itbi as double) / 0.85) * 100.0
            )
        ) as norm_razao_pgv
    from {{ ref('fct_iptu_itbi_cadastro_precos_governanca_municipio') }} f
    inner join tx_agg tx on f.id_municipio = tx.id_municipio
),

scored as (
    select
        *,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.40 * norm_preco
                    + 0.30 * norm_var
                    + 0.30 * norm_liq
                )
            ),
            2
        ) as score_dinamismo_mercado_imobiliario_itbi,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.30 * cast(taxa_adimplencia_iptu_pct as double)
                    + 0.30 * cast(cobertura_cadastral_georreferenciada_ctm_pct as double)
                    + 0.25 * cast(integracao_iptu_sinter_cno_onr_pct as double)
                    + 0.15 * norm_razao_pgv
                )
            ),
            2
        ) as score_governanca_cadastral_fiscal_iptu
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    ano_exercicio_fiscal,
    inscricoes_imobiliarias_ativas_iptu,
    area_construida_tributada_mil_m2,
    valor_venal_total_bilhoes_brl,
    valor_venal_medio_m2_brl,
    aliquota_media_efetiva_iptu_pct,
    arrecadacao_iptu_milhoes_brl,
    taxa_adimplencia_iptu_pct,
    indice_atualizacao_pgv_pct,
    total_transacoes_itbi_ano,
    volume_total_itbi_milhoes_brl,
    preco_medio_m2_itbi_declarado_brl,
    variacao_anual_preco_m2_itbi_pct,
    razao_venal_pgv_sobre_mercado_itbi,
    participacao_lancamentos_novos_pct,
    concentracao_uso_residencial_pct,
    indice_liquidez_imobiliaria_itbi_pct,
    taxa_vacancia_cadastral_estimada_pct,
    cobertura_cadastral_georreferenciada_ctm_pct,
    transacoes_itbi_emissao_digital_pct,
    integracao_iptu_sinter_cno_onr_pct,
    arrecadacao_itbi_milhoes_brl,
    participacao_iptu_itbi_receita_propria_pct,
    receita_imobiliaria_per_capita_brl,
    nivel_transparencia_dados_abertos_pgv,
    score_dinamismo_mercado_imobiliario_itbi,
    score_governanca_cadastral_fiscal_iptu,
    case
        when score_dinamismo_mercado_imobiliario_itbi >= 77.0
            and score_governanca_cadastral_fiscal_iptu >= 86.0
            then 'METROPOLE_ALTA_LIQUIDEZ_E_GOVERNANCA_CTM'
        when score_dinamismo_mercado_imobiliario_itbi >= 63.0
            and score_governanca_cadastral_fiscal_iptu >= 80.0
            then 'MERCADO_IMOBILIARIO_CONSOLIDADO_PGV_ATUALIZADA'
        when score_dinamismo_mercado_imobiliario_itbi >= 56.0
            then 'MERCADO_URBANO_EM_EXPANSAO_CADASTRAL'
        else 'CAPITAL_PRIORITARIA_MODERNIZACAO_CTM_PGV'
    end as classificacao_mercado_imobiliario_capital,
    reference_date,
    extracted_at
from scored
