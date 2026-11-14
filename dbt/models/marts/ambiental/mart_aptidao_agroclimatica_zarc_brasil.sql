with fct as (
    select * from {{ ref('fct_zarc_risco_agroclimatico_municipio') }}
),
scored as (
    select
        f.id_municipio,
        f.nome_municipio,
        f.sigla_uf,
        f.id_uf,
        f.regiao,
        f.bioma_predominante,
        f.ano_safra,
        f.qtd_culturas_zoneadas,
        f.decendios_aptos_media,
        f.adesao_zarc_credito_custeio_pct,
        f.taxa_sinistralidade_climatica_pct,
        f.indice_aptidao_climatica_zarc,
        f.indice_protecao_proagro_psr,
        f.indice_resiliencia_solo_ad,
        cast(
            round(
                cast(
                    (
                        0.30 * f.indice_aptidao_climatica_zarc
                        + 0.30 * f.indice_protecao_proagro_psr
                        + 0.25 * f.indice_resiliencia_solo_ad
                        + 0.15 * (100.0 - f.taxa_sinistralidade_climatica_pct)
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_aptidao_zarc,
        f.reference_date
    from fct as f
)
select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    bioma_predominante,
    ano_safra,
    qtd_culturas_zoneadas,
    decendios_aptos_media,
    adesao_zarc_credito_custeio_pct,
    taxa_sinistralidade_climatica_pct,
    indice_aptidao_climatica_zarc,
    indice_protecao_proagro_psr,
    indice_resiliencia_solo_ad,
    score_aptidao_zarc,
    case
        when score_aptidao_zarc >= 78.0 and adesao_zarc_credito_custeio_pct >= 90.0
            then 'ZARC_BAIXO_RISCO_PROAGRO_OURO'
        when score_aptidao_zarc >= 65.0
            then 'ZARC_APTIDAO_MODERADA_REGULAR'
        when score_aptidao_zarc >= 52.0
            then 'ZARC_RISCO_HIDRICO_EM_MONITORAMENTO'
        else 'ZARC_VULNERABILIDADE_CLIMATICA_ALTA'
    end as tier_risco_zarc,
    reference_date
from scored
