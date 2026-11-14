with fct as (
    select * from {{ ref('fct_dadosgov_ckan_transparencia_municipio') }}
),
scored as (
    select
        f.id_municipio,
        f.nome_municipio,
        f.sigla_uf,
        f.id_uf,
        f.regiao,
        f.qtd_orgaos_publicadores_ckan,
        f.qtd_conjuntos_dados_abertos,
        f.taxa_pda_vigente_pct,
        f.indice_oferta_dados_abertos,
        f.indice_governanca_lai_ckan,
        f.indice_reuso_dados_abertos,
        cast(
            round(
                cast(
                    (
                        0.35 * f.indice_oferta_dados_abertos
                        + 0.35 * f.indice_governanca_lai_ckan
                        + 0.30 * f.indice_reuso_dados_abertos
                    ) as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_transparencia_ckan,
        f.reference_date
    from fct as f
)
select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    qtd_orgaos_publicadores_ckan,
    qtd_conjuntos_dados_abertos,
    taxa_pda_vigente_pct,
    indice_oferta_dados_abertos,
    indice_governanca_lai_ckan,
    indice_reuso_dados_abertos,
    score_transparencia_ckan,
    case
        when score_transparencia_ckan >= 82.0 and taxa_pda_vigente_pct >= 92.0
            then 'CKAN_TRANSPARENCIA_OURO_LAI'
        when score_transparencia_ckan >= 68.0
            then 'CKAN_CATALOGO_ABERTO_CONSOLIDADO'
        when score_transparencia_ckan >= 54.0
            then 'CKAN_ABERTURA_PARCIAL_EXPANSAO'
        else 'CKAN_DEFICIT_DADOS_ABERTOS'
    end as tier_transparencia_ckan,
    reference_date
from scored
