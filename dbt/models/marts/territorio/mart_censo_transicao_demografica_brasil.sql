{{ config(materialized='table') }}

with censo as (
    select * from {{ ref('fct_censo_demografico_municipios') }}
),

div as (
    select * from {{ ref('fct_censo_diversidade_etnico_racial') }}
)

select
    c.id_municipio,
    c.nome_municipio,
    c.sigla_uf,
    c.regiao,
    c.is_capital,
    c.ano_censo,
    c.populacao_residente,
    c.total_domicilios_particulares_ocupados,
    c.total_moradores_domicilios_ocupados,
    c.media_moradores_por_domicilio,
    c.area_territorial_km2,
    c.densidade_demografica_hab_km2,
    case
        when c.densidade_demografica_hab_km2 >= 1000.0 then 'ALTA_DENSIDADE'
        when c.densidade_demografica_hab_km2 >= 100.0 then 'MEDIA_DENSIDADE'
        else 'BAIXA_DENSIDADE'
    end as classificacao_densidade,
    c.idade_mediana_anos,
    c.indice_envelhecimento_razao,
    case
        when c.indice_envelhecimento_razao >= 80.0 then 'ENVELHECIDA'
        when c.indice_envelhecimento_razao >= 50.0 then 'TRANSICAO_AVANCADA'
        else 'JOVEM'
    end as classificacao_etaria,
    c.razao_sexo_homens_por_100_mulheres,
    c.populacao_homens,
    c.populacao_mulheres,
    c.populacao_15_anos_ou_mais,
    c.populacao_alfabetizada,
    c.taxa_alfabetizacao_pct,
    c.taxa_analfabetismo_pct,
    c.populacao_urbana,
    c.populacao_rural,
    c.taxa_urbanizacao_pct,
    c.predominancia_territorial,
    coalesce(d.populacao_indigena, 0) as populacao_indigena,
    coalesce(d.populacao_quilombola, 0) as populacao_quilombola,
    coalesce(d.percentual_indigena_pct, 0.0) as percentual_indigena_pct,
    coalesce(d.percentual_quilombola_pct, 0.0) as percentual_quilombola_pct,
    round(
        (coalesce(c.taxa_alfabetizacao_pct, 90.0) * 0.6) +
        (coalesce(c.taxa_urbanizacao_pct, 80.0) * 0.4),
        2
    ) as score_maturidade_social,
    round(
        (coalesce(c.taxa_analfabetismo_pct, 10.0) * 0.5) +
        ((100.0 - coalesce(c.taxa_urbanizacao_pct, 80.0)) * 0.3) +
        (case when coalesce(c.indice_envelhecimento_razao, 60.0) > 80.0 then 20.0 else 10.0 end),
        2
    ) as score_vulnerabilidade_demografica,
    case
        when c.populacao_residente >= 1000000 then 'METROPOLE'
        when c.populacao_residente >= 500000 then 'GRANDE_PORTE'
        when c.populacao_residente >= 100000 then 'MEDIO_PORTE'
        else 'PEQUENO_PORTE'
    end as classificacao_porte_censo,
    c.reference_date,
    c.extracted_at
from censo c
left join div d on c.id_municipio = d.id_municipio and c.ano_censo = d.ano_censo
