{{ config(materialized='table') }}

with pat as (
    select
        id_municipio,
        count(*) as qtd_naturezas_pi_rpi,
        sum(concessoes_anuais_inpi) as concessoes_patentes_software_12m
    from {{ ref('stg_inpi_patentes_depositos_municipio') }}
    group by id_municipio
),

mrc as (
    select
        id_municipio,
        desenhos_industriais_vigentes,
        taxa_concessao_marcas_pct,
        participacao_saas_tech_pct
    from {{ ref('stg_inpi_marcas_desenhos_industriais') }}
),

sw as (
    select
        id_municipio,
        ict_universidades_depositantes,
        indicacoes_geograficas_locais
    from {{ ref('stg_inpi_software_transferencia_tecnologia') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.patentes_depositadas_12m,
    r.marcas_concedidas_12m,
    r.registros_software_total,
    r.contratos_tecnologia_ativos,
    r.patentes_por_100k_hab,
    coalesce(p.qtd_naturezas_pi_rpi, 2) as qtd_naturezas_pi_rpi,
    coalesce(p.concessoes_patentes_software_12m, 0) as concessoes_patentes_software_12m,
    coalesce(m.desenhos_industriais_vigentes, 0) as desenhos_industriais_vigentes,
    coalesce(m.taxa_concessao_marcas_pct, 78.0) as taxa_concessao_marcas_pct,
    coalesce(m.participacao_saas_tech_pct, 25.0) as participacao_saas_tech_pct,
    coalesce(s.ict_universidades_depositantes, 0) as ict_universidades_depositantes,
    coalesce(s.indicacoes_geograficas_locais, 0) as indicacoes_geograficas_locais
from {{ ref('stg_inpi_pi_resumo_municipal') }} r
left join pat p on r.id_municipio = p.id_municipio
left join mrc m on r.id_municipio = m.id_municipio
left join sw s on r.id_municipio = s.id_municipio
