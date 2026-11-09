{{ config(materialized='table') }}

with densidade as (
    select
        id_municipio,
        sigla_uf,
        nome_municipio,
        total_enderecos_municipio,
        total_logradouros_distintos,
        total_ceps_distintos,
        taxa_cobertura_georreferenciada_pct,
        densidade_total_enderecos_km2,
        proporcao_enderecos_comerciais_pct,
        reference_date,
        extracted_at
    from {{ ref('fct_cnefe_perfil_edificacoes_municipio') }}
    group by
        id_municipio,
        sigla_uf,
        nome_municipio,
        total_enderecos_municipio,
        total_logradouros_distintos,
        total_ceps_distintos,
        taxa_cobertura_georreferenciada_pct,
        densidade_total_enderecos_km2,
        proporcao_enderecos_comerciais_pct,
        reference_date,
        extracted_at
),

especies as (
    select
        id_municipio,
        sum(case when tipo_edificacao = 'Domicílio particular' then total_edificacoes else 0 end) as total_domicilios_particulares,
        sum(case when tipo_edificacao = 'Domicílio coletivo' then total_edificacoes else 0 end) as total_domicilios_coletivos,
        sum(case when tipo_edificacao like 'Outras finalidades%' then total_edificacoes else 0 end) as total_estabelecimentos_comerciais,
        sum(case when tipo_edificacao = 'Estabelecimento de saúde' then total_edificacoes else 0 end) as total_estabelecimentos_saude,
        sum(case when tipo_edificacao = 'Estabelecimento de ensino' then total_edificacoes else 0 end) as total_estabelecimentos_ensino,
        sum(case when tipo_edificacao = 'Estabelecimento agropecuário' then total_edificacoes else 0 end) as total_estabelecimentos_agropecuarios,
        sum(case when tipo_edificacao = 'Estabelecimento religioso' then total_edificacoes else 0 end) as total_estabelecimentos_religiosos,
        sum(case when tipo_edificacao like 'Edificação em construção%' then total_edificacoes else 0 end) as total_edificacoes_construcao
    from {{ ref('fct_cnefe_perfil_edificacoes_municipio') }}
    group by id_municipio
)

select
    d.id_municipio,
    d.sigla_uf,
    d.nome_municipio,
    d.total_enderecos_municipio,
    d.total_logradouros_distintos,
    d.total_ceps_distintos,
    d.taxa_cobertura_georreferenciada_pct,
    d.densidade_total_enderecos_km2,
    d.proporcao_enderecos_comerciais_pct,
    coalesce(e.total_domicilios_particulares, 0) as total_domicilios_particulares,
    coalesce(e.total_domicilios_coletivos, 0) as total_domicilios_coletivos,
    coalesce(e.total_estabelecimentos_comerciais, 0) as total_estabelecimentos_comerciais,
    coalesce(e.total_estabelecimentos_saude, 0) as total_estabelecimentos_saude,
    coalesce(e.total_estabelecimentos_ensino, 0) as total_estabelecimentos_ensino,
    coalesce(e.total_estabelecimentos_agropecuarios, 0) as total_estabelecimentos_agropecuarios,
    coalesce(e.total_estabelecimentos_religiosos, 0) as total_estabelecimentos_religiosos,
    coalesce(e.total_edificacoes_construcao, 0) as total_edificacoes_construcao,
    round(
        least(100.0, (coalesce(e.total_estabelecimentos_comerciais, 0) * 1.0 / greatest(d.total_enderecos_municipio, 1)) * 500.0),
        2
    ) as score_densidade_comercial,
    round(
        least(100.0, ((coalesce(e.total_estabelecimentos_saude, 0) + coalesce(e.total_estabelecimentos_ensino, 0)) * 1.0 / greatest(d.total_enderecos_municipio, 1)) * 2500.0),
        2
    ) as score_infraestrutura_social,
    case
        when d.densidade_total_enderecos_km2 >= 300.0 and d.proporcao_enderecos_comerciais_pct >= 9.0
            then 'METROPOLE_ALTA_DENSIDADE_MISTA'
        when d.densidade_total_enderecos_km2 >= 100.0
            then 'POLO_URBANO_CONSOLIDADO'
        when coalesce(e.total_estabelecimentos_agropecuarios, 0) > coalesce(e.total_estabelecimentos_comerciais, 0)
            then 'AGROPOVOAMENTO_EXPANSAO'
        else 'MUNICIPIO_RESIDENCIAL_EQUILIBRADO'
    end as classificacao_dinamica_territorial,
    d.reference_date,
    d.extracted_at
from densidade d
left join especies e on d.id_municipio = e.id_municipio
