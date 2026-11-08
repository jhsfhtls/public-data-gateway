{{ config(materialized='table') }}

with projetos_agg as (
    select
        cnpj_14,
        count(distinct codigo_projeto) as total_projetos_cas,
        count(distinct case when status_projeto = 'EM_OPERACAO' then codigo_projeto end) as total_projetos_em_operacao,
        coalesce(sum(faturamento_anual_estimado), 0.0) as faturamento_anual_incentivado,
        coalesce(sum(empregos_diretos_estimados), 0) as total_empregos_diretos_gerados,
        bool_or(cumpre_ppb) as cumpre_processo_produtivo_basico
    from {{ ref('fct_projeto_incentivado_suframa') }}
    group by cnpj_14
),

empresas as (
    select
        numero_inscricao_suframa,
        cnpj_14,
        razao_social,
        nome_fantasia,
        situacao_cadastral,
        tipo_area_incentivada,
        codigo_area_suframa,
        id_municipio_ibge,
        sigla_uf,
        subsetor_industrial,
        nome_subsetor,
        relevancia_pim,
        is_cadastro_regular_vigente
    from {{ ref('dim_empresa_suframa') }}
)

select
    e.cnpj_14,
    e.numero_inscricao_suframa,
    e.razao_social,
    e.nome_fantasia,
    e.situacao_cadastral,
    e.is_cadastro_regular_vigente,
    e.tipo_area_incentivada,
    e.codigo_area_suframa,
    e.id_municipio_ibge,
    e.sigla_uf,
    e.subsetor_industrial,
    e.nome_subsetor,
    e.relevancia_pim,
    coalesce(p.total_projetos_cas, 0) as total_projetos_cas,
    coalesce(p.total_projetos_em_operacao, 0) as total_projetos_em_operacao,
    coalesce(p.faturamento_anual_incentivado, 0.0) as faturamento_anual_incentivado,
    coalesce(p.total_empregos_diretos_gerados, 0) as total_empregos_diretos_gerados,
    coalesce(p.cumpre_processo_produtivo_basico, false) as cumpre_processo_produtivo_basico,
    case
        when e.situacao_cadastral = 'ATIVA' and e.tipo_area_incentivada in ('ZONA_FRANCA_MANAUS', 'AREA_LIVRE_COMERCIO') then true
        else false
    end as has_isencao_ipi,
    case
        when e.situacao_cadastral = 'ATIVA' and e.tipo_area_incentivada = 'ZONA_FRANCA_MANAUS' and coalesce(p.cumpre_processo_produtivo_basico, false) then true
        else false
    end as has_reducao_ii,
    case
        when e.situacao_cadastral = 'ATIVA' and e.sigla_uf = 'AM' and coalesce(p.cumpre_processo_produtivo_basico, false) then true
        else false
    end as has_credito_presumido_icms,
    case
        when e.situacao_cadastral = 'ATIVA' then true
        else false
    end as has_desoneracao_pis_cofins,
    case
        when e.situacao_cadastral = 'ATIVA' and p.total_empregos_diretos_gerados >= 2000 then 'GRANDE_PORTE'
        when e.situacao_cadastral = 'ATIVA' and p.total_empregos_diretos_gerados >= 200 then 'MEDIO_PORTE'
        when e.situacao_cadastral = 'ATIVA' then 'PEQUENO_PORTE'
        else 'INEXISTENTE'
    end as capacidade_fabril_estimada,
    case
        when e.situacao_cadastral = 'ATIVA' and p.faturamento_anual_incentivado >= 1000000000.0 then 95.0
        when e.situacao_cadastral = 'ATIVA' and p.total_projetos_cas > 0 then 80.0
        when e.situacao_cadastral = 'ATIVA' then 50.0
        else 0.0
    end as tax_incentive_score,
    current_timestamp as mart_generated_at
from empresas e
left join projetos_agg p
    on e.cnpj_14 = p.cnpj_14
