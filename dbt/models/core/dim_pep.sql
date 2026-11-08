{{ config(materialized='table') }}

with mandatos as (
    select * from {{ ref('fct_mandato_pep') }}
),

agrupado as (
    select
        cpf_pep,
        max(nome_pep) as nome_pep,
        max(sigla_uf_orgao) as sigla_uf_referencia,
        max(nome_orgao) as ultimo_orgao_exercido,
        max(descricao_funcao) as ultima_funcao_exercida,
        min(ranking_risco_pld) as menor_ranking_risco,
        count(*) as total_mandatos_registrados,
        sum(case when is_pep_ativo then 1 else 0 end) as total_mandatos_ativos,
        max(case when is_pep_ativo then 1 else 0 end) as flag_pep_ativo,
        max(data_fim_carencia) as data_limite_carencia
    from mandatos
    group by cpf_pep
)

select
    cpf_pep,
    nome_pep,
    sigla_uf_referencia,
    ultimo_orgao_exercido,
    ultima_funcao_exercida,
    total_mandatos_registrados,
    total_mandatos_ativos,
    cast(flag_pep_ativo as boolean) as is_pep_ativo,
    data_limite_carencia,
    case
        when flag_pep_ativo = 1 and menor_ranking_risco = 1 then 100.0
        when flag_pep_ativo = 1 and menor_ranking_risco = 2 then 80.0
        when flag_pep_ativo = 1 then 60.0
        when menor_ranking_risco = 1 then 40.0
        else 20.0
    end as score_risco_pld,
    case
        when flag_pep_ativo = 1 and menor_ranking_risco = 1 then 'CRITICO'
        when flag_pep_ativo = 1 then 'ALTO'
        when menor_ranking_risco = 1 then 'MODERADO'
        else 'BAIXO'
    end as categoria_risco_pld,
    case
        when flag_pep_ativo = 1 then 'PEP_ATIVO_OU_EM_CARENCIA'
        else 'PEP_HISTORICO_REGULARIZADO'
    end as status_exposicao_politica
from agrupado
