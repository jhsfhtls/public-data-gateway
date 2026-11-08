{{ config(materialized='table') }}

with debitos_agg as (
    select
        cnpj_ou_cpf,
        count(distinct id_debito_bndt) as total_debitos_bndt,
        count(distinct case when situacao_exigibilidade = 'EXIGIVEL' then id_debito_bndt end) as total_debitos_exigiveis,
        coalesce(sum(valor_debito), 0.0) as valor_total_debitos_trabalhistas,
        coalesce(max(valor_debito), 0.0) as maior_debito_trabalhista,
        count(distinct tribunal_origem) as total_trts_envolvidos
    from {{ ref('fct_debito_trabalhista_bndt') }}
    group by cnpj_ou_cpf
),

certidoes_latest as (
    select
        numero_certidao,
        ano_certidao,
        cnpj_ou_cpf,
        razao_social_ou_nome,
        tipo_documento,
        status_certidao,
        is_regular,
        permite_licitar,
        data_expedicao,
        data_validade,
        codigo_autenticidade,
        is_certidao_ativa_valida,
        nivel_risco_trabalhista
    from {{ ref('fct_certidao_trabalhista') }}
)

select
    c.cnpj_ou_cpf,
    c.razao_social_ou_nome,
    c.tipo_documento,
    c.numero_certidao as ultima_certidao_numero,
    c.status_certidao as situacao_cndt,
    c.is_regular as is_regular_trabalhista,
    c.permite_licitar as is_habilitado_licitacao,
    c.is_certidao_ativa_valida,
    c.data_expedicao,
    c.data_validade,
    coalesce(d.total_debitos_bndt, 0) as total_debitos_bndt,
    coalesce(d.total_debitos_exigiveis, 0) as total_debitos_exigiveis,
    coalesce(d.valor_total_debitos_trabalhistas, 0.0) as valor_total_passivo_bndt,
    coalesce(d.maior_debito_trabalhista, 0.0) as maior_debito_bndt,
    coalesce(d.total_trts_envolvidos, 0) as qtd_trts_com_passivo,
    case
        when c.status_certidao = 'NEGATIVA' then 5.0
        when c.status_certidao = 'POSITIVA_COM_EFEITO_DE_NEGATIVA' then 35.0
        when c.status_certidao = 'POSITIVA' then 85.0
        else 50.0
    end as labor_risk_score,
    c.nivel_risco_trabalhista as classe_risco_trabalhista,
    current_timestamp as mart_generated_at
from certidoes_latest c
left join debitos_agg d
    on c.cnpj_ou_cpf = d.cnpj_ou_cpf
