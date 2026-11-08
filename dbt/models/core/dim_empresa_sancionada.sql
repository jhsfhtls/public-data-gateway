{{ config(materialized='table') }}

with sancoes as (
    select * from {{ ref('fct_sancao_administrativa') }}
),

agrupado as (
    select
        cpf_cnpj_sancionado,
        max(cnpj_basico) as cnpj_basico,
        max(tipo_pessoa) as tipo_pessoa,
        max(nome_sancionado) as nome_sancionado,
        max(sigla_uf_orgao) as sigla_uf_referencia,
        count(*) as total_sancoes_historicas,
        sum(case when is_sancao_ativa then 1 else 0 end) as total_sancoes_ativas,
        max(case when cadastro_origem = 'CEIS' and is_sancao_ativa then 1 else 0 end) as flag_ceis_ativo,
        max(case when cadastro_origem = 'CNEP' and is_sancao_ativa then 1 else 0 end) as flag_cnep_ativo,
        max(case when cadastro_origem = 'CEPIM' and is_sancao_ativa then 1 else 0 end) as flag_cepim_ativo,
        sum(valor_multa) as valor_total_multas_anticorrupcao
    from sancoes
    group by cpf_cnpj_sancionado
)

select
    cpf_cnpj_sancionado,
    cnpj_basico,
    tipo_pessoa,
    nome_sancionado,
    sigla_uf_referencia,
    total_sancoes_historicas,
    total_sancoes_ativas,
    cast(flag_ceis_ativo as boolean) as is_inidoneo_ceis_ativo,
    cast(flag_cnep_ativo as boolean) as is_punido_cnep_ativo,
    cast(flag_cepim_ativo as boolean) as is_impedido_cepim_ativo,
    case
        when (flag_ceis_ativo = 1 or flag_cnep_ativo = 1 or flag_cepim_ativo = 1) then true
        else false
    end as has_active_sanction,
    valor_total_multas_anticorrupcao,
    case
        when flag_cnep_ativo = 1 and flag_ceis_ativo = 1 then 100.0
        when flag_cnep_ativo = 1 then 85.0
        when flag_ceis_ativo = 1 then 75.0
        when flag_cepim_ativo = 1 then 60.0
        when total_sancoes_ativas > 0 then 50.0
        else 20.0
    end as score_risco_compliance,
    case
        when (flag_ceis_ativo = 1 or flag_cnep_ativo = 1 or flag_cepim_ativo = 1) then 'INIDONEO_SANCIONADO'
        else 'HISTORICO_REGULARIZADO'
    end as status_compliance
from agrupado
