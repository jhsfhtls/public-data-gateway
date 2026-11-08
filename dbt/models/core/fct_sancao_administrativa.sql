{{ config(materialized='table') }}

with ceis as (
    select
        'CEIS_' || id_sancao as id_sancao_global,
        id_sancao as id_sancao_origem,
        'CEIS' as cadastro_origem,
        case
            when upper(tipo_sancao) like '%INIDONEIDADE%' then 'CEIS_INIDONEIDADE'
            when upper(tipo_sancao) like '%IMPEDIMENTO%' then 'CEIS_IMPEDIMENTO'
            else 'CEIS_SUSPENSAO'
        end as id_tipo_sancao,
        tipo_pessoa,
        cpf_cnpj_sancionado,
        cnpj_basico,
        nome_sancionado,
        tipo_sancao,
        data_inicio_sancao,
        data_fim_sancao,
        data_publicacao_sancao,
        orgao_sancionador,
        sigla_uf_orgao,
        poder_orgao,
        esfera_orgao,
        fundamentacao_legal,
        numero_processo,
        0.0 as valor_multa,
        is_sancao_ativa
    from {{ ref('stg_cgu_ceis') }}
),

cnep as (
    select
        'CNEP_' || id_sancao as id_sancao_global,
        id_sancao as id_sancao_origem,
        'CNEP' as cadastro_origem,
        'CNEP_MULTA_ANTICORRUPCAO' as id_tipo_sancao,
        tipo_pessoa,
        cpf_cnpj_sancionado,
        cnpj_basico,
        nome_sancionado,
        tipo_sancao,
        data_inicio_sancao,
        data_fim_sancao,
        data_publicacao_sancao,
        orgao_sancionador,
        sigla_uf_orgao,
        'E' as poder_orgao,
        'F' as esfera_orgao,
        fundamentacao_legal,
        numero_processo,
        valor_multa,
        is_sancao_ativa
    from {{ ref('stg_cgu_cnep') }}
),

cepim as (
    select
        'CEPIM_' || id_impedimento as id_sancao_global,
        id_impedimento as id_sancao_origem,
        'CEPIM' as cadastro_origem,
        'CEPIM_IMPEDIMENTO_REPASSE' as id_tipo_sancao,
        'PJ' as tipo_pessoa,
        cnpj_entidade as cpf_cnpj_sancionado,
        cnpj_basico,
        nome_entidade as nome_sancionado,
        'Impedimento de Convênios e Parcerias' as tipo_sancao,
        data_referencia as data_inicio_sancao,
        cast(null as date) as data_fim_sancao,
        data_referencia as data_publicacao_sancao,
        orgao_concedente as orgao_sancionador,
        'DF' as sigla_uf_orgao,
        'E' as poder_orgao,
        'F' as esfera_orgao,
        motivo_impedimento as fundamentacao_legal,
        numero_convenio as numero_processo,
        0.0 as valor_multa,
        is_ativo as is_sancao_ativa
    from {{ ref('stg_cgu_cepim') }}
),

unificada as (
    select * from ceis
    union all
    select * from cnep
    union all
    select * from cepim
),

tipos as (
    select
        id_tipo_sancao,
        nivel_gravidade,
        ranking_gravidade
    from {{ ref('dim_tipo_sancao') }}
)

select
    u.id_sancao_global,
    u.id_sancao_origem,
    u.cadastro_origem,
    u.id_tipo_sancao,
    coalesce(t.nivel_gravidade, 'MEDIA') as nivel_gravidade,
    coalesce(t.ranking_gravidade, 3) as ranking_gravidade,
    u.tipo_pessoa,
    u.cpf_cnpj_sancionado,
    u.cnpj_basico,
    u.nome_sancionado,
    u.tipo_sancao,
    u.data_inicio_sancao,
    u.data_fim_sancao,
    u.data_publicacao_sancao,
    u.orgao_sancionador,
    u.sigla_uf_orgao,
    u.poder_orgao,
    u.esfera_orgao,
    u.fundamentacao_legal,
    u.numero_processo,
    u.valor_multa,
    u.is_sancao_ativa
from unificada u
left join tipos t
    on u.id_tipo_sancao = t.id_tipo_sancao
