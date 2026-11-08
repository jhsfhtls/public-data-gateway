{{ config(materialized='table') }}

with empresas_tributario as (
    select
        emp.cnpj_basico,
        coalesce(est.sigla_uf, 'ND') as sigla_uf,
        coalesce(simp.status_simples_atual, 'NAO_OPTANTE') as status_simples_atual,
        coalesce(simp.is_optante_ativo, false) as is_simples_ativo,
        coalesce(mei.status_simei_atual, 'NAO_ENQUADRADO') as status_simei_atual,
        coalesce(mei.is_mei_ativo, false) as is_mei_ativo,
        case when simp.data_exclusao_simples is not null then true else false end as is_excluido_simples,
        case when mei.data_exclusao_simei is not null then true else false end as is_desenquadrado_mei
    from {{ ref('dim_empresa') }} emp
    left join {{ ref('dim_estabelecimento') }} est
        on emp.cnpj_basico = est.cnpj_basico and est.identificador_matriz_filial = '1'
    left join {{ ref('dim_simples_nacional') }} simp
        on emp.cnpj_basico = simp.cnpj_basico
    left join {{ ref('dim_simei') }} mei
        on emp.cnpj_basico = mei.cnpj_basico
),

sublimites_atuais as (
    select
        sigla_uf,
        sublimite_icms_iss_anual,
        adotou_sublimite_diferenciado,
        categoria_sublimite_estadual
    from {{ ref('dim_sublimites_icms') }}
    where ano_calendario = 2026
)

select
    et.sigla_uf,
    count(distinct et.cnpj_basico) as total_empresas_cadastradas,
    count(case when et.is_simples_ativo then 1 end) as total_optantes_simples_ativos,
    count(case when et.is_mei_ativo then 1 end) as total_meis_enquadrados_ativos,
    count(case when et.is_excluido_simples then 1 end) as total_historico_excluidos_simples,
    count(case when et.is_desenquadrado_mei then 1 end) as total_historico_desenquadrados_mei,
    coalesce(
        round(
            count(case when et.is_simples_ativo then 1 end) * 100.0 / nullif(count(distinct et.cnpj_basico), 0),
            2
        ),
        0.0
    ) as taxa_adesao_simples_pct,
    coalesce(
        round(
            count(case when et.is_mei_ativo then 1 end) * 100.0 / nullif(count(case when et.is_simples_ativo then 1 end), 0),
            2
        ),
        0.0
    ) as taxa_mei_sobre_simples_pct,
    coalesce(max(sub.sublimite_icms_iss_anual), 4800000.0) as sublimite_icms_vigente_reais,
    coalesce(max(sub.categoria_sublimite_estadual), 'TETO_NACIONAL_4_8M') as categoria_sublimite_estadual,
    current_date as data_processamento
from empresas_tributario et
left join sublimites_atuais sub on et.sigla_uf = sub.sigla_uf
where et.sigla_uf != 'ND'
group by et.sigla_uf
