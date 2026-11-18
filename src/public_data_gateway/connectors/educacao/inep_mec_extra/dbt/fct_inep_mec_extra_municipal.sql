{{ config(materialized='table') }}

with saeb as (
    select
        id_municipio,
        count(*) as qtd_etapas_avaliadas_inep,
        round(avg(proficiencia_media_saeb_lp_mat), 2) as media_geral_saeb_lp_mat
    from {{ ref('stg_inep_saeb_proficiencia_municipio') }}
    group by id_municipio
),

acesso as (
    select
        id_municipio,
        bolsas_prouni_ativas,
        vagas_ofertadas_sisu,
        nota_corte_media_sisu,
        taxa_ocupacao_vagas_pct
    from {{ ref('stg_mec_prouni_sisu_acesso_superior') }}
),

fomento as (
    select
        id_municipio,
        investimento_aluno_ano_fundeb_brl,
        bolsas_pesquisa_cnpq_ativas,
        projetos_ct_cnpq_vigentes
    from {{ ref('stg_fnde_fundeb_cnpq_fomento') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.proficiencia_saeb_matematica,
    r.proficiencia_saeb_linguagens,
    r.beneficiarios_prouni_sisu_total,
    r.repasses_fundeb_mil_brl,
    r.bolsas_cnpq_100k_hab,
    coalesce(sb.qtd_etapas_avaliadas_inep, 2) as qtd_etapas_avaliadas_inep,
    coalesce(sb.media_geral_saeb_lp_mat, 275.0) as media_geral_saeb_lp_mat,
    coalesce(ac.bolsas_prouni_ativas, 0) as bolsas_prouni_ativas,
    coalesce(ac.vagas_ofertadas_sisu, 0) as vagas_ofertadas_sisu,
    coalesce(ac.nota_corte_media_sisu, 680.0) as nota_corte_media_sisu,
    coalesce(ac.taxa_ocupacao_vagas_pct, 90.0) as taxa_ocupacao_vagas_pct,
    coalesce(fn.investimento_aluno_ano_fundeb_brl, 8500.0) as investimento_aluno_ano_fundeb_brl,
    coalesce(fn.bolsas_pesquisa_cnpq_ativas, 0) as bolsas_pesquisa_cnpq_ativas,
    coalesce(fn.projetos_ct_cnpq_vigentes, 0) as projetos_ct_cnpq_vigentes
from {{ ref('stg_inep_mec_extra_resumo_municipal') }} r
left join saeb sb on r.id_municipio = sb.id_municipio
left join acesso ac on r.id_municipio = ac.id_municipio
left join fomento fn on r.id_municipio = fn.id_municipio
