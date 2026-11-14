{{ config(materialized='table') }}

with areas as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        regiao,
        ano_edicao_enem,
        inscritos_confirmados_enem_ano,
        participantes_presentes_2dias_ano,
        taxa_presenca_efetiva_2dias_pct,
        nota_media_linguagens_codigos_lc,
        nota_media_ciencias_humanas_ch,
        nota_media_ciencias_natureza_cn,
        nota_media_matematica_mt,
        nota_media_objetivas_tri_geral,
        reference_date,
        extracted_at
    from {{ ref('stg_enem_desempenho_municipal_areas') }}
),

redacao as (
    select
        id_municipio,
        nota_media_redacao_enem,
        nota_media_comp1_norma_padrao,
        nota_media_comp3_argumentacao,
        nota_media_comp5_proposta_intervencao,
        redacoes_nota_900_mais_ano,
        proporcao_redacao_900_mais_pct,
        taxa_redacao_zero_ou_anulada_pct
    from {{ ref('stg_enem_redacao_competencias_municipio') }}
),

equidade as (
    select
        id_municipio,
        proporcao_isencao_taxa_inscricao_pct,
        proporcao_concluintes_escola_publica_pct,
        acesso_internet_domiciliar_q025_pct,
        gap_nota_objetiva_privada_vs_publica_pts,
        razao_equidade_publica_privada_enem,
        aptos_sisu_prouni_fies_nota_450_pct,
        indice_paridade_socioeconomica_qse
    from {{ ref('stg_enem_equidade_socioeconomica_acesso') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    a.id_municipio,
    a.nome_municipio,
    a.sigla_uf,
    a.id_uf,
    coalesce(m.nome_uf, a.sigla_uf) as nome_uf,
    coalesce(m.regiao, a.regiao) as regiao,
    a.ano_edicao_enem,
    a.inscritos_confirmados_enem_ano,
    a.participantes_presentes_2dias_ano,
    a.taxa_presenca_efetiva_2dias_pct,
    a.nota_media_linguagens_codigos_lc,
    a.nota_media_ciencias_humanas_ch,
    a.nota_media_ciencias_natureza_cn,
    a.nota_media_matematica_mt,
    a.nota_media_objetivas_tri_geral,
    r.nota_media_redacao_enem,
    r.nota_media_comp1_norma_padrao,
    r.nota_media_comp3_argumentacao,
    r.nota_media_comp5_proposta_intervencao,
    r.redacoes_nota_900_mais_ano,
    r.proporcao_redacao_900_mais_pct,
    r.taxa_redacao_zero_ou_anulada_pct,
    e.proporcao_isencao_taxa_inscricao_pct,
    e.proporcao_concluintes_escola_publica_pct,
    e.acesso_internet_domiciliar_q025_pct,
    e.gap_nota_objetiva_privada_vs_publica_pts,
    e.razao_equidade_publica_privada_enem,
    e.aptos_sisu_prouni_fies_nota_450_pct,
    e.indice_paridade_socioeconomica_qse,
    a.reference_date,
    a.extracted_at
from areas a
inner join redacao r on a.id_municipio = r.id_municipio
inner join equidade e on a.id_municipio = e.id_municipio
left join municipios m on a.id_municipio = m.id_municipio
