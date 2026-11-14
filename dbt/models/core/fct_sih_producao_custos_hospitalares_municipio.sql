{{ config(materialized='table') }}

with internacoes as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_mes_competencia,
        total_internacoes_aih,
        internacoes_carater_urgencia,
        internacoes_carater_eletivo,
        proporcao_urgencia_pct,
        diarias_hospitalares_totais,
        tempo_medio_permanencia_dias,
        internacoes_por_1000_hab_ano,
        reference_date,
        extracted_at
    from {{ ref('stg_sih_internacoes_aih_municipio') }}
),

custos as (
    select
        id_municipio,
        valor_total_aih_brl,
        valor_servicos_hospitalares_brl,
        valor_servicos_profissionais_brl,
        valor_uti_alta_complexidade_brl,
        ticket_medio_aih_brl,
        custo_medio_diaria_brl,
        proporcao_custo_uti_pct
    from {{ ref('stg_sih_custos_hospitalares_sus') }}
),

desfechos as (
    select
        id_municipio,
        altas_hospitalares,
        obitos_hospitalares,
        taxa_mortalidade_hospitalar_pct,
        internacoes_com_uti,
        taxa_utilizacao_uti_pct,
        internacoes_icsap_evitaveis,
        proporcao_icsap_pct
    from {{ ref('stg_sih_desfechos_mortalidade_hospitalar') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    i.id_municipio,
    i.nome_municipio,
    i.sigla_uf,
    i.id_uf,
    coalesce(m.nome_uf, i.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    i.ano_mes_competencia,
    i.total_internacoes_aih,
    i.internacoes_carater_urgencia,
    i.internacoes_carater_eletivo,
    i.proporcao_urgencia_pct,
    i.diarias_hospitalares_totais,
    i.tempo_medio_permanencia_dias,
    i.internacoes_por_1000_hab_ano,
    c.valor_total_aih_brl,
    c.valor_servicos_hospitalares_brl,
    c.valor_servicos_profissionais_brl,
    c.valor_uti_alta_complexidade_brl,
    c.ticket_medio_aih_brl,
    c.custo_medio_diaria_brl,
    c.proporcao_custo_uti_pct,
    d.altas_hospitalares,
    d.obitos_hospitalares,
    d.taxa_mortalidade_hospitalar_pct,
    d.internacoes_com_uti,
    d.taxa_utilizacao_uti_pct,
    d.internacoes_icsap_evitaveis,
    d.proporcao_icsap_pct,
    i.reference_date,
    i.extracted_at
from internacoes i
inner join custos c on i.id_municipio = c.id_municipio
inner join desfechos d on i.id_municipio = d.id_municipio
left join municipios m on i.id_municipio = m.id_municipio
