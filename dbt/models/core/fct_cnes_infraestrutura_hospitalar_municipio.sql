{{ config(materialized='table') }}

with estab as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        ano_mes_competencia,
        codigo_cnes_referencia,
        cnpj_14,
        cep_8,
        estabelecimento_referencia_nome,
        total_estabelecimentos_ativos,
        hospitais_gerais_especializados,
        unidades_basicas_saude_ubs,
        unidades_pronto_atendimento_upa,
        estabelecimentos_atendimento_sus,
        proporcao_rede_sus_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_cnes_estabelecimentos_saude') }}
),

leitos as (
    select
        id_municipio,
        total_leitos_hospitalares,
        leitos_sus,
        leitos_nao_sus,
        total_leitos_uti,
        leitos_uti_adulto,
        leitos_uti_pediatrico_neonatal,
        leitos_uti_sus,
        leitos_por_1000_hab,
        leitos_uti_por_10k_hab
    from {{ ref('stg_cnes_leitos_hospitalares_uti') }}
),

equipamentos as (
    select
        id_municipio,
        tomografos_computadorizados,
        ressonancia_magnetica,
        mamografos_em_uso,
        aparelhos_hemodialise,
        aparelhos_raio_x,
        proporcao_equipamentos_sus_pct,
        indice_disponibilidade_diagnostica
    from {{ ref('stg_cnes_equipamentos_diagnostico') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    e.id_municipio,
    e.nome_municipio,
    e.sigla_uf,
    e.id_uf,
    coalesce(m.nome_uf, e.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    e.ano_mes_competencia,
    e.codigo_cnes_referencia,
    e.cnpj_14,
    e.cep_8,
    e.estabelecimento_referencia_nome,
    e.total_estabelecimentos_ativos,
    e.hospitais_gerais_especializados,
    e.unidades_basicas_saude_ubs,
    e.unidades_pronto_atendimento_upa,
    e.estabelecimentos_atendimento_sus,
    e.proporcao_rede_sus_pct,
    l.total_leitos_hospitalares,
    l.leitos_sus,
    l.leitos_nao_sus,
    round(
        (cast(l.leitos_sus as double) / nullif(l.total_leitos_hospitalares, 0)) * 100.0,
        2
    ) as proporcao_leitos_sus_pct,
    l.total_leitos_uti,
    l.leitos_uti_adulto,
    l.leitos_uti_pediatrico_neonatal,
    l.leitos_uti_sus,
    l.leitos_por_1000_hab,
    l.leitos_uti_por_10k_hab,
    q.tomografos_computadorizados,
    q.ressonancia_magnetica,
    q.mamografos_em_uso,
    q.aparelhos_hemodialise,
    q.aparelhos_raio_x,
    q.proporcao_equipamentos_sus_pct,
    q.indice_disponibilidade_diagnostica,
    e.reference_date,
    e.extracted_at
from estab e
inner join leitos l on e.id_municipio = l.id_municipio
inner join equipamentos q on e.id_municipio = q.id_municipio
left join municipios m on e.id_municipio = m.id_municipio
