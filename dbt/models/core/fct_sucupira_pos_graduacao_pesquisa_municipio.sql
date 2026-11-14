with programas as (
    select * from {{ ref('stg_sucupira_programas_avaliacao') }}
),

docentes as (
    select * from {{ ref('stg_sucupira_docentes_pesquisa') }}
),

producao as (
    select * from {{ ref('stg_sucupira_producao_cientifica_teses') }}
),

discentes_agg as (
    select
        id_municipio,
        sum(matriculados_mestrado_area + matriculados_doutorado_area) as total_matriculados_pos_graduacao_ano,
        sum(titulados_mestres_ano + titulados_doutores_ano) as total_titulados_mestres_doutores_ano,
        sum(bolsistas_capes_area) as total_bolsistas_capes_ano
    from {{ ref('stg_sucupira_discentes_titulados_area') }}
    group by id_municipio
)

select
    p.id_municipio,
    p.nome_municipio,
    p.sigla_uf,
    p.id_uf,
    p.regiao,
    p.ano_base_sucupira,
    p.total_programas_stricto_sensu_ano,
    p.programas_nota_6_7_excelencia_ano,
    p.programas_doutorado_ativo_ano,
    p.ies_ofertantes_pos_graduacao_ano,
    p.nota_media_capes_programas,
    p.prop_programas_excelencia_6_7_pct,
    p.prop_programas_profissionais_pct,
    da.total_matriculados_pos_graduacao_ano,
    da.total_titulados_mestres_doutores_ano,
    da.total_bolsistas_capes_ano,
    d.total_docentes_permanentes_ano,
    d.docentes_bolsistas_pq_cnpq_ano,
    d.razao_discentes_por_docente,
    d.taxa_docentes_dedicacao_exclusiva_pct,
    d.taxa_doutores_internacionalizacao_pct,
    d.linhas_pesquisa_ativas_ano,
    d.projetos_financiamento_externo_ano,
    pr.artigos_qualis_a1_a2_ano,
    pr.teses_dissertacoes_defendidas_ano,
    pr.producao_tecnica_patentes_software_ano,
    pr.indice_impacto_producao_por_docente,
    pr.taxa_acesso_aberto_repositorio_pct,
    pr.colaboracoes_internacionais_coautoria_pct,
    pr.fomento_bolsas_estimado_milhoes_brl,
    p.reference_date,
    p.extracted_at
from programas p
inner join discentes_agg da
    on p.id_municipio = da.id_municipio
inner join docentes d
    on p.id_municipio = d.id_municipio
inner join producao pr
    on p.id_municipio = pr.id_municipio
