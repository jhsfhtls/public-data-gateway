with base as (
    select
        f.id_municipio,
        f.nome_municipio,
        f.sigla_uf,
        f.id_uf,
        f.regiao,
        f.ano_base_sucupira,
        f.total_programas_stricto_sensu_ano,
        f.programas_nota_6_7_excelencia_ano,
        f.nota_media_capes_programas,
        f.prop_programas_excelencia_6_7_pct,
        f.total_matriculados_pos_graduacao_ano,
        f.total_titulados_mestres_doutores_ano,
        f.total_docentes_permanentes_ano,
        f.razao_discentes_por_docente,
        f.artigos_qualis_a1_a2_ano,
        f.indice_impacto_producao_por_docente,
        f.fomento_bolsas_estimado_milhoes_brl,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    ((f.nota_media_capes_programas - 3.0) / 4.0) * 45.0
                    + f.prop_programas_excelencia_6_7_pct * 0.90
                    + least(30.0, f.total_programas_stricto_sensu_ano / 8.0)
                )
            ),
            2
        ) as score_densidade_excelencia_stricto_sensu,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    f.indice_impacto_producao_por_docente * 18.0
                    + f.colaboracoes_internacionais_coautoria_pct * 0.85
                    + f.taxa_acesso_aberto_repositorio_pct * 0.15
                )
            ),
            2
        ) as score_producao_cientifica_inovacao,
        f.reference_date,
        f.extracted_at
    from {{ ref('fct_sucupira_pos_graduacao_pesquisa_municipio') }} f
),

scored as (
    select
        *,
        cast(
            round(
                cast(
                    score_densidade_excelencia_stricto_sensu * 0.50
                    + score_producao_cientifica_inovacao * 0.50 as decimal(10, 4)
                ),
                2
            ) as double
        ) as score_composto_ecossistema_pos_graduacao
    from base
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    ano_base_sucupira,
    total_programas_stricto_sensu_ano,
    programas_nota_6_7_excelencia_ano,
    nota_media_capes_programas,
    prop_programas_excelencia_6_7_pct,
    total_matriculados_pos_graduacao_ano,
    total_titulados_mestres_doutores_ano,
    total_docentes_permanentes_ano,
    razao_discentes_por_docente,
    artigos_qualis_a1_a2_ano,
    indice_impacto_producao_por_docente,
    fomento_bolsas_estimado_milhoes_brl,
    score_densidade_excelencia_stricto_sensu,
    score_producao_cientifica_inovacao,
    score_composto_ecossistema_pos_graduacao,
    case
        when score_densidade_excelencia_stricto_sensu >= 74.0
            and score_producao_cientifica_inovacao >= 78.0
            then 'HUB_EXCELENCIA_CIENTIFICA_INTERNACIONAL'
        when score_composto_ecossistema_pos_graduacao >= 62.0
            then 'POLO_POS_GRADUACAO_CONSOLIDADO_NACIONAL'
        when score_composto_ecossistema_pos_graduacao >= 46.0
            then 'CENTRO_REGIONAL_EXPANSAO_MESTRADO_DOUTORADO'
        else 'ECOSISTEMA_EMERGENTE_FOMENTO_CAPES'
    end as perfil_pos_graduacao_sucupira,
    reference_date,
    extracted_at
from scored
