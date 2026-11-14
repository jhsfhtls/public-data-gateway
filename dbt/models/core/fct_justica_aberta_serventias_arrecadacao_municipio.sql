{{ config(materialized='table') }}

with serventias as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        semestre_referencia,
        total_serventias_ativas,
        serventias_providas_titular,
        serventias_vagas_interino,
        taxa_provimento_titular_pct,
        oficios_registro_imoveis,
        tabelionatos_notas_protesto,
        oficios_registro_civil_rcpn,
        reference_date,
        extracted_at
    from {{ ref('stg_justica_aberta_serventias_cadastro') }}
),

arrecadacao as (
    select
        id_municipio,
        arrecadacao_bruta_emolumentos_brl,
        repasses_fundos_judiciario_funarpen_brl,
        custeio_operacional_serventias_brl,
        arrecadacao_media_por_serventia_brl,
        ticket_medio_emolumento_por_ato_brl,
        taxa_repasse_institucional_pct,
        atos_gratuitos_ressarcidos_rcpn
    from {{ ref('stg_justica_aberta_arrecadacao_emolumentos') }}
),

regularidade as (
    select
        id_municipio,
        acervo_historico_digitalizado_pct,
        serventias_integradas_srei_onr_crc_pct,
        indice_transparencia_prestacao_contas_cnj_pct,
        inspecoes_corregedoria_em_dia_pct,
        reclamacoes_disciplinares_por_100k_atos,
        forca_trabalho_prepostos_escreventes,
        atos_por_escrevente_semestre
    from {{ ref('stg_justica_aberta_regularidade_digitalizacao') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('stg_ibge_municipios') }}
)

select
    s.id_municipio,
    s.nome_municipio,
    s.sigla_uf,
    s.id_uf,
    coalesce(m.nome_uf, s.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    s.semestre_referencia,
    s.total_serventias_ativas,
    s.serventias_providas_titular,
    s.serventias_vagas_interino,
    s.taxa_provimento_titular_pct,
    s.oficios_registro_imoveis,
    s.tabelionatos_notas_protesto,
    s.oficios_registro_civil_rcpn,
    a.arrecadacao_bruta_emolumentos_brl,
    a.repasses_fundos_judiciario_funarpen_brl,
    a.custeio_operacional_serventias_brl,
    a.arrecadacao_media_por_serventia_brl,
    a.ticket_medio_emolumento_por_ato_brl,
    a.taxa_repasse_institucional_pct,
    a.atos_gratuitos_ressarcidos_rcpn,
    r.acervo_historico_digitalizado_pct,
    r.serventias_integradas_srei_onr_crc_pct,
    r.indice_transparencia_prestacao_contas_cnj_pct,
    r.inspecoes_corregedoria_em_dia_pct,
    r.reclamacoes_disciplinares_por_100k_atos,
    r.forca_trabalho_prepostos_escreventes,
    r.atos_por_escrevente_semestre,
    s.reference_date,
    s.extracted_at
from serventias s
inner join arrecadacao a on s.id_municipio = a.id_municipio
inner join regularidade r on s.id_municipio = r.id_municipio
left join municipios m on s.id_municipio = m.id_municipio
