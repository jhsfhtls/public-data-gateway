{{ config(materialized='table') }}

with prod_agg as (
    select
        id_municipio,
        sum(atos_praticados_semestre) as total_atos_praticados_semestre,
        sum(
            case
                when atribuicao_cartorial = 'Registro_Imoveis'
                    then atos_praticados_semestre
                else 0
            end
        ) as atos_registro_imoveis_semestre,
        sum(certidoes_expedidas_semestre) as total_certidoes_expedidas_semestre,
        round(avg(proporcao_atos_eletronicos_e_notariado_pct), 2) as proporcao_atos_eletronicos_media_pct,
        round(avg(prazo_medio_emissao_dias), 2) as prazo_medio_emissao_global_dias
    from {{ ref('fct_justica_aberta_produtividade_atribuicao_municipio') }}
    group by id_municipio
),

base as (
    select
        f.*,
        p.total_atos_praticados_semestre,
        p.atos_registro_imoveis_semestre,
        p.total_certidoes_expedidas_semestre,
        p.proporcao_atos_eletronicos_media_pct,
        p.prazo_medio_emissao_global_dias,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.35 * least(100.0, greatest(0.0, f.acervo_historico_digitalizado_pct))
                    + 0.35 * least(100.0, greatest(0.0, f.serventias_integradas_srei_onr_crc_pct))
                    + 0.30 * least(100.0, greatest(0.0, f.taxa_provimento_titular_pct))
                )
            ),
            2
        ) as score_eficiencia_modernizacao_cartorial,
        round(
            least(
                100.0,
                greatest(
                    0.0,
                    0.40 * least(100.0, greatest(0.0, (f.arrecadacao_media_por_serventia_brl / 8000000.0) * 100.0))
                    + 0.35 * least(100.0, greatest(0.0, p.proporcao_atos_eletronicos_media_pct))
                    + 0.25 * least(100.0, greatest(0.0, f.indice_transparencia_prestacao_contas_cnj_pct))
                )
            ),
            2
        ) as score_dinamismo_notarial_imobiliario
    from {{ ref('fct_justica_aberta_serventias_arrecadacao_municipio') }} f
    inner join prod_agg p on f.id_municipio = p.id_municipio
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    nome_uf,
    regiao,
    semestre_referencia,
    total_serventias_ativas,
    serventias_providas_titular,
    serventias_vagas_interino,
    taxa_provimento_titular_pct,
    oficios_registro_imoveis,
    tabelionatos_notas_protesto,
    oficios_registro_civil_rcpn,
    total_atos_praticados_semestre,
    atos_registro_imoveis_semestre,
    total_certidoes_expedidas_semestre,
    proporcao_atos_eletronicos_media_pct,
    prazo_medio_emissao_global_dias,
    arrecadacao_bruta_emolumentos_brl,
    repasses_fundos_judiciario_funarpen_brl,
    custeio_operacional_serventias_brl,
    arrecadacao_media_por_serventia_brl,
    ticket_medio_emolumento_por_ato_brl,
    taxa_repasse_institucional_pct,
    atos_gratuitos_ressarcidos_rcpn,
    acervo_historico_digitalizado_pct,
    serventias_integradas_srei_onr_crc_pct,
    indice_transparencia_prestacao_contas_cnj_pct,
    inspecoes_corregedoria_em_dia_pct,
    reclamacoes_disciplinares_por_100k_atos,
    forca_trabalho_prepostos_escreventes,
    atos_por_escrevente_semestre,
    score_eficiencia_modernizacao_cartorial,
    score_dinamismo_notarial_imobiliario,
    case
        when score_eficiencia_modernizacao_cartorial >= 86.0
            then 'HUB_NOTARIAL_DIGITAL_ALTA_EFICIENCIA'
        when score_eficiencia_modernizacao_cartorial >= 76.0
            then 'REDE_CARTORIAL_CONSOLIDADA_INTEGRADA'
        when score_eficiencia_modernizacao_cartorial >= 66.0
            then 'REDE_REGIONAL_EM_MODERNIZACAO_DIGITAL'
        else 'REDE_EXTRAJUDICIAL_EM_ESTRUTURACAO'
    end as classificacao_polo_notarial_cartorial,
    reference_date,
    extracted_at
from base
