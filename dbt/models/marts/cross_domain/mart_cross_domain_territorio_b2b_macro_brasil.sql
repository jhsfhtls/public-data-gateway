{{ config(materialized='table') }}

WITH macro_latest AS (
    SELECT
        ano_mes,
        ipca_acumulado_12m_pct,
        selic_meta_fim_mes_pct,
        juros_real_ex_post_pct,
        regime_politica_monetaria
    FROM {{ ref('mart_cenario_macroeconomico_brasil') }}
    ORDER BY ano_mes DESC
    LIMIT 1
),

territorio_uf AS (
    SELECT
        sigla_uf,
        MAX(regiao) AS regiao,
        COUNT(DISTINCT id_municipio) AS qtd_municipios_uf,
        ROUND(SUM(pib_mil_reais), 2) AS pib_total_uf_mil_reais,
        SUM(populacao_residente) AS populacao_total_uf,
        ROUND(AVG(pib_per_capita_reais), 2) AS pib_per_capita_medio_uf,
        ROUND(AVG(indice_gini_uf), 4) AS indice_gini_uf
    FROM {{ ref('mart_perfil_socioeconomico_municipios') }}
    GROUP BY sigla_uf
),

emprego_uf AS (
    SELECT
        sigla_uf,
        SUM(saldo_movimentacao) AS saldo_emprego_formal_uf,
        ROUND(AVG(salario_medio_admissao_reais), 2) AS salario_medio_admissao_uf,
        ROUND(AVG(score_aquecimento_mercado_trabalho), 2) AS score_aquecimento_trabalho_uf
    FROM {{ ref('mart_dinamica_emprego_formal_brasil') }}
    GROUP BY sigla_uf
),

b2b_uf AS (
    SELECT
        sigla_uf,
        total_estabelecimentos_ativos,
        total_empresas_distintas,
        ROUND(capital_social_total_uf, 2) AS capital_social_total_uf,
        taxa_optantes_simples_pct
    FROM {{ ref('mart_densidade_empresarial_brasil') }}
),

compras_uf AS (
    SELECT
        sigla_uf,
        SUM(total_contratacoes) AS total_contratacoes_publicas_uf,
        ROUND(SUM(valor_total_homologado), 2) AS valor_homologado_compras_uf,
        ROUND(AVG(taxa_economia_licitatoria_pct), 2) AS taxa_economia_licitatoria_uf_pct
    FROM {{ ref('mart_compras_publicas_brasil') }}
    GROUP BY sigla_uf
),

compliance_uf AS (
    SELECT
        sigla_uf_orgao AS sigla_uf,
        SUM(total_sancoes_ativas) AS total_sancoes_ativas_uf,
        ROUND(SUM(valor_multas_ativas), 2) AS valor_multas_ativas_uf
    FROM {{ ref('mart_compliance_sancoes_brasil') }}
    GROUP BY sigla_uf_orgao
),

siconfi_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(despesa_pessoal_rcl_pct), 2) AS avg_despesa_pessoal_rcl_siconfi_pct,
        ROUND(AVG(score_solidez_contabil_siconfi), 2) AS avg_score_solidez_siconfi_uf
    FROM {{ ref('mart_contabilidade_fiscal_siconfi') }}
    GROUP BY sigla_uf
),

capag_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(ifgf_geral), 4) AS avg_ifgf_gestao_fiscal_uf,
        ROUND(AVG(CASE WHEN elegivel_garantia_uniao THEN 100.0 ELSE 0.0 END), 2) AS pct_municipios_elegiveis_garantia_capag_uf
    FROM {{ ref('mart_capacidade_pagamento_capag_ifgf_brasil') }}
    GROUP BY sigla_uf
),

estban_uf AS (
    SELECT
        sigla_uf,
        ROUND(SUM(operacoes_credito_total_brl), 2) AS volume_credito_bancario_estban_uf_brl,
        ROUND(SUM(depositos_totais_brl), 2) AS volume_depositos_estban_uf_brl
    FROM {{ ref('mart_estatistica_bancaria_estban_brasil') }}
    GROUP BY sigla_uf
),

agencias_uf AS (
    SELECT
        sigla_uf,
        SUM(total_agencias_fisicas) AS total_agencias_bancarias_bcb_uf,
        ROUND(AVG(score_capilaridade_bancaria_bcb), 2) AS avg_score_capilaridade_bancaria_uf
    FROM {{ ref('mart_capilaridade_agencias_bancarias_brasil') }}
    GROUP BY sigla_uf
),

cno_uf AS (
    SELECT
        sigla_uf,
        SUM(obras_ativas_em_andamento) AS total_obras_ativas_cno_uf,
        ROUND(SUM(area_total_registrada_m2), 2) AS area_construida_cno_uf_m2
    FROM {{ ref('mart_obras_construcao_civil_cno_brasil') }}
    GROUP BY sigla_uf
),

frota_uf AS (
    SELECT
        sigla_uf,
        SUM(frota_total_veiculos) AS qtd_veiculos_frota_uf,
        SUM(qtd_eletrificados_bevs_hevs) AS frota_eletrificados_hibridos_uf,
        ROUND(AVG(score_modernizacao_frota_mobilidade), 2) AS avg_score_modernizacao_frota_uf
    FROM {{ ref('mart_frota_mobilidade_senatran_brasil') }}
    GROUP BY sigla_uf
),

sinesp_uf AS (
    SELECT
        sigla_uf,
        ROUND(AVG(taxa_mvi_por_100k_hab), 2) AS mvi_por_100k_hab_sinesp,
        ROUND(AVG(taxa_roubo_carga_100k_hab), 2) AS roubo_carga_por_100k_hab_sinesp,
        ROUND(AVG(score_seguranca_publica_sinesp), 2) AS avg_score_seguranca_publica_uf
    FROM {{ ref('mart_seguranca_publica_sinesp_brasil') }}
    GROUP BY sigla_uf
),

prf_uf AS (
    SELECT
        sigla_uf,
        SUM(total_sinistros_prf_ano) AS total_sinistros_prf_uf,
        SUM(total_obitos_prf_ano) AS total_obitos_rodovias_prf_uf,
        ROUND(AVG(score_seguranca_viaria_prf), 2) AS avg_score_seguranca_viaria_prf_uf
    FROM {{ ref('mart_sinistralidade_rodoviaria_prf_brasil') }}
    GROUP BY sigla_uf
),

cgu_gastos_uf AS (
    SELECT
        sigla_uf,
        ROUND(SUM(total_gastos_federais_mil_brl), 2) AS total_gastos_federais_cgu_uf_mil_brl,
        ROUND(SUM(total_transferencias_mil_brl), 2) AS total_transferencias_cgu_uf_mil_brl,
        ROUND(AVG(score_integridade_execucao_cgu), 2) AS avg_score_integridade_execucao_cgu_uf
    FROM {{ ref('mart_transparencia_gastos_cgu_brasil') }}
    GROUP BY sigla_uf
),

siape_uf AS (
    SELECT
        sigla_uf,
        SUM(total_servidores_federais_siape) AS total_servidores_siape_uf,
        ROUND(SUM(folha_mensal_bruta_siape_mil_brl), 2) AS folha_mensal_siape_uf_mil_brl,
        ROUND(AVG(score_integridade_funcional_siape), 2) AS avg_score_integridade_siape_uf
    FROM {{ ref('mart_servidores_federais_siape_brasil') }}
    GROUP BY sigla_uf
),

eleicoes_uf AS (
    SELECT
        sigla_uf,
        SUM(eleitorado_apto_total) AS eleitorado_apto_tse_uf,
        ROUND(AVG(comparecimento_pct), 2) AS avg_comparecimento_eleitoral_uf_pct,
        ROUND(AVG(score_integridade_eleitoral_tse), 2) AS avg_score_integridade_eleitoral_uf
    FROM {{ ref('mart_eleicoes_tse_brasil') }}
    GROUP BY sigla_uf
),

bndes_uf AS (
    SELECT
        sigla_uf,
        ROUND(SUM(valor_total_contratado_mil_brl), 2) AS valor_contratado_bndes_uf_mil_brl,
        ROUND(SUM(desembolso_anual_mil_brl), 2) AS desembolso_anual_bndes_uf_mil_brl,
        ROUND(AVG(score_dinamismo_fomento_bndes), 2) AS avg_score_fomento_bndes_uf
    FROM {{ ref('mart_bndes_operacoes_fomento_brasil') }}
    GROUP BY sigla_uf
),

cvm_extra_uf AS (
    SELECT
        sigla_uf,
        ROUND(SUM(volume_captado_rcvm160_mil_brl), 2) AS volume_captado_rcvm160_cvm_uf_mil_brl,
        SUM(assessores_investimento_ativos) AS assessores_investimento_cvm_uf,
        ROUND(AVG(score_sofisticacao_mercado_cvm), 2) AS avg_score_mercado_capitais_cvm_uf
    FROM {{ ref('mart_cvm_mercado_extra_brasil') }}
    GROUP BY sigla_uf
),

ans_uf AS (
    SELECT
        sigla_uf,
        SUM(total_beneficiarios_med_odo) AS total_beneficiarios_ans_uf,
        ROUND(AVG(idss_medio_ponderado), 3) AS avg_idss_ans_uf,
        ROUND(AVG(score_maturidade_saude_suplementar_ans), 2) AS avg_score_saude_suplementar_ans_uf
    FROM {{ ref('mart_ans_saude_suplementar_brasil') }}
    GROUP BY sigla_uf
),

diarios_uf AS (
    SELECT
        sigla_uf,
        SUM(edicoes_indexadas_12m) AS edicoes_diario_oficial_okbr_uf,
        SUM(qtd_atos_licitatorios_12m) AS atos_licitatorios_diario_uf,
        ROUND(AVG(score_transparencia_diario_oficial), 2) AS avg_score_transparencia_diario_uf
    FROM {{ ref('mart_querido_diario_okbr_brasil') }}
    GROUP BY sigla_uf
),

mineracao_uf AS (
    SELECT
        sigla_uf,
        SUM(arrecadacao_cfem_total_mil_brl) AS arrecadacao_cfem_anm_uf_mil_brl,
        SUM(processos_ativos_sigmine) AS processos_ativos_anm_uf,
        ROUND(AVG(score_maturidade_mineral_esg), 2) AS avg_score_mineracao_esg_uf
    FROM {{ ref('mart_anm_mineracao_brasil') }}
    GROUP BY sigla_uf
),

seguros_uf AS (
    SELECT
        sigla_uf,
        SUM(premios_emitidos_total_mil_brl) AS premios_seguros_susep_uf_mil_brl,
        SUM(corretoras_ativas_susep) AS corretoras_ativas_susep_uf,
        ROUND(AVG(score_maturidade_seguros_susep), 2) AS avg_score_maturidade_seguros_uf
    FROM {{ ref('mart_susep_seguros_brasil') }}
    GROUP BY sigla_uf
),

inovacao_pi_uf AS (
    SELECT
        sigla_uf,
        SUM(patentes_depositadas_12m) AS patentes_depositadas_inpi_uf,
        SUM(registros_software_total) AS registros_software_inpi_uf,
        ROUND(AVG(score_inovacao_propriedade_intelectual), 2) AS avg_score_inovacao_pi_uf
    FROM {{ ref('mart_inpi_propriedade_intelectual_brasil') }}
    GROUP BY sigla_uf
),

sus_extra_uf AS (
    SELECT
        sigla_uf,
        SUM(producao_sia_total_12m) AS producao_ambulatorial_sia_uf,
        SUM(notificacoes_sinan_total_12m) AS notificacoes_sinan_uf,
        ROUND(AVG(score_resolutividade_sus_datasus), 2) AS avg_score_resolutividade_sus_uf
    FROM {{ ref('mart_datasus_extra_brasil') }}
    GROUP BY sigla_uf
),

educacao_extra_uf AS (
    SELECT
        sigla_uf,
        SUM(repasses_fundeb_mil_brl) AS repasses_fundeb_uf_mil_brl,
        SUM(bolsas_pesquisa_cnpq_ativas) AS bolsas_cnpq_ativas_uf,
        ROUND(AVG(score_capital_humano_mec_extra), 2) AS avg_score_capital_humano_mec_uf
    FROM {{ ref('mart_inep_mec_extra_brasil') }}
    GROUP BY sigla_uf
),
ibge_inst_uf AS (
    SELECT
        sigla_uf,
        SUM(servidores_estatutarios_municipio) AS servidores_estatutarios_munic_uf,
        ROUND(AVG(despesa_familiar_media_pof_brl), 2) AS avg_despesa_familiar_pof_uf_brl,
        ROUND(AVG(score_capacidade_institucional_ibge), 2) AS avg_score_capacidade_institucional_uf
    FROM {{ ref('mart_ibge_institucional_brasil') }}
    GROUP BY sigla_uf
),
rfb_arr_uf AS (
    SELECT
        sigla_uf,
        SUM(arrecadacao_federal_total_mi_brl) AS arrecadacao_federal_rfb_uf_mi_brl,
        SUM(contribuintes_pj_ativos_municipio) AS contribuintes_pj_rfb_uf,
        ROUND(AVG(score_dinamica_arrecadacao_rfb), 2) AS avg_score_arrecadacao_rfb_uf
    FROM {{ ref('mart_rfb_arrecadacao_brasil') }}
    GROUP BY sigla_uf
),
cgu_ben_uf AS (
    SELECT
        sigla_uf,
        SUM(volume_mensal_transferencias_mil_brl) AS volume_transferencias_sociais_cgu_uf_mil_brl,
        SUM(familias_bolsa_familia) AS familias_bolsa_familia_uf,
        ROUND(AVG(score_eficacia_protecao_social_cgu), 2) AS avg_score_protecao_social_cgu_uf
    FROM {{ ref('mart_cgu_beneficios_cidadao_brasil') }}
    GROUP BY sigla_uf
),

base AS (
    SELECT
        t.sigla_uf,
        t.regiao,
        t.qtd_municipios_uf,
        t.pib_total_uf_mil_reais,
        t.populacao_total_uf,
        t.pib_per_capita_medio_uf,
        t.indice_gini_uf,
        COALESCE(e.saldo_emprego_formal_uf, 0) AS saldo_emprego_formal_uf,
        COALESCE(e.salario_medio_admissao_uf, 2400.0) AS salario_medio_admissao_uf,
        COALESCE(e.score_aquecimento_trabalho_uf, 50.0) AS score_aquecimento_trabalho_uf,
        COALESCE(b.total_estabelecimentos_ativos, 0) AS total_estabelecimentos_ativos,
        COALESCE(b.total_empresas_distintas, 0) AS total_empresas_distintas,
        COALESCE(b.capital_social_total_uf, 0.0) AS capital_social_total_uf,
        COALESCE(b.taxa_optantes_simples_pct, 0.0) AS taxa_optantes_simples_pct,
        COALESCE(c.total_contratacoes_publicas_uf, 0) AS total_contratacoes_publicas_uf,
        COALESCE(c.valor_homologado_compras_uf, 0.0) AS valor_homologado_compras_uf,
        COALESCE(c.taxa_economia_licitatoria_uf_pct, 0.0) AS taxa_economia_licitatoria_uf_pct,
        COALESCE(s.total_sancoes_ativas_uf, 0) AS total_sancoes_ativas_uf,
        COALESCE(s.valor_multas_ativas_uf, 0.0) AS valor_multas_ativas_uf,
        COALESCE(sf.avg_despesa_pessoal_rcl_siconfi_pct, 45.0) AS avg_despesa_pessoal_rcl_siconfi_pct,
        COALESCE(sf.avg_score_solidez_siconfi_uf, 65.0) AS avg_score_solidez_siconfi_uf,
        COALESCE(cg.avg_ifgf_gestao_fiscal_uf, 0.0) AS avg_ifgf_gestao_fiscal_uf,
        COALESCE(cg.pct_municipios_elegiveis_garantia_capag_uf, 0.0) AS pct_municipios_elegiveis_garantia_capag_uf,
        COALESCE(eb.volume_credito_bancario_estban_uf_brl, 0.0) AS volume_credito_bancario_estban_uf_brl,
        COALESCE(eb.volume_depositos_estban_uf_brl, 0.0) AS volume_depositos_estban_uf_brl,
        COALESCE(ag.total_agencias_bancarias_bcb_uf, 0) AS total_agencias_bancarias_bcb_uf,
        COALESCE(ag.avg_score_capilaridade_bancaria_uf, 0.0) AS avg_score_capilaridade_bancaria_uf,
        COALESCE(cn.total_obras_ativas_cno_uf, 0) AS total_obras_ativas_cno_uf,
        COALESCE(cn.area_construida_cno_uf_m2, 0.0) AS area_construida_cno_uf_m2,
        COALESCE(fr.qtd_veiculos_frota_uf, 0) AS qtd_veiculos_frota_uf,
        COALESCE(fr.frota_eletrificados_hibridos_uf, 0) AS frota_eletrificados_hibridos_uf,
        COALESCE(fr.avg_score_modernizacao_frota_uf, 60.0) AS avg_score_modernizacao_frota_uf,
        COALESCE(sn.mvi_por_100k_hab_sinesp, 22.0) AS mvi_por_100k_hab_sinesp,
        COALESCE(sn.roubo_carga_por_100k_hab_sinesp, 12.0) AS roubo_carga_por_100k_hab_sinesp,
        COALESCE(sn.avg_score_seguranca_publica_uf, 65.0) AS avg_score_seguranca_publica_uf,
        COALESCE(pf.total_sinistros_prf_uf, 0) AS total_sinistros_prf_uf,
        COALESCE(pf.total_obitos_rodovias_prf_uf, 0) AS total_obitos_rodovias_prf_uf,
        COALESCE(pf.avg_score_seguranca_viaria_prf_uf, 65.0) AS avg_score_seguranca_viaria_prf_uf,
        COALESCE(cgf.total_gastos_federais_cgu_uf_mil_brl, 0.0) AS total_gastos_federais_cgu_uf_mil_brl,
        COALESCE(cgf.total_transferencias_cgu_uf_mil_brl, 0.0) AS total_transferencias_cgu_uf_mil_brl,
        COALESCE(cgf.avg_score_integridade_execucao_cgu_uf, 75.0) AS avg_score_integridade_execucao_cgu_uf,
        COALESCE(sp.total_servidores_siape_uf, 0) AS total_servidores_siape_uf,
        COALESCE(sp.folha_mensal_siape_uf_mil_brl, 0.0) AS folha_mensal_siape_uf_mil_brl,
        COALESCE(sp.avg_score_integridade_siape_uf, 80.0) AS avg_score_integridade_siape_uf,
        COALESCE(el.eleitorado_apto_tse_uf, 0) AS eleitorado_apto_tse_uf,
        COALESCE(el.avg_comparecimento_eleitoral_uf_pct, 75.0) AS avg_comparecimento_eleitoral_uf_pct,
        COALESCE(el.avg_score_integridade_eleitoral_uf, 80.0) AS avg_score_integridade_eleitoral_uf,
        COALESCE(bn.valor_contratado_bndes_uf_mil_brl, 0.0) AS valor_contratado_bndes_uf_mil_brl,
        COALESCE(bn.desembolso_anual_bndes_uf_mil_brl, 0.0) AS desembolso_anual_bndes_uf_mil_brl,
        COALESCE(bn.avg_score_fomento_bndes_uf, 70.0) AS avg_score_fomento_bndes_uf,
        COALESCE(cvx.volume_captado_rcvm160_cvm_uf_mil_brl, 0.0) AS volume_captado_rcvm160_cvm_uf_mil_brl,
        COALESCE(cvx.assessores_investimento_cvm_uf, 0) AS assessores_investimento_cvm_uf,
        COALESCE(cvx.avg_score_mercado_capitais_cvm_uf, 75.0) AS avg_score_mercado_capitais_cvm_uf,
        COALESCE(an.total_beneficiarios_ans_uf, 0) AS total_beneficiarios_ans_uf,
        COALESCE(an.avg_idss_ans_uf, 0.78) AS avg_idss_ans_uf,
        COALESCE(an.avg_score_saude_suplementar_ans_uf, 75.0) AS avg_score_saude_suplementar_ans_uf,
        COALESCE(qd.edicoes_diario_oficial_okbr_uf, 0) AS edicoes_diario_oficial_okbr_uf,
        COALESCE(qd.atos_licitatorios_diario_uf, 0) AS atos_licitatorios_diario_uf,
        COALESCE(qd.avg_score_transparencia_diario_uf, 80.0) AS avg_score_transparencia_diario_uf,
        COALESCE(mn.arrecadacao_cfem_anm_uf_mil_brl, 0.0) AS arrecadacao_cfem_anm_uf_mil_brl,
        COALESCE(mn.processos_ativos_anm_uf, 0) AS processos_ativos_anm_uf,
        COALESCE(mn.avg_score_mineracao_esg_uf, 75.0) AS avg_score_mineracao_esg_uf,
        COALESCE(sg.premios_seguros_susep_uf_mil_brl, 0.0) AS premios_seguros_susep_uf_mil_brl,
        COALESCE(sg.corretoras_ativas_susep_uf, 0) AS corretoras_ativas_susep_uf,
        COALESCE(sg.avg_score_maturidade_seguros_uf, 75.0) AS avg_score_maturidade_seguros_uf,
        COALESCE(ip.patentes_depositadas_inpi_uf, 0) AS patentes_depositadas_inpi_uf,
        COALESCE(ip.registros_software_inpi_uf, 0) AS registros_software_inpi_uf,
        COALESCE(ip.avg_score_inovacao_pi_uf, 70.0) AS avg_score_inovacao_pi_uf,
        COALESCE(sx.producao_ambulatorial_sia_uf, 0) AS producao_ambulatorial_sia_uf,
        COALESCE(sx.notificacoes_sinan_uf, 0) AS notificacoes_sinan_uf,
        COALESCE(sx.avg_score_resolutividade_sus_uf, 75.0) AS avg_score_resolutividade_sus_uf,
        COALESCE(ex.repasses_fundeb_uf_mil_brl, 0.0) AS repasses_fundeb_uf_mil_brl,
        COALESCE(ex.bolsas_cnpq_ativas_uf, 0) AS bolsas_cnpq_ativas_uf,
        COALESCE(ex.avg_score_capital_humano_mec_uf, 75.0) AS avg_score_capital_humano_mec_uf,
        COALESCE(ibi.servidores_estatutarios_munic_uf, 0) AS servidores_estatutarios_munic_uf,
        COALESCE(ibi.avg_despesa_familiar_pof_uf_brl, 5500.0) AS avg_despesa_familiar_pof_uf_brl,
        COALESCE(ibi.avg_score_capacidade_institucional_uf, 75.0) AS avg_score_capacidade_institucional_uf,
        COALESCE(rfb.arrecadacao_federal_rfb_uf_mi_brl, 0.0) AS arrecadacao_federal_rfb_uf_mi_brl,
        COALESCE(rfb.contribuintes_pj_rfb_uf, 0) AS contribuintes_pj_rfb_uf,
        COALESCE(rfb.avg_score_arrecadacao_rfb_uf, 75.0) AS avg_score_arrecadacao_rfb_uf,
        COALESCE(cben.volume_transferencias_sociais_cgu_uf_mil_brl, 0.0) AS volume_transferencias_sociais_cgu_uf_mil_brl,
        COALESCE(cben.familias_bolsa_familia_uf, 0) AS familias_bolsa_familia_uf,
        COALESCE(cben.avg_score_protecao_social_cgu_uf, 75.0) AS avg_score_protecao_social_cgu_uf,
        COALESCE(m.ano_mes, '2025-03') AS macro_ano_mes_referencia,
        COALESCE(m.selic_meta_fim_mes_pct, 14.25) AS selic_meta_fim_mes_pct,
        COALESCE(m.ipca_acumulado_12m_pct, 5.06) AS ipca_acumulado_12m_pct,
        COALESCE(m.juros_real_ex_post_pct, 8.75) AS juros_real_ex_post_pct,
        COALESCE(m.regime_politica_monetaria, 'CONTRACIONISTA') AS regime_politica_monetaria
    FROM territorio_uf t
    LEFT JOIN emprego_uf e ON t.sigla_uf = e.sigla_uf
    LEFT JOIN b2b_uf b ON t.sigla_uf = b.sigla_uf
    LEFT JOIN compras_uf c ON t.sigla_uf = c.sigla_uf
    LEFT JOIN compliance_uf s ON t.sigla_uf = s.sigla_uf
    LEFT JOIN siconfi_uf sf ON t.sigla_uf = sf.sigla_uf
    LEFT JOIN capag_uf cg ON t.sigla_uf = cg.sigla_uf
    LEFT JOIN estban_uf eb ON t.sigla_uf = eb.sigla_uf
    LEFT JOIN agencias_uf ag ON t.sigla_uf = ag.sigla_uf
    LEFT JOIN cno_uf cn ON t.sigla_uf = cn.sigla_uf
    LEFT JOIN frota_uf fr ON t.sigla_uf = fr.sigla_uf
    LEFT JOIN sinesp_uf sn ON t.sigla_uf = sn.sigla_uf
    LEFT JOIN prf_uf pf ON t.sigla_uf = pf.sigla_uf
    LEFT JOIN cgu_gastos_uf cgf ON t.sigla_uf = cgf.sigla_uf
    LEFT JOIN siape_uf sp ON t.sigla_uf = sp.sigla_uf
    LEFT JOIN eleicoes_uf el ON t.sigla_uf = el.sigla_uf
    LEFT JOIN bndes_uf bn ON t.sigla_uf = bn.sigla_uf
    LEFT JOIN cvm_extra_uf cvx ON t.sigla_uf = cvx.sigla_uf
    LEFT JOIN ans_uf an ON t.sigla_uf = an.sigla_uf
    LEFT JOIN diarios_uf qd ON t.sigla_uf = qd.sigla_uf
    LEFT JOIN mineracao_uf mn ON t.sigla_uf = mn.sigla_uf
    LEFT JOIN seguros_uf sg ON t.sigla_uf = sg.sigla_uf
    LEFT JOIN inovacao_pi_uf ip ON t.sigla_uf = ip.sigla_uf
    LEFT JOIN sus_extra_uf sx ON t.sigla_uf = sx.sigla_uf
    LEFT JOIN educacao_extra_uf ex ON t.sigla_uf = ex.sigla_uf
    LEFT JOIN ibge_inst_uf ibi ON t.sigla_uf = ibi.sigla_uf
    LEFT JOIN rfb_arr_uf rfb ON t.sigla_uf = rfb.sigla_uf
    LEFT JOIN cgu_ben_uf cben ON t.sigla_uf = cben.sigla_uf
    CROSS JOIN macro_latest m
),

scored AS (
    SELECT
        *,
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    0.35 * LEAST(100.0, (pib_per_capita_medio_uf / 65000.0) * 100.0)
                    + 0.25 * score_aquecimento_trabalho_uf
                    + 0.25 * LEAST(100.0, (LN(1.0 + total_estabelecimentos_ativos) / 14.0) * 100.0)
                    + 0.15 * LEAST(100.0, (LN(1.0 + valor_homologado_compras_uf) / 22.0) * 100.0)
                )
            ),
            2
        ) AS score_dinamismo_b2b_territorial
    FROM base
)

SELECT
    *,
    CASE
        WHEN score_dinamismo_b2b_territorial >= 72.0 THEN 'UF_EPICENTRO_B2B_EXPANSAO'
        WHEN score_dinamismo_b2b_territorial >= 58.0 THEN 'UF_ALTO_POTENCIAL_COMERCIAL'
        WHEN score_dinamismo_b2b_territorial >= 44.0 THEN 'UF_CONSOLIDACAO_REGIONAL'
        else 'UF_FRONTEIRA_DESENVOLVIMENTO'
    END AS tier_oportunidade_b2b_uf
FROM scored
ORDER BY score_dinamismo_b2b_territorial DESC, sigla_uf ASC
