"""Connectors package for Brazil Data Gateway.

Registers domain-specific data source connectors across demografia, macroeconomia,
energia, saude, transito, etc.
"""

# Ensure built-in connectors are imported and registered
import public_data_gateway.connectors.agregadores.aneel_anp_ckan  # noqa: F401
import public_data_gateway.connectors.agregadores.brasilapi_core  # noqa: F401
import public_data_gateway.connectors.agregadores.dadosgov_ckan  # noqa: F401
import public_data_gateway.connectors.agregadores.datalake_federado_bq  # noqa: F401
import public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria  # noqa: F401
import public_data_gateway.connectors.ambiental.inpe_terrabrasilis  # noqa: F401
import public_data_gateway.connectors.ambiental.mapa_zarc  # noqa: F401
import public_data_gateway.connectors.ambiental.mapbiomas_cobertura  # noqa: F401
import public_data_gateway.connectors.ambiental.sfb_sicar  # noqa: F401
import public_data_gateway.connectors.comercio_exterior.mdic_comexstat  # noqa: F401
import public_data_gateway.connectors.compliance.camara_senado_dados_abertos  # noqa: F401
import public_data_gateway.connectors.compliance.cgu_ceis_cnep  # noqa: F401
import public_data_gateway.connectors.compliance.cgu_pep  # noqa: F401
import public_data_gateway.connectors.compras_publicas.comprasgov_catmat  # noqa: F401
import public_data_gateway.connectors.compras_publicas.pncp_contratacoes  # noqa: F401
import public_data_gateway.connectors.demografico.ibge_censo2022  # noqa: F401
import public_data_gateway.connectors.demografico.ibge_localidades  # noqa: F401
import public_data_gateway.connectors.demografico.ibge_pib_municipal  # noqa: F401
import public_data_gateway.connectors.demografico.ibge_sidra  # noqa: F401
import public_data_gateway.connectors.educacao.capes_sucupira  # noqa: F401
import public_data_gateway.connectors.educacao.inep_censo_escolar  # noqa: F401
import public_data_gateway.connectors.educacao.inep_censo_superior  # noqa: F401
import public_data_gateway.connectors.educacao.inep_enem  # noqa: F401
import public_data_gateway.connectors.educacao.inep_ideb  # noqa: F401
import public_data_gateway.connectors.empresarial.rfb_cnpj_lakehouse  # noqa: F401
import public_data_gateway.connectors.empresarial.rfb_simples_mei  # noqa: F401
import public_data_gateway.connectors.empresarial.suframa_pim_zfm  # noqa: F401
import public_data_gateway.connectors.geografico.anatel_mosaico  # noqa: F401
import public_data_gateway.connectors.geografico.antt_malha_transporte  # noqa: F401
import public_data_gateway.connectors.geografico.ibge_cnefe  # noqa: F401
import public_data_gateway.connectors.geografico.ibge_malhas  # noqa: F401
import public_data_gateway.connectors.imobiliario.capitais_iptu_itbi  # noqa: F401
import public_data_gateway.connectors.imobiliario.onr_registro_imoveis  # noqa: F401
import public_data_gateway.connectors.judicial.cnj_datajud  # noqa: F401
import public_data_gateway.connectors.judicial.cnj_justica_aberta  # noqa: F401
import public_data_gateway.connectors.judicial.tst_cndt  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.awesomeapi_fx  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_ifdata  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_olinda_ptax  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.bcb_sgs  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.brapi_b3  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.cvm_fundos  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.ipeadata_odata  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente  # noqa: F401
import public_data_gateway.connectors.macrofinanceiro.world_bank_v2  # noqa: F401
import public_data_gateway.connectors.saude.anvisa_cmed  # noqa: F401
import public_data_gateway.connectors.saude.datasus_cnes  # noqa: F401
import public_data_gateway.connectors.saude.datasus_sih  # noqa: F401
import public_data_gateway.connectors.saude.datasus_sim  # noqa: F401
import public_data_gateway.connectors.saude.datasus_sinasc  # noqa: F401
import public_data_gateway.connectors.saude.opendatasus_vigilancia  # noqa: F401
import public_data_gateway.connectors.saude.who_gho_odata  # noqa: F401
import public_data_gateway.connectors.trabalho.ibge_pnad_continua  # noqa: F401
import public_data_gateway.connectors.trabalho.mte_novo_caged  # noqa: F401
import public_data_gateway.connectors.trabalho.mte_rais  # noqa: F401
