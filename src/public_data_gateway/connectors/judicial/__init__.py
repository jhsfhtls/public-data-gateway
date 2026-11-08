"""Judicial and legal data connectors."""

from public_data_gateway.connectors.judicial.cnj_datajud.connector import CNJDataJudConnector
from public_data_gateway.connectors.judicial.tst_cndt.connector import TstCndtConnector

__all__ = ["CNJDataJudConnector", "TstCndtConnector"]

