import os

import pymysql
from pymysql.cursors import DictCursor

KAFKA = os.environ.get("KAFKA_SERVIDORES", "kafka:9092")

TOPICO_ORDENS = "investimentos.ordens"
TOPICO_LIQUIDACOES = "backoffice.liquidacoes"
TOPICO_DLQ = "ledger.dlq"

SENHAS = {
    "app": "SENHA_APP",
    "fechamento": "SENHA_FECHAMENTO",
    "conciliacao": "SENHA_CONCILIACAO",
    "consulta": "SENHA_CONSULTA",
    "outbox": "SENHA_OUTBOX",
    "demo": "SENHA_DEMO",
}


def conectar(usuario):
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "mysql"),
        user=usuario,
        password=os.environ[SENHAS[usuario]],
        database="ledger",
        cursorclass=DictCursor,
        autocommit=False,
    )
