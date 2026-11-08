"""Jobs diários: fechamento e conciliação."""

import uuid
from decimal import Decimal

import pymysql

from app.ledger import CHAVE_DUPLICADA


def _contas(cur, conta_id):
    if conta_id:
        return [conta_id]
    cur.execute("SELECT conta_id FROM saldo_conta")
    return [linha["conta_id"] for linha in cur.fetchall()]


def fechar_dia(conn, dia, conta_id=None):
    """Fecha conta por conta. Rodar de novo só faz o que faltou."""
    with conn.cursor() as cur:
        contas = _contas(cur, conta_id)
    conn.commit()

    fechadas = 0
    for conta in contas:
        with conn.cursor() as cur:
            conn.begin()
            cur.execute("SELECT saldo_efetivo FROM saldo_conta WHERE conta_id = %s FOR UPDATE", (conta,))
            cur.execute("SELECT 1 FROM fechamento_diario WHERE conta_id = %s AND data = %s", (conta, dia))
            if cur.fetchone():
                conn.rollback()
                continue
            cur.execute(
                "SELECT COALESCE(SUM(efeito_efetivo), 0) AS total FROM lancamento WHERE conta_id = %s AND data_contabil < %s",
                (conta, dia),
            )
            anterior = cur.fetchone()["total"]
            cur.execute(
                "SELECT COALESCE(SUM(efeito_efetivo), 0) AS total FROM lancamento WHERE conta_id = %s AND data_contabil = %s",
                (conta, dia),
            )
            do_dia = cur.fetchone()["total"]
            cur.execute(
                """INSERT INTO fechamento_diario (conta_id, data, saldo_anterior, total_liquidado, saldo_fechamento)
                   VALUES (%s, %s, %s, %s, %s)""",
                (conta, dia, anterior, do_dia, anterior + do_dia),
            )
        conn.commit()
        fechadas += 1
    return fechadas


def conciliar(conn, dia, fontes, conta_id=None):
    """Compara o total liquidado do dia com cada fonte externa e registra as diferenças."""
    registradas = 0
    with conn.cursor() as cur:
        filtro = " AND conta_id = %s" if conta_id else ""
        cur.execute(
            f"SELECT conta_id, total_liquidado FROM fechamento_diario WHERE data = %s{filtro}",
            (dia, conta_id) if conta_id else (dia,),
        )
        fechamentos = cur.fetchall()
    conn.commit()

    for linha in fechamentos:
        for fonte in ("BACKOFFICE", "B3"):
            valor_fonte = fontes.get((linha["conta_id"], fonte), Decimal(0))
            if valor_fonte == linha["total_liquidado"]:
                continue
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """INSERT INTO divergencia
                           (divergencia_id, data, conta_id, fonte, valor_ledger, valor_fonte, status)
                           VALUES (%s, %s, %s, %s, %s, %s, 'ABERTA')""",
                        (str(uuid.uuid4()), dia, linha["conta_id"], fonte, linha["total_liquidado"], valor_fonte),
                    )
                    registradas += 1
                except pymysql.err.IntegrityError as erro:
                    if erro.args[0] != CHAVE_DUPLICADA:
                        raise
            conn.commit()
    return registradas
