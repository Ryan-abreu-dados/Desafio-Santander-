"""Regras do livro-razão. Cada função pública roda em uma única transação."""

import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

import pymysql

CHAVE_DUPLICADA = 1062
MINUTOS_RESERVA = 15

TRANSICOES = {
    "PROVISIONADO": {"LIQUIDADO", "PENDENTE", "CANCELADO"},
    "PENDENTE": {"LIQUIDADO", "CANCELADO"},
    "LIQUIDADO": set(),
    "CANCELADO": set(),
}


class OperacaoDesconhecida(Exception):
    """O evento citou uma operação que ainda não chegou. Vale tentar de novo."""


class TransicaoInvalida(Exception):
    """A mudança de estado não é permitida. Vai para a DLQ."""


class SaldoInsuficiente(Exception):
    pass


def _sinal(natureza):
    return Decimal(-1) if natureza == "DEBITO" else Decimal(1)


def _registrar_evento(cur, id_evento, tipo, conta_id):
    try:
        cur.execute(
            "INSERT INTO evento_processado (id_evento, tipo, conta_id) VALUES (%s, %s, %s)",
            (id_evento, tipo, conta_id),
        )
    except pymysql.err.IntegrityError as erro:
        if erro.args[0] == CHAVE_DUPLICADA:
            return False
        raise
    return True


def _travar_conta(cur, conta_id):
    cur.execute("SELECT saldo_disponivel FROM saldo_conta WHERE conta_id = %s FOR UPDATE", (conta_id,))
    linha = cur.fetchone()
    if linha is None:
        raise ValueError(f"conta inexistente: {conta_id}")
    return linha["saldo_disponivel"]


def _travar_operacao(cur, operacao_id):
    cur.execute("SELECT * FROM operacao WHERE operacao_id = %s FOR UPDATE", (operacao_id,))
    return cur.fetchone()


def _dia_contabil(cur, conta_id, dia):
    """Evento de um dia já fechado conta no primeiro dia aberto."""
    cur.execute("SELECT MAX(data) AS ultimo FROM fechamento_diario WHERE conta_id = %s", (conta_id,))
    ultimo = cur.fetchone()["ultimo"]
    if ultimo is not None and dia <= ultimo:
        return ultimo + timedelta(days=1)
    return dia


def _lancar(cur, op, id_evento, status, efetivo, provisionado, data_contabil, ocorrido_em, tipo=None, ref=None):
    cur.execute(
        """INSERT INTO lancamento
           (lancamento_id, conta_id, operacao_id, id_evento, tipo, natureza, status, valor,
            efeito_efetivo, efeito_provisionado, data_liquidacao, data_contabil, ocorrido_em, lancamento_ref_id)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (str(uuid.uuid4()), op["conta_id"], op["operacao_id"], id_evento, tipo or op["tipo"], op["natureza"], status,
         op["valor"], efetivo, provisionado, op["data_liquidacao"], data_contabil, ocorrido_em, ref),
    )
    coluna = "provisionado_debitos" if op["natureza"] == "DEBITO" else "provisionado_creditos"
    cur.execute(
        f"UPDATE saldo_conta SET saldo_efetivo = saldo_efetivo + %s, {coluna} = {coluna} + %s WHERE conta_id = %s",
        (efetivo, provisionado, op["conta_id"]),
    )


def _mudar_status(cur, op, novo):
    if novo not in TRANSICOES[op["status"]]:
        raise TransicaoInvalida(f"{op['operacao_id']}: {op['status']} para {novo}")
    cur.execute("UPDATE operacao SET status = %s, expira_em = NULL WHERE operacao_id = %s", (novo, op["operacao_id"]))


def _linha_original(cur, operacao_id):
    cur.execute(
        "SELECT lancamento_id FROM lancamento WHERE operacao_id = %s AND status = 'PROVISIONADO' LIMIT 1",
        (operacao_id,),
    )
    linha = cur.fetchone()
    return linha["lancamento_id"] if linha else None


def abrir_conta(conn, id_evento, conta_id, valor, dia, ocorrido_em):
    with conn.cursor() as cur:
        conn.begin()
        if not _registrar_evento(cur, id_evento, "ContaAberta", conta_id):
            conn.rollback()
            return False
        cur.execute(
            "INSERT INTO saldo_conta (conta_id, saldo_efetivo) VALUES (%s, %s)",
            (conta_id, valor),
        )
        cur.execute(
            """INSERT INTO lancamento
               (lancamento_id, conta_id, operacao_id, id_evento, tipo, natureza, status, valor,
                efeito_efetivo, efeito_provisionado, data_liquidacao, data_contabil, ocorrido_em)
               VALUES (%s, %s, %s, %s, 'ABERTURA', 'CREDITO', 'LIQUIDADO', %s, %s, 0, %s, %s, %s)""",
            (str(uuid.uuid4()), conta_id, f"ABERTURA-{conta_id}", id_evento, valor, valor, dia, dia, ocorrido_em),
        )
    conn.commit()
    return True


def reservar(conn, id_pedido, conta_id, operacao_id, valor, data_liquidacao, ocorrido_em, agora=None):
    """Confere o disponível e reserva o dinheiro antes de a compra ser aceita."""
    agora = agora or datetime.now()
    with conn.cursor() as cur:
        conn.begin()
        if not _registrar_evento(cur, id_pedido, "ReservaSolicitada", conta_id):
            conn.rollback()
            return False
        disponivel = _travar_conta(cur, conta_id)
        if disponivel < valor:
            conn.rollback()
            raise SaldoInsuficiente(f"disponível de R$ {disponivel}, compra de R$ {valor}")
        op = {
            "operacao_id": operacao_id, "conta_id": conta_id, "tipo": "COMPRA", "natureza": "DEBITO",
            "valor": valor, "data_liquidacao": data_liquidacao, "status": "PROVISIONADO",
        }
        cur.execute(
            """INSERT INTO operacao (operacao_id, conta_id, tipo, natureza, valor, data_liquidacao, status, expira_em)
               VALUES (%s, %s, 'COMPRA', 'DEBITO', %s, %s, 'PROVISIONADO', %s)""",
            (operacao_id, conta_id, valor, data_liquidacao, agora + timedelta(minutes=MINUTOS_RESERVA)),
        )
        dia = _dia_contabil(cur, conta_id, ocorrido_em.date())
        _lancar(cur, op, id_pedido, "PROVISIONADO", Decimal(0), -valor, dia, ocorrido_em)
    conn.commit()
    return True


def aplicar_evento(conn, evento):
    """Aplica um evento do Kafka. Retorna False se ele já tinha sido processado."""
    tipo = evento["tipo"]
    conta_id = evento["conta_id"]
    operacao_id = evento["operacao_id"]
    ocorrido_em = datetime.fromisoformat(evento["ocorrido_em"])
    try:
        with conn.cursor() as cur:
            conn.begin()
            if not _registrar_evento(cur, evento["id_evento"], tipo, conta_id):
                conn.rollback()
                return False
            _travar_conta(cur, conta_id)
            op = _travar_operacao(cur, operacao_id)
            dia = _dia_contabil(cur, conta_id, ocorrido_em.date())

            if tipo == "VendaCriada":
                _criar_venda(cur, evento, op, dia, ocorrido_em)
            elif op is None:
                raise OperacaoDesconhecida(operacao_id)
            elif tipo == "CompraCriada":
                _confirmar_compra(cur, op)
            elif tipo == "LiquidacaoConfirmada":
                _liquidar(cur, op, evento, dia, ocorrido_em)
            elif tipo == "LiquidacaoFalhou":
                _mudar_status(cur, op, "PENDENTE")
                _lancar(cur, op, evento["id_evento"], "PENDENTE", Decimal(0), Decimal(0), dia, ocorrido_em)
            elif tipo == "OperacaoCancelada":
                _cancelar(cur, op, evento["id_evento"], dia, ocorrido_em)
            else:
                raise TransicaoInvalida(f"tipo de evento desconhecido: {tipo}")
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except pymysql.err.Error:
            pass  # a conexão caiu, e o banco já desfez a transação
        raise


def _criar_venda(cur, evento, op, dia, ocorrido_em):
    if op is not None:
        raise TransicaoInvalida(f"operação já existe: {evento['operacao_id']}")
    dados = evento["dados"]
    nova = {
        "operacao_id": evento["operacao_id"], "conta_id": evento["conta_id"], "tipo": "VENDA", "natureza": "CREDITO",
        "valor": Decimal(dados["valor"]), "data_liquidacao": date.fromisoformat(dados["data_liquidacao"]),
    }
    cur.execute(
        """INSERT INTO operacao (operacao_id, conta_id, tipo, natureza, valor, data_liquidacao, status)
           VALUES (%s, %s, 'VENDA', 'CREDITO', %s, %s, 'PROVISIONADO')""",
        (nova["operacao_id"], nova["conta_id"], nova["valor"], nova["data_liquidacao"]),
    )
    _lancar(cur, nova, evento["id_evento"], "PROVISIONADO", Decimal(0), nova["valor"], dia, ocorrido_em)


def _confirmar_compra(cur, op):
    if op["status"] == "CANCELADO":
        raise TransicaoInvalida(f"reserva já vencida ou cancelada: {op['operacao_id']}")
    cur.execute(
        "UPDATE operacao SET confirmada_em = CURRENT_TIMESTAMP(6), expira_em = NULL WHERE operacao_id = %s",
        (op["operacao_id"],),
    )


def _liquidar(cur, op, evento, dia, ocorrido_em):
    _mudar_status(cur, op, "LIQUIDADO")
    sinal = _sinal(op["natureza"])
    atrasado = dia != ocorrido_em.date()
    _lancar(
        cur, op, evento["id_evento"], "LIQUIDADO", sinal * op["valor"], -sinal * op["valor"], dia, ocorrido_em,
        tipo="AJUSTE" if atrasado else None, ref=_linha_original(cur, op["operacao_id"]),
    )


def _cancelar(cur, op, id_evento, dia, ocorrido_em):
    _mudar_status(cur, op, "CANCELADO")
    _lancar(
        cur, op, id_evento, "CANCELADO", Decimal(0), -_sinal(op["natureza"]) * op["valor"], dia, ocorrido_em,
        ref=_linha_original(cur, op["operacao_id"]),
    )


def expirar_reservas(conn, agora=None):
    """Devolve ao disponível as reservas que não receberam o CompraCriada no prazo."""
    agora = agora or datetime.now()
    with conn.cursor() as cur:
        conn.begin()
        cur.execute(
            """SELECT operacao_id, conta_id FROM operacao
               WHERE status = 'PROVISIONADO' AND confirmada_em IS NULL AND expira_em < %s""",
            (agora,),
        )
        vencidas = cur.fetchall()
        conn.commit()

    expiradas = 0
    for item in vencidas:
        with conn.cursor() as cur:
            conn.begin()
            _travar_conta(cur, item["conta_id"])
            op = _travar_operacao(cur, item["operacao_id"])
            if op["status"] != "PROVISIONADO" or op["confirmada_em"] is not None:
                conn.rollback()
                continue
            id_evento = str(uuid.uuid4())
            _registrar_evento(cur, id_evento, "ReservaVencida", op["conta_id"])
            _cancelar(cur, op, id_evento, _dia_contabil(cur, op["conta_id"], agora.date()), agora)
        conn.commit()
        expiradas += 1
    return expiradas
