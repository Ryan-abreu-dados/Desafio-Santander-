"""API da prova de conceito. As rotas /demo simulam as fontes externas e não têm login."""

import json
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, Optional

import pymysql
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app import jobs, ledger
from app.config import KAFKA, TOPICO_DLQ, TOPICO_LIQUIDACOES, TOPICO_ORDENS, conectar

app = FastAPI(title="Conta Corrente Virtual de Investimentos")
produtor = Producer({"bootstrap.servers": KAFKA})

TABELAS = {
    "lancamento": ("conta_id", "registrado_em DESC"),
    "operacao": ("conta_id", "atualizado_em DESC"),
    "saldo_conta": ("conta_id", "conta_id"),
    "fechamento_diario": ("conta_id", "data DESC"),
    "evento_processado": ("conta_id", "processado_em DESC"),
    "outbox": ("chave", "criado_em DESC"),
    "divergencia": ("conta_id", "registrado_em DESC"),
}


@contextmanager
def usar(usuario):
    conn = conectar(usuario)
    try:
        yield conn
    finally:
        conn.close()


def texto(linha):
    def converter(valor):
        if isinstance(valor, Decimal):
            return f"{valor:.2f}"
        if isinstance(valor, (date, datetime)):
            return valor.isoformat(sep=" ", timespec="seconds") if isinstance(valor, datetime) else valor.isoformat()
        return valor
    return {chave: converter(valor) for chave, valor in linha.items()}


def novo_evento(tipo, origem, conta_id, operacao_id, ocorrido_em, dados=None):
    return {
        "id_evento": str(uuid.uuid4()), "tipo": tipo, "versao_schema": 1, "origem": origem,
        "ocorrido_em": ocorrido_em.isoformat(), "conta_id": conta_id, "operacao_id": operacao_id,
        "dados": dados or {},
    }


def gravar_outbox(evento, topico):
    with usar("outbox") as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO outbox (id_evento, topico, chave, payload) VALUES (%s, %s, %s, %s)",
            (evento["id_evento"], topico, evento["conta_id"], json.dumps(evento)),
        )
        conn.commit()


def buscar_operacao(operacao_id):
    with usar("consulta") as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM operacao WHERE operacao_id = %s", (operacao_id,))
        op = cur.fetchone()
    if op is None:
        raise HTTPException(404, "operação não encontrada")
    return op


def efetivo(cur, conta_id, ate):
    cur.execute(
        "SELECT COALESCE(SUM(efeito_efetivo), 0) AS total FROM lancamento WHERE conta_id = %s AND data_contabil <= %s",
        (conta_id, ate),
    )
    return cur.fetchone()["total"]


def projetado(cur, conta_id, hoje, dia):
    if dia < hoje:
        return efetivo(cur, conta_id, dia)
    cur.execute(
        "SELECT COALESCE(SUM(efeito_provisionado), 0) AS total FROM lancamento WHERE conta_id = %s AND data_liquidacao <= %s",
        (conta_id, dia),
    )
    provisionado = cur.fetchone()["total"]
    return efetivo(cur, conta_id, hoje) + provisionado


class Ordem(BaseModel):
    conta_id: str
    valor: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    data_liquidacao: date
    ocorrido_em: datetime
    id_pedido: Optional[str] = None
    simular_queda: bool = False


class Abertura(BaseModel):
    conta_id: str
    valor: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    dia: date


class Liquidacao(BaseModel):
    operacao_id: str
    ocorrido_em: datetime


class Fechamento(BaseModel):
    conta_id: Optional[str] = None
    dia: date


class Conciliacao(BaseModel):
    conta_id: str
    dia: date
    erro_b3: Decimal = Decimal(0)


class Alteracao(BaseModel):
    conta_id: str
    usuario: Literal["app", "demo"]


class Reenvio(BaseModel):
    conta_id: str


class Expiracao(BaseModel):
    agora: datetime


@app.post("/compras")
def comprar(ordem: Ordem):
    id_pedido = ordem.id_pedido or str(uuid.uuid4())
    operacao_id = f"OP-{uuid.uuid4().hex[:12]}"
    try:
        with usar("app") as conn:
            novo = ledger.reservar(
                conn, id_pedido, ordem.conta_id, operacao_id, ordem.valor, ordem.data_liquidacao, ordem.ocorrido_em
            )
    except ledger.SaldoInsuficiente as erro:
        raise HTTPException(409, f"Compra recusada: {erro}")
    except ValueError as erro:
        raise HTTPException(404, str(erro))
    if not novo:
        return {"situacao": "pedido repetido", "id_pedido": id_pedido}
    if not ordem.simular_queda:
        evento = novo_evento(
            "CompraCriada", "plataforma-investimentos", ordem.conta_id, operacao_id, ordem.ocorrido_em,
            {"valor": str(ordem.valor), "data_liquidacao": ordem.data_liquidacao.isoformat()},
        )
        gravar_outbox(evento, TOPICO_ORDENS)
    return {"situacao": "reservada", "operacao_id": operacao_id, "id_pedido": id_pedido}


@app.post("/vendas")
def vender(ordem: Ordem):
    operacao_id = f"OP-{uuid.uuid4().hex[:12]}"
    evento = novo_evento(
        "VendaCriada", "plataforma-investimentos", ordem.conta_id, operacao_id, ordem.ocorrido_em,
        {"valor": str(ordem.valor), "data_liquidacao": ordem.data_liquidacao.isoformat()},
    )
    gravar_outbox(evento, TOPICO_ORDENS)
    return {"situacao": "publicada", "operacao_id": operacao_id}


@app.post("/operacoes/{operacao_id}/cancelar")
def cancelar(operacao_id: str, corpo: Liquidacao):
    op = buscar_operacao(operacao_id)
    evento = novo_evento("OperacaoCancelada", "plataforma-investimentos", op["conta_id"], operacao_id, corpo.ocorrido_em)
    gravar_outbox(evento, TOPICO_ORDENS)
    return {"id_evento": evento["id_evento"]}


@app.get("/contas/{conta_id}/saldos")
def saldos(conta_id: str, hoje: date, data_projecao: Optional[date] = None):
    with usar("consulta") as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM saldo_conta WHERE conta_id = %s", (conta_id,))
        conta = cur.fetchone()
        if conta is None:
            raise HTTPException(404, "conta não encontrada")
        resposta = {
            "efetivo": efetivo(cur, conta_id, hoje),
            "disponivel": conta["saldo_disponivel"],
            "provisionado_debitos": conta["provisionado_debitos"],
            "provisionado_creditos": conta["provisionado_creditos"],
        }
        if data_projecao:
            resposta["projetado"] = projetado(cur, conta_id, hoje, data_projecao)
    return texto(resposta)


@app.get("/contas/{conta_id}/extrato")
def extrato(conta_id: str, inicio: date, fim: date, hoje: date, visao: Literal["efetivo", "projetado"] = "efetivo"):
    with usar("consulta") as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(efeito_efetivo), 0) AS total FROM lancamento WHERE conta_id = %s AND data_contabil < %s",
            (conta_id, inicio),
        )
        saldo_inicial = cur.fetchone()["total"]
        cur.execute(
            """SELECT data_contabil AS data, tipo, status, efeito_efetivo AS valor, operacao_id
               FROM lancamento
               WHERE conta_id = %s AND data_contabil BETWEEN %s AND %s AND efeito_efetivo <> 0
               ORDER BY data_contabil, registrado_em""",
            (conta_id, inicio, fim),
        )
        linhas = [{**texto(l), "aberto": False, "marca": "AJUSTE" if l["tipo"] == "AJUSTE" else "Liquidado"} for l in cur.fetchall()]
        saldo_final = saldo_inicial + sum((Decimal(l["valor"]) for l in linhas), Decimal(0))

        if visao == "projetado":
            cur.execute(
                """SELECT data_liquidacao AS data, tipo, natureza, status, valor, operacao_id FROM operacao
                   WHERE conta_id = %s AND status IN ('PROVISIONADO', 'PENDENTE') AND data_liquidacao <= %s
                   ORDER BY data_liquidacao""",
                (conta_id, fim),
            )
            for op in cur.fetchall():
                valor = -op["valor"] if op["natureza"] == "DEBITO" else op["valor"]
                saldo_final += valor
                marca = "Em análise" if op["status"] == "PENDENTE" else f"A liquidar em {op['data']:%d/%m}"
                linhas.append({**texto({**op, "valor": valor}), "aberto": True, "marca": marca})
    return {"saldo_inicial": f"{saldo_inicial:.2f}", "linhas": linhas, "saldo_final": f"{saldo_final:.2f}"}


@app.get("/demo/proxima-conta")
def proxima_conta():
    with usar("consulta") as conn, conn.cursor() as cur:
        cur.execute("SELECT conta_id FROM saldo_conta WHERE conta_id LIKE 'CONTA-%'")
        usados = [int(l["conta_id"].split("-")[1]) for l in cur.fetchall()]
    return {"conta_id": f"CONTA-{max(usados, default=0) + 1:03d}"}


@app.post("/demo/abrir")
def abrir(corpo: Abertura):
    with usar("app") as conn:
        try:
            ledger.abrir_conta(conn, str(uuid.uuid4()), corpo.conta_id, corpo.valor, corpo.dia, datetime.combine(corpo.dia, datetime.min.time()))
        except pymysql.err.IntegrityError:
            raise HTTPException(409, "conta já existe")
    return {"conta_id": corpo.conta_id}


def publicar_liquidacao(tipo, corpo):
    op = buscar_operacao(corpo.operacao_id)
    evento = novo_evento(
        tipo, "back-office", op["conta_id"], corpo.operacao_id, corpo.ocorrido_em,
        {"valor": f"{op['valor']:.2f}", "natureza": op["natureza"]},
    )
    gravar_outbox(evento, TOPICO_LIQUIDACOES)
    return {"id_evento": evento["id_evento"]}


@app.post("/demo/liquidar")
def liquidar(corpo: Liquidacao):
    return publicar_liquidacao("LiquidacaoConfirmada", corpo)


@app.post("/demo/falhar")
def falhar(corpo: Liquidacao):
    return publicar_liquidacao("LiquidacaoFalhou", corpo)


@app.post("/demo/reenviar")
def reenviar(corpo: Reenvio):
    """Publica de novo, direto no Kafka, o último evento da conta."""
    with usar("demo") as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id_evento, topico, chave, payload FROM outbox WHERE chave = %s ORDER BY criado_em DESC LIMIT 1",
            (corpo.conta_id,),
        )
        linha = cur.fetchone()
    if linha is None:
        raise HTTPException(404, "nenhum evento para reenviar")
    produtor.produce(linha["topico"], key=linha["chave"], value=linha["payload"])
    produtor.flush(10)
    return {"id_evento": linha["id_evento"], "tipo": json.loads(linha["payload"])["tipo"]}


@app.post("/demo/fechar")
def fechar(corpo: Fechamento):
    with usar("fechamento") as conn:
        return {"contas_fechadas": jobs.fechar_dia(conn, corpo.dia, corpo.conta_id)}


@app.post("/demo/conciliar")
def conciliar(corpo: Conciliacao):
    with usar("demo") as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT payload FROM outbox WHERE chave = %s AND topico = %s", (corpo.conta_id, TOPICO_LIQUIDACOES)
        )
        eventos = [json.loads(l["payload"]) for l in cur.fetchall()]
    total = Decimal(0)
    for evento in eventos:
        if evento["tipo"] == "LiquidacaoConfirmada" and datetime.fromisoformat(evento["ocorrido_em"]).date() == corpo.dia:
            valor = Decimal(evento["dados"]["valor"])
            total += -valor if evento["dados"]["natureza"] == "DEBITO" else valor
    fontes = {(corpo.conta_id, "BACKOFFICE"): total, (corpo.conta_id, "B3"): total + corpo.erro_b3}
    with usar("conciliacao") as conn:
        return {"divergencias": jobs.conciliar(conn, corpo.dia, fontes, corpo.conta_id)}


@app.post("/demo/expirar")
def expirar(corpo: Expiracao):
    with usar("app") as conn:
        return {"expiradas": ledger.expirar_reservas(conn, corpo.agora)}


@app.post("/demo/alterar")
def alterar(corpo: Alteracao):
    """Tenta mudar o histórico. O esperado é o banco recusar."""
    with usar(corpo.usuario) as conn, conn.cursor() as cur:
        try:
            alteradas = cur.execute("UPDATE lancamento SET valor = 1 WHERE conta_id = %s", (corpo.conta_id,))
        except pymysql.err.MySQLError as erro:
            return {"recusado": True, "erro": erro.args[1]}
        conn.rollback()
    return {"recusado": False, "linhas": alteradas}


@app.get("/demo/tabelas/{nome}")
def tabela(nome: str, conta_id: Optional[str] = None):
    if nome not in TABELAS:
        raise HTTPException(404, "tabela desconhecida")
    coluna, ordem = TABELAS[nome]
    filtro = f"WHERE {coluna} = %s" if conta_id else ""
    with usar("demo") as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {nome} {filtro} ORDER BY {ordem} LIMIT 200", (conta_id,) if conta_id else ())
        linhas = cur.fetchall()
    return [texto(linha) for linha in linhas]


@app.get("/demo/dlq")
def dlq():
    leitor = Consumer({
        "bootstrap.servers": KAFKA, "group.id": f"leitor-{uuid.uuid4()}", "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    particoes = [TopicPartition(TOPICO_DLQ, p, OFFSET_BEGINNING) for p in range(3)]
    leitor.assign(particoes)
    mensagens, fim = [], set()
    try:
        for _ in range(200):
            msg = leitor.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                fim.add(msg.partition())
                if len(fim) == len(particoes):
                    break
                continue
            mensagens.append(json.loads(msg.value()))
    finally:
        leitor.close()
    return mensagens


@app.get("/")
def tela():
    return FileResponse(Path(__file__).parent / "static" / "index.html")
