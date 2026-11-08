"""Serviço de Ledger: lê os eventos e só confirma a leitura depois de gravar no banco."""

import json
import signal
import threading
import time

import pymysql
from confluent_kafka import Consumer, Producer

from app import ledger
from app.config import KAFKA, TOPICO_DLQ, TOPICO_LIQUIDACOES, TOPICO_ORDENS, conectar
from app.kafka import criar_topicos

ESPERAS = (0.5, 1, 2, 4)


def enviar_dlq(produtor, msg, erro):
    produtor.produce(
        TOPICO_DLQ,
        key=msg.key(),
        value=json.dumps({
            "erro": str(erro),
            "topico": msg.topic(),
            "particao": msg.partition(),
            "offset": msg.offset(),
            "mensagem": msg.value().decode(),
        }),
    )
    produtor.flush(10)
    print(f"DLQ: {erro}", flush=True)


def processar(conn, produtor, msg):
    try:
        evento = json.loads(msg.value())
    except ValueError as erro:
        enviar_dlq(produtor, msg, f"formato inválido: {erro}")
        return

    for espera in (*ESPERAS, None):
        try:
            aplicado = ledger.aplicar_evento(conn, evento)
            situacao = "aplicado" if aplicado else "ignorado por ser repetido"
            print(f"{evento['tipo']} {evento['id_evento']}: {situacao}", flush=True)
            return
        except (ledger.TransicaoInvalida, ValueError, KeyError) as erro:
            enviar_dlq(produtor, msg, repr(erro))
            return
        except (ledger.OperacaoDesconhecida, pymysql.err.OperationalError) as erro:
            if espera is None:
                enviar_dlq(produtor, msg, repr(erro))
                return
            time.sleep(espera)
            conn.ping(reconnect=True)


def vigiar_reservas():
    conn = conectar("app")
    while True:
        time.sleep(30)
        conn.ping(reconnect=True)
        ledger.expirar_reservas(conn)


def main():
    criar_topicos()
    consumidor = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": "ledger",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 6000,  # uma cópia que morre é substituída em segundos
        "heartbeat.interval.ms": 2000,
    })
    produtor = Producer({"bootstrap.servers": KAFKA})
    conn = conectar("app")
    threading.Thread(target=vigiar_reservas, daemon=True).start()
    consumidor.subscribe([TOPICO_ORDENS, TOPICO_LIQUIDACOES])

    parar = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: parar.set())
    print("ledger iniciado", flush=True)
    while not parar.is_set():
        msg = consumidor.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"kafka: {msg.error()}", flush=True)
            continue
        processar(conn, produtor, msg)
        consumidor.commit(message=msg, asynchronous=False)
    consumidor.close()


if __name__ == "__main__":
    main()
