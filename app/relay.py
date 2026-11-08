"""Lê a tabela outbox e publica no Kafka. Se cair no meio, o evento é reenviado."""

import time

from confluent_kafka import Producer

from app.config import KAFKA, conectar
from app.kafka import criar_topicos


def publicar_pendentes(conn, produtor):
    with conn.cursor() as cur:
        conn.begin()
        cur.execute(
            """SELECT id_evento, topico, chave, payload FROM outbox
               WHERE publicado_em IS NULL ORDER BY criado_em LIMIT 100"""
        )
        linhas = cur.fetchall()
        for linha in linhas:
            produtor.produce(linha["topico"], key=linha["chave"], value=linha["payload"])
        produtor.flush(10)
        for linha in linhas:
            cur.execute("UPDATE outbox SET publicado_em = CURRENT_TIMESTAMP(6) WHERE id_evento = %s", (linha["id_evento"],))
    conn.commit()
    return len(linhas)


def main():
    criar_topicos()
    produtor = Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True})
    conn = conectar("outbox")
    print("relay iniciado", flush=True)
    while True:
        if publicar_pendentes(conn, produtor) == 0:
            time.sleep(0.2)


if __name__ == "__main__":
    main()
