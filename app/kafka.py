import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import KAFKA, TOPICO_DLQ, TOPICO_LIQUIDACOES, TOPICO_ORDENS


def criar_topicos():
    admin = AdminClient({"bootstrap.servers": KAFKA})
    novos = [NewTopic(topico, num_partitions=3, replication_factor=1)
             for topico in (TOPICO_ORDENS, TOPICO_LIQUIDACOES, TOPICO_DLQ)]
    for tentativa in range(30):
        try:
            for futuro in admin.create_topics(novos).values():
                try:
                    futuro.result(10)
                except KafkaException as erro:
                    if erro.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise
            return
        except KafkaException:
            time.sleep(1)
    raise RuntimeError("não foi possível criar os tópicos")
