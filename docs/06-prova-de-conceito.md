# 6. Prova de conceito

Esta é a solução rodando de verdade no seu computador: **Kafka, MySQL, Serviço de Ledger, relay do outbox, jobs diários, API e tela de extrato**. Só as fontes externas (plataforma, back-office e B3) são simuladas.

## Como rodar

Precisa do Docker Desktop aberto.

```bash
cp .env.example .env
```

Troque cada senha do `.env` por um valor seu e suba tudo:

```bash
docker compose up -d --build --wait
```

Abra <http://localhost:8000>. À esquerda fica o aplicativo do cliente, com investimento, agenda e extrato. À direita ficam os **bastidores**: a data do sistema, a liquidação simulada da B3, as provas de falha e as tabelas do banco.

## Testes

```bash
docker compose exec api python -m pytest -p no:cacheprovider -v tests
```

Os testes passam pela API, pelo Kafka e pelo MySQL:

| O que é provado | Teste |
|---|---|
| Efetivo de 70 mil, projetado de 30 mil e 50 mil, disponível de 30 mil | `test_saldos_do_case_em_01_11` |
| Efetivo de 30 mil em 11/11 e 50 mil em 01/12 | `test_saldos_do_case_ate_a_ultima_liquidacao` |
| Mesmo evento enviado duas vezes não muda o saldo | `test_mesmo_evento_duas_vezes_nao_muda_o_saldo` |
| Evento atrasado vira ajuste e não altera o dia fechado | `test_evento_atrasado_vira_ajuste_e_nao_altera_dia_fechado` |
| Oito compras simultâneas não usam o mesmo dinheiro | `test_duas_compras_ao_mesmo_tempo_nao_usam_o_mesmo_dinheiro` |
| Transição proibida vai para a DLQ | `test_cancelar_operacao_liquidada_vai_para_a_dlq` |
| Reserva sem confirmação expira e devolve o dinheiro | `test_reserva_sem_confirmacao_expira_e_devolve_o_dinheiro` |
| Histórico não aceita `UPDATE` | `test_historico_nao_pode_ser_alterado` |

## Ver o banco direto

```bash
docker compose exec mysql sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" ledger'
```

```sql
SELECT status, efeito_efetivo, efeito_provisionado, data_contabil
FROM lancamento WHERE conta_id = 'CONTA-001' ORDER BY registrado_em;
```

## Teste de queda

Com o Ledger derrubado à força, o evento fica esperando no Kafka. Quando ele volta, o evento é aplicado uma única vez:

```bash
docker compose kill ledger
```

```bash
docker compose start ledger
```

A volta leva alguns segundos, o tempo de o Kafka perceber que a cópia anterior morreu.

## Começar do zero

A tela tem a opção de recomeçar com uma conta nova, porque o livro não aceita apagar nada. Para limpar tudo, incluindo o banco:

```bash
docker compose down -v
```

## Diferenças para a produção

| Na prova de conceito | Na produção |
|---|---|
| Plataforma, back-office e B3 simulados pela API | Sistemas reais |
| Sem login, acessível só em `localhost` | API Gateway, WAF e Cognito |
| O outbox fica no mesmo banco do ledger | Cada sistema de origem tem o seu |
| Relay lendo a tabela | MSK Connect com Debezium |
| Uma cópia de cada serviço | Várias cópias em ECS Fargate |
