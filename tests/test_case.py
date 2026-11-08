"""Testes de ponta a ponta. Passam pela API, pelo Kafka e pelo MySQL de verdade."""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

api = httpx.Client(base_url="http://localhost:8000", timeout=30)

D0, D10, D11, D30 = "2026-11-01", "2026-11-11", "2026-11-12", "2026-12-01"


def post(rota, **corpo):
    resposta = api.post(rota, json=corpo)
    resposta.raise_for_status()
    return resposta.json()


def nova_conta(valor=100000):
    conta = f"T-{uuid.uuid4().hex[:12]}"
    post("/demo/abrir", conta_id=conta, valor=valor, dia="2026-10-31")
    return conta


def saldos(conta, hoje, projecao=None):
    params = {"hoje": hoje}
    if projecao:
        params["data_projecao"] = projecao
    resposta = api.get(f"/contas/{conta}/saldos", params=params)
    resposta.raise_for_status()
    return resposta.json()


def tabela(nome, conta):
    return api.get(f"/demo/tabelas/{nome}", params={"conta_id": conta}).json()


def linhas(conta):
    return len(tabela("lancamento", conta))


def esperar(condicao, segundos=20):
    limite = time.time() + segundos
    while time.time() < limite:
        if condicao():
            return True
        time.sleep(0.2)
    return False


def comprar(conta, valor, liquida, hoje=D0):
    return post("/compras", conta_id=conta, valor=valor, data_liquidacao=liquida, ocorrido_em=f"{hoje}T10:00:00")["operacao_id"]


def vender(conta, valor, liquida, hoje=D0):
    total = linhas(conta)
    op = post("/vendas", conta_id=conta, valor=valor, data_liquidacao=liquida, ocorrido_em=f"{hoje}T10:00:00")["operacao_id"]
    assert esperar(lambda: linhas(conta) == total + 1)
    return op


def liquidar(conta, op, dia):
    total = linhas(conta)
    post("/demo/liquidar", operacao_id=op, ocorrido_em=f"{dia}T18:00:00")
    assert esperar(lambda: linhas(conta) == total + 1)


def eventos_confirmados(conta, tipo):
    return [e for e in tabela("evento_processado", conta) if e["tipo"] == tipo]


@pytest.fixture
def cenario_do_case():
    conta = nova_conta()
    c30 = comprar(conta, 30000, D0)
    liquidar(conta, c30, D0)
    c40 = comprar(conta, 40000, D10)
    v20 = vender(conta, 20000, D30)
    return conta, c40, v20


def test_saldos_do_case_em_01_11(cenario_do_case):
    conta, _, _ = cenario_do_case
    assert saldos(conta, D0)["efetivo"] == "70000.00"
    assert saldos(conta, D0)["disponivel"] == "30000.00"
    assert saldos(conta, D0, D10)["projetado"] == "30000.00"
    assert saldos(conta, D0, D30)["projetado"] == "50000.00"


def test_saldos_do_case_ate_a_ultima_liquidacao(cenario_do_case):
    conta, c40, v20 = cenario_do_case
    liquidar(conta, c40, D10)
    assert saldos(conta, D10)["efetivo"] == "30000.00"
    assert saldos(conta, D10, D10)["projetado"] == "30000.00"
    liquidar(conta, v20, D30)
    assert saldos(conta, D30)["efetivo"] == "50000.00"
    assert saldos(conta, D30, D30)["projetado"] == "50000.00"
    assert saldos(conta, D30)["disponivel"] == "50000.00"


def test_mesmo_evento_duas_vezes_nao_muda_o_saldo(cenario_do_case):
    conta, c40, _ = cenario_do_case
    liquidar(conta, c40, D10)
    antes, total = saldos(conta, D10), linhas(conta)
    post("/demo/reenviar", conta_id=conta)
    time.sleep(3)
    assert saldos(conta, D10) == antes
    assert linhas(conta) == total


def test_evento_atrasado_vira_ajuste_e_nao_altera_dia_fechado(cenario_do_case):
    conta, c40, _ = cenario_do_case
    post("/demo/fechar", conta_id=conta, dia=D10)
    foto = tabela("fechamento_diario", conta)[0]
    assert foto["saldo_fechamento"] == "70000.00"

    liquidar(conta, c40, D10)

    ajuste = [l for l in tabela("lancamento", conta) if l["tipo"] == "AJUSTE"][0]
    assert ajuste["data_contabil"] == D11
    assert ajuste["lancamento_ref_id"]
    assert tabela("fechamento_diario", conta)[0] == foto
    assert saldos(conta, D10)["efetivo"] == "70000.00"
    assert saldos(conta, D11)["efetivo"] == "30000.00"


def test_duas_compras_ao_mesmo_tempo_nao_usam_o_mesmo_dinheiro():
    conta = nova_conta(30000)

    def tentar(_):
        return api.post("/compras", json={
            "conta_id": conta, "valor": 30000, "data_liquidacao": D10, "ocorrido_em": f"{D0}T10:00:00",
        }).status_code

    with ThreadPoolExecutor(max_workers=8) as grupo:
        respostas = list(grupo.map(tentar, range(8)))

    assert sorted(respostas) == [200] + [409] * 7
    assert saldos(conta, D0)["disponivel"] == "0.00"


def test_compra_sem_saldo_e_recusada():
    conta = nova_conta(10000)
    resposta = api.post("/compras", json={
        "conta_id": conta, "valor": 10000.01, "data_liquidacao": D10, "ocorrido_em": f"{D0}T10:00:00",
    })
    assert resposta.status_code == 409
    assert saldos(conta, D0)["disponivel"] == "10000.00"


def test_mesmo_pedido_de_reserva_nao_reserva_duas_vezes():
    conta = nova_conta()
    pedido = str(uuid.uuid4())
    corpo = dict(conta_id=conta, valor=1000, data_liquidacao=D10, ocorrido_em=f"{D0}T10:00:00", id_pedido=pedido)
    post("/compras", **corpo)
    assert post("/compras", **corpo)["situacao"] == "pedido repetido"
    assert saldos(conta, D0)["disponivel"] == "99000.00"


def test_cancelamento_devolve_ao_disponivel():
    conta = nova_conta()
    op = comprar(conta, 40000, D10)
    assert saldos(conta, D0)["disponivel"] == "60000.00"
    total = linhas(conta)
    post(f"/operacoes/{op}/cancelar", operacao_id=op, ocorrido_em=f"{D0}T11:00:00")
    assert esperar(lambda: linhas(conta) == total + 1)
    assert saldos(conta, D0)["disponivel"] == "100000.00"
    assert saldos(conta, D0, D10)["projetado"] == "100000.00"


def test_cancelar_operacao_liquidada_vai_para_a_dlq():
    conta = nova_conta()
    op = comprar(conta, 1000, D0)
    liquidar(conta, op, D0)
    total = linhas(conta)
    evento = post(f"/operacoes/{op}/cancelar", operacao_id=op, ocorrido_em=f"{D0}T19:00:00")["id_evento"]
    assert esperar(lambda: any(evento in m["mensagem"] for m in api.get("/demo/dlq").json()))
    assert linhas(conta) == total
    assert tabela("operacao", conta)[0]["status"] == "LIQUIDADO"


def test_liquidacao_que_falhou_mantem_o_valor_reservado():
    conta = nova_conta()
    op = comprar(conta, 40000, D10)
    total = linhas(conta)
    post("/demo/falhar", operacao_id=op, ocorrido_em=f"{D10}T18:00:00")
    assert esperar(lambda: linhas(conta) == total + 1)
    assert tabela("operacao", conta)[0]["status"] == "PENDENTE"
    assert saldos(conta, D10)["disponivel"] == "60000.00"
    assert saldos(conta, D10)["efetivo"] == "100000.00"


def test_reserva_sem_confirmacao_expira_e_devolve_o_dinheiro():
    conta = nova_conta()
    post("/compras", conta_id=conta, valor=5000, data_liquidacao=D10, ocorrido_em=f"{D0}T10:00:00", simular_queda=True)
    assert saldos(conta, D0)["disponivel"] == "95000.00"
    assert post("/demo/expirar", agora="2099-01-01T00:00:00")["expiradas"] >= 1
    assert tabela("operacao", conta)[0]["status"] == "CANCELADO"
    assert saldos(conta, D0)["disponivel"] == "100000.00"


def test_rodar_o_fechamento_de_novo_nao_duplica():
    conta = nova_conta()
    assert post("/demo/fechar", conta_id=conta, dia=D0)["contas_fechadas"] == 1
    assert post("/demo/fechar", conta_id=conta, dia=D0)["contas_fechadas"] == 0
    assert len(tabela("fechamento_diario", conta)) == 1


def test_conciliacao_registra_diferenca_uma_vez(cenario_do_case):
    conta, c40, _ = cenario_do_case
    liquidar(conta, c40, D10)
    post("/demo/fechar", conta_id=conta, dia=D10)
    assert post("/demo/conciliar", conta_id=conta, dia=D10, erro_b3=1000)["divergencias"] == 1
    assert post("/demo/conciliar", conta_id=conta, dia=D10, erro_b3=1000)["divergencias"] == 0
    divergencia = tabela("divergencia", conta)[0]
    assert divergencia["fonte"] == "B3"
    assert divergencia["diferenca"] == "-1000.00"


@pytest.mark.parametrize("usuario, trecho", [("app", "denied"), ("demo", "imutavel")])
def test_historico_nao_pode_ser_alterado(usuario, trecho):
    conta = nova_conta()
    resposta = post("/demo/alterar", conta_id=conta, usuario=usuario)
    assert resposta["recusado"] is True
    assert trecho in resposta["erro"]
    assert tabela("lancamento", conta)[0]["valor"] == "100000.00"


def test_extrato_do_periodo(cenario_do_case):
    conta, _, _ = cenario_do_case
    post("/demo/fechar", conta_id=conta, dia="2026-10-31")
    params = {"inicio": D0, "fim": D30, "hoje": D0}
    efetivo = api.get(f"/contas/{conta}/extrato", params={**params, "visao": "efetivo"}).json()
    assert efetivo["saldo_inicial"] == "100000.00"
    assert efetivo["saldo_final"] == "70000.00"
    projetado = api.get(f"/contas/{conta}/extrato", params={**params, "visao": "projetado"}).json()
    assert projetado["saldo_final"] == "50000.00"
    assert [l["marca"] for l in projetado["linhas"] if l["aberto"]] == ["A liquidar em 11/11", "A liquidar em 01/12"]
