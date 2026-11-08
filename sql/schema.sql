-- Modelo de dados do ledger. Requer MySQL 8.0.16 ou superior.
-- Explicação de cada tabela em docs/02-modelo-de-dados.md.

CREATE DATABASE IF NOT EXISTS ledger
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;

USE ledger;

-- O livro-razão. Só recebe INSERT: cada mudança de estado é uma linha nova.
-- O saldo sai da soma de efeito_efetivo e efeito_provisionado.
CREATE TABLE lancamento (
    lancamento_id       CHAR(36)      NOT NULL,
    conta_id            VARCHAR(20)   NOT NULL,
    operacao_id         VARCHAR(36)   NOT NULL,
    id_evento           CHAR(36)      NOT NULL,
    tipo                ENUM('ABERTURA', 'COMPRA', 'VENDA', 'AJUSTE') NOT NULL,
    natureza            ENUM('DEBITO', 'CREDITO') NOT NULL,
    status              ENUM('PROVISIONADO', 'LIQUIDADO', 'CANCELADO', 'PENDENTE') NOT NULL,
    valor               DECIMAL(18, 2) NOT NULL,
    efeito_efetivo      DECIMAL(18, 2) NOT NULL,
    efeito_provisionado DECIMAL(18, 2) NOT NULL,
    data_liquidacao     DATE          NOT NULL,  -- quando deve liquidar
    data_contabil       DATE          NOT NULL,  -- em que dia a linha conta no saldo
    ocorrido_em         DATETIME(6)   NOT NULL,  -- quando o fato aconteceu na origem
    registrado_em       DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),  -- quando o ledger gravou
    lancamento_ref_id   CHAR(36)      NULL,      -- linha original, em estornos e ajustes
    PRIMARY KEY (lancamento_id, data_contabil),
    KEY ix_lancamento_extrato (conta_id, data_contabil),
    KEY ix_lancamento_projecao (conta_id, data_liquidacao),
    KEY ix_lancamento_operacao (operacao_id),
    CONSTRAINT ck_lancamento_valor CHECK (valor > 0),
    CONSTRAINT ck_lancamento_efetivo CHECK (efeito_efetivo IN (0, valor, -valor)),
    CONSTRAINT ck_lancamento_provisionado CHECK (efeito_provisionado IN (0, valor, -valor))
)
PARTITION BY RANGE COLUMNS (data_contabil) (
    PARTITION p_ate_2026_10 VALUES LESS THAN ('2026-11-01'),
    PARTITION p_2026_11     VALUES LESS THAN ('2026-12-01'),
    PARTITION p_2026_12     VALUES LESS THAN ('2027-01-01'),
    PARTITION p_futuro      VALUES LESS THAN (MAXVALUE)
);

-- Garante que cada evento seja processado uma única vez.
-- Fica fora de lancamento porque, numa tabela particionada, toda chave única
-- precisaria incluir a data_contabil.
CREATE TABLE evento_processado (
    id_evento     CHAR(36)     NOT NULL,
    tipo          VARCHAR(40)  NOT NULL,
    conta_id      VARCHAR(20)  NOT NULL,
    processado_em DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id_evento)
);

-- Estado atual de cada operação. Pode ser reconstruída a partir de lancamento.
CREATE TABLE operacao (
    operacao_id     VARCHAR(36)   NOT NULL,
    conta_id        VARCHAR(20)   NOT NULL,
    tipo            ENUM('COMPRA', 'VENDA') NOT NULL,
    natureza        ENUM('DEBITO', 'CREDITO') NOT NULL,
    valor           DECIMAL(18, 2) NOT NULL,
    data_liquidacao DATE          NOT NULL,
    status          ENUM('PROVISIONADO', 'LIQUIDADO', 'CANCELADO', 'PENDENTE') NOT NULL,
    expira_em       DATETIME(6)   NULL,  -- prazo para a reserva receber o CompraCriada
    confirmada_em   DATETIME(6)   NULL,  -- quando o CompraCriada chegou
    atualizado_em   DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (operacao_id),
    KEY ix_operacao_conta (conta_id, status),
    KEY ix_operacao_expiracao (status, expira_em),
    CONSTRAINT ck_operacao_valor CHECK (valor > 0)
);

-- Visão de saldos para leitura rápida. Pode ser reconstruída a partir de lancamento.
-- A linha da conta também é travada na reserva, com SELECT ... FOR UPDATE.
CREATE TABLE saldo_conta (
    conta_id             VARCHAR(20)   NOT NULL,
    saldo_efetivo        DECIMAL(18, 2) NOT NULL DEFAULT 0,
    provisionado_debitos  DECIMAL(18, 2) NOT NULL DEFAULT 0,  -- zero ou negativo
    provisionado_creditos DECIMAL(18, 2) NOT NULL DEFAULT 0,  -- zero ou positivo
    saldo_disponivel     DECIMAL(18, 2) GENERATED ALWAYS AS (saldo_efetivo + provisionado_debitos) STORED,
    atualizado_em        DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (conta_id)
);

-- Foto do saldo no fim de cada dia. Existir linha significa que o dia está fechado.
CREATE TABLE fechamento_diario (
    conta_id         VARCHAR(20)   NOT NULL,
    data             DATE          NOT NULL,
    saldo_anterior   DECIMAL(18, 2) NOT NULL,
    total_liquidado  DECIMAL(18, 2) NOT NULL,
    saldo_fechamento DECIMAL(18, 2) NOT NULL,
    fechado_em       DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (conta_id, data),
    CONSTRAINT ck_fechamento_diario_soma CHECK (saldo_fechamento = saldo_anterior + total_liquidado)
);

-- Eventos à espera de publicação no Kafka. Na produção, cada sistema de origem
-- tem a sua. Aqui ela representa a origem na prova de conceito.
CREATE TABLE outbox (
    id_evento    CHAR(36)     NOT NULL,
    topico       VARCHAR(100) NOT NULL,
    chave        VARCHAR(20)  NOT NULL,  -- conta_id, para manter a ordem por conta
    payload      JSON         NOT NULL,
    criado_em    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    publicado_em DATETIME(6)  NULL,
    PRIMARY KEY (id_evento),
    KEY ix_outbox_pendentes (publicado_em, criado_em)
);

-- Diferenças achadas na conciliação. Só recebe INSERT: resolver uma divergência
-- é gravar uma linha RESOLVIDA apontando para a original.
-- A chave única impede que rodar a conciliação de novo duplique a mesma diferença.
CREATE TABLE divergencia (
    divergencia_id     CHAR(36)      NOT NULL,
    divergencia_ref_id CHAR(36)      NULL,  -- linha ABERTA que esta linha resolve
    data               DATE          NOT NULL,
    conta_id           VARCHAR(20)   NOT NULL,
    fonte              ENUM('BACKOFFICE', 'B3') NOT NULL,
    valor_ledger       DECIMAL(18, 2) NOT NULL,
    valor_fonte        DECIMAL(18, 2) NOT NULL,
    diferenca          DECIMAL(18, 2) GENERATED ALWAYS AS (valor_ledger - valor_fonte) STORED,
    status             ENUM('ABERTA', 'RESOLVIDA') NOT NULL,
    observacao         VARCHAR(255)  NULL,
    registrado_em      DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (divergencia_id),
    UNIQUE KEY uq_divergencia (data, conta_id, fonte, status, valor_ledger, valor_fonte),
    KEY ix_divergencia_conta (conta_id, data)
);

-- Imutabilidade: nem quem tem permissão de escrita consegue alterar o histórico.
CREATE TRIGGER trg_lancamento_sem_update BEFORE UPDATE ON lancamento
    FOR EACH ROW SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'lancamento e imutavel: registre um estorno ou ajuste';

CREATE TRIGGER trg_lancamento_sem_delete BEFORE DELETE ON lancamento
    FOR EACH ROW SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'lancamento e imutavel: registre um estorno ou ajuste';

CREATE TRIGGER trg_fechamento_diario_sem_update BEFORE UPDATE ON fechamento_diario
    FOR EACH ROW SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'dia fechado nao pode ser alterado: registre um ajuste';

CREATE TRIGGER trg_fechamento_diario_sem_delete BEFORE DELETE ON fechamento_diario
    FOR EACH ROW SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'dia fechado nao pode ser alterado: registre um ajuste';

CREATE TRIGGER trg_divergencia_sem_update BEFORE UPDATE ON divergencia
    FOR EACH ROW SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'divergencia e imutavel: registre uma linha RESOLVIDA';

CREATE TRIGGER trg_divergencia_sem_delete BEFORE DELETE ON divergencia
    FOR EACH ROW SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'divergencia e imutavel: registre uma linha RESOLVIDA';

-- Permissões mínimas por papel. Usuários e senhas são criados fora do repositório.
CREATE ROLE IF NOT EXISTS 'ledger_app', 'ledger_fechamento', 'ledger_conciliacao', 'ledger_consulta', 'ledger_outbox';

GRANT SELECT, INSERT         ON ledger.lancamento        TO 'ledger_app';
GRANT SELECT, INSERT         ON ledger.evento_processado TO 'ledger_app';
GRANT SELECT, INSERT, UPDATE ON ledger.operacao          TO 'ledger_app';
GRANT SELECT, INSERT, UPDATE ON ledger.saldo_conta       TO 'ledger_app';
GRANT SELECT                 ON ledger.fechamento_diario TO 'ledger_app';

GRANT SELECT                 ON ledger.lancamento        TO 'ledger_fechamento';
GRANT SELECT, INSERT         ON ledger.fechamento_diario TO 'ledger_fechamento';
GRANT SELECT, UPDATE         ON ledger.saldo_conta       TO 'ledger_fechamento';  -- UPDATE é exigido pelo FOR UPDATE

GRANT SELECT                 ON ledger.lancamento        TO 'ledger_conciliacao';
GRANT SELECT                 ON ledger.fechamento_diario TO 'ledger_conciliacao';
GRANT SELECT, INSERT         ON ledger.divergencia       TO 'ledger_conciliacao';
GRANT SELECT                 ON ledger.saldo_conta       TO 'ledger_conciliacao';

GRANT SELECT                 ON ledger.lancamento        TO 'ledger_consulta';
GRANT SELECT                 ON ledger.operacao          TO 'ledger_consulta';
GRANT SELECT                 ON ledger.saldo_conta       TO 'ledger_consulta';
GRANT SELECT                 ON ledger.fechamento_diario TO 'ledger_consulta';

GRANT SELECT, INSERT, UPDATE ON ledger.outbox            TO 'ledger_outbox';
