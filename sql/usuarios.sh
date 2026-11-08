#!/bin/bash
# Cria os usuários da prova de conceito. As senhas vêm do arquivo .env.
set -e

mysql -uroot -p"$MYSQL_ROOT_PASSWORD" <<SQL
CREATE USER 'app'@'%' IDENTIFIED BY '$SENHA_APP';
GRANT 'ledger_app' TO 'app'@'%';
SET DEFAULT ROLE ALL TO 'app'@'%';

CREATE USER 'fechamento'@'%' IDENTIFIED BY '$SENHA_FECHAMENTO';
GRANT 'ledger_fechamento' TO 'fechamento'@'%';
SET DEFAULT ROLE ALL TO 'fechamento'@'%';

CREATE USER 'conciliacao'@'%' IDENTIFIED BY '$SENHA_CONCILIACAO';
GRANT 'ledger_conciliacao' TO 'conciliacao'@'%';
SET DEFAULT ROLE ALL TO 'conciliacao'@'%';

CREATE USER 'consulta'@'%' IDENTIFIED BY '$SENHA_CONSULTA';
GRANT 'ledger_consulta' TO 'consulta'@'%';
SET DEFAULT ROLE ALL TO 'consulta'@'%';

CREATE USER 'outbox'@'%' IDENTIFIED BY '$SENHA_OUTBOX';
GRANT 'ledger_outbox' TO 'outbox'@'%';
SET DEFAULT ROLE ALL TO 'outbox'@'%';

-- Só para a demonstração: lê todas as tabelas e tem permissão de alterar o
-- livro, para mostrar que mesmo assim o trigger recusa.
CREATE USER 'demo'@'%' IDENTIFIED BY '$SENHA_DEMO';
GRANT SELECT ON ledger.* TO 'demo'@'%';
GRANT UPDATE, DELETE ON ledger.lancamento TO 'demo'@'%';
SQL
