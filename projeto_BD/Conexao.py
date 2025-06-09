#Sistema de Integração com Banco de Dados#

import os
import time
import mysql.connector
from mysql.connector import Error
from tabulate import tabulate

#---------------------------------------- funções gerais ------------------------------------------#

def limpeza():
    if os.name == "nt":
        os.system("cls")

def timer(seconds):
    start_time = time.time()
    end_time = start_time + seconds

    while time.time() < end_time:
        current_time = int(end_time - time.time())
        print(f"Tempo de view: {current_time} segundos", end="\r")
        time.sleep(1)

#---------------------------------------- classse principal ---------------------------------------#

class MySQLConnector:
    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                print("Conectado ao MySQL Server versão ", db_info)
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Conexão fechada.")

    def execute_query(self, query, params=None):
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                rows = cursor.fetchall()
                table_data = []
                if cursor.description:
                    table_data.append([i[0] for i in cursor.description])
                    table_data.extend(rows)
                cursor.close()
                return table_data
            else:
                self.connection.commit()
                affected_rows = cursor.rowcount
                cursor.close()
                return f"Operação executada com sucesso. Linhas afetadas: {affected_rows}"
        except Error as e:
            self.connection.rollback()
            print(f"Erro ao executar a consulta: {e}")
            return None

    def print_table(self, table_name, columns='*', condition=None, order_by=None):
        query = f"SELECT {columns} FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"
        if order_by:
            query += f" ORDER BY {order_by}"
        table_data = self.execute_query(query)
        if table_data:
            print(f"\nTabela {table_name}:")
            print(tabulate(table_data, headers="firstrow", tablefmt="fancy_grid"))
        else:
            print(f"Tabela {table_name} está vazia ou não existe.")

#---------------------------------------- gerador do progama --------------------------------------#

if __name__ == "__main__":
    limpeza()
    config = {
        'host': '******',
        'user': '******',
        'password': '******',
        'database': '******',
        'port': ----
    }
    mysql_conn = MySQLConnector(**config)
    mysql_conn.connect()

#------------------------------------------- exercicio 1 ------------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome as nome_aluno, a.sexo as sexo_aluno, a.matricula as matricula_aluno
FROM aluno a;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 2 -----------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.data_nascimento AS idade, a.nome AS nome, a.matricula AS matricula
FROM aluno a
ORDER BY idade;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 3 ----------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT c.nome AS nome_curso, m.nome AS nome_materia, m.preco AS preco_materia
FROM curso c, curso_materia cm, materia m
WHERE c.id = cm.id_curso
AND cm.id_materia = m.id;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 4 ---------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT c.nome AS nome_curso, m.nome AS nome_materia, a.nome AS nome_aluno, a.sexo AS sexo_aluno
FROM curso c, curso_materia cm, materia m, aluno_materia am, aluno a
WHERE c.id = cm.id_curso
AND m.id = cm.id_materia
AND am.id_materia = cm.id_materia
AND a.id = am.id_aluno;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 5 ---------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome AS nome_aluno
FROM aluno a, curso_aluno ca, curso c
WHERE a.id = ca.id_aluno
AND ca.id_curso = c.id
AND a.sexo = 'M'
AND c.nome = 'Análise de Sistemas';
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 6 ---------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome AS nome_aluno
FROM aluno a, curso_aluno ca, curso c
WHERE a.id = ca.id_aluno
AND ca.id_curso = c.id
AND a.sexo = 'F'
AND c.nome = 'Engenharia Mecânica';
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 7 ---------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT 
    (SELECT nome FROM curso WHERE id = cm.id_curso) AS nome_curso,
    (SELECT nome FROM materia WHERE id = cm.id_materia) AS nome_materia,
    (SELECT nome FROM aluno WHERE id = ca.id_aluno) AS nome_aluno,
    am.nota_p1 AS nota_P1,
    am.nota_p2 AS nota_P2
FROM 
    aluno_materia am, curso_materia cm, curso_aluno ca
WHERE 
    am.id_materia = cm.id_materia
    AND am.id_aluno = ca.id_aluno
    AND cm.id_curso = ca.id_curso
    AND am.nota_p1 >= 5
    AND am.nota_p2 < 5
ORDER BY 
    nome_curso, nome_materia, nome_aluno;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 8 ----------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT c.nome AS nome_curso, COUNT(DISTINCT ca.id_aluno) AS quantidade_aluno
FROM curso c, curso_materia cm, curso_aluno ca
WHERE c.id = cm.id_curso
AND cm.id_curso = ca.id_curso
GROUP BY c.nome;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 9 ----------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT c.nome AS nome_curso, m.nome AS nome_materia, COUNT(am.id_aluno) AS Quantia_aluno
FROM curso_materia cm
JOIN curso c ON cm.id_curso = c.id
JOIN materia m ON cm.id_materia = m.id
LEFT JOIN aluno_materia am ON am.id_materia = cm.id_materia
GROUP BY c.nome, m.nome
ORDER BY nome_curso, nome_materia
LIMIT 100;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 10 --------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT m.nome AS nome_materia,
ROUND(AVG(am.nota_P1), 2) AS media_nota_P1,
ROUND(AVG(am.nota_P2), 2) AS media_nota_P2
FROM aluno_materia am
JOIN materia m ON am.id_materia = m.id
GROUP BY m.nome;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 11 --------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT 
(SELECT nome FROM materia WHERE id = am.id_materia) AS nome_materia,
(SELECT nome FROM aluno WHERE id = am.id_aluno) AS nome_aluno,
am.nota_P1 + am.nota_P2 AS media_nota
FROM aluno_materia am
WHERE am.nota_P1 + am.nota_P2 < 5
ORDER BY nome_materia, nome_aluno;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 12 --------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome AS nome_aluno, m.nome AS nome_materia, am.nota_P1 AS maior_nota
FROM aluno a, aluno_materia am, materia m
WHERE a.id = am.id_aluno
AND am.id_materia = m.id
AND am.nota_P1 = 
(SELECT MAX(am2.nota_P1)
FROM aluno_materia am2
WHERE am2.id_materia = am.id_materia)
LIMIT 0, 1000;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 13 --------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome AS nome_aluno, m.nome AS nome_materia, am.nota_P1 AS menor_nota
FROM aluno a, aluno_materia am, materia m
WHERE a.id = am.id_aluno
AND am.id_materia = m.id
AND am.nota_P1 = 
(SELECT MIN(am2.nota_P1)
FROM aluno_materia am2
WHERE am2.id_materia = am.id_materia)
LIMIT 0, 1000;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 14 --------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome AS nome_aluno, SUM(m.preco) AS valor_total
FROM aluno a, aluno_materia am, materia m
WHERE a.id = am.id_aluno
AND am.id_materia = m.id
GROUP BY a.id, a.nome;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 15.1 ------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome AS nome_aluno, a.matricula AS matricula_aluno, c.nome AS curso
FROM aluno a, curso_aluno ca, curso c
WHERE a.id = ca.id_aluno
AND ca.id_curso = c.id
AND c.nome = "Análise de Sistemas"

SELECT a.nome AS nome_aluno, a.matricula AS matricula_aluno, c.nome AS curso
FROM aluno a, curso_aluno ca, curso c
WHERE a.id = ca.id_aluno
AND ca.id_curso = c.id
AND c.nome = "Engenharia Mecânica";
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 15.2 ------------------------------------#  

    select_result = mysql_conn.execute_query("UPDATE materia SET preco = preco * 1.2 WHERE id IN (1, 5, 6, 7);")
    if select_result:
        print("\nTabela atualizada")
    timer(10)

#------------------------------------------- exercicio 16.1 ------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT a.nome AS nome_aluno, a.matricula AS matricula_aluno
FROM aluno a
WHERE a.id NOT IN (SELECT ca.id_aluno FROM curso_aluno ca);
);
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 16.2 ------------------------------------#

    select_result = mysql_conn.execute_query(
'''
UPDATE aluno_materia am
SET am.nota_P2 = am.nota_P2 + 1
WHERE (am.nota_P1 + am.nota_P2) / 2 <= 4;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 17.1 -----------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT m.nome AS nome_materia
FROM materia m
WHERE m.id NOT IN (
    SELECT cm.id_materia
    FROM curso_materia cm
);
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 17.2 -----------------------------------#

    select_result = mysql_conn.execute_query(
'''
CREATE TABLE alunos_maior (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100),
    sexo CHAR(1),
    matricula VARCHAR(20),
    data_nascimento DATE
);
INSERT INTO alunos_maior (nome, sexo, matricula, data_nascimento)
SELECT nome, sexo, matricula, data_nascimento
FROM aluno
WHERE TIMESTAMPDIFF(YEAR, data_nascimento, CURDATE()) > 30;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 17.3 -----------------------------------#

    select_result = mysql_conn.execute_query(
'''
CREATE TABLE professores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(50) NOT NULL,
    sexo CHAR(1) CHECK (sexo IN ('M', 'F')),
    data_de_nascimento DATE
);

CREATE TABLE professores_materia (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_professor INT,
    id_materia INT,
    FOREIGN KEY (id_professor) REFERENCES professores(id),
    FOREIGN KEY (id_materia) REFERENCES materia(id)
);
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 18 ------------------------------------#

    select_result = mysql_conn.execute_query(
'''
CREATE TABLE historico_materia (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_materia INT NOT NULL,
    horario TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valor_anterior DECIMAL(10, 2),
    valor_novo DECIMAL(10, 2),
    FOREIGN KEY (id_materia) REFERENCES materia(id)
);

DELIMITER //

CREATE TRIGGER after_update_preco
AFTER UPDATE ON materia FOR EACH ROW
BEGIN
    -- Verifica se houve mudança na coluna Preço
    IF OLD.preco <> NEW.preco THEN
        -- Insere no Historico
        INSERT INTO historico_materia (valor_anterior, valor_novo)
        VALUES (OLD.preco, NEW.preco);
    END IF;
END //

DELIMITER ;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 19 ------------------------------------#

    select_result = mysql_conn.execute_query(
'''
DELIMITER //
CREATE TRIGGER before_mensalidade_insert
BEFORE INSERT ON materia
FOR EACH ROW
BEGIN
    IF NEW.preco < 100 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Não é permitido incluir valores de mensalidades abaixo de 100 reais.';
    END IF;
END //

CREATE TRIGGER before_mensalidade_update
BEFORE UPDATE ON materia
FOR EACH ROW
BEGIN
    IF NEW.preco < 100 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Não é permitido alterar valores de mensalidades para abaixo de 100 reais.';
    END IF;
END //
DELIMITER ;
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 20 ------------------------------------#

    select_result = mysql_conn.execute_query(
'''
SELECT DISTINCT a.nome AS nome_aluno
FROM aluno a
WHERE a.id NOT IN (
    SELECT am.id
    FROM alunos_maior am
);
''')
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()

#------------------------------------------- exercicio 21 ------------------------------------#

    select_result = mysql_conn.execute_query("SELECT * FROM aluno WHERE data_nascimento > '1986-09-29';")
    if select_result:
        print("\nResultado da consulta:")
        print(tabulate(select_result, headers="firstrow", tablefmt="fancy_grid"))
    timer(10)
    limpeza()
