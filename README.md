# Sistema de intregação Python com o Banco de Dados

Este projeto contém uma série de exercícios em Python que realizam consultas, atualizações, criação de tabelas, triggers e outras operações utilizando o banco de dados MySQL. Alem de possuir um controle de visual de tela usando funções e bibliotecas como tabulete, limpeza() e timer().

## Tecnologias utilizadas
- Python
- MySQL
- Biblioteca `tabulate` para formatação de tabelas no terminal
- Conexão com banco via classe `mysql_conn` personalizada
- Funções auxiliares: `timer()` e `limpeza()`

## Estrutura dos Exercícios
- Exercicios de Select
- Exercicios de Join
- Exercicios de Trigges
- Exercicios de Update
- Exercicios de Criação de tabelas
- Exercicios de Insert
- Exercicios de Ordenação
- Exercicios com Aritimetica
- Exercicios com Funções

## Observações
- Todas as Querys foram testadas e funcionam corretamente.
- As tabelas utilizadas incluem: `aluno`, `materia`, `aluno_materia`, `curso`, `curso_aluno`, `curso_materia`, `alunos_maior`, entre outras.
- As consultas utilizam JOINs, subconsultas, funções de agregação, `GROUP BY`, `HAVING`, `NOT IN`, e outras funcionalidades SQL.
- Algumas operações foram feitas em lote (como atualizações e inserções baseadas em condições).
