# Pipeline ETL de Cotacoes de Acoes

Pipeline ETL em Python para coletar cotacoes de acoes brasileiras pelo Yahoo Finance, transformar os dados e carregar em PostgreSQL. O fluxo pode ser executado manualmente ou orquestrado pelo Apache Airflow.

## Fluxo
<img width="1244" height="399" alt="Analytics Engineering - ETL de acompanhamento de preço de açoes" src="https://github.com/user-attachments/assets/6d25549c-b62c-4a21-9edd-4bb3eb1e45be" />

O pipeline coleta dados diarios dos tickers definidos no projeto, padroniza colunas e datas, remove campos desnecessarios, adiciona o identificador de cada ativo e grava os dados no banco com upsert para evitar duplicidades.

## Stack

- Python 3.12
- Pandas
- yfinance
- SQLAlchemy
- PostgreSQL
- Apache Airflow
- Docker Compose
- Power BI


## Estrutura

```text
.
|-- dags/acoes_dag.py      # DAG do Airflow
|-- src/extract.py         # Extracao dos dados no Yahoo Finance
|-- src/transform.py       # Tratamento e padronizacao
|-- src/load.py            # Carga no PostgreSQL
|-- main.py                # Execucao local do pipeline
|-- docker-compose.yaml    # Airflow, Redis e PostgreSQL
`-- pyproject.toml         # Dependencias do projeto
```

## Modelo de Dados

O banco utiliza duas tabelas principais:

- `dim_ticker`: dimensao com `Id_Ticker` e `Ticker`.
- `fato_acoes`: tabela fato com `Date`, `Open`, `High`, `Low`, `Close`, `Volume` e `Id_Ticker`.

A carga usa `INSERT ON CONFLICT` nas tabelas para permitir reprocessamentos sem gerar registros duplicados.

## Configuracao

Crie o arquivo `config/.env` com as variaveis de conexao do banco:

```env
user=etl_user
password=etl_password
host=etl-postgres
port=5432
database=etl_db
sslmode=disable
```

Para o Airflow via Docker, crie tambem um `.env` na raiz com:

```env
AIRFLOW_UID=1000
```

## Execucao

Instale as dependencias:

```bash
uv sync
```

Execute localmente:

```bash
uv run python main.py
```

Ou suba o ambiente com Airflow e PostgreSQL:

```bash
docker compose up airflow-init
docker compose up -d
```

Acesse o Airflow em `http://localhost:8080` e execute a DAG `acoes_financas_etl`.

Credenciais padrao do Airflow:

```text
usuario: airflow
senha: airflow
```

## Agendamento

A DAG esta configurada para executar de segunda a sexta-feira as 05:00:

```cron
0 5 * * 1-5
```

Se a API nao retornar dados, o pipeline interrompe a execucao antes da carga no banco.

## Visualizacao

Os dados carregados no PostgreSQL podem ser conectados ao Power BI para dashboards de acompanhamento de mercado, volume negociado, variacao de precos, volatilidade e comparacao entre ativos.
