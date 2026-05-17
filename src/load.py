import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, table, column
from sqlalchemy.dialects.postgresql import insert as pg_insert


# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# Carrega variáveis de ambiente
env_path = Path(__file__).resolve().parent.parent / "config" / ".env"

load_dotenv(env_path)

user = os.getenv("user")
password = os.getenv("password")
host = os.getenv("host")
port = os.getenv("port", "5432")
database = os.getenv("database")
sslmode = os.getenv("sslmode")


ssl_params = f"?sslmode={sslmode}" if sslmode else ""

url = f"postgresql://{user}:{password}@{host}:{port}/{database}{ssl_params}"


def get_engine():
    """
    Cria e retorna o engine de conexão com o banco.

    Returns
    -------
    sqlalchemy.engine.Engine
        Engine de conexão com PostgreSQL.
    """

    logger.info("Criando engine de conexão com o banco.")

    return create_engine(url)


# Instância global do engine
engine = get_engine()


def ensure_table(engine, table_name):
    """
    Garante que a tabela existe com a constraint de unicidade (Date, Id_Ticker).

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Engine de conexão com o banco.

    table_name : str
        Nome da tabela de destino.
    """

    constraint_name = f"uq_{table_name}_date_id_ticker"

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                "Date" DATE NOT NULL,
                "Open" DOUBLE PRECISION,
                "High" DOUBLE PRECISION,
                "Low" DOUBLE PRECISION,
                "Close" DOUBLE PRECISION,
                "Volume" DOUBLE PRECISION,
                "Id_Ticker" INTEGER NOT NULL
            )
        """))

    with engine.begin() as conn:
        conn.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = '{constraint_name}'
                ) THEN
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT {constraint_name}
                    UNIQUE ("Date", "Id_Ticker");
                END IF;
            END $$;
        """))

    logger.info(
        "Tabela '%s' pronta com constraint '%s'.",
        table_name, constraint_name,
    )


def ensure_dim_ticker(engine):
    """
    Garante que a tabela dimensao dim_ticker existe.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Engine de conexao com o banco.
    """

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_ticker (
                "Id_Ticker" INTEGER PRIMARY KEY,
                "Ticker" VARCHAR(10) NOT NULL
            )
        """))

    logger.info("Tabela 'dim_ticker' pronta.")


def load_dim_ticker(df):
    """
    Realiza upsert dos tickers na tabela dimensao dim_ticker.

    Extrai os pares unicos (Id_Ticker, Ticker) do DataFrame
    e insere ou atualiza na dim_ticker.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame contendo as colunas Id_Ticker e ticker.
    """

    ensure_dim_ticker(engine)

    dim_df = df[["Id_Ticker", "ticker"]].drop_duplicates()
    dim_df = dim_df.rename(columns={"ticker": "Ticker"})

    records = dim_df.to_dict(orient="records")

    dim_tbl = table(
        "dim_ticker",
        column("Id_Ticker"),
        column("Ticker"),
    )

    stmt = pg_insert(dim_tbl).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["Id_Ticker"],
        set_={
            "Ticker": stmt.excluded.Ticker,
        },
    )

    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info(
        "dim_ticker atualizada com %d ticker(s).",
        len(records),
    )


def load_data(table_name, df):
    """
    Realiza upsert dos dados no PostgreSQL.

    Primeiro alimenta a tabela dimensao dim_ticker com os
    pares unicos (Id_Ticker, Ticker). Em seguida, remove
    a coluna ticker do DataFrame e faz o upsert na tabela fato.

    Se um registro com a mesma combinacao (Date, Id_Ticker) ja existir,
    os valores de Open, High, Low, Close e Volume sao atualizados.

    Parameters
    ----------
    table_name : str
        Nome da tabela de destino.

    df : pd.DataFrame
        DataFrame com os dados a serem carregados.
    """

    logger.info("Iniciando processo de upsert dos dados.")

    try:

        load_dim_ticker(df)

        df = df.drop(columns=["ticker"], errors="ignore")

        ensure_table(engine, table_name)

        with engine.begin() as connection:

            records = df.to_dict(orient="records")

            tbl = table(
                table_name,
                column("Date"),
                column("Open"),
                column("High"),
                column("Low"),
                column("Close"),
                column("Volume"),
                column("Id_Ticker"),
            )

            stmt = pg_insert(tbl).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["Date", "Id_Ticker"],
                set_={
                    "Open": stmt.excluded.Open,
                    "High": stmt.excluded.High,
                    "Low": stmt.excluded.Low,
                    "Close": stmt.excluded.Close,
                    "Volume": stmt.excluded.Volume,
                },
            )

            logger.info("Executando upsert de %d registros.", len(records))

            connection.execute(stmt)

        total_rows = pd.read_sql(
            f"SELECT COUNT(*) AS total FROM {table_name}",
            con=engine
        )

        total_rows = total_rows.iloc[0, 0]

        logger.info(
            "Upsert concluído com sucesso. "
            "Tabela '%s' possui %d registros.",
            table_name, total_rows,
        )

    except Exception as e:

        logger.exception("Erro durante o processo de carga: %s", e)

        raise