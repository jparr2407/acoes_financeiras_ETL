import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from src.extract import extract_data
from src.load import load_data
from src.transform import transform_data


# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# Caminho base do projeto
BASE_PATH = Path(__file__).resolve().parent


# Caminhos dos arquivos
extracted_data_file_path = (
    BASE_PATH / "data" / "extracted_data_tickers.csv"
)

transformed_data_file_path = (
    BASE_PATH / "data" / "transformed_data_tickers.parquet"
)


list_tickers = [
    "PETR4.SA",   # Petrobras
    "VALE3.SA",   # Vale
    "ITUB4.SA",   # Itaú Unibanco
    "BBDC4.SA",   # Bradesco
    "BBAS3.SA",   # Banco do Brasil
    "ABEV3.SA",   # Ambev
    "WEGE3.SA",   # WEG
    "MGLU3.SA",   # Magazine Luiza
    "LREN3.SA",   # Lojas Renner
    "SUZB3.SA",   # Suzano
    "RENT3.SA",   # Localiza
    "RAIL3.SA",   # Rumo
    "GGBR4.SA",   # Gerdau
    "CSNA3.SA",   # CSN
    "B3SA3.SA",   # B3
    "PRIO3.SA",   # PRIO
    "VIVT3.SA",   # Vivo (Telefônica Brasil)
    "TIMS3.SA",   # TIM
]

acoes_id_map = {
    "PETR4": 1,
    "VALE3": 2,
    "ITUB4": 3,
    "BBDC4": 4,
    "BBAS3": 5,
    "ABEV3": 6,
    "WEGE3": 7,
    "MGLU3": 8,
    "LREN3": 9,
    "SUZB3": 10,
    "RENT3": 11,
    "RAIL3": 12,
    "GGBR4": 13,
    "CSNA3": 14,
    "B3SA3": 15,
    "PRIO3": 16,
    "VIVT3": 17,
    "TIMS3": 18
}

# Colunas que serão removidas durante a transformação
columns_to_drop = [
    "Dividends",
    "Stock Splits"
]


# Data inicial da coleta
date = str(datetime.now().date())
# date = "2026-05-15"


# Nome da tabela de destino no PostgreSQL
table_name = "fato_acoes"


if __name__ == "__main__":

    logger.info("Iniciando pipeline ETL financeiro.")

    try:

        # Etapa de extração
        extract_data(
            extracted_data_file_path,
            list_tickers,
            date
        )

        logger.info("Etapa de extração finalizada.")

        # Etapa de transformação
        df = transform_data(
            extracted_data_file_path,
            transformed_data_file_path,
            columns_to_drop,
            acoes_id_map
        )


        logger.info("Etapa de transformação finalizada.")


        # Etapa de carga
        load_data(
            table_name,
            df
        )

        logger.info("Pipeline ETL finalizado com sucesso.")

    except Exception as e:

        logger.exception(f"Erro durante execução do pipeline: {e}")

        # Relança erro para Airflow/orquestrador
        raise