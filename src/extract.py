import logging
import pandas as pd
import yfinance as yf


# Configuração básica de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def extract_data(file_path, list_tickers, date):
    """
    Realiza a extração de dados financeiros utilizando a API do yfinance.

    Parameters
    ----------
    file_path : str
        Caminho onde o arquivo CSV será salvo.

    list_tickers : list
        Lista de tickers que serão coletados.

    date : str
        Data inicial da coleta dos dados.
    """

    # Lista responsável por armazenar os DataFrames de cada ticker
    list_df_tickers = []

    logger.info("Iniciando extração de dados.")

    try:

        # Itera sobre todos os tickers informados
        for ticker_name in list_tickers:

            logger.info(f"Coletando dados do ticker: {ticker_name}")

            # Cria objeto do ticker
            ticker = yf.Ticker(ticker_name)

            # Realiza coleta histórica
            df = ticker.history(start=date)

            # Adiciona coluna identificando o ticker
            df["ticker"] = ticker_name

            # Adiciona DataFrame na lista
            list_df_tickers.append(df)

        # Concatena todos os DataFrames em apenas um
        df = pd.concat(list_df_tickers)

        # Validação para evitar persistência de arquivos vazios
        if df.empty:
            logger.error("Nenhum dado encontrado para os tickers informados.")
            raise ValueError("Sem dados para subir pro banco!")

        # Salva dados extraídos em CSV
        df.to_csv(file_path)

        logger.info(f"Arquivo salvo com sucesso no caminho: {file_path}")

    except Exception as e:

        logger.exception(f"Erro durante a extração dos dados: {e}")

        # Relança exceção para camadas superiores (ex: Airflow)
        raise