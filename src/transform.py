import logging
import pandas as pd


# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def transform_drop_columns(df, columns_to_drop) -> pd.DataFrame:
    """
    Remove colunas desnecessárias do DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame original.

    columns_to_drop : list
        Lista de colunas que serão removidas.

    Returns
    -------
    pd.DataFrame
        DataFrame sem as colunas removidas.
    """

    logger.info(f"Removendo colunas: {columns_to_drop}")

    df = df.drop(columns=columns_to_drop)

    return df


def transform_date(df) -> pd.DataFrame:
    """
    Converte a coluna Date para o formato de data.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame contendo a coluna Date.

    Returns
    -------
    pd.DataFrame
        DataFrame com a coluna Date formatada.
    """

    logger.info("Transformando coluna Date para formato de data.")

    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    return df


def transform_name_tickers(df) -> pd.DataFrame:
    """
    Remove o sufixo '.SA' dos tickers.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame contendo a coluna ticker.

    Returns
    -------
    pd.DataFrame
        DataFrame com os tickers padronizados.
    """

    logger.info("Padronizando nomes dos tickers.")

    df["ticker"] = df["ticker"].str.replace(".SA", "", regex=False)

    return df

def transform_map_tickers(df, ticker_map) -> pd.DataFrame:

    logger.info("Adicionando os Ids das ações")

    df["Id_Ticker"] = df["ticker"].map(ticker_map)

    return df



def transform_data(file_path, file_path_to_save, columns_to_drop, ticker_map):
    """
    Executa o pipeline de transformação dos dados.

    Etapas:
    - Leitura do CSV extraído
    - Formatação da data
    - Padronização dos tickers
    - Adição dos Ids dos Tickers
    - Removendo colunas desnecessárias
    - Persistência em formato parquet

    Parameters
    ----------
    file_path : str
        Caminho do arquivo CSV extraído.

    file_path_to_save : str
        Caminho onde o parquet será salvo.

    columns_to_drop : list
        Lista de colunas que serão removidas.
    """

    logger.info("Iniciando transformação dos dados.")

    try:

        # Leitura do arquivo bruto
        df = pd.read_csv(file_path)

        logger.info(f"Arquivo lido com sucesso: {file_path}")

        # Execução das transformações
        df = transform_date(df)

        df = transform_name_tickers(df)

        df = transform_map_tickers(df, ticker_map)

        df = transform_drop_columns(df, columns_to_drop)

        logger.info(
            f"Transformação concluída com sucesso. "
            f"Arquivo salvo em: {file_path_to_save}"
        )

        return df


    except Exception as e:

        logger.exception(f"Erro durante a transformação dos dados: {e}")

        # Relança exceção para a camada superior
        raise