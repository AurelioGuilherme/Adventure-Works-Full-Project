# Import load_dotenv para carregar variáveis do arquivo .env
from dotenv import load_dotenv
from airflow.exceptions import AirflowException
import os

# Carrega o arquivo .env
load_dotenv()


def get_env_vars():
    """
    Carrega e valida variáveis de ambiente obrigatórias a partir
    do arquivo `.env`.

    Se alguma variavel necessária estiver ausente no arquivo .env,
    retorna um erro identificando que as variáveis estão susentes

    Returns:
        dict: Um dicionário contendo as variáveis de ambiente obrigatórias
        e seus respectivos valores.

    Raises:
        AirflowException: Se alguma variável obrigatória estiver ausente
        ou não definida.
    """

    # Variáveis obrigatórias
    required_vars = [
        "MELTANO_PROJECT_HOST_PATH",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_CATALOG",
        "DATABRICKS_CATALOG_SCHEMA",
        "DATABRICKS_VOLUME",
        "DATABRICKS_JOB_ID"
    ]

    env_vars = {}
    missing_vars = []

    # Loop para verificar a presença das variáveis obrigatórias
    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            missing_vars.append(var)
        env_vars[var] = value

    if missing_vars:
        raise AirflowException(f"""
                               Variáveis de ambiente obrigatórias
                               não encontradas: {missing_vars}
                                """)
    return env_vars
