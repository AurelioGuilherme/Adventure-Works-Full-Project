import os
import pandas as pd
from databricks import sql

# Variáveis de ambiente
server_hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("http_path")
access_token = os.getenv("DATABRICKS_TOKEN")

# Verificação e tratamento de erro
if not server_hostname or not http_path or not access_token:
    raise ValueError("Uma ou mais variáveis de ambiente do Databricks estão ausentes ou vazias. Por favor, verifique: 'DATABRICKS_HOST', 'http_path', 'DATABRICKS_TOKEN'.")


def get_connections_and_load_data(query):
    """
    Função para estabelecer conexão e ler dados do Databricks
    """
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        df = pd.read_sql(query, connection)
        return df
