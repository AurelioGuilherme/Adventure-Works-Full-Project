# Importação de bibliotecas
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

# Importação de funções customizadas
from utils.general_helpers import get_env_vars

# Leitura das variáveis de ambiente
try:
    env_vars = get_env_vars()
except Exception as e:
    raise e

# Parâmetros padrão aplicados a todas as tasks da DAG
default_args = {
    "retries": 1,  # Se a task falhar, o Airflow tenta mais uma vez
    "retry_delay": pendulum.duration(seconds=20),  # Espera 10s antes de tentar de novo
}

# Define a data de início da DAG usando pendulum com fuso UTC
start = pendulum.datetime(2025, 1, 1, tz="UTC")

# Definição da DAG
with DAG(
    dag_id="Extract_raw_data_to_databricks",  # Nome da DAG
    start_date=start,                         # Data de início de execução
    catchup=False,                            # Cancela execução retroativa
    schedule="0 0 * * *",                     # Agendamento: todo dia à meia-noite
    default_args=default_args,
    tags=["Meltano",
          "Databricks",
          "Extract API",
          "Extract DB",
          "Ingestion",
          "Job Run"]

) as dag:

    # Volume temporário entre containers (output dos dados extraídos)
    shared_volume_temp = Mount(
        source="output_temp",
        target="/output_temp",
        type="volume"
    )

    # Mount do projeto Meltano no host para dentro do container
    meltano_project_mount = Mount(
        source=env_vars["MELTANO_PROJECT_HOST_PATH"],
        target="/project",
        type="bind"
    )

    # Instalação das dependencias do Meltano (tap e target)
    task_meltano_install = DockerOperator(
        task_id="install_tap_and_target",
        image="meltano/meltano:latest",
        command="install",
        working_dir='/project',
        auto_remove="success",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="aw-checkpoint2_default",
        mounts=[meltano_project_mount]
        )

    # Executa a extração de dados via API usando Meltano
    task_meltano_extract_api = DockerOperator(
        task_id="meltano_extract_api",
        image="meltano/meltano:latest",
        command="run extraction_api",
        working_dir="/project",
        auto_remove="success",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="aw-checkpoint2_default",
        mounts=[meltano_project_mount, shared_volume_temp],
        # Adiciona uma variável de ambiente ao Meltano referente ao output
        environment={
           "TARGET_PARQUET_API_DESTINATION_PATH": "/output_temp/output/api_data"
        }
    )

    # Executa a extração de dados do banco de dados usando Meltano
    task_meltano_extract_db = DockerOperator(
        task_id="meltano_extract_db",
        image="meltano/meltano:latest",
        command="run extraction_db",
        mount_tmp_dir=False,
        working_dir="/project",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="aw-checkpoint2_default",
        mounts=[meltano_project_mount, shared_volume_temp],
        # Adiciona uma variável de ambiente ao Meltano referente ao output
        environment={
           "TARGET_PARQUET_DB_DESTINATION_PATH": "/output_temp/output/db_data"
        }
    )

    # Ingestão dos dados para o DBFS do Databricks
    task_databricks_ingestion_api = DockerOperator(
        task_id='databricks_ingestion_api',
        image="ghcr.io/databricks/cli:latest",
        entrypoint="sh",
        # Remove a pasta 'api_data' caso ela exista no volume raw do databricks, após isso faz a ingestão dos dados com dbfs
        command=(
            "-c '"
            f"databricks fs rm -r dbfs:/Volumes/{env_vars['DATABRICKS_CATALOG']}/{env_vars['DATABRICKS_CATALOG_SCHEMA']}/{env_vars['DATABRICKS_VOLUME']}/api_data || true && "
            "databricks fs cp --recursive /output_temp/output/api_data "
            f"dbfs:/Volumes/{env_vars['DATABRICKS_CATALOG']}/{env_vars['DATABRICKS_CATALOG_SCHEMA']}/{env_vars['DATABRICKS_VOLUME']}/api_data'"
        ),
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[shared_volume_temp],
        environment={
             "DATABRICKS_HOST": env_vars["DATABRICKS_HOST"],
             "DATABRICKS_TOKEN": env_vars["DATABRICKS_TOKEN"],
             "DATABRICKS_CATALOG": env_vars["DATABRICKS_CATALOG"],
             "DATABRICKS_CATALOG_SCHEMA": env_vars["DATABRICKS_CATALOG_SCHEMA"],
             "DATABRICKS_VOLUME": env_vars["DATABRICKS_VOLUME"]
            }
    )

    # Ingestão dos dados para o DBFS do Databricks
    task_databricks_ingestion_db = DockerOperator(
        task_id='databricks_ingestion_db',
        image="ghcr.io/databricks/cli:latest",
        mount_tmp_dir=False,
        entrypoint="sh",
        # Remove a pasta 'db_data' caso ela exista no volume raw do databricks, após isso faz a ingestão dos dados com dbfs
        command=(
            "-c '"
            f"databricks fs rm -r dbfs:/Volumes/{env_vars['DATABRICKS_CATALOG']}/{env_vars['DATABRICKS_CATALOG_SCHEMA']}/{env_vars['DATABRICKS_VOLUME']}/db_data || true && "
            "databricks fs cp --recursive /output_temp/output/db_data "
            f"dbfs:/Volumes/{env_vars['DATABRICKS_CATALOG']}/{env_vars['DATABRICKS_CATALOG_SCHEMA']}/{env_vars['DATABRICKS_VOLUME']}/db_data'"
        ),
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mounts=[shared_volume_temp],
        environment={
             "DATABRICKS_HOST": env_vars["DATABRICKS_HOST"],
             "DATABRICKS_TOKEN": env_vars["DATABRICKS_TOKEN"],
             "DATABRICKS_CATALOG": env_vars["DATABRICKS_CATALOG"],
             "DATABRICKS_CATALOG_SCHEMA": env_vars["DATABRICKS_CATALOG_SCHEMA"],
             "DATABRICKS_VOLUME": env_vars["DATABRICKS_VOLUME"]
            }
        )

    # Dispara uma job do Databricks para converter os dados para delta table
    task_databricks_job_run = DockerOperator(
        task_id='databricks_job_run',
        image="ghcr.io/databricks/cli:latest",
        mount_tmp_dir=False,
        entrypoint="sh",
        command=f"-c 'databricks jobs run-now {env_vars['DATABRICKS_JOB_ID']}'",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        environment={
            "DATABRICKS_HOST": env_vars["DATABRICKS_HOST"],
            "DATABRICKS_TOKEN": env_vars["DATABRICKS_TOKEN"],
            "DATABRICKS_JOB_ID": env_vars["DATABRICKS_JOB_ID"]
            }
    )

    # Remove o volume temporário caso ele exista
    task_remove_temp_volume = BashOperator(
        task_id="remover_volume_temp",
        bash_command="docker volume rm output_temp || true"
    )

    # Instalação das dependencias do Meltano e executa a extração do BD e API de forma paralela
    task_meltano_install >> [task_meltano_extract_db, task_meltano_extract_api]

    # Após a extração dos dados da API faz a ingestão dos dados no Databricks
    task_meltano_extract_api >> task_databricks_ingestion_api

    # Após a extração dos dados do BD faz a ingestão dos dados no Databricks
    task_meltano_extract_db >> task_databricks_ingestion_db

    # Após concluir as duas ingestões remove o volume temporário do docker
    [task_databricks_ingestion_api, task_databricks_ingestion_db] >> task_remove_temp_volume

    # Após remover o volume temporário executa o job no Databricks para converter os arquivos para Delta table
    task_remove_temp_volume >> task_databricks_job_run
