# aw-checkpoint2

Projeto de teste para execução do checkpoint 2.

## Execução com Docker
```bash

docker compose up
```

## Execução full do projeto

Clone o repositório

### 1. Criar o ambiente virtual Python

Execute o comando no terminal para criar o ambiente virtual:

```bash
python3 -m venv .venv
```

### 2. Ative o ambiente virtual

Comando **linux** para ativar o ambiente virtual.
```bash
source .venv/bin/activate  
```

Comando **Windows** para ativar o ambiente virtual.
```bash
.venv/Scripts/activate  
```
### 3 Instale o meltano

```bash
pip install meltano
```

### 4 Inicialize o projeto
```bash
meltano init .
```

### 5 Adicione os plugins

**Extractor** `tap-mssql`
```bash
meltano add extractor tap-mssql
```

**Target** `parquet`
```bash
meltano add loader target-parquet
```

### 6 Adicione as variáveis de ambiente
Renomeie o arquivo `.env-example` para `.env`

Edite os valores conforme as credenciais do banco SQL Server.

### 7 Adicione o diretório onde os arquivos serão extraidos como parquet

```bash
meltano config target-parquet set --interactive
```
seleciona a opção `destination_path` e adicione o nome da pasta, por exemplo: `./extract`


### 8 Execute a extração
```bash
meltano run tap-mssql target-parquet
```
