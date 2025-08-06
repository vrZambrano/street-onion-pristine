# street-onion-pristine

## Visão Geral do Projeto
Este projeto contém um script Python para baixar dados do IBOV (Índice Bovespa) do site da B3 (Brasil, Bolsa, Balcão). Ele utiliza dois métodos para esse fim: Selenium WebDriver e requisições HTTP diretas. Após o download, os arquivos são automaticamente convertidos para o formato Parquet e enviados para um bucket AWS S3.

## Funcionalidades Principais
- Baixa dados do IBOV do site da B3.
- Emprega duas estratégias de download:
    1.  **Selenium/WebDriver:** O método principal e mais confiável.
    2.  **Requisições HTTP:** Um método de fallback.
- Salva os arquivos baixados (CSV) no diretório `./src/data/`.
- Converte automaticamente arquivos CSV baixados para o formato Parquet com estrutura particionada.
- Remove downloads duplicados localmente.
- Envia os arquivos convertidos para um bucket AWS S3, com particionamento por data (`ibov_data/ano=YYYY/mes=MM/dia=DD/`).
- Remove arquivos duplicados do bucket S3.
- Utiliza o Chrome em modo headless para web scraping.
- Preserva sempre os arquivos CSV originais durante o processo de conversão.

## Como Executar

### Configuração Inicial
1.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```
    ou usando Poetry:
    ```bash
    poetry install
    ```

2.  **Configure as variáveis de ambiente:**
    Crie um arquivo `.env` na raiz do projeto com base no `.env.example`:
    ```bash
    # Credenciais AWS
    AWS_ACCESS_KEY=sua_chave_de_acesso
    AWS_SECRET=sua_chave_secreta
    AWS_REGION=us-east-1
    AWS_BUCKET=seu_nome_de_bucket
    AWS_ENDPOINT=https://s3.us-east-1.amazonaws.com/{AWS_BUCKET}

    ```

### Execução Principal
3.  **Execute o script principal:**
    ```bash
    python src/main.py
    ```

### Conversão Manual de Arquivos
4.  **Converter arquivos CSV existentes para Parquet:**
    ```bash
    # Com interação do usuário
    python csv_to_parquet_converter.py
    
    # Conversão automática (remove arquivos CSV originais)
    python convert_all_csv.py
    ```

## Estrutura de Arquivos
```
src/
├── main.py                 # Script principal de download
├── data/                   # Pasta de dados
│   ├── *.csv              # Arquivos CSV baixados
│   └── ibov-data/         # Estrutura particionada de arquivos Parquet
│       └── ano=YYYY/
│           └── mes=MM/
│               └── dia=DD/
│                   └── *.parquet
convert_all_csv.py         # Script de conversão automática
csv_to_parquet_converter.py # Classe de conversão de CSV para Parquet
requirements.txt           # Dependências do projeto
pyproject.toml             # Configuração do Poetry
.env.example              # Exemplo de variáveis de ambiente
```

## Dependências
- `requests>=2.25.1`
- `selenium>=4.0.0`
- `boto3>=1.26.0`
- `python-dotenv>=0.19.0`
- `pandas>=1.3.0`
- `pyarrow>=5.0.0`

## Testes
Não há um framework de testes específico configurado neste projeto. Para garantir a funcionalidade do script, você precisará executá-lo e verificar a saída no diretório `src/data/` e no bucket S3.
