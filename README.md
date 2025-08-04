# street-onion-pristine

## Visão Geral do Projeto
Este projeto contém um script Python para baixar dados do IBOV (Índice Bovespa) do site da B3 (Brasil, Bolsa, Balcão). Ele utiliza dois métodos para esse fim: Selenium WebDriver e requisições HTTP diretas. Após o download, os arquivos são enviados automaticamente para um bucket AWS S3.

## Funcionalidades Principais
- Baixa dados do IBOV do site da B3.
- Emprega duas estratégias de download:
    1.  **Selenium/WebDriver:** O método principal e mais confiável.
    2.  **Requisições HTTP:** Um método de fallback.
- Salva os arquivos baixados (CSV ou ZIP) no diretório `./data/`.
- Converte arquivos CSV baixados para o formato Parquet.
- Remove downloads duplicados localmente.
- Envia os arquivos baixados para um bucket AWS S3, com particionamento por data (`ibov_data/ano=YYYY/mes=MM/dia=DD/`).
- Remove arquivos duplicados do bucket S3.
- Utiliza o Chrome em modo headless para web scraping.

## Como Executar
1.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Configure as variáveis de ambiente:**
    Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:
    ```
    # Credenciais AWS
    AWS_ACCESS_KEY=sua_chave_de_acesso
    AWS_SECRET=sua_chave_secreta
    AWS_REGION=sua_regiao
    AWS_BUCKET=seu_nome_de_bucket

    # Outras Chaves de API (se necessário)
    HF_TOKEN=
    OPENROUTER_API_BASE=
    OPENROUTER_MODEL_NAME=
    OPENROUTER_API_KEY=
    ANTHROPIC_API_KEY=
    ```
3.  **Execute o script:**
    ```bash
    python src/main.py
    ```

## Dependências
- `requests>=2.25.1`
- `selenium>=4.0.0`
- `boto3>=1.26.0`
- `python-dotenv>=0.19.0`

## Testes
Não há um framework de testes específico configurado neste projeto. Para garantir a funcionalidade do script, você precisará executá-lo e verificar a saída no diretório `data/` e no bucket S3.
