import requests
import os
from datetime import datetime
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import zipfile
from dotenv import load_dotenv
import boto3
import re

class B3DataDownloader:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        self.base_url = "https://sistemaswebb3-listados.b3.com.br"
        self.page_url = f"{self.base_url}/indexPage/day/IBOV?language=pt-br"
        # Criar pasta data na raiz do projeto de forma dinâmica
        project_root = os.path.dirname(os.path.abspath(__file__))
        self.data_folder = os.path.join(project_root, "data")
        self.ensure_data_folder()
        
        # AWS S3 configuration
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY')
        self.aws_secret = os.getenv('AWS_SECRET')
        self.aws_region = os.getenv('AWS_REGION')
        self.aws_bucket = os.getenv('AWS_BUCKET', 'zambra-ibovespa')
        
        # Initialize S3 client
        self.s3_client = self.init_s3_client()
    
    def ensure_data_folder(self):
        """Cria a pasta /data se ela não existir"""
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
            print(f"Pasta '{self.data_folder}' criada.")
    
    def init_s3_client(self):
        """Inicializa o cliente S3"""
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret,
                region_name=self.aws_region
            )
            # Test connection
            s3_client.head_bucket(Bucket=self.aws_bucket)
            print(f"Conectado ao bucket S3: {self.aws_bucket}")
            return s3_client
        except Exception as e:
            print(f"Erro ao conectar ao S3: {str(e)}")
            return None
    
    def download_with_selenium(self):
        """
        Método usando Selenium para lidar com JavaScript
        """
        # Configurações do Chrome para download automático
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Executar em modo headless
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Configurar pasta de download
        download_path = os.path.abspath(self.data_folder)
        prefs = {
            "download.default_directory": download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            print("Acessando a página...")
            driver.get(self.page_url)
            
            # Aguardar página carregar
            wait = WebDriverWait(driver, 20)
            
            # Procurar pelo botão/link de download
            # Primeiro, tentar encontrar o link de download
            download_link = None
            
            # Estratégia 1: Procurar por link que contém "Download"
            try:
                download_link = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Download')]"))
                )
                print("Link de download encontrado!")
            except:
                print("Link de download com texto 'Download' não encontrado.")
            
            # Estratégia 2: Procurar por elementos com ícone de download
            if not download_link:
                try:
                    download_elements = driver.find_elements(By.XPATH, "//img[contains(@src, 'download')]/..")
                    if download_elements:
                        download_link = download_elements[0]
                        print("Elemento com ícone de download encontrado!")
                except:
                    print("Elemento com ícone de download não encontrado.")
            
            # Estratégia 3: Procurar por qualquer link que possa ser de download
            if not download_link:
                try:
                    # Procurar por links que contenham IBOV na URL
                    links = driver.find_elements(By.TAG_NAME, "a")
                    for link in links:
                        href = link.get_attribute("href")
                        if href and ("download" in href.lower() or "IBOV" in href):
                            download_link = link
                            print(f"Link potencial encontrado: {href}")
                            break
                except:
                    pass
            
            if download_link:
                print("Clicando no link de download...")
                driver.execute_script("arguments[0].click();", download_link)
                
                # Aguardar download iniciar/completar
                time.sleep(10)
                
                # Limpar downloads duplicados
                self.remove_duplicate_downloads()
                
                # Verificar se arquivo foi baixado
                files = os.listdir(self.data_folder)
                csv_files = [f for f in files if f.endswith('.csv') or f.endswith('.zip')]
                
                if csv_files:
                    latest_file = max([os.path.join(self.data_folder, f) for f in csv_files], 
                                    key=os.path.getctime)
                    print(f"Arquivo baixado com sucesso: {latest_file}")
                    
                    # Renomear o arquivo com o novo formato
                    renamed_file = self.rename_file_with_date_format(latest_file)
                    if renamed_file:
                        # Upload para S3 com o novo nome
                        self.upload_to_s3(renamed_file)
                        return renamed_file
                    else:
                        # Se falhar no rename, fazer upload com o nome original
                        self.upload_to_s3(latest_file)
                        return latest_file
                else:
                    print("Nenhum arquivo CSV/ZIP foi encontrado na pasta de download.")
                    return None
            else:
                print("Não foi possível encontrar o link de download.")
                return None
                
        except Exception as e:
            print(f"Erro ao baixar com Selenium: {str(e)}")
            return None
        finally:
            if driver:
                driver.quit()
    
    def remove_duplicate_downloads(self):
        """
        Remove arquivos de download duplicados, mantendo o original.
        Ex: IBOVDia_22-07-25.csv (original)
            IBOVDia_22-07-25 (1).csv (duplicado)
        """
        print("Verificando arquivos duplicados...")
        files = os.listdir(self.data_folder)
        
        # Regex para encontrar arquivos duplicados como "IBOVDia_dd-mm-yy (n).csv"
        duplicate_pattern = re.compile(r"(IBOVDia_\d{2}-\d{2}-\d{2}) \(\d+\)\.csv")
        
        originals = set()
        duplicates = []
        
        for f in files:
            match = duplicate_pattern.match(f)
            if match:
                # É um arquivo duplicado
                duplicates.append(f)
            elif f.startswith("IBOVDia_") and f.endswith(".csv"):
                # É um arquivo original
                originals.add(f)
                
        if not duplicates:
            print("Nenhum arquivo duplicado encontrado.")
            return
            
        for dup in duplicates:
            try:
                dup_path = os.path.join(self.data_folder, dup)
                os.remove(dup_path)
                print(f"Arquivo duplicado removido: {dup}")
            except OSError as e:
                print(f"Erro ao remover {dup}: {e}")

    def extract_date_from_csv(self, file_path):
        """
        Extrai a data da primeira linha do arquivo CSV.
        
        Args:
            file_path (str): Caminho completo para o arquivo CSV
            
        Returns:
            str: Data no formato yy-mm-dd extraída do arquivo, ou None se não encontrar
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                first_line = file.readline().strip()
                # Procurar padrão "IBOV - Carteira do Dia dd/mm/yy"
                import re
                date_match = re.search(r'IBOV - Carteira do Dia (\d{2}/\d{2}/\d{2})', first_line)
                if date_match:
                    date_str = date_match.group(1)
                    # Converter dd/mm/yy para yy-mm-dd
                    day, month, year = date_str.split('/')
                    return f"{year}-{month}-{day}"
                return None
        except Exception as e:
            print(f"Erro ao extrair data do CSV {file_path}: {str(e)}")
            return None
            
    def rename_file_with_date_format(self, file_path):
        """
        Renomeia o arquivo para o formato IBOVDia-yy-mm-dd.csv com base na data extraída do conteúdo.
        
        Args:
            file_path (str): Caminho completo para o arquivo CSV
            
        Returns:
            str: Novo caminho do arquivo, ou None se falhar
        """
        if not os.path.exists(file_path):
            print(f"Arquivo não encontrado: {file_path}")
            return None
            
        # Extrair a data do conteúdo do CSV
        date_str = self.extract_date_from_csv(file_path)
        if not date_str:
            print(f"Não foi possível extrair a data do arquivo: {file_path}")
            return None
            
        # Criar novo nome de arquivo
        new_filename = f"IBOVDia-{date_str}.csv"
        new_file_path = os.path.join(self.data_folder, new_filename)
        
        # Verificar se o arquivo com o novo nome já existe
        if os.path.exists(new_file_path):
            print(f"Arquivo já existe com o novo nome: {new_file_path}")
            return new_file_path
            
        try:
            # Renomear o arquivo
            os.rename(file_path, new_file_path)
            print(f"Arquivo renomeado: {os.path.basename(file_path)} -> {os.path.basename(new_file_path)}")
            return new_file_path
        except Exception as e:
            print(f"Erro ao renomear arquivo {file_path}: {str(e)}")
            return None
    
    def download_with_requests(self):
        """
        Método usando requests para tentar download direto
        """
        session = requests.Session()
        
        # Headers para simular um navegador
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        try:
            print("Tentando acessar a página para obter cookies...")
            response = session.get(self.page_url, headers=headers)
            
            if response.status_code == 200:
                print("Página acessada com sucesso!")
                
                # Tentar algumas URLs possíveis para download
                download_urls = [
                    f"{self.base_url}/indexPage/day/IBOV/download?language=pt-br",
                    f"{self.base_url}/indexPage/day/IBOV.csv?language=pt-br",
                    f"{self.base_url}/indexPage/day/IBOV?download=true&language=pt-br",
                ]
                
                for url in download_urls:
                    print(f"Tentando baixar de: {url}")
                    try:
                        download_response = session.get(url, headers=headers)
                        
                        if download_response.status_code == 200:
                            content_type = download_response.headers.get('content-type', '')
                            
                            if 'csv' in content_type or 'application/octet-stream' in content_type:
                                filename = f"IBOV_{datetime.now().strftime('%Y%m%d')}.csv"
                                filepath = os.path.join(self.data_folder, filename)
                                
                                with open(filepath, 'wb') as f:
                                    f.write(download_response.content)
                                
                                print(f"Arquivo baixado com sucesso: {filepath}")
                                
                                # Upload para S3
                                self.upload_to_s3(filepath)
                                
                                return filepath
                            
                    except Exception as e:
                        print(f"Erro ao tentar URL {url}: {str(e)}")
                        continue
                
                print("Não foi possível baixar o arquivo com requests.")
                return None
            else:
                print(f"Erro ao acessar a página: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Erro geral no método requests: {str(e)}")
            return None
    
    def upload_to_s3(self, file_path):
        """
        Faz upload do arquivo para o bucket S3
        
        Args:
            file_path (str): Caminho do arquivo local
            
        Returns:
            bool: True se upload bem-sucedido, False caso contrário
        """
        if not self.s3_client:
            print("Cliente S3 não está configurado.")
            return False
        
        try:
            filename = os.path.basename(file_path)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"ibov_data/{timestamp}_{filename}"
            
            print(f"Fazendo upload para S3: {s3_key}")
            self.s3_client.upload_file(file_path, self.aws_bucket, s3_key)
            
            print(f"Upload para S3 concluído com sucesso!")
            print(f"Arquivo disponível em: s3://{self.aws_bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"Erro ao fazer upload para S3: {str(e)}")
            return False
    
    def clean_s3_bucket(self):
        """
        Remove arquivos duplicados do bucket S3.
        """
        if not self.s3_client:
            print("Cliente S3 não está configurado.")
            return

        print("Verificando arquivos duplicados no bucket S3...")
        
        try:
            # Listar todos os objetos no bucket
            response = self.s3_client.list_objects_v2(Bucket=self.aws_bucket, Prefix="ibov_data/")
            if 'Contents' not in response:
                print("Nenhum arquivo encontrado no bucket.")
                return

            all_files = [item['Key'] for item in response['Contents']]
            
            # Regex para encontrar o padrão de duplicado
            duplicate_pattern = re.compile(r"ibov_data/(\d{8}_\d{6})_(IBOVDia_\d{2}-\d{2}-\d{2}) \(\d+\)\.csv")
            
            to_delete = []

            for key in all_files:
                if duplicate_pattern.match(key):
                    to_delete.append({'Key': key})

            if not to_delete:
                print("Nenhum arquivo duplicado encontrado no S3.")
                return

            print(f"Encontrados {len(to_delete)} arquivos duplicados para remover.")
            
            # Remover os arquivos duplicados
            delete_response = self.s3_client.delete_objects(
                Bucket=self.aws_bucket,
                Delete={'Objects': to_delete}
            )
            
            if 'Errors' in delete_response and delete_response['Errors']:
                for error in delete_response['Errors']:
                    print(f"Erro ao deletar {error['Key']}: {error['Message']}")
            else:
                print("Arquivos duplicados removidos com sucesso do S3.")

        except Exception as e:
            print(f"Erro ao limpar o bucket S3: {str(e)}")
    
    def download_data(self, method="selenium"):
        """
        Método principal para baixar os dados
        
        Args:
            method (str): "selenium" ou "requests"
        """
        print(f"Iniciando download dos dados do IBOV usando método: {method}")
        print(f"URL: {self.page_url}")
        
        if method == "selenium":
            return self.download_with_selenium()
        elif method == "requests":
            return self.download_with_requests()
        else:
            print("Método inválido. Use 'selenium' ou 'requests'")
            return None

def main():
    downloader = B3DataDownloader()
    
    # Limpar o bucket S3 antes de começar
    downloader.clean_s3_bucket()
    
    # Tentar primeiro com Selenium (mais confiável)
    print("=== Tentativa 1: Selenium ===")
    result = downloader.download_data("selenium")
    
    if not result:
        print("\n=== Tentativa 2: Requests ===")
        result = downloader.download_data("requests")
    
    if result:
        print(f"\nDownload concluído com sucesso!")
        print(f"Arquivo salvo localmente em: {result}")
        print(f"Arquivo também enviado para o bucket S3: {downloader.aws_bucket}")
    else:
        print("\nNão foi possível baixar o arquivo.")
        print("Possíveis soluções:")
        print("1. Verificar se o ChromeDriver está instalado")
        print("2. Verificar se a URL ainda está correta")
        print("3. O site pode ter proteções anti-bot")
        print("4. Verificar configurações do S3 no arquivo .env")

if __name__ == "__main__":
    main()
