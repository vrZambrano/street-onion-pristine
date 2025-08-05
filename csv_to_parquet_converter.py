import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import glob
import re
from datetime import datetime

class CSVToParquetConverter:
    def __init__(self, data_folder_path):
        """
        Inicializa o conversor com o caminho da pasta de dados
        
        Args:
            data_folder_path (str): Caminho para a pasta contendo os arquivos CSV
        """
        self.data_folder = Path(data_folder_path)
        self.ibov_data_folder = self.data_folder / "ibov-data"
        
        if not self.data_folder.exists():
            raise FileNotFoundError(f"Pasta não encontrada: {data_folder_path}")
        
        # Criar pasta ibov-data se não existir
        self.ibov_data_folder.mkdir(exist_ok=True)
        print(f"Pasta de destino: {self.ibov_data_folder}")
    
    def extract_date_from_filename(self, filename):
        """
        Extrai a data do nome do arquivo no formato IBOVDia_dd-mm-yy.csv
        
        Args:
            filename (str): Nome do arquivo
            
        Returns:
            tuple: (dia, mes, ano) ou None se não conseguir extrair
        """
        # Padrão para IBOVDia_dd-mm-yy.csv
        pattern = r'IBOVDia_(\d{2})-(\d{2})-(\d{2})\.csv'
        match = re.search(pattern, filename)
        
        if match:
            day, month, year = match.groups()
            # Converter ano de 2 dígitos para 4 dígitos
            full_year = f"20{year}" if int(year) < 50 else f"19{year}"
            return day, month, full_year
        
        return None
    
    def create_partitioned_path(self, day, month, year):
        """
        Cria o caminho particionado ano=YYYY/mes=MM/dia=DD
        
        Args:
            day (str): Dia (DD)
            month (str): Mês (MM)
            year (str): Ano (YYYY)
            
        Returns:
            Path: Caminho da pasta particionada
        """
        partition_path = self.ibov_data_folder / f"ano={year}" / f"mes={month.zfill(2)}" / f"dia={day.zfill(2)}"
        partition_path.mkdir(parents=True, exist_ok=True)
        return partition_path
    
    def convert_csv_to_parquet(self, csv_file_path, remove_original=True):
        """
        Converte um arquivo CSV específico para formato Parquet com estrutura particionada
        
        Args:
            csv_file_path (str): Caminho do arquivo CSV
            remove_original (bool): Se True, remove o arquivo CSV original após conversão
            
        Returns:
            str: Caminho do arquivo Parquet gerado, ou None se falhar
        """
        try:
            filename = os.path.basename(csv_file_path)
            print(f"Convertendo: {filename}")
            
            # Extrair data do nome do arquivo
            date_info = self.extract_date_from_filename(filename)
            if not date_info:
                print(f"✗ Não foi possível extrair a data do arquivo: {filename}")
                return None
            
            day, month, year = date_info
            print(f"  Data extraída: {day}/{month}/{year}")
            
            # Ler CSV ignorando as 2 primeiras linhas e sem header
            df = pd.read_csv(
                csv_file_path, 
                encoding='latin1', 
                sep=';', 
                skiprows=2,  # Pular as duas primeiras linhas
                skipfooter=2,  # Pular as duas últimas linhas com informações agregadas
                engine='python',  # Necessário para skipfooter
                header=None  # Não há header
            )
            
            print(f"  Colunas encontradas: {len(df.columns)}")
            
            # Remover a última coluna (que é vazia devido ao ; no final de cada linha)
            df = df.iloc[:, :-1]
            print(f"  Removida última coluna vazia. Colunas restantes: {len(df.columns)}")
            
            # Verificar se temos exatamente 5 colunas após remoção
            if len(df.columns) != 5:
                raise ValueError(f"Esperado 5 colunas após remoção da coluna vazia, mas encontrado {len(df.columns)}")
            
            # Definir nomes das colunas conforme especificado
            df.columns = ['codigo', 'acao', 'tipo', 'qtde_teorica', 'participacao']
            
            print(f"  Primeiras linhas após processamento:\n{df.head(2)}")
            
            # Limpar dados
            # Converter qtde_teorica para numérico (remover pontos de milhares)
            df['qtde_teorica'] = df['qtde_teorica'].astype(str).str.replace('.', '').astype(float)
            
            # Converter participacao para numérico (trocar vírgula por ponto)
            df['participacao'] = df['participacao'].astype(str).str.replace(',', '.').astype(float)
            
            # Limpar espaços em branco das colunas de texto
            df['codigo'] = df['codigo'].astype(str).str.strip()
            df['acao'] = df['acao'].astype(str).str.strip()
            df['tipo'] = df['tipo'].astype(str).str.strip()
            
            # Criar coluna data do tipo date no formato yyyy-mm-dd
            data_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            df['data'] = pd.to_datetime(data_str).date()
            
            print(f"  Data adicionada: {data_str}")
            
            # Criar timestamp de carga (formato: YYYYMMDD_HHMMSS)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Criar caminho particionado
            partition_path = self.create_partitioned_path(day, month, year)
            
            # Nome do arquivo com timestamp
            parquet_filename = f"{timestamp}_IBOVDia_{day}-{month}-{year[-2:]}.parquet"
            parquet_path = partition_path / parquet_filename
            
            # Converter para Parquet
            df.to_parquet(parquet_path, index=False, engine='pyarrow')
            
            print(f"✓ Convertido para: {parquet_path.relative_to(self.ibov_data_folder)}")
            
            # Remover arquivo CSV original se solicitado
            if remove_original:
                os.remove(csv_file_path)
                print(f"✓ Arquivo CSV original removido: {filename}")
            
            return str(parquet_path)
            
        except Exception as e:
            print(f"✗ Erro ao converter {os.path.basename(csv_file_path)}: {str(e)}")
            return None
    
    def convert_all_csv_files(self, remove_originals=False):
        """
        Converte todos os arquivos CSV da pasta para Parquet
        
        Args:
            remove_originals (bool): Se True, remove os arquivos CSV originais após conversão
            
        Returns:
            dict: Dicionário com estatísticas da conversão
        """
        # Encontrar todos os arquivos CSV na pasta
        csv_pattern = os.path.join(self.data_folder, "*.csv")
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            print("Nenhum arquivo CSV encontrado na pasta.")
            return {"total": 0, "converted": 0, "failed": 0}
        
        print(f"Encontrados {len(csv_files)} arquivo(s) CSV para conversão:")
        for csv_file in csv_files:
            print(f"  - {os.path.basename(csv_file)}")
        
        print("\nIniciando conversão...\n")
        
        converted_count = 0
        failed_count = 0
        converted_files = []
        failed_files = []
        
        for csv_file in csv_files:
            result = self.convert_csv_to_parquet(csv_file, remove_originals)
            if result:
                converted_count += 1
                converted_files.append(result)
            else:
                failed_count += 1
                failed_files.append(csv_file)
            print()  # Linha em branco para separar
        
        # Resumo da conversão
        print("=" * 50)
        print("RESUMO DA CONVERSÃO")
        print("=" * 50)
        print(f"Total de arquivos CSV encontrados: {len(csv_files)}")
        print(f"Convertidos com sucesso: {converted_count}")
        print(f"Falhas na conversão: {failed_count}")
        
        if converted_files:
            print("\nArquivos convertidos:")
            for file in converted_files:
                print(f"  ✓ {os.path.basename(file)}")
        
        if failed_files:
            print("\nArquivos com falha:")
            for file in failed_files:
                print(f"  ✗ {os.path.basename(file)}")
        
        return {
            "total": len(csv_files),
            "converted": converted_count,
            "failed": failed_count,
            "converted_files": converted_files,
            "failed_files": failed_files
        }
    
    def list_files_in_folder(self):
        """
        Lista todos os arquivos na pasta de dados e na estrutura ibov-data
        """
        print(f"Arquivos na pasta {self.data_folder}:")
        
        csv_files = list(self.data_folder.glob("*.csv"))
        parquet_files = list(self.data_folder.glob("*.parquet"))
        other_files = [f for f in self.data_folder.iterdir() 
                      if f.is_file() and f.suffix not in ['.csv', '.parquet']]
        
        if csv_files:
            print(f"\nArquivos CSV ({len(csv_files)}):")
            for file in sorted(csv_files):
                print(f"  - {file.name}")
        
        if parquet_files:
            print(f"\nArquivos Parquet ({len(parquet_files)}):")
            for file in sorted(parquet_files):
                print(f"  - {file.name}")
        
        if other_files:
            print(f"\nOutros arquivos ({len(other_files)}):")
            for file in sorted(other_files):
                print(f"  - {file.name}")
        
        # Listar estrutura da pasta ibov-data se existir
        if self.ibov_data_folder.exists():
            print(f"\nEstrutura da pasta {self.ibov_data_folder.name}:")
            self._list_ibov_structure()
        
        if not csv_files and not parquet_files and not other_files and not self.ibov_data_folder.exists():
            print("  (pasta vazia)")
    
    def _list_ibov_structure(self):
        """
        Lista a estrutura hierárquica da pasta ibov-data
        """
        if not self.ibov_data_folder.exists():
            print("  (pasta ibov-data não existe)")
            return
        
        # Encontrar todos os arquivos parquet na estrutura
        parquet_files = list(self.ibov_data_folder.rglob("*.parquet"))
        
        if not parquet_files:
            print("  (pasta ibov-data vazia)")
            return
        
        # Organizar por ano/mês/dia
        structure = {}
        for file in parquet_files:
            # Extrair ano, mês, dia do caminho
            parts = file.parts
            if len(parts) >= 4:  # ibov-data/ano=YYYY/mes=MM/dia=DD/arquivo.parquet
                try:
                    ano = parts[-4].split('=')[1] if '=' in parts[-4] else parts[-4]
                    mes = parts[-3].split('=')[1] if '=' in parts[-3] else parts[-3]
                    dia = parts[-2].split('=')[1] if '=' in parts[-2] else parts[-2]
                    
                    if ano not in structure:
                        structure[ano] = {}
                    if mes not in structure[ano]:
                        structure[ano][mes] = {}
                    if dia not in structure[ano][mes]:
                        structure[ano][mes][dia] = []
                    
                    structure[ano][mes][dia].append(file.name)
                except:
                    continue
        
        # Exibir estrutura organizada
        for ano in sorted(structure.keys()):
            print(f"  ano={ano}/")
            for mes in sorted(structure[ano].keys()):
                print(f"    mes={mes}/")
                for dia in sorted(structure[ano][mes].keys()):
                    print(f"      dia={dia}/")
                    for arquivo in sorted(structure[ano][mes][dia]):
                        print(f"        - {arquivo}")

def main():
    """
    Função principal para executar a conversão
    """
    # Caminho para a pasta de dados
    data_folder = "src/data"
    
    try:
        # Criar instância do conversor
        converter = CSVToParquetConverter(data_folder)
        
        print("CSV to Parquet Converter")
        print("=" * 50)
        
        # Listar arquivos antes da conversão
        print("ANTES DA CONVERSÃO:")
        converter.list_files_in_folder()
        print()
        
        # Perguntar se deve remover os arquivos originais
        while True:
            remove_choice = input("Remover arquivos CSV originais após conversão? (s/n): ").lower().strip()
            if remove_choice in ['s', 'sim', 'y', 'yes']:
                remove_originals = True
                break
            elif remove_choice in ['n', 'não', 'nao', 'no']:
                remove_originals = False
                break
            else:
                print("Por favor, responda 's' para sim ou 'n' para não.")
        
        print()
        
        # Executar conversão
        stats = converter.convert_all_csv_files(remove_originals)
        
        print()
        
        # Listar arquivos após a conversão
        print("APÓS A CONVERSÃO:")
        converter.list_files_in_folder()
        
        # Verificar se houve alguma conversão
        if stats["converted"] > 0:
            print(f"\n🎉 Conversão concluída! {stats['converted']} arquivo(s) convertido(s) com sucesso.")
        else:
            print("\n⚠️  Nenhum arquivo foi convertido.")
            
    except FileNotFoundError as e:
        print(f"Erro: {e}")
        print("Verifique se o caminho da pasta está correto.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()
