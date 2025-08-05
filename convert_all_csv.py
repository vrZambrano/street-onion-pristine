#!/usr/bin/env python3
"""
Script para conversão automática de todos os arquivos CSV para Parquet
sem interação do usuário.
"""

from csv_to_parquet_converter import CSVToParquetConverter

def main():
    """
    Executa a conversão de forma automática
    """
    # Caminho para a pasta de dados
    data_folder = "src/data"
    
    try:
        # Criar instância do conversor
        converter = CSVToParquetConverter(data_folder)
        
        print("Conversão Automática CSV para Parquet")
        print("=" * 50)
        
        # Listar arquivos antes da conversão
        print("ANTES DA CONVERSÃO:")
        converter.list_files_in_folder()
        print()
        
        # Executar conversão (removendo arquivos originais por padrão)
        print("Iniciando conversão automática (removendo arquivos CSV originais)...\n")
        stats = converter.convert_all_csv_files(remove_originals=True)
        
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
