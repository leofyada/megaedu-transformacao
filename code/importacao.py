# Importa biblioteca pandas
import pandas as pd
import openpyxl

# Função para realizar a importação dos arquivos
def converte_parquet(caminho_arquivo, caminho_destino):
    
    # Importa arquivos .xlsx
    arquivo_excel = pd.read_excel(caminho_arquivo)
 
    # Retorna a base de dados em .parquet
    arquivo_excel.to_parquet(caminho_destino, engine="pyarrow", index=False)

