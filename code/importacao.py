# Importa biblioteca pandas
import pandas as pd
import openpyxl
from io import StringIO
from xlsx2csv import Xlsx2csv

# Função para realizar a importação dos arquivos
def converte_parquet(caminho_arquivo, caminho_destino):
    
    # Importa arquivos .xlsx
    buffer = StringIO()
    Xlsx2csv(caminho_arquivo, outputencoding="utf-8").convert(buffer)
    buffer.seek(0)
    df = pd.read_csv(buffer)
 
    # Retorna a base de dados em .parquet
    df.to_parquet(caminho_destino, engine="pyarrow", index=False)

