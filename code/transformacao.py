# Importa bibliotecas
import pandas as pd
import numpy as np
import logging

# Função para realizar o join das bases de dados
def limpeza_basica(caminho_baseline, caminho_fonteunica, caminho_base_limpa):
    
    # Importa arquivos .parquet
    df_baseline = pd.read_parquet(caminho_baseline, engine="pyarrow")
    df_fonteunica = pd.read_parquet(caminho_fonteunica, engine="pyarrow")

    # Remove colunas de index
    df_baseline = df_baseline.drop(columns=["Unnamed: 0"], errors="ignore")
    df_fonteunica = df_fonteunica.drop(columns=["Unnamed: 0"], errors="ignore")

    # Converte as variáveis "CO_ENTIDADE" para str
    df_baseline["CO_ENTIDADE_KEY"] = df_baseline["CO_ENTIDADE"].astype(str).str.strip().str.zfill(8)
    df_fonteunica["CO_ENTIDADE_KEY"]    = df_fonteunica["CO_ENTIDADE"].astype(str).str.strip().str.zfill(8)
    
    # Mantém apenas as colunas da base de baseline necessárias
    df_baseline = df_baseline.drop(columns=["CO_ENTIDADE"], errors="ignore")
   
    # Join das tabelas
    df_limpa = df_fonteunica.merge(df_baseline, on="CO_ENTIDADE_KEY", how="left", validate="many_to_one")
    
    # Filtra apenas as escolas com pelo menos 1 matrícula
    df_limpa = df_limpa[df_limpa['QT_MAT_BAS'] > 0]   
    
    # Cria as seguintes variáveis BASELINE_ENCAMINHADAS e AFERICAO_ENCAMINHADAS
    condicoes_baseline = [
        df_limpa["END_VELOCIDADE_1MBPS_ENEC_DECRETO_BASELINE"] == "5. Atendida",
        df_limpa["END_VELOCIDADE_1MBPS_ENEC_DECRETO_POLITICA_PUBLICA_BASELINE"].isin(["PIEC 2024", "LEI 14172", "EACE FASE 4 ETAPA 2"]),
        df_limpa["END_VELOCIDADE_1MBPS_ENEC_DECRETO_BASELINE"].isin(["2. Endereçada: Tem recurso previsto e já possui RFP", "3. Contratado: Contrato já foi firmado com fornecedores", "4. Implementado: A escola já recebeu a infraestrutura"]) 
    ]
    
    valores_baseline = ["Conectada", "Não encaminhada", "Encaminhada"]
    df_limpa['BASELINE_ENCAMINHADAS'] = np.select(condicoes_baseline, valores_baseline, default="Não encaminhada")

    condicoes_afericao = [
        df_limpa["END_VELOCIDADE_1MBPS_ENEC_DECRETO"] == "5. Atendida",
        df_limpa["END_VELOCIDADE_1MBPS_ENEC_DECRETO_POLITICA_PUBLICA"].isin(["PIEC 2024", "LEI 14172"]),
        df_limpa["END_VELOCIDADE_1MBPS_ENEC_DECRETO"].isin(["2. Endereçada: Tem recurso previsto e já possui RFP", "3. Contratado: Contrato já foi firmado com fornecedores", "4. Implementado: A escola já recebeu a infraestrutura"])
    ]

    valores_afericao = ["Conectada", "Não encaminhada", "Encaminhada"]
    df_limpa['AFERICAO_ENCAMINHADAS'] = np.select(condicoes_afericao, valores_afericao, default="Não encaminhada")

    # Exporta base limpa
    df_limpa.to_parquet(caminho_base_limpa, engine="pyarrow", index=False)
    
