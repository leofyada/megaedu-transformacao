# Importa bibliotecas
import pandas as pd
import numpy as np

# Função para criar o modelo para escolas conectadas e encaminhadas
def modelo_conectividade(caminho_base_limpa, caminho_destino):

    # Importação da base limpa
    df = pd.read_parquet(caminho_base_limpa)

    # Quantidade de escolas conectadas 
    escolas_conectadas = len(df[df['conect_atendida']==1])
    # Quantidade de escolas encaminhadas
    escolas_encaminhadas = len(df[df['conect_encaminhada']==1])

    # Tabela wide com baseline e meta
    df_modelo = pd.DataFrame({'categoria': ['2024 BASELINE', '2025 YTD', '2025 META'], 'esc_conc': [62068, escolas_conectadas, 91466], 'esc_enc': [15102, escolas_encaminhadas, 9815]})
    # Tabela long
    df_modelo = df_modelo.melt(id_vars = 'categoria', value_vars = ['esc_conc', 'esc_enc'], var_name='cat', value_name='valor')

    # Exporta modelo
    df_modelo.to_parquet(caminho_destino, engine="pyarrow", index=False)


# Função para criar o modelo para escolas conectadas e encaminhadas + projeção (100 kbps)
def modelo_conectividade_projecao(caminho_base_limpa, caminho_destino):

    # Importação da base limpa
    df = pd.read_parquet(caminho_base_limpa)

    # Quantidade de escolas conectadas
    escolas_conectadas = len(df[df['conect_atendida']==1])
    # Quantidade de escolas encaminhadas
    escolas_encaminhadas = len(df[df['conect_encaminhada']==1])
    # Quantidade de escolas com velocidade acima de 100 kbps
    escolas_100kbps = len(df[df['3_ST_CONECTIVIDADE_CL_100KBPS']=='Acima de 100kbps'])-(escolas_conectadas+escolas_encaminhadas)

    # Tabela wide com baseline e meta
    df_modelo = pd.DataFrame({'categoria': ['2024 BASELINE', '2025 YTD', '2025 META'], 'esc_conc': [62068, escolas_conectadas, 91466], 'esc_enc': [15102, escolas_encaminhadas, 9815], 'esc_proj': [(102396-62068-15102), escolas_100kbps, (115281-91466-9815)]})
    # Tabela long
    df_modelo = df_modelo.melt(id_vars = 'categoria', value_vars = ['esc_conc', 'esc_enc', 'esc_proj'], var_name='cat', value_name='valor')

    # Exporta modelo
    df_modelo.to_parquet(caminho_destino, engine="pyarrow", index=False)


# Função para criar o modelo para as escolas conectadas e encaminhadas por fonte de recurso
def modelo_conectividade_recurso(caminho_base_limpa, caminho_destino):

    # Importação da base limpa
    df = pd.read_parquet(caminho_base_limpa)

    # Escolas conectadas
    eace_conectadas = df[df['escolas_conectadas_recurso']=='eace_conectadas']['CO_ENTIDADE'].nunique()
    fust_conectadas = df[df['escolas_conectadas_recurso']=='fust_conectadas']['CO_ENTIDADE'].nunique()
    piec_conectadas = df[df['escolas_conectadas_recurso']=='piec_conectadas']['CO_ENTIDADE'].nunique()
    lei14172_conectadas = df[df['escolas_conectadas_recurso']=='lei14172_conectadas']['CO_ENTIDADE'].nunique()
    
    soma_conectadas = eace_conectadas+fust_conectadas+piec_conectadas+lei14172_conectadas

    baseline_conectadas = baseline_encaminhadas = df[df['BASELINE_ENCAMINHADAS']=='Conectada']['CO_ENTIDADE'].nunique()

    delta_conectadas = df[df['conect_atendida']==1]['CO_ENTIDADE'].nunique()-baseline_conectadas
    saidas_conectadas = -df[(df['BASELINE_ENCAMINHADAS']=='Conectada') & (df['AFERICAO_ENCAMINHADAS'].isin(['Encaminhada', 'Não encaminhada']))]['CO_ENTIDADE'].nunique()
    monitoramento_conectadas = delta_conectadas - saidas_conectadas - (soma_conectadas)

    # Escolas encaminhadas
    eace_encaminhadas = df[df['escolas_encaminhadas_recurso']=='eace_encaminhadas']['CO_ENTIDADE'].nunique()
    fust_encaminhadas = df[df['escolas_encaminhadas_recurso']=='fust_encaminhadas']['CO_ENTIDADE'].nunique()
    piec_encaminhadas = df[df['escolas_encaminhadas_recurso']=='piec_encaminhadas']['CO_ENTIDADE'].nunique()
    lei14172_encaminhadas = df[df['escolas_encaminhadas_recurso']=='lei14172_encaminhadas']['CO_ENTIDADE'].nunique()
    
    soma_encaminhadas = eace_encaminhadas+fust_encaminhadas+piec_encaminhadas+lei14172_encaminhadas
    
    baseline_encaminhadas = df[df['BASELINE_ENCAMINHADAS']=='Encaminhada']['CO_ENTIDADE'].nunique()

    delta_encaminhadas = df[(df['conect_encaminhada']==1) & (df['END_VELOCIDADE_1MBPS_ENEC_DECRETO_POLITICA_PUBLICA'] != 'LEI 14172')]['CO_ENTIDADE'].nunique()-baseline_encaminhadas
    #saidas_encaminhadas = -df[(df['BASELINE_ENCAMINHADAS']=='Encaminhada') & (df['END_VELOCIDADE_1MBPS_ENEC_DECRETO_POLITICA_PUBLICA'] != 'LEI 14172') & (df['AFERICAO_ENCAMINHADAS']=='Não encaminhada')]['CO_ENTIDADE'].nunique()
    
    saidas_encaminhadas = -df[(df['BASELINE_ENCAMINHADAS']=='Encaminhada') & (df['AFERICAO_ENCAMINHADAS']=='Não encaminhada')]['CO_ENTIDADE'].nunique()

    monitoramento_encaminhadas = delta_encaminhadas - saidas_encaminhadas - (soma_encaminhadas)

    # Construção do dataframe
    dados = {
        'fonte': ['EACE', 'FUST', 'PIEC', '14.172', 'Monitoramento', 'Saídas do baseline', 'Delta'],
        'conectadas': [eace_conectadas, fust_conectadas, piec_conectadas, lei14172_conectadas, monitoramento_conectadas, saidas_conectadas, delta_conectadas],
        'encaminhadas': [eace_encaminhadas, fust_encaminhadas, piec_encaminhadas, lei14172_encaminhadas, monitoramento_encaminhadas, saidas_encaminhadas, delta_encaminhadas]
    }
    df_modelo = pd.DataFrame(dados)
    
    # Exporta modelo
    df_modelo.to_parquet(caminho_destino, engine="pyarrow", index=False)


# Função para criar o modelo de escolas com dispositivos encaminhados (retirar Lei 14.172)
def modelo_dispositivo(caminho_base_limpa, caminho_destino):

    # Importação da base limpa
    df = pd.read_parquet(caminho_base_limpa)

    # Quantidade de escolas estaduais adequadas
    escolas_atendidas = df[(df['disp_1_10_adq']==1) & (df['TP_DEPENDENCIA_CENSO']=='Estadual')]['CO_ENTIDADE'].nunique()
    # Quantidade de escolas encaminhadas
    escolas_encaminhadas = df[(df['END_DISPOSITIVOS_ADQ']=='3. Contratado: Contrato já foi firmado com fornecedores') & (df['TP_DEPENDENCIA_CENSO']=='Estadual')]['CO_ENTIDADE'].nunique()

    # Construção do dataframe
    dados = {
        'categoria': ['2024 BASELINE', '2025 YTD', '2025 META'], 'esc_at': [10305, escolas_atendidas, 0], 'esc_enc': [0, escolas_encaminhadas, 18181]
    }
    df_modelo = pd.DataFrame(dados)

    # Exporta modelo
    df_modelo.to_parquet(caminho_destino, engine="pyarrow", index=False)

# Função para criar o modelo de escolas com dispositivos encaminhados por UF (para elaboração do mapa)
def modelo_dispositivo_uf(caminho_base_limpa, caminho_destino):

    # Importação da base limpa
    df = pd.read_parquet(caminho_base_limpa)
    # Filtro de escolas estaduais
    df = df[df['TP_DEPENDENCIA_CENSO']=='Estadual']

    # Quantidade de escolas estaduais encaminhadas
    df['disp_enc'] = np.where((df['disp_1_10_adq']==1) | (df['END_DISPOSITIVOS_ADQ']=='3. Contratado: Contrato já foi firmado com fornecedores'), 'sim', 'nao')
    
    # Cálculo por UF
    df_modelo = df.groupby(["CO_UF", "SG_UF", "NO_UF", "disp_enc"], dropna=False)["CO_ENTIDADE"].nunique().reset_index(name="qt_entidades").sort_values(["CO_UF", "SG_UF", "NO_UF", "disp_enc"])
    df_modelo = df_modelo.pivot(index = ['CO_UF', 'SG_UF', 'NO_UF'], columns='disp_enc', values='qt_entidades').fillna(0)
    df_modelo = df_modelo.reset_index()
    df_modelo['p_disp_enc'] = 100*(df_modelo['sim'] / (df_modelo['sim']+df_modelo['nao']))
    df_modelo['country'] = 'Brasil'
    df_modelo = df_modelo[['CO_UF', 'SG_UF', 'NO_UF', 'country', 'p_disp_enc']]

    # Exporta modelo
    df_modelo.to_parquet(caminho_destino, engine="pyarrow", index=False)


# Função para criar modelo de escolas com wifi
def modelo_wifi(caminho_base_limpa, caminho_destino):
    
    # Importação da base limpa
    df = pd.read_parquet(caminho_base_limpa)

    # Quantidade de escolas com wifi adequado
    escolas_adequadas = df[df['wifi_adq']==1]['CO_ENTIDADE'].nunique() 
    # Quantidade de escolas com wifi encaminhado
    escolas_encaminhadas = df[df['wifi_enc']==1]['CO_ENTIDADE'].nunique()

    # Tabela wide com baseline e meta
    df_modelo = pd.DataFrame({'categoria': ['2024 BASELINE', '2025 YTD', '2025 Expectativa'], 'esc_adq': [78608, escolas_adequadas, 98018], 'esc_enc': [17607, escolas_encaminhadas, 14976]})
    # Tabela long
    df_modelo = df_modelo.melt(id_vars = 'categoria', value_vars = ['esc_adq', 'esc_enc'], var_name='cat', value_name='valor')

    # Exporta modelo
    df_modelo.to_parquet(caminho_destino, engine="pyarrow", index=False)
        







