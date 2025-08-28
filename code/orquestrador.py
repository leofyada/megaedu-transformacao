from io_fino import read_excel, read_parquet, write_parquet
from transformacao import recursos, dispositivos, conectividade
from modelos import modelo_conectividade, modelo_conectividade_projecao, modelo_conectividade_recurso, modelo_dispositivo, modelo_dispositivo_uf, modelo_wifi

from config import ETLConfig

def run_etl(cfg: ETLConfig) -> None:

    base_baseline = read_excel(cfg.base_baseline)
    base_fonteunica = read_excel(cfg.base_fonte_unica)

    df_recursos = recursos(base_baseline, base_fonteunica)
    write_parquet(df_recursos, cfg.out_prata)
    df_dispositivos = dispositivos(df_recursos)
    write_parquet(df_dispositivos, cfg.out_prata)
    df_conectividade = conectividade(df_dispositivos)
    write_parquet(df_conectividade, cfg.out_prata)
    
    df_mod_conectividade = modelo_conectividade(df_conectividade)
    write_parquet(df_mod_conectividade, cfg.out_mod_conectividade)

    df_mod_conectividade_proj = modelo_conectividade_projecao(df_conectividade)
    write_parquet(df_mod_conectividade_proj, cfg.out_mod_conectividade_proj)

    df_mod_conectividade_recurso = modelo_conectividade_recurso(df_conectividade)
    write_parquet(df_mod_conectividade_recurso, cfg.out_mod_conectividade_recurso)

    df_mod_dispositivo = modelo_dispositivo(df_conectividade)
    write_parquet(df_mod_dispositivo, cfg.out_mod_dispositivo)

    df_mod_dispositivo_uf = modelo_dispositivo_uf(df_conectividade)
    write_parquet(df_mod_dispositivo_uf, cfg.out_mod_dispositivo_uf)

    df_mod_wifi = modelo_wifi(df_conectividade)
    write_parquet(df_mod_wifi, cfg.out_mod_wifi)
    


