from dataclasses import dataclass

@dataclass
class ETLConfig:

    base_baseline: str
    base_fonte_unica: str

    out_prata: str
    out_mod_conectividade: str
    out_mod_conectividade_proj: str
    out_mod_conectividade_recurso: str
    out_mod_dispositivo: str
    out_mod_dispositivo_uf: str
    out_mod_wifi: str


