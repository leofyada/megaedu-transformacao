import argparse
from config import ETLConfig
from orquestrador import run_etl
from pathlib import Path
from datetime import datetime
from typing import Optional

def make_outputs(out_dir: Path, run_id: Optional[str] = None) -> dict[str, str]:
    out_dir = Path(out_dir)
    run_tag = f"_{run_id}" if run_id else ""
    paths = {
        "out_prata": out_dir / "prata" / f"base_limpa{run_tag}.parquet",

        "out_mod_conectividade": out_dir / "ouro"  / f"mod_conectividade{run_tag}.parquet",
        "out_mod_conectividade_proj": out_dir / "ouro"  / f"mod_conectividade_proj{run_tag}.parquet",
        "out_mod_conectividade_recurso": out_dir / "ouro" / f"mod_conectividade_recurso{run_tag}.parquet",
        "out_mod_dispositivo": out_dir / "ouro"  / f"mod_dispositivo{run_tag}.parquet",
        "out_mod_dispositivo_uf": out_dir / "ouro"  / f"mod_dispositivo_uf{run_tag}.parquet",
        "out_mod_wifi": out_dir / "ouro"  / f"mod_wifi{run_tag}.parquet",
    }
    # garante pastas
    for p in paths.values():
        Path(p).parent.mkdir(parents=True, exist_ok=True)
    return {k: str(v) for k, v in paths.items()}

def parse_args():
    p = argparse.ArgumentParser(allow_abbrev=False, description="ETL MegaEdu")
    p.add_argument("--baseline", required=True)
    p.add_argument("--fonteunica", required=True)
    p.add_argument("--out-dir", required=True, help="Diretório raiz para salvar todas as saídas")
    p.add_argument("--run-id", required=False, default=None, help="Sufixo opcional p/ versionar (ex.: 20250828)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    outs = make_outputs(Path(args.out_dir), run_id=args.run_id)
    cfg = ETLConfig(
        base_baseline=args.baseline,
        base_fonte_unica=args.fonteunica,
        **outs  
    )
    run_etl(cfg)


