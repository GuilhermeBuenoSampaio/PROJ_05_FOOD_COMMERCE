from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass(frozen=True)
class LakePaths:
    project: str
    datalake_root: Path

    @property
    def bronze(self) -> Path: return self.datalake_root / "01_bronze_raw" / self.project
    @property
    def silver(self) -> Path: return self.datalake_root / "02_silver" / self.project
    @property
    def gold(self) -> Path: return self.datalake_root / "03_gold" / self.project
    @property
    def exports(self) -> Path: return self.datalake_root / "03_gold_exports" / self.project

def _env(name: str, default: Optional[str]=None) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else (default or "")

def resolve_paths() -> LakePaths:
    project = _env("TOT_PROJECT", "PROJ_05_FOOD_COMMERCE")
    root = _env("TOT_DATALAKE_ROOT")
    if not root:
        raise RuntimeError("Defina TOT_DATALAKE_ROOT (via setx) apontando para a pasta datalake.")
    return LakePaths(project=project, datalake_root=Path(root).expanduser().resolve())

def ensure_dirs(p: LakePaths) -> None:
    for d in (p.bronze, p.silver, p.gold, p.exports):
        d.mkdir(parents=True, exist_ok=True)

def run_bronze_to_silver(p: LakePaths) -> None:
    print("[STEP] bronze_to_silver")
    print("  bronze :", p.bronze)
    print("  silver :", p.silver)

def run_silver_to_gold(p: LakePaths) -> None:
    print("[STEP] silver_to_gold")
    print("  silver :", p.silver)
    print("  gold   :", p.gold)

def run_gold_to_exports(p: LakePaths) -> None:
    print("[STEP] gold_to_exports")
    print("  gold   :", p.gold)
    print("  exports:", p.exports)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["bronze_to_silver","silver_to_gold","gold_to_exports","all"])
    args = ap.parse_args()

    p = resolve_paths()
    ensure_dirs(p)

    print("[INFO] project =", p.project)
    print("[INFO] datalake_root =", p.datalake_root)

    if args.stage in ("bronze_to_silver","all"):
        run_bronze_to_silver(p)
    if args.stage in ("silver_to_gold","all"):
        run_silver_to_gold(p)
    if args.stage in ("gold_to_exports","all"):
        run_gold_to_exports(p)

    print("[OK] done")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
