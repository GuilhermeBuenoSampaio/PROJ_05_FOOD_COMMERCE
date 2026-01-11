from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class LakePaths:
    project: str
    datalake_root: Path

    @property
    def bronze(self) -> Path:
        return self.datalake_root / "01_bronze_raw" / self.project


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else (default or "")


def resolve_paths() -> LakePaths:
    project = _env("TOT_PROJECT", "PROJ_05_FOOD_COMMERCE")
    root = _env("TOT_DATALAKE_ROOT")
    if not root:
        raise RuntimeError("Defina TOT_DATALAKE_ROOT (via setx) apontando para a pasta datalake.")
    return LakePaths(project=project, datalake_root=Path(root).expanduser().resolve())


def find_xlsx_files(bronze_project_root: Path, year: int) -> List[Path]:
    base = bronze_project_root / "transactions" / str(year)
    if not base.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {base}")
    files = sorted(base.glob("*.xlsx"))
    return files


def normalize_cols(cols: List[str]) -> List[str]:
    # Mantém o nome original, mas normaliza espaços
    return [str(c).strip() for c in cols]


def pick_candidate_keys(columns: List[str]) -> Dict[str, List[str]]:
    """
    Heurística (sem assumir schema):
    - tenta chaves comuns se existirem
    """
    cols = [c.lower() for c in columns]
    candidates = {}

    # id cliente
    id_cliente = None
    for c in columns:
        if c.lower() in ("id_cliente", "cliente_id", "customer_id"):
            id_cliente = c
            break

    # data
    dt = None
    for c in columns:
        if c.lower() in ("data", "dt", "data_compra", "purchase_date", "order_date"):
            dt = c
            break

    # pedido / transação
    id_pedido = None
    for c in columns:
        if c.lower() in ("id_pedido", "pedido_id", "order_id", "id_transacao", "transacao_id", "transaction_id"):
            id_pedido = c
            break

    if id_pedido:
        candidates["dup_order_id"] = [id_pedido]
    if id_cliente and dt:
        candidates["dup_cliente_data"] = [id_cliente, dt]

    return candidates


def basic_profile(df: pd.DataFrame) -> Dict:
    n_rows, n_cols = df.shape
    null_counts = df.isna().sum().to_dict()
    total_nulls = int(df.isna().sum().sum())

    # top 10 colunas por nulos
    top_nulls = sorted(null_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    return {
        "rows": int(n_rows),
        "cols": int(n_cols),
        "total_nulls": total_nulls,
        "top_nulls": [(k, int(v)) for k, v in top_nulls if int(v) > 0],
    }


def safe_read_excel(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    """
    Garante retorno DataFrame.
    - Se sheet=None, lê a primeira aba.
    - Se por algum motivo o pandas retornar dict (múltiplas abas), pega a primeira.
    """
    if sheet is None:
        # força primeira aba para evitar dict
        df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    else:
        df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")

    if isinstance(df, dict):
        # fallback: pega a primeira aba
        first_key = next(iter(df.keys()))
        df = df[first_key]

    return df



def validate_year(year: int, sheet: Optional[str], strict_name_pattern: bool) -> Dict:
    p = resolve_paths()
    bronze_root = p.bronze
    files = find_xlsx_files(bronze_root, year)

    if not files:
        return {
            "year": year,
            "status": "no_files",
            "message": f"Nenhum .xlsx encontrado em {bronze_root / 'transactions' / str(year)}",
        }

    # valida padrão de nomes (opcional)
    name_issues: List[str] = []
    if strict_name_pattern:
        # esperado: YYYY-MM.xlsx
        for f in files:
            if not (f.stem.startswith(f"{year}-") and len(f.stem) == 7):
                name_issues.append(f.name)

    # arquivo de referência (primeiro da lista)
    ref_path = files[0]
    ref_df = safe_read_excel(ref_path, sheet=sheet)
    ref_cols = normalize_cols(list(ref_df.columns))

    results = []
    schema_mismatches = []
    read_errors = []

    # chaves candidatas para duplicidade
    key_candidates = pick_candidate_keys(ref_cols)

    for f in files:
        try:
            df = safe_read_excel(f, sheet=sheet)
            cols = normalize_cols(list(df.columns))

            mismatch = None
            if cols != ref_cols:
                missing = [c for c in ref_cols if c not in cols]
                extra = [c for c in cols if c not in ref_cols]
                order_diff = (sorted(cols) == sorted(ref_cols)) and (cols != ref_cols)
                mismatch = {
                    "file": f.name,
                    "missing_cols": missing,
                    "extra_cols": extra,
                    "order_diff_only": bool(order_diff),
                }
                schema_mismatches.append(mismatch)

            prof = basic_profile(df)

            # duplicidades
            dup_info = {}
            # duplicata de linha inteira
            dup_info["dup_full_row_count"] = int(df.duplicated().sum())

            # duplicatas em chaves candidatas
            for kname, kcols in key_candidates.items():
                if all(c in df.columns for c in kcols):
                    dup_info[kname] = int(df.duplicated(subset=kcols).sum())

            results.append({
                "file": f.name,
                "path": str(f),
                "profile": prof,
                "duplicates": dup_info,
                "schema_matches_reference": (cols == ref_cols),
            })

        except Exception as e:
            read_errors.append({"file": f.name, "error": repr(e)})

    status = "ok"
    if read_errors:
        status = "error"
    elif schema_mismatches or name_issues:
        status = "warning"

    return {
        "year": year,
        "status": status,
        "base_dir": str((bronze_root / "transactions" / str(year)).resolve()),
        "reference_file": ref_path.name,
        "reference_columns": ref_cols,
        "strict_name_pattern": strict_name_pattern,
        "name_issues": name_issues,
        "schema_mismatches": schema_mismatches,
        "read_errors": read_errors,
        "files": results,
    }


def write_reports(repo_root: Path, payload: Dict) -> Tuple[Path, Path]:
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = logs_dir / f"bronze_validate_{payload['year']}_{ts}.json"

    # também gera um CSV resumo
    csv_path = logs_dir / f"bronze_validate_{payload['year']}_{ts}_summary.csv"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # flatten para CSV
    rows = []
    for item in payload.get("files", []):
        prof = item.get("profile", {})
        dups = item.get("duplicates", {})
        rows.append({
            "year": payload["year"],
            "file": item.get("file"),
            "rows": prof.get("rows"),
            "cols": prof.get("cols"),
            "total_nulls": prof.get("total_nulls"),
            "dup_full_row_count": dups.get("dup_full_row_count"),
            "dup_order_id": dups.get("dup_order_id"),
            "dup_cliente_data": dups.get("dup_cliente_data"),
            "schema_matches_reference": item.get("schema_matches_reference"),
        })

    pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8")

    return json_path, csv_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Ano a validar (ex: 2023)")
    ap.add_argument("--sheet", type=str, default=None, help="Nome da aba (se aplicável). Se omitido, usa a primeira.")
    ap.add_argument("--strict-names", action="store_true", help="Exige padrão de nome YYYY-MM.xlsx")
    args = ap.parse_args()

    # repo root = pasta atual (assumindo execução a partir do repo)
    repo_root = Path.cwd().resolve()

    payload = validate_year(
        year=args.year,
        sheet=args.sheet,
        strict_name_pattern=bool(args.strict_names),
    )

    json_path, csv_path = write_reports(repo_root, payload)

    print(f"[OK] Validação concluída: year={args.year} status={payload['status']}")
    print(f"     JSON: {json_path}")
    print(f"     CSV : {csv_path}")

    if payload.get("name_issues"):
        print(f"[WARN] Arquivos fora do padrão YYYY-MM.xlsx: {len(payload['name_issues'])}")
    if payload.get("schema_mismatches"):
        print(f"[WARN] Divergências de colunas vs referência: {len(payload['schema_mismatches'])}")
    if payload.get("read_errors"):
        print(f"[ERROR] Erros de leitura: {len(payload['read_errors'])}")

    # status code
    return 0 if payload["status"] != "error" else 2


if __name__ == "__main__":
    raise SystemExit(main())
