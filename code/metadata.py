"""Code metadata generation and codes.parquet output."""
from pathlib import Path

import polars as pl

DOMAIN_LABELS = {
    "VITAL": "Vital sign", "LAB": "Laboratory", "MED_CON": "Continuous medication",
    "MED_INT": "Intermittent medication", "RESP": "Respiratory support",
    "PA": "Patient assessment", "CODE_STATUS": "Code status",
    "HOSP": "Hospitalization", "PATIENT": "Patient", "ADT": "ADT",
    "POS": "Position", "CRRT": "CRRT", "ECMO_MCS": "ECMO/MCS",
    "PROC": "Procedure", "PATIENT_DX": "Patient diagnosis", "HOSP_DX": "Hospital diagnosis",
}


def generate_description(code: str) -> str:
    """Auto-generate a human-readable description from an ELF code string."""
    if code == "MEDS_BIRTH":
        return "Patient birth event"
    if code == "MEDS_DEATH":
        return "Patient death event"

    parts = code.split("//")
    domain = parts[0]
    label = DOMAIN_LABELS.get(domain, domain)
    concept = parts[1].replace("_", " ") if len(parts) > 1 else ""
    qualifiers = [p for p in parts[2:] if p not in ("NA", "UNK")]

    desc = f"{label}: {concept}"
    if qualifiers:
        desc += f" ({', '.join(qualifiers)})"
    return desc


def code_to_domain(code: str) -> str | None:
    """Extract the domain prefix from an ELF code for version lookup."""
    if code.startswith("MEDS_"):
        return "PATIENT"
    parts = code.split("//")
    return parts[0] if parts[0] in DOMAIN_LABELS else None


def write_codes_parquet(output_dir: Path, domain_versions: dict[str, str]):
    """Scan output parquet files and write metadata/codes.parquet."""
    data_dir = output_dir / "data"
    codes: set[str] = set()

    for parquet_file in data_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file, columns=["code"])
        codes.update(
            c for c in df["code"].unique().to_list() if c is not None and c != ""
        )

    rows = []
    for code_val in sorted(codes):
        domain = code_to_domain(code_val)
        rows.append({
            "code": code_val,
            "description": generate_description(code_val),
            "parent_codes": None,
            "concept_version": domain_versions.get(domain, "0.0.0") if domain else "0.0.0",
        })

    codes_df = pl.DataFrame(rows, schema={
        "code": pl.Utf8,
        "description": pl.Utf8,
        "parent_codes": pl.List(pl.Utf8),
        "concept_version": pl.Utf8,
    })

    out_path = output_dir / "metadata" / "codes.parquet"
    codes_df.write_parquet(out_path)
    print(f"  codes.parquet -> {out_path} ({len(codes_df)} codes)")
