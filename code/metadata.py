"""Code metadata generation and codes.parquet output."""
from pathlib import Path

import polars as pl

DOMAIN_LABELS = {
    "VITAL": "Vital sign", "LAB": "Laboratory", "MED_CON": "Continuous medication",
    "MED_INT": "Intermittent medication", "RESP": "Respiratory support",
    "PA": "Patient assessment", "CODE_STATUS": "Code status",
    "HOSP": "Hospitalization", "PATIENT": "Patient", "ADT": "ADT",
    "POS": "Position", "CRRT": "CRRT", "ECMO_MCS": "ECMO/MCS",
    "PROC": "Procedure", "HOSP_DX": "Hospital diagnosis",
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
    count_frames: list[pl.DataFrame] = []

    for parquet_file in data_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file, columns=["code", "numeric_value", "text_value"])
        df = df.filter(pl.col("code").is_not_null() & (pl.col("code") != ""))
        counts = df.group_by("code").agg(
            pl.len().alias("len"),
            pl.col("numeric_value").is_not_null().any().alias("has_numeric"),
            pl.col("text_value").is_not_null().any().alias("has_text"),
        )
        count_frames.append(counts)

    if count_frames:
        code_stats = pl.concat(count_frames).group_by("code").agg(
            pl.col("len").sum(),
            pl.col("has_numeric").any(),
            pl.col("has_text").any(),
        )
    else:
        code_stats = pl.DataFrame(
            {"code": [], "len": [], "has_numeric": [], "has_text": []},
            schema={"code": pl.Utf8, "len": pl.UInt32, "has_numeric": pl.Boolean, "has_text": pl.Boolean},
        )

    phi_replaced = (code_stats["len"] < 10).any()
    code_stats = code_stats.with_columns(
        pl.when(pl.col("len") < 10).then(10).otherwise(pl.col("len")).alias("event_count").cast(pl.Int64),
        pl.col("has_numeric").cast(pl.Int8).alias("is_numeric_value"),
        pl.col("has_text").cast(pl.Int8).alias("is_text_value"),
    ).drop("len", "has_numeric", "has_text")

    rows = []
    stats_map = {
        row["code"]: row
        for row in code_stats.iter_rows(named=True)
    }
    for code_val in sorted(stats_map.keys()):
        domain = code_to_domain(code_val)
        s = stats_map[code_val]
        rows.append({
            "code": code_val,
            "description": generate_description(code_val),
            "parent_codes": None,
            "concept_version": domain_versions.get(domain, "0.0.0") if domain else "0.0.0",
            "event_count": s["event_count"],
            "is_numeric_value": s["is_numeric_value"],
            "is_text_value": s["is_text_value"],
        })

    codes_df = pl.DataFrame(rows, schema={
        "code": pl.Utf8,
        "description": pl.Utf8,
        "parent_codes": pl.List(pl.Utf8),
        "concept_version": pl.Utf8,
        "event_count": pl.Int64,
        "is_numeric_value": pl.Int8,
        "is_text_value": pl.Int8,
    })

    out_path = output_dir / "metadata" / "codes.parquet"
    codes_df.write_parquet(out_path)
    print(f"  codes.parquet -> {out_path} ({len(codes_df)} codes)")
    if phi_replaced:
        print("  Note: All event counts < 10 have been replaced with 10 (PHI protection)")
