"""Domain processing — load CLIF tables and convert to MEDS parquet."""
from pathlib import Path

import polars as pl
from clifpy.utils.io import load_data
from tqdm import tqdm

from code.config import _CONFIG_META_KEYS
from code.process_med_con import process_med_con
from code.resolve import resolve_code, resolve_time
from code.transforms import normalize_categories, strip_tz


def _process_domain(config: dict, domain_config: dict, data_dir: Path, output_dir: Path, domain_name: str):
    """Generic domain processor: load CLIF table → MEDS parquet."""
    filetype = config["filetype"]
    site_tz = config.get("timezone")

    subject_id_col = domain_config.get("subject_id_col", config.get("subject_id_col", "patient_id"))

    # concepts_key doubles as the clifpy table_name
    concepts_key = [k for k in domain_config if k not in _CONFIG_META_KEYS][0]
    concepts = domain_config[concepts_key]

    pdf = load_data(
        table_name=concepts_key,
        table_path=str(data_dir),
        table_format_type=filetype,
        site_tz=site_tz,
    )

    df = strip_tz(pl.from_pandas(pdf))
    df = normalize_categories(df)

    # Collect columns referenced in concept mappings (time, code col() refs)
    needed_cols = set()
    for concept_name, mapping in concepts.items():
        time_spec = mapping.get("time")
        if isinstance(time_spec, str) and time_spec.startswith("col("):
            needed_cols.add(time_spec[4:-1])
        code_spec = mapping.get("code")
        if isinstance(code_spec, list):
            for part in code_spec:
                if isinstance(part, dict):
                    col_expr = next(iter(part))
                    if col_expr.startswith("col(") and col_expr.endswith(")"):
                        needed_cols.add(col_expr[4:-1])
                elif isinstance(part, str) and part.startswith("col(") and part.endswith(")"):
                    needed_cols.add(part[4:-1])

    # Join with hospitalization table if subject_id or any needed columns are missing
    missing_cols = {c for c in needed_cols if c not in df.columns}
    if subject_id_col not in df.columns:
        missing_cols.add(subject_id_col)

    if missing_cols and "hospitalization_id" in df.columns:
        hosp_pdf = load_data(
            table_name="hospitalization",
            table_path=str(data_dir),
            table_format_type=filetype,
            site_tz=site_tz,
        )
        hosp_df = strip_tz(pl.from_pandas(hosp_pdf))
        # Select hospitalization_id + any missing columns available in hospitalization
        join_cols = ["hospitalization_id"] + [c for c in missing_cols if c in hosp_df.columns]
        hosp_df = hosp_df.select(join_cols).unique(subset=["hospitalization_id"])
        df = df.join(hosp_df, on="hospitalization_id", how="left")

    rows = []
    for row_dict in tqdm(df.iter_rows(named=True), total=len(df), desc=f"  {domain_name}"):
        subject_id = row_dict[subject_id_col]

        for concept_name, mapping in concepts.items():
            code = resolve_code(mapping["code"], row_dict)

            time_spec = mapping.get("time")
            time_val = resolve_time(time_spec, row_dict)

            # If config expects a time column but it resolved to null, skip this event
            if time_spec is not None and time_val is None:
                continue

            numeric_value = None
            text_value = None
            if "numeric_value" in mapping:
                nv = row_dict.get(mapping["numeric_value"])
                if nv is not None:
                    numeric_value = float(nv)
            if "text_value" in mapping:
                tv = row_dict.get(mapping["text_value"])
                if tv is not None:
                    text_value = str(tv)

            row_out = {
                "subject_id": int(subject_id),
                "time": time_val,
                "code": code,
                "numeric_value": numeric_value,
                "text_value": text_value,
            }
            if domain_name != "PATIENT" and "hospitalization_id" in row_dict:
                row_out["hospitalization_id"] = int(row_dict["hospitalization_id"])
            rows.append(row_out)

    schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime,
        "code": pl.Utf8,
        "numeric_value": pl.Float32,
        "text_value": pl.Utf8,
    }
    if domain_name != "PATIENT" and "hospitalization_id" in df.columns:
        schema["hospitalization_id"] = pl.Int64
    out_df = pl.DataFrame(rows, schema=schema)

    out_path = output_dir / "data" / f"{domain_name}.parquet"
    out_df.write_parquet(out_path)
    print(f"  {domain_name} -> {out_path} ({len(out_df)} events)")


def process_patient(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "PATIENT")


def process_hosp(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "HOSP")


def process_adt(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "ADT")


def process_vital(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "VITAL")


def process_lab(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "LAB")


def process_pos(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "POS")


def process_code_status(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "CODE_STATUS")


def process_hosp_dx(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "HOSP_DX")


def process_proc(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    _process_domain(config, domain_config, data_dir, output_dir, "PROC")


# Domain dispatcher — maps domain name to processing function
DOMAIN_PROCESSORS = {
    "PATIENT": process_patient,
    "HOSP": process_hosp,
    "ADT": process_adt,
    "VITAL": process_vital,
    "LAB": process_lab,
    "POS": process_pos,
    "CODE_STATUS": process_code_status,
    "HOSP_DX": process_hosp_dx,
    "PROC": process_proc,
    "MED_CON": process_med_con,
}
