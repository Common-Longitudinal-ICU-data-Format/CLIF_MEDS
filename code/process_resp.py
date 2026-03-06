"""RESP domain processor for respiratory support."""
from pathlib import Path

import polars as pl
from clifpy.utils.io import load_data
from tqdm import tqdm

from code.config import _CONFIG_META_KEYS
from code.resolve import resolve_code, resolve_time
from code.transforms import normalize_categories, strip_tz


def process_resp(config: dict, domain_config: dict, data_dir: Path, output_dir: Path):
    """RESP processor: load respiratory_support, emit MEDS parquet."""
    filetype = config["filetype"]
    site_tz = config.get("timezone")
    subject_id_col = domain_config.get("subject_id_col", config.get("subject_id_col", "patient_id"))

    concepts_key = [k for k in domain_config if k not in _CONFIG_META_KEYS][0]
    concepts = domain_config[concepts_key]

    resp_pdf = load_data(
        table_name=concepts_key,
        table_path=str(data_dir),
        table_format_type=filetype,
        site_tz=site_tz,
    )

    df = strip_tz(pl.from_pandas(resp_pdf))
    del resp_pdf
    df = normalize_categories(df)

    # Drop vent_brand_name entirely
    if "vent_brand_name" in df.columns:
        df = df.drop("vent_brand_name")

    # Collect columns referenced in concept mappings
    needed_cols = set()
    for concept_name, concept_mapping in concepts.items():
        time_spec = concept_mapping.get("time")
        if isinstance(time_spec, str) and time_spec.startswith("col("):
            needed_cols.add(time_spec[4:-1])
        code_spec = concept_mapping.get("code")
        if isinstance(code_spec, list):
            for part in code_spec:
                if isinstance(part, str) and part.startswith("col(") and part.endswith(")"):
                    needed_cols.add(part[4:-1])

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
        join_cols = ["hospitalization_id"] + [c for c in missing_cols if c in hosp_df.columns]
        hosp_df = hosp_df.select(join_cols).unique(subset=["hospitalization_id"])
        if df["hospitalization_id"].dtype != hosp_df["hospitalization_id"].dtype:
            df = df.with_columns(pl.col("hospitalization_id").cast(pl.Utf8))
            hosp_df = hosp_df.with_columns(pl.col("hospitalization_id").cast(pl.Utf8))
        df = df.join(hosp_df, on="hospitalization_id", how="left")

    # Outlier shaping: build clamp limits if enabled
    outlier_cfg = domain_config.get("outlier_shaping", {})
    clamp_limits = {}
    if outlier_cfg.get("enabled", False):
        for col_name, bounds in outlier_cfg.get("limits", {}).items():
            clamp_limits[col_name] = (float(bounds[0]), float(bounds[1]))

    rows = []
    for row_dict in tqdm(df.iter_rows(named=True), total=len(df), desc="  RESP"):
        subject_id = row_dict[subject_id_col]

        for concept_name, concept_mapping in concepts.items():
            code = resolve_code(concept_mapping["code"], row_dict)

            time_spec = concept_mapping.get("time")
            time_val = resolve_time(time_spec, row_dict)
            if time_spec is not None and time_val is None:
                continue

            numeric_value = None
            text_value = None
            if "numeric_value" in concept_mapping:
                nv_col = concept_mapping["numeric_value"]
                nv = row_dict.get(nv_col)
                if nv is not None:
                    numeric_value = float(nv)
                    if nv_col in clamp_limits:
                        lower, upper = clamp_limits[nv_col]
                        numeric_value = max(lower, min(upper, numeric_value))
            if "text_value" in concept_mapping:
                tv = row_dict.get(concept_mapping["text_value"])
                if tv is not None:
                    text_value = str(tv)

            # Skip events where both numeric_value and text_value are null (sparse data)
            if numeric_value is None and text_value is None:
                continue

            row_out = {
                "subject_id": int(subject_id),
                "time": time_val,
                "code": code,
                "numeric_value": numeric_value,
                "text_value": text_value,
            }
            if "hospitalization_id" in row_dict:
                row_out["hospitalization_id"] = int(row_dict["hospitalization_id"])
            rows.append(row_out)

    has_hosp_id = "hospitalization_id" in df.columns
    del df

    schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime,
        "code": pl.Utf8,
        "numeric_value": pl.Float32,
        "text_value": pl.Utf8,
    }
    if has_hosp_id:
        schema["hospitalization_id"] = pl.Int64
    out_df = pl.DataFrame(rows, schema=schema)

    out_path = output_dir / "data" / "RESP.parquet"
    out_df.write_parquet(out_path)
    print(f"  RESP -> {out_path} ({len(out_df)} events)")
    del rows, out_df
