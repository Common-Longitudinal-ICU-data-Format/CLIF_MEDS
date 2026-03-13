"""ETL orchestration — the main pipeline loop."""
import argparse
import gc
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from code.config import ensure_output_dirs, get_enabled_domains, get_output_mode, get_subjects_per_shard, load_config
from code.metadata import write_codes_parquet
from code.process import DOMAIN_PROCESSORS
from code.shard import shard_data


def main():
    parser = argparse.ArgumentParser(description="CLIF -> MEDS ETL pipeline")
    parser.add_argument("--config", required=True, help="Path to clif_config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    output_dir = Path(config["output_directory"])
    data_dir = Path(config["data_directory"])
    concept_config_dir = Path(config.get("concept_config_directory", "./config"))

    ensure_output_dirs(output_dir)

    enabled = get_enabled_domains(config)
    # Always process PATIENT and HOSP — they provide subject/hospitalization mappings
    for required in ["PATIENT", "HOSP"]:
        if required not in enabled:
            enabled.insert(0, required)
    # Ensure PATIENT and HOSP are processed first
    enabled = [d for d in ["PATIENT", "HOSP"] if d in enabled] + [d for d in enabled if d not in ("PATIENT", "HOSP")]
    print(f"Enabled domains: {enabled}")

    domain_versions: dict[str, str] = {}

    for domain in enabled:
        domain_config_path = concept_config_dir / f"{domain}.yaml"
        if not domain_config_path.exists():
            print(f"  Warning: Config not found: {domain_config_path}, skipping")
            continue

        with open(domain_config_path) as f:
            domain_config = yaml.safe_load(f)

        domain_versions[domain] = domain_config.get("elf_version", "0.0.0")

        if domain not in DOMAIN_PROCESSORS:
            print(f"  {domain} -- not yet implemented, skipping")
            continue

        print(f"Processing {domain}...")
        DOMAIN_PROCESSORS[domain](config, domain_config, data_dir, output_dir)
        gc.collect()

    output_mode = get_output_mode(config)
    if output_mode == "shards":
        subjects_per_shard = get_subjects_per_shard(config)
        shard_data(output_dir, subjects_per_shard)

    write_codes_parquet(output_dir, domain_versions)

    metadata_schema = config.get("DatasetMetadataSchema", {})
    metadata_schema["other_extension_columns"] = [
        "hospitalization_id",
        "event_count",
        "is_numeric_value",
        "is_text_value",
    ]
    metadata_schema["created_at"] = datetime.now(timezone.utc).isoformat()
    metadata_path = output_dir / "metadata" / "dataset_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata_schema, f, indent=2)
    print(f"  dataset_metadata.json -> {metadata_path}")

    print("Done.")
