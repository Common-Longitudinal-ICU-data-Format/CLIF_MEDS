"""Configuration loading and output directory setup."""
from pathlib import Path

import yaml

# Metadata keys in domain YAML configs (not concept sections)
_CONFIG_META_KEYS = {"subject_id_col", "elf_version", "outlier_shaping"}


def load_config(config_path: str) -> dict:
    """Load and return the YAML config."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def ensure_output_dirs(output_dir: Path):
    """Create output_directory/data/ and output_directory/metadata/ if needed."""
    (output_dir / "data").mkdir(parents=True, exist_ok=True)
    (output_dir / "metadata").mkdir(parents=True, exist_ok=True)


def get_enabled_domains(config: dict) -> list[str]:
    """Return list of domain names where value == 1."""
    return [domain for domain, enabled in config["domains"].items() if enabled == 1]


def get_output_mode(config: dict) -> str:
    """Return 'domain' (default) or 'shards'."""
    return config.get("meds", {}).get("output_mode", "domain")


def get_subjects_per_shard(config: dict) -> int:
    """Return subjects per shard (default 1000)."""
    return int(config.get("meds", {}).get("subjects_per_shard", 1000))
