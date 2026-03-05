# CLIF_MEDS

CLIF-to-MEDS ETL with ELF data harmonization for multi-site ICU research.

## Overview

CLIF_MEDS is a config-driven ETL pipeline that converts clinical data from the [Common Longitudinal ICU data Format (CLIF)](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF) into the [Medical Event Data Standard (MEDS)](https://github.com/Medical-Event-Data-Standard/meds). Every event code is encoded using the [Event Language Format (ELF)](https://github.com/Common-Longitudinal-ICU-data-Format/ELF) v1.0.0-beta, a hierarchical vocabulary layer built on the mCIDE (minimum Common ICU Data Elements) ontology. The result is a portable, ML-ready dataset where models trained at one ICU site transfer to any other CLIF site without code remapping.

## Architecture

```
CLIF Tables ──> CLIF_MEDS ETL ──> MEDS Parquet
(per-site)      (config-driven)    (ELF-coded)
```

## How it works

Each CLIF table (vitals, labs, medications, etc.) is processed by a domain-specific handler. Per-domain YAML configs in `config/` define how source columns map to ELF-formatted codes. Each row becomes a MEDS event with `(subject_id, time, code, numeric_value, text_value)`. The pipeline supports all 15 mCIDE domains, each toggled on or off in a single site config file.

## ELF code format

Every clinical event code follows the pattern:

```
{domain}//{level_1}//{level_2}//{level_3}
```

- `//` (double slash) separates hierarchy levels
- `/` (single slash) is reserved for use within values (e.g., `mg/dL`, `mcg/kg/min`)
- Sentinel values: `NA` = not applicable, `UNK` = unknown

Examples:

```
VITAL//spo2
LAB//bicarbonate//mmol/L//bmp
MED_CON//dexmedetomidine//mcg/kg/hr//start
MED_INT//vancomycin//dose//given
HOSP_DX//ICD10CM//N186
```

Explore all possible codes in the [mCIDE explorer](https://clif-icu.com/mcide-explorer).

## Domains

| Domain | Prefix | Levels | Example |
|---|---|---|---|
| Vitals | `VITAL//` | 1 | `VITAL//spo2` |
| Labs | `LAB//` | 3 | `LAB//bicarbonate//mmol/L//bmp` |
| Meds (continuous) | `MED_CON//` | 3 | `MED_CON//dexmedetomidine//mcg/kg/hr//start` |
| Meds (intermittent) | `MED_INT//` | 3 | `MED_INT//vancomycin//dose//given` |
| Respiratory | `RESP//` | 2 | `RESP//device_category//imv` |
| Patient Assessments | `PA//` | 1 | `PA//gcs_verbal` |
| Code Status | `CODE_STATUS//` | 1 | `CODE_STATUS//full` |
| Hospitalization | `HOSP//` | 2 | `HOSP//discharge_category//expired` |
| Demographics | `PATIENT//` | 2 | `PATIENT//sex//female` |
| ADT | `ADT//` | 3 | `ADT//TRANSFER_IN//icu//cvicu_icu` |
| Position | `POS//` | 1 | `POS//prone` |
| CRRT | `CRRT//` | 2 | `CRRT//crrt_mode_category//cvvhdf` |
| ECMO/MCS | `ECMO_MCS//` | 3 | `ECMO_MCS//ecmo_mcs//UNK//presence` |
| Procedures | `PROC//` | 2 | `PROC//ICD10PCS//02H633Z` |
| Hospital Dx | `HOSP_DX//` | 2 | `HOSP_DX//ICD10CM//N186` |

HOSP_DX carries post-hoc hospital discharge diagnoses (labels, not real-time features). PROC and HOSP_DX are pass-through domains where codes come from the source data (ICD, CPT/HCPCS).

## Output

The ETL produces three outputs:

| File | Description |
|---|---|
| `data/{DOMAIN}.parquet` | Event rows per domain: `subject_id`, `time`, `code`, `numeric_value`, `text_value`, `hospitalization_id` |
| `metadata/codes.parquet` | Code metadata: `code`, `description`, `parent_codes`, `concept_version` |
| `metadata/dataset_metadata.json` | Dataset-level metadata (MEDS schema, ELF version, site info) |

## Quick start

See [run.md](run.md) for installation and usage instructions.

Quick test with included demo data:

```bash
uv run python main.py --config test/clif_config.yaml
```

## Open-source development with CLIF-MIMIC

[CLIF-MIMIC](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC) converts [MIMIC-IV](https://physionet.org/content/mimiciv/) into CLIF format. Researchers can use CLIF-MIMIC as a freely available development and evaluation dataset:

1. Generate CLIF tables from MIMIC-IV using CLIF-MIMIC
2. Run this ETL to produce ELF-coded MEDS output
3. Train and evaluate ML models against the resulting data

Models built on ELF-coded MEDS will generalize to any CLIF site's data without code remapping, since every site produces the same standardized vocabulary through ELF.

## References

- **MEDS**: https://github.com/Medical-Event-Data-Standard/meds
- **MEDS paper**: https://openreview.net/pdf?id=IsHy2ebjIG
- **ELF spec**: https://github.com/Common-Longitudinal-ICU-data-Format/ELF
- **mCIDE explorer**: https://clif-icu.com/mcide-explorer
- **CLIF**: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF
- **CLIF paper**: https://link.springer.com/article/10.1007/s00134-025-07848-7
- **CLIF-MIMIC**: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC

## License

Apache 2.0
