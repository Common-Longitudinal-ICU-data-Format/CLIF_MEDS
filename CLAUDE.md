## Project Overview

CLIF_MEDS connects two clinical data standards:
- **CLIF** (Common Longitudinal ICU data Format) — a multi-site ICU data consortium format with tables for vitals, labs, medications, respiratory support, etc.
- **MEDS** (Medical Event Data Standard) — a flat event schema representing clinical events as `(subject_id, time, code, numeric_value, text_value)` rows

The goal of this project is to **convert CLIF data into the MEDS standard, with events encoded using the ELF convention**. The conversion pipeline reads CLIF tables and produces MEDS-compatible parquet output where every event's `code` column follows the ELF hierarchical format.

**ELF** (Event Language Format), defined in the `ELF/` subdirectory (separate git repo), constrains MEDS' free-form `code` column with a hierarchical format and the **mCIDE** (minimum Common ICU Data Elements) vocabulary.

## Architecture

### ELF Code Format

Every clinical event code follows: `{domain}//{level_1}//{level_2}//{level_3}`

- `//` (double slash) separates hierarchy levels
- `/` (single slash) is reserved for use within values (e.g., `mg/dL`, `mcg/kg/min`)
- Sentinel values: `NA` = not applicable, `UNK` = unknown

### mCIDE Domains (16 total)

| Domain | Prefix | Levels used | Example |
|---|---|---|---|
| Vitals | `VITAL//` | 2 | `VITAL//heart_rate//NA` |
| Labs | `LAB//` | 3 | `LAB//creatinine//mg/dL//bmp` |
| Meds (continuous) | `MED_CON//` | 3 | `MED_CON//norepinephrine//UNK//start` |
| Meds (intermittent) | `MED_INT//` | 3 | `MED_INT//vancomycin//mg//given` |
| Respiratory | `RESP//` | 3 | `RESP//peep//cmH2O//set` |
| Patient Assessments | `PA//` | 1 | `PA//gcs_total` |
| Code Status | `CODE_STATUS//` | 1 | `CODE_STATUS//full_code` |
| Hospitalization | `HOSP//` | 3 | `HOSP//admission_type//...` |
| Demographics | `DEMO//` | 2 | `DEMO//sex//female` |
| ADT | `ADT//` | 3 | `ADT//TRANSFER_IN//icu//neuro_icu` |
| Position | `POS//` | 1 | `POS//prone` |
| CRRT | `CRRT//` | 3 | `CRRT//crrt//UNK//presence` |
| ECMO/MCS | `ECMO_MCS//` | 3 | — |
| Procedures | `PROC//` | 2 | pass-through (CPT/HCPCS) |
| Patient Dx | `PATIENT_DX//` | 3 | `PATIENT_DX//ICD//10//J96` |
| Hospital Dx | `HOSP_DX//` | 3 | `HOSP_DX//ICD//10//A41` |

### Key Design Decisions

- **PATIENT_DX vs HOSP_DX split**: Prevents label leakage in predictive models. PATIENT_DX = available during stay; HOSP_DX = post-hoc discharge labels.
- **Pass-through domains** (PROC, PATIENT_DX, HOSP_DX): Codes come from source data (ICD, CPT), not from a predefined catalog.
- **Config-driven conversion**: Per-domain YAML files in `config/concepts/` define how source data maps to ELF codes. User extensions go in `config/extensions/`.
- **Semantic versioning** per domain config: `concept_version` in `codes.parquet` traces which config version produced each concept.

### Output Schema

ELF produces MEDS-compatible output with three parquet files:
- `data.parquet` — event rows (subject_id, time, code, numeric_value, text_value)
- `codes.parquet` — code metadata (code, description, parent_codes, concept_version)
- `metadata/subject_splits.parquet` — train/tuning/held_out splits

## Tools

The `clif-icu` skill is available for working with CLIF data — use it for loading/filtering CLIF tables, computing clinical scores (SOFA, CCI, Elixhauser), and data transformations via the `clifpy` library.

## Key References

- ELF full spec: `ELF/efl/ELF.md`
- Domain guides: `ELF/efl/domains/{DOMAIN}.md` (one per domain)
- mCIDE source CSVs: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF/tree/main/mCIDE
- MEDS schema: https://github.com/Medical-Event-Data-Standard/meds

## License

Apache 2.0
