# Running CLIF_MEDS

Step-by-step guide for converting CLIF data into MEDS with ELF-coded events.

## Prerequisites

- Python 3.13+
- [`uv`](https://docs.astral.sh/uv/) (recommended) or `pip`

## Installation

```bash
git clone <repo-url>
cd CLIF_MEDS
uv sync
```

## 1. Prepare your data

Place your CLIF parquet tables in a single directory. The full set of filenames is:

```
clif_patient.parquet
clif_hospitalization.parquet
clif_vitals.parquet
clif_labs.parquet
clif_medication_admin_continuous.parquet
clif_medication_admin_intermittent.parquet
clif_respiratory_support.parquet
clif_patient_assessments.parquet
clif_code_status.parquet
clif_adt.parquet
clif_position.parquet
clif_crrt_therapy.parquet
clif_ecmo_mcs.parquet
clif_patient_procedures.parquet
clif_hospital_diagnosis.parquet
```

You only need the files for domains you enable in your config (set to `1`). Disabled domains (set to `0`) are skipped and their files are not required. **`clif_patient.parquet` and `clif_hospitalization.parquet` are always required** — the pipeline forces PATIENT and HOSP on for subject and hospitalization mappings.

See the [CLIF schema](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF) for column definitions.

## 2. Configure

Copy the template and fill in your site details:

```bash
cp clif_config_template.yaml my_config.yaml
```

Key fields to edit:

| Field | Description |
|---|---|
| `site` | Your site identifier |
| `data_directory` | Path to your CLIF parquet files |
| `output_directory` | Where MEDS output will be written |
| `timezone` | Timezone for timestamp normalization (e.g., `America/Chicago`) |
| `domains` | Toggle each domain on (`1`) or off (`0`) |

The `DatasetMetadataSchema` section captures dataset-level metadata (name, version, license, etc.) that is written to `dataset_metadata.json`.

## 3. Run the ETL

```bash
uv run python main.py --config my_config.yaml
```

The pipeline processes PATIENT and HOSP first (required for subject/hospitalization mappings), then all other enabled domains.

## 4. Output

The ETL produces the following directory structure:

```
<output_directory>/
  data/
    PATIENT.parquet
    HOSP.parquet
    VITAL.parquet
    LAB.parquet
    ...
  metadata/
    codes.parquet
    dataset_metadata.json
```

- **`data/*.parquet`** -- One file per domain. Each row is a MEDS event with columns: `subject_id`, `time`, `code`, `numeric_value`, `text_value`, `hospitalization_id`.
- **`metadata/codes.parquet`** -- Code registry with `code`, `description`, `parent_codes`, and `concept_version` (tracks which config version produced each code).
- **`metadata/dataset_metadata.json`** -- Dataset metadata following the MEDS schema (ELF version, site info, split ratios, etc.).

## Quick test with demo data

The repository includes synthetic demo data for all 15 domains:

```bash
uv run python main.py --config test/clif_config.yaml
```

Output will be written to `test/output/`.

## Using CLIF-MIMIC for development

[CLIF-MIMIC](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC) converts MIMIC-IV into CLIF format and provides a freely available dataset for development:

1. Clone CLIF-MIMIC and follow its instructions to generate CLIF tables from MIMIC-IV
2. Point `data_directory` in your config at the CLIF-MIMIC output directory
3. Run the ETL:
   ```bash
   uv run python main.py --config my_config.yaml
   ```
4. You now have ELF-coded MEDS data to train and evaluate models against

Models built on this output will generalize to any CLIF site, since ELF standardizes the code vocabulary across sites.

## Customizing domain configs

Domain-specific YAML files live in `config/`. Each file defines:

- **`elf_version`** -- Semantic version for the domain config
- **`code`** -- List of ELF hierarchy levels; use `col(column_name)` to reference a CLIF source column
- **`time`** -- Source timestamp column (with `time_format`)
- **`numeric_value` / `text_value`** -- Source columns for event values

For example, from `config/LAB.yaml`:

```yaml
labs:
  lab:
    code:
      - LAB
      - col(lab_category)
      - col(reference_unit)
      - col(lab_order_category)
    time: col(lab_result_dttm)
    numeric_value: lab_value_numeric
    text_value: lab_value
```

This produces codes like `LAB//creatinine//mg/dL//bmp` by joining the domain prefix with values from the `lab_category`, `reference_unit`, and `lab_order_category` columns.
