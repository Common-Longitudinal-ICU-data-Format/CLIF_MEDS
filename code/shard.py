"""Post-processing sharding — merge domain parquets into subject-partitioned shards."""
from pathlib import Path

import polars as pl


def shard_data(output_dir: Path, subjects_per_shard: int) -> None:
    """Merge all domain parquet files into numbered shards partitioned by subject_id.

    Each shard holds all events for a contiguous bucket of unique subjects.
    No subject_id overlap exists across shards.
    """
    data_dir = output_dir / "data"
    domain_files = sorted(data_dir.glob("*.parquet"))
    if not domain_files:
        print("  Sharding: no parquet files found, skipping")
        return

    # --- 1. Collect unique subject IDs across all domain files (streaming) ---
    subject_ids: set[int] = set()
    for f in domain_files:
        ids = (
            pl.scan_parquet(f)
            .select("subject_id")
            .unique()
            .collect(streaming=True)
        )
        subject_ids.update(ids["subject_id"].to_list())
    sorted_subjects = sorted(subject_ids)
    del subject_ids
    print(f"  Sharding: {len(sorted_subjects)} unique subjects across {len(domain_files)} domain files")

    # --- 2. Partition into buckets ---
    buckets: list[list[int]] = []
    for i in range(0, len(sorted_subjects), subjects_per_shard):
        buckets.append(sorted_subjects[i : i + subjects_per_shard])
    del sorted_subjects
    print(f"  Sharding: {len(buckets)} shards (subjects_per_shard={subjects_per_shard})")

    # --- 3. Unified schema (superset of all domains) ---
    unified_columns = {
        "subject_id": pl.Int64,
        "time": pl.Datetime,
        "code": pl.Utf8,
        "numeric_value": pl.Float32,
        "text_value": pl.Utf8,
        "hospitalization_id": pl.Int64,
    }

    # --- 4. Write each shard ---
    for shard_idx, bucket in enumerate(buckets):
        shard_frames: list[pl.DataFrame] = []

        for f in domain_files:
            df = (
                pl.scan_parquet(f)
                .filter(pl.col("subject_id").is_in(bucket))
                .collect(streaming=True)
            )
            if df.is_empty():
                continue

            # Schema alignment: add missing columns as null
            for col_name, col_dtype in unified_columns.items():
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit(None).cast(col_dtype).alias(col_name))

            # Ensure consistent column order
            df = df.select(list(unified_columns.keys()))
            shard_frames.append(df)

        if not shard_frames:
            continue

        shard_df = pl.concat(shard_frames)
        del shard_frames

        # Sort: subject_id ASC, time ASC (nulls first), code ASC
        shard_df = shard_df.sort(
            ["subject_id", "time", "code"],
            descending=[False, False, False],
            nulls_last=[False, False, False],
        )

        shard_path = data_dir / f"data_{shard_idx}.parquet"
        shard_df.write_parquet(shard_path)
        print(f"  Shard {shard_idx}: {len(shard_df)} events, {len(bucket)} subjects -> {shard_path}")
        del shard_df

    # --- 5. Delete intermediate domain files ---
    for f in domain_files:
        f.unlink()
        print(f"  Removed intermediate: {f.name}")
