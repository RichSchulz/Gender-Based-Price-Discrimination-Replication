# Data Directory

This replication package does not ship Treatwell data. Generate all files locally.

## Directory Layout

- `data/snapshots/`: dated scraper outputs and venue metadata files.
- `data/derived/`: regenerated intermediate analysis files.
- `data/audits/`: diagnostic samples and audit outputs from robustness scripts.
- `data/final/`: reserved for curated, reproducibility-critical local inputs if needed later.

## Expected Snapshot Files

- `treatwell-all-{date}.csv`
- `treatwell_without_raw-all-{date}.csv`
- `treatwell_kids-{date}.csv`
- `treatwell_without_raw_kids-{date}.csv`
- `venue_info-{date}.csv`

## Expected Derived Files

- `treatment_ids_analysis.csv`
- `matched_category_model_summary.json`
- `matched_category_summary.csv`

## Terms Of Service

Do not commit or redistribute generated platform data from these directories.
