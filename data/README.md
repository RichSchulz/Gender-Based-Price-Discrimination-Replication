# Data Directory

This replication package does not ship Treatwell data. Generate all files locally.

## Directory Layout

- `data/snapshots/`: dated scraper outputs and venue metadata files.
- `data/derived/`: regenerated intermediate analysis files required by the paper table workflow.
- `data/final/`: reserved for curated, reproducibility-critical local inputs if needed later.

## Expected Snapshot Files

- `treatwell-all-{date}.csv`
- `treatwell_without_raw-all-{date}.csv`
- `treatwell_kids-{date}.csv`
- `treatwell_without_raw_kids-{date}.csv`
- `venue_info-{date}.csv`

## Expected Derived Files

- `matched_category_model_summary.json`

## Terms Of Service

Do not commit or redistribute generated platform data from these directories.
