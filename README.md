# Gender-Based Price Discrimination in Personal Care Services: Replication Package

This repository contains the scraping and analysis code needed to reproduce the paper's empirical workflow without redistributing platform data.

## Repository Map

- `analysis/`: core analysis scripts, robustness checks, and notebooks.
- `scraping/`: Treatwell collection scripts for adult, children, and venue metadata.
- `paper/`: generated LaTeX table inputs and lightweight paper workflow notes.
- `data/`: empty directory structure plus documentation for where locally generated files belong.

## Data Policy

Treatwell data are not included here. The platform's Terms of Service do not permit redistribution of the scraped datasets or raw API payloads.

The package therefore includes:

- code
- notebooks
- table-generation scripts
- generated LaTeX table files
- directory-level documentation

The package does not include:

- raw platform extracts
- cleaned CSV snapshots
- venue-response dumps
- derived audit CSVs

## Python Requirements

Install the main dependencies with:

```bash
pip install pandas numpy scipy statsmodels requests reverse_geocoder stargazer jupyter
```

## Replication Workflow

### 1. Collect raw data locally

```bash
python scraping/crawl-treatwell.py
python scraping/crawl-treatwell.py --kids
python scraping/scrape-venue-info.py
```

These scripts write dated files into `data/snapshots/`.

### 2. Build intermediate analysis inputs

```bash
python analysis/analyze_treatment_ids.py
```

This writes `data/derived/treatment_ids_analysis.csv`.

### 3. Rebuild paper tables and diagnostics

```bash
python analysis/generate_paper_tables.py
python analysis/country_heterogeneity_check.py
python analysis/matched_category_robustness.py
python analysis/structured_option_subsample.py
python analysis/linearity_check.py
```

Generated outputs are written to:

- `paper/tables/`
- `data/derived/`
- `data/audits/`

### 4. Inspect notebook analyses

The repository also includes:

- `analysis/analysis-new.ipynb`
- `analysis/analysis-more-countries.ipynb`

These notebooks are retained for transparency, but the scripted workflow above is the cleaner replication path.

## Notes On Scope

This package intentionally excludes internal review artifacts, scratch notebooks, local machine paths, and restricted data files.
