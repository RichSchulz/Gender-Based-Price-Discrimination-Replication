# Gender-Based Price Discrimination in Personal Care Services: Replication Package

This repository contains the code and documentation to replicate the analysis in our study of gender-based price discrimination in haircut services across European markets.

## Overview

We analyze pricing data from Treatwell, a major European beauty services marketplace, to examine whether salons charge different prices for equivalent services based on the customer's gender (the "Pink Tax" phenomenon).

## Repository Structure

```
.
├── data/                    # Data files (see data/README.md)
├── scraping/                # Data collection scripts
│   ├── crawl-treatwell.py   # Main scraping script for haircut data
│   └── scrape-venue-info.py # Venue metadata collection
├── analysis/                # Analysis code
│   ├── utils.py             # Helper functions and filter words
│   ├── analyze_treatment_ids.py  # Treatment category analysis
│   ├── analysis-new.ipynb   # Main regression analysis
│   └── analysis-more-countries.ipynb  # Extended multi-country analysis
└── README.md
```

## Requirements

### Python Dependencies

```bash
pip install pandas numpy scipy statsmodels requests reverse_geocoder stargazer jupyter
```

## Replication Steps

### 1. Data Collection

```bash
# Collect haircut pricing data from Treatwell
cd scraping
python crawl-treatwell.py

# Collect venue metadata
python scrape-venue-info.py
```

**Note:** Data collection requires approximately 24-48 hours due to rate limiting. The scraping scripts are designed to be resumable if interrupted.

### 2. Data Analysis

```bash
# Analyze treatment categories
cd analysis
python analyze_treatment_ids.py

# Run regression analysis
jupyter notebook analysis-more-countries.ipynb
```

## Key Variables

| Variable | Description |
|----------|-------------|
| `is_female` / `is_male` | Gender category of the haircut service |
| `simpleCutSalePrice` | Listed price in local currency |
| `simpleCutDurationMin/Max` | Service duration range |
| `averageRating` | Venue's average customer rating |
| `ratingCount` | Number of customer reviews |

## Data Availability

Due to Treatwell's Terms of Service, we cannot publicly share the scraped data. Researchers wishing to replicate our analysis should:

1. Use the provided scraping scripts to collect their own data
2. Contact the authors for access to anonymized summary statistics

## Citation

If you use this code, please cite:

```bibtex
@article{schulz2025pinktax,
  title={Gender-Based Price Discrimination in Personal Care Services: Evidence from European Haircut Markets},
  author={Schulz, Richard},
  year={2025}
}
```

## License

MIT License - see LICENSE file for details.

## Contact

For questions about replication, please open an issue or contact the authors.
