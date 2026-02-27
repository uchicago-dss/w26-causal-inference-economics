# w26-causal-inference-economics

This project explores a key topic in economics: causal inference. With has a heavy emphasis on statistics and data science, causal inference’s goal is to convincingly claim, for example, that college increases your net income by x dollars after 10 years, holding everything else constant. This project will be heavily focusing on regressions: cleaning the data in preparation, creating the model in accordance with causal inference theory, interpreting the model in the context of the problem, and finally applying advanced statistical theory to answer adjacent questions.

The central topic members will assess is the impact of China’s WTO accession in 2001 on costs and quantities of American goods.

This project mainly aims to teach theory and technical skills.

## Project layout

- `raw_data/`: external/raw source files (ignored in git).
- `outside_data/`: external transformed inputs and release-downloaded assets (ignored in git).
- `analysis/notebooks/`: analysis notebooks and reproducible code.
- `analysis/results/data/`: generated analysis datasets (ignored in git).
- `data_cleaning_scripts/`: scripts to fetch/clean/build panel data.
- `scripts/download_data.sh`: installs large files from GitHub Releases.

## Data setup

Prerequisites:

- `bash`, `curl`, `jq`, and `shasum` available in shell.
- R + Quarto installed for rendering notebooks.
- Python 3 only if you plan to pull fresh API data.

Recommended:

```bash
cd w26-causal-inference-economics

# Download all assets in the tagged release to their manifest paths.
bash scripts/download_data.sh --tag data-v1
```

Verify key inputs exist:

```bash
ls -lh analysis/results/data/panel_hts10_monthly.csv
ls -lh outside_data/data_files_aer_2013/tar_val.dta
```

Optional (rebuild raw trade data from API instead of release assets, but takes a few hours):

```bash
cd w26-causal-inference-economics

# Option A: set token as an environment variable for this shell session.
export DATAWEB_TOKEN="<your_usitc_dataweb_token>"

# Option B: create a local .env file (gitignored) used by data_cleaning_scripts/api.py.
cp data_cleaning_scripts/.env.example data_cleaning_scripts/.env
# then edit data_cleaning_scripts/.env and set DATAWEB_TOKEN

python3 data_cleaning_scripts/api.py --output-dir raw_data --start-year 1996 --end-year 2005
Rscript data_cleaning_scripts/build_panel_hts10_monthly.R
```

This rebuild path writes the monthly panel to `analysis/results/data/panel_hts10_monthly.csv`.

## Pipeline order

From repo root, run the thesis notebooks in this order:

```bash
quarto render analysis/notebooks/thesis/01_build_did_panel.qmd
quarto render analysis/notebooks/thesis/02_did_regression_ladder.qmd
quarto render analysis/notebooks/thesis/03_event_study.qmd
quarto render analysis/notebooks/thesis/04_seasonality_sanity_checks.qmd
```

Expected generated outputs:

- `01_build_did_panel.qmd` -> `analysis/results/data/did_panel_monthly.csv`
- `02_did_regression_ladder.qmd` -> `analysis/results/tables/did/*`
- `03_event_study.qmd` -> `analysis/results/tables/event_study/*` and `analysis/results/figures/event_study/*`
- `04_seasonality_sanity_checks.qmd` -> `analysis/results/figures/seasonality/*`

Optional robustness pass:

```bash
quarto render analysis/notebooks/robustness/rauch_homogeneous_goods_robustness.qmd
```
