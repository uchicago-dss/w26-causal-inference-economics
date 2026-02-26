# Analysis Workspace

This folder contains notebooks and canonical generated outputs for the honors thesis analysis.

## Folders

- `notebooks/thesis`: Main modular thesis workflows.
- `notebooks/robustness`: Robustness checks (Rauch homogeneous-goods restriction).
- `results/data`: Generated datasets and diagnostics CSV outputs.
- `results/tables`: Regression and model tables (`did/`, `rauch/`).
- `results/figures`: Generated plots (`event_study/`, `seasonality/`, `rauch/`).
- `reports`: Optional location for rendered PDF outputs (if an `output-dir` is configured).

## Output Conventions

- Canonical analysis outputs live in `analysis/results/`.
- Notebook-rendered PDFs default to the notebook directory unless Quarto `output-dir` is set.
- `notebooks/thesis/thesis_full_pipeline.qmd` is a legacy monolithic workflow; analysis outputs are routed to `analysis/results/`.

## Suggested Notebook Run Order

1. `notebooks/thesis/01_build_did_panel.qmd`
2. `notebooks/thesis/02_did_regression_ladder.qmd`
3. `notebooks/thesis/03_event_study.qmd`
4. `notebooks/thesis/04_seasonality_sanity_checks.qmd`
5. `notebooks/robustness/rauch_homogeneous_goods_robustness.qmd`
