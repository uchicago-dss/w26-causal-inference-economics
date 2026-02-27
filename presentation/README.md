# Presentation Deck (LaTeX)

This folder contains a 12-minute Beamer draft for the project.

## Files

- `deck.tex`: main slide deck.

## Build

From repo root:

```bash
cd presentation
pdflatex deck.tex
```

If you want cleaner output:

```bash
cd presentation
pdflatex -interaction=nonstopmode deck.tex
pdflatex -interaction=nonstopmode deck.tex
```
