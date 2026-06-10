# WC 2026 Quiniela

Minimal Odds API pipeline scaffold.

## Usage

1. Add `ODDS_IO_API_KEY` to `.env`.
2. Run `uv sync`.
3. Run `uv run python src/run_pipeline.py`.

## Outputs

Pipeline outputs are written under `data/processed` and `results`. Logs are written under `logs`.

If the provider currently has no `international-world-cup` events, outputs will be header-only until Odds-API.io publishes the fixtures under that league slug.
