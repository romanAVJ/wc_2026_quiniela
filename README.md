# WC 2026 Quiniela

Minimal Odds API pipeline scaffold.

## Usage

1. Add `ODDS_IO_API_KEY` to `.env`.
2. Run `uv sync`.
3. Run `uv run python src/run_pipeline.py`.

## Outputs

Pipeline outputs are written under `data/processed` and `results`. Logs are written under `logs`.

If the provider has no events for the configured `international-fifa-world-cup` slug/window, outputs may be header-only.
