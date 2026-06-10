import argparse
import sys
from pathlib import Path

from odds_io_utils import (
    average_probabilities,
    ensure_directories,
    fetch_events,
    fetch_odds,
    filter_events,
    final_results,
    load_config,
    load_environment,
    save_outputs,
    setup_logging,
    transform_correct_scores,
    utc_timestamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch World Cup Correct Score odds and build result CSVs.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--refresh", action="store_true", help="Ignore cache TTL and fetch fresh API data")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = root / config_path

    timestamp = utc_timestamp()
    config = load_config(config_path)
    load_environment(root)
    paths = ensure_directories(config, root)
    logger = setup_logging(paths["logs"], timestamp)

    try:
        events = fetch_events(config, paths, timestamp, args.refresh, logger)
        filtered_events = filter_events(events, config, logger)
        odds_payloads = fetch_odds(config, paths, filtered_events, timestamp, args.refresh, logger)
        correct_scores = transform_correct_scores(odds_payloads, logger)
        avg_scores = average_probabilities(correct_scores)
        results = final_results(correct_scores, avg_scores)
        save_outputs(correct_scores, avg_scores, results, paths, timestamp)
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        return 1

    logger.info("Pipeline complete: %d Correct Score rows, %d final result rows", len(correct_scores), len(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
