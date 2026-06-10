import json
import logging
import math
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv


SCORE_RE = re.compile(r"^(\d+)-(\d+)$")
CORRECT_SCORE_COLUMNS = [
    "event_id",
    "match",
    "home",
    "away",
    "date",
    "bookmaker",
    "score",
    "odd",
    "updated_at",
    "home_goals",
    "away_goals",
    "raw_prob",
    "prob",
]


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def load_config(path: str | Path) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_environment(root: Path) -> None:
    root_env = root / ".env"
    fallback_env = root / "src" / "human_exploration" / ".env"
    if root_env.exists():
        load_dotenv(root_env)
    elif fallback_env.exists():
        load_dotenv(fallback_env)


def ensure_directories(config: dict, root: Path) -> dict[str, Path]:
    paths = {name: root / rel_path for name, rel_path in config["paths"].items()}
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def setup_logging(log_dir: Path, timestamp: str) -> logging.Logger:
    logger = logging.getLogger("odds_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()

    log_file = log_dir / f"pipeline_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info("Logging to %s", log_file)
    return logger


def config_datetime_to_rfc3339(value: str) -> str:
    dt = pd.to_datetime(value)
    if dt.tzinfo is not None:
        dt = dt.tz_convert("UTC").tz_localize(None)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_api_datetime(value: str) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True).tz_convert(None)


def latest_cache(cache_dir: Path, suffix: str, ttl_minutes: int) -> Path | None:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=ttl_minutes)
    candidates = sorted(cache_dir.glob(f"*_{suffix}.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for candidate in candidates:
        modified = datetime.fromtimestamp(candidate.stat().st_mtime, tz=timezone.utc)
        if modified >= cutoff:
            return candidate
    return None


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def api_get(base_url: str, endpoint: str, api_key: str, params: dict, timeout: int):
    request_params = {"apiKey": api_key, **params}
    try:
        response = requests.get(f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}", params=request_params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        status = getattr(exc.response, "status_code", "unknown") if getattr(exc, "response", None) is not None else "unknown"
        raise RuntimeError(f"GET /{endpoint.lstrip('/')} failed with status {status}") from exc
    return response.json()


def fetch_events(config: dict, paths: dict[str, Path], timestamp: str, refresh: bool, logger: logging.Logger):
    date_suffix = timestamp.split("_", 1)[0]
    ttl = int(config["api"].get("cache_ttl_minutes", 30))
    if not refresh:
        cache = latest_cache(paths["data_cached"], "events", ttl)
        if cache:
            logger.info("Reusing events cache %s", cache)
            return read_json(cache)

    api_key = require_api_key(config)
    params = {
        "sport": config["api"]["sport"],
        "league": config["api"]["league_slug"],
        "from": config_datetime_to_rfc3339(config["tournament"]["group_stage_start"]),
        "to": config_datetime_to_rfc3339(config["tournament"]["group_stage_end"]),
        "limit": 5000,
    }
    logger.info("Fetching events for %s/%s", params["sport"], params["league"])
    events = api_get(
        config["api"]["base_url"],
        "events",
        api_key,
        params,
        int(config["api"].get("request_timeout_seconds", 30)),
    )
    write_json(paths["data_cached"] / f"{date_suffix}_events.json", events)
    return events


def require_api_key(config: dict) -> str:
    env_name = config["api"].get("api_key_env", "ODDS_IO_API_KEY")
    api_key = os.getenv(env_name)
    if not api_key:
        raise RuntimeError(f"Missing required environment variable {env_name}")
    return api_key


def event_id(event: dict):
    return event.get("id") or event.get("eventId") or event.get("event_id")


def filter_events(events, config: dict, logger: logging.Logger) -> list[dict]:
    start = parse_api_datetime(config_datetime_to_rfc3339(config["tournament"]["group_stage_start"]))
    end = parse_api_datetime(config_datetime_to_rfc3339(config["tournament"]["group_stage_end"]))
    league_slug = config["api"]["league_slug"]
    filtered = []
    for event in events:
        if not isinstance(event, dict) or not event.get("date"):
            continue
        league = event.get("league") or {}
        if isinstance(league, dict) and league.get("slug") and league.get("slug") != league_slug:
            continue
        date = parse_api_datetime(event["date"])
        if start <= date <= end:
            filtered.append(event)
    logger.info("Filtered %d events from %d fetched events", len(filtered), len(events))
    return filtered


def has_correct_score_market(odds_data, bookmakers: list[str]) -> bool:
    for bookmaker, market in iter_bookmaker_markets(odds_data, bookmakers):
        if market_name(market).lower() == "correct score":
            return True
    return False


def smoke_check_first_event(config: dict, first_event: dict, logger: logging.Logger) -> None:
    api_key = require_api_key(config)
    bookmakers = config["api"].get("bookmakers", [])
    odds_data = api_get(
        config["api"]["base_url"],
        "odds",
        api_key,
        {"eventId": event_id(first_event), "bookmakers": ",".join(bookmakers)},
        int(config["api"].get("request_timeout_seconds", 30)),
    )
    if not has_correct_score_market(odds_data, bookmakers):
        match = event_match(first_event)
        logger.error("Smoke check failed: no configured bookmaker has a Correct Score market for %s", match)
        raise RuntimeError("Correct Score market not available in smoke check")
    logger.info("Smoke check passed for %s", event_match(first_event))


def fetch_odds(config: dict, paths: dict[str, Path], events: list[dict], timestamp: str, refresh: bool, logger: logging.Logger):
    date_suffix = timestamp.split("_", 1)[0]
    ttl = int(config["api"].get("cache_ttl_minutes", 30))
    if not refresh:
        cache = latest_cache(paths["data_cached"], "odds", ttl)
        if cache:
            logger.info("Reusing odds cache %s", cache)
            return read_json(cache)

    if not events:
        logger.warning("No events to fetch odds for")
        write_json(paths["data_cached"] / f"{date_suffix}_odds.json", [])
        return []

    smoke_check_first_event(config, events[0], logger)
    api_key = require_api_key(config)
    bookmakers = config["api"].get("bookmakers", [])
    batch_size = min(int(config["api"].get("multi_batch_size", 10)), 10)
    event_ids = [event_id(event) for event in events if event_id(event) is not None]
    timeout = int(config["api"].get("request_timeout_seconds", 30))
    all_odds = []

    try:
        for index in range(0, len(event_ids), batch_size):
            batch = event_ids[index : index + batch_size]
            data = api_get(
                config["api"]["base_url"],
                "odds/multi",
                api_key,
                {"eventIds": ",".join(map(str, batch)), "bookmakers": ",".join(bookmakers)},
                timeout,
            )
            all_odds.extend(normalize_odds_payload(data))
    except RuntimeError as exc:
        logger.warning("/odds/multi failed; falling back to individual /odds requests: %s", exc)
        all_odds = []
        for one_event_id in event_ids:
            data = api_get(
                config["api"]["base_url"],
                "odds",
                api_key,
                {"eventId": one_event_id, "bookmakers": ",".join(bookmakers)},
                timeout,
            )
            all_odds.extend(normalize_odds_payload(data))

    write_json(paths["data_cached"] / f"{date_suffix}_odds.json", all_odds)
    logger.info("Fetched odds for %d event payloads", len(all_odds))
    return all_odds


def normalize_odds_payload(data) -> list[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "events", "odds"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return []


def iter_bookmaker_markets(odds_data: dict, configured_bookmakers: list[str]):
    bookmakers = odds_data.get("bookmakers", {}) if isinstance(odds_data, dict) else {}
    configured_lower = {name.lower(): name for name in configured_bookmakers}
    if isinstance(bookmakers, dict):
        for bookmaker, markets in bookmakers.items():
            if configured_lower and bookmaker.lower() not in configured_lower:
                continue
            for market in markets or []:
                yield bookmaker, market
    elif isinstance(bookmakers, list):
        for bookmaker_entry in bookmakers:
            bookmaker = bookmaker_entry.get("name") or bookmaker_entry.get("key") or bookmaker_entry.get("title")
            if not bookmaker or (configured_lower and bookmaker.lower() not in configured_lower):
                continue
            for market in bookmaker_entry.get("markets", []) or bookmaker_entry.get("odds", []) or []:
                yield bookmaker, market


def market_name(market: dict) -> str:
    return str(market.get("name") or market.get("key") or market.get("market") or "")


def event_match(event: dict) -> str:
    home = event.get("home") or event.get("home_team") or ""
    away = event.get("away") or event.get("away_team") or ""
    return f"{home} vs {away}".strip()


def transform_correct_scores(odds_payloads: list[dict], logger: logging.Logger) -> pd.DataFrame:
    rows = []
    for event in odds_payloads:
        if not isinstance(event, dict):
            continue
        for bookmaker, market in iter_bookmaker_markets(event, []):
            if market_name(market).lower() != "correct score":
                continue
            for outcome in market.get("odds", []) or market.get("outcomes", []) or []:
                score = str(outcome.get("label") or outcome.get("name") or "")
                match = SCORE_RE.match(score)
                if not match:
                    logger.warning("Skipping non-standard score label %r for %s", score, event_match(event))
                    continue
                odd_value = outcome.get("odds") or outcome.get("price")
                try:
                    odd = float(odd_value)
                except (TypeError, ValueError):
                    logger.warning("Skipping score %r with invalid odd %r for %s", score, odd_value, event_match(event))
                    continue
                if odd <= 0:
                    continue
                rows.append(
                    {
                        "event_id": event_id(event),
                        "match": event_match(event),
                        "home": event.get("home") or event.get("home_team"),
                        "away": event.get("away") or event.get("away_team"),
                        "date": parse_api_datetime(event["date"]),
                        "bookmaker": bookmaker,
                        "score": score,
                        "odd": odd,
                        "updated_at": parse_api_datetime(market["updatedAt"]) if market.get("updatedAt") else pd.NaT,
                        "home_goals": int(match.group(1)),
                        "away_goals": int(match.group(2)),
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=CORRECT_SCORE_COLUMNS)
    df["raw_prob"] = 1 / df["odd"]
    df["prob"] = df["raw_prob"] / df.groupby(["event_id", "bookmaker"])["raw_prob"].transform("sum")
    return df[CORRECT_SCORE_COLUMNS]


def average_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["event_id", "match", "home", "away", "date", "score", "num_bookmakers", "mean_prob"])
    return (
        df.groupby(["event_id", "match", "home", "away", "date", "score"], as_index=False)
        .agg(num_bookmakers=("bookmaker", "nunique"), mean_prob=("prob", "mean"))
        .sort_values(["date", "event_id", "mean_prob"], ascending=[True, True, False])
    )


def final_results(df: pd.DataFrame, avg_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "match",
        "date",
        "most_probable_score",
        "second_most_probable_score",
        "proba_first",
        "proba_second",
        "num_bookmakers_that_had_this_score",
        "pp_diff_between_bookmakers",
    ]
    if avg_df.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    for event_id_value, event_scores in avg_df.groupby("event_id", sort=False):
        ranked = event_scores.sort_values("mean_prob", ascending=False).reset_index(drop=True)
        first = ranked.iloc[0]
        second = ranked.iloc[1] if len(ranked) > 1 else None
        bookmaker_probs = df[(df["event_id"] == event_id_value) & (df["score"] == first["score"])]["prob"]
        pp_diff = bookmaker_probs.max() - bookmaker_probs.min() if len(bookmaker_probs) >= 2 else math.nan
        rows.append(
            {
                "match": first["match"],
                "date": first["date"],
                "most_probable_score": first["score"],
                "second_most_probable_score": second["score"] if second is not None else math.nan,
                "proba_first": first["mean_prob"],
                "proba_second": second["mean_prob"] if second is not None else math.nan,
                "num_bookmakers_that_had_this_score": int(first["num_bookmakers"]),
                "pp_diff_between_bookmakers": pp_diff,
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values("date")


def save_outputs(df: pd.DataFrame, avg_df: pd.DataFrame, final_df: pd.DataFrame, paths: dict[str, Path], timestamp: str) -> None:
    date_suffix = timestamp.split("_", 1)[0]
    df.to_csv(paths["data_processed"] / f"all_correct_scores_{date_suffix}.csv", index=False)
    avg_df.to_csv(paths["data_processed"] / f"avg_score_probabilities_{date_suffix}.csv", index=False)
    final_df.to_csv(paths["results"] / f"most_probable_scores_{date_suffix}.csv", index=False)
