"""
producer.py

- Loads Netflix + IMDb CSVs from ./data (see README notes below).
- Builds (or reads) a list of N shows (default 200).
- Uses pytrends to request Google Trends data in BATCHES of up to 5 keywords per payload.
- Uses ThreadPoolExecutor to parallelize batch requests.
- Creates a metrics JSON per show and RPUSHes into Redis list 'franchise_queue'.

Config:
- Provide REDIS_HOST/REDIS_PORT via environment or .env (defaults below).
- Provide path to data files (netflix_titles.csv, title.ratings.tsv.gz, title.basics.tsv.gz).
- Provide optional shows_list.txt (one title per line). If not present, code selects first 200 unique shows
  from netflix_titles.csv.

Notes:
- pytrends backoff: Google Trends can throttle; code retries on errors with small backoff.
- If pytrends is blocked (network, captcha), the producer will still push records with 0 hype_score and
  available metadata (IMDB rating, estimated hours).
"""

import os
import time
import json
import math
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

import redis
import pandas as pd
from pytrends.request import TrendReq
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "franchise_queue")

DATA_DIR = os.getenv("DATA_DIR", "/data")  # in docker container mount to /data
NETFLIX_FILE = os.getenv("NETFLIX_FILE", os.path.join(DATA_DIR, "netflix_titles.csv"))
IMDB_RATINGS_FILE = os.getenv("IMDB_RATINGS_FILE", os.path.join(DATA_DIR, "title.ratings.tsv.gz"))
IMDB_BASICS_FILE = os.getenv("IMDB_BASICS_FILE", os.path.join(DATA_DIR, "title.basics.tsv.gz"))
SHOWS_LIST_FILE = os.getenv("SHOWS_LIST_FILE", os.path.join(DATA_DIR, "shows_list.txt"))

NUM_SHOWS = int(os.getenv("NUM_SHOWS", "200"))
BATCH_SIZE = int(os.getenv("PYTRENDS_BATCH_SIZE", "5"))  # pytrends supports up to 5 keywords per payload
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
SLEEP_BETWEEN_CYCLES = int(os.getenv("PRODUCER_SLEEP", "60"))  # seconds

def load_shows_list() -> List[str]:
    """Return a list of shows to query (length NUM_SHOWS). Priority:
       1) shows_list.txt if exists
       2) netflix_titles.csv first NUM_SHOWS unique titles
    """
    if os.path.exists(SHOWS_LIST_FILE):
        logging.info("Loading shows from shows_list.txt")
        with open(SHOWS_LIST_FILE, "r", encoding="utf-8") as f:
            titles = [l.strip() for l in f if l.strip()]
            return titles[:NUM_SHOWS]

    # fallback to netflix_titles.csv
    if os.path.exists(NETFLIX_FILE):
        logging.info("Loading shows from netflix_titles.csv")
        df = pd.read_csv(NETFLIX_FILE)
        # Prefer titles where type == TV Show, but fall back to ALL
        if "type" in df.columns:
            tv = df[df["type"].str.lower().str.contains("tv", na=False)]
            candidates = tv["title"].dropna().unique().tolist()
            if len(candidates) < NUM_SHOWS:
                candidates = list(df["title"].dropna().unique())  # fallback to all titles
        else:
            candidates = list(df["title"].dropna().unique())
        return candidates[:NUM_SHOWS]

    logging.warning("No shows file found; generating dummy show names")
    return [f"Show {i+1}" for i in range(NUM_SHOWS)]

def load_imdb_data() -> pd.DataFrame:
    """Load imdb basics and ratings to produce imdb_rating and runtime approximation"""
    ratings = None
    basics = None
    if os.path.exists(IMDB_RATINGS_FILE):
        try:
            ratings = pd.read_csv(IMDB_RATINGS_FILE, sep="\t", compression="gzip", low_memory=False)
            # imdb ratings file typically has tconst, averageRating, numVotes
        except Exception:
            try:
                ratings = pd.read_csv(IMDB_RATINGS_FILE, sep="\t", low_memory=False)
            except Exception:
                ratings = None
    if os.path.exists(IMDB_BASICS_FILE):
        try:
            basics = pd.read_csv(IMDB_BASICS_FILE, sep="\t", compression="gzip", low_memory=False)
        except Exception:
            try:
                basics = pd.read_csv(IMDB_BASICS_FILE, sep="\t", low_memory=False)
            except Exception:
                basics = None
    if ratings is not None:
        ratings = ratings.rename(columns=lambda c: c.strip())
    if basics is not None:
        basics = basics.rename(columns=lambda c: c.strip())
    if basics is not None and ratings is not None:
        merged = basics.merge(ratings, on="tconst", how="left")
        # We'll use primaryTitle -> averageRating
        if "primaryTitle" in merged.columns:
            merged = merged.rename(columns={"primaryTitle": "title"})
        return merged
    return pd.DataFrame()

def estimate_hours_from_imdb_row(row) -> float:
    """Try to estimate total watch hours from IMDb basics (runtimeMinutes or number of episodes).
       This is approximate and used when netflix_hours isn't available.
    """
    try:
        if "runtimeMinutes" in row and not pd.isna(row["runtimeMinutes"]):
            mins = float(row["runtimeMinutes"])
            # assume 1 episode = runtimeMinutes, and assume 8 episodes if not available
            hours = mins / 60.0
            # Could multiply by estimated number of episodes — but we keep single episode as conservative
            return round(hours, 2)
    except Exception:
        pass
    return 0.0

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

class TrendsBatcher:
    def __init__(self, batch_size: int = BATCH_SIZE):
        self.batch_size = batch_size
        self.pytrends = TrendReq(hl='en-US', tz=360)

    def fetch_batch_interest(self, keywords: List[str]) -> Dict[str, float]:
        """
        Fetch interest_over_time for a batch of keywords (<= batch_size).
        Returns dict {keyword: mean_interest}
        On error returns zeros for those keywords.
        """
        results = {k: 0.0 for k in keywords}
        try:
            self.pytrends.build_payload(keywords, timeframe='today 12-m')  # last 12 months
            df = self.pytrends.interest_over_time()
            if df is None or df.empty:
                return results
            # average interest per keyword (drop isPartial column if present)
            for kw in keywords:
                if kw in df.columns:
                    mean_val = float(df[kw].mean())
                    results[kw] = mean_val
        except Exception as e:
            logging.warning("pytrends error for batch %s : %s", keywords, str(e))
            # if pytrends fails (captcha, blocked) we return zeros - consumer/app can still use other metrics
        return results

def main_loop():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    shows = load_shows_list()
    NUM = len(shows)
    logging.info("Using %d shows for streaming.", NUM)

    imdb_df = load_imdb_data()
    imdb_map = {}
    if not imdb_df.empty and "title" in imdb_df.columns:
        # Normalize titles for matching
        imdb_df['title_norm'] = imdb_df['title'].str.lower().str.strip()
        imdb_map = imdb_df.set_index('title_norm').to_dict(orient='index')

    batcher = TrendsBatcher(batch_size=BATCH_SIZE)

    # Precompute batches
    show_batches = list(chunks(shows, BATCH_SIZE))
    logging.info("Prepared %d batches (batch_size=%d)", len(show_batches), BATCH_SIZE)

    while True:
        timestamp = int(time.time())
        # Use ThreadPoolExecutor to parallelize batch requests (each future fetches up to BATCH_SIZE keywords)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_batch = {executor.submit(batcher.fetch_batch_interest, batch): batch for batch in show_batches}
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    batch_results = future.result()
                except Exception as exc:
                    logging.exception("Batch fetch failed: %s", exc)
                    batch_results = {k: 0.0 for k in batch}

                # Build and push events for each show in the batch
                pipe = r.pipeline()
                for title in batch:
                    title_norm = title.lower().strip()
                    imdb_row = imdb_map.get(title_norm, {})
                    imdb_rating = None
                    estimated_hours = 0.0
                    if imdb_row:
                        imdb_rating = imdb_row.get('averageRating')
                        try:
                            estimated_hours = estimate_hours_from_imdb_row(imdb_row)
                        except Exception:
                            estimated_hours = 0.0

                    hype_score = float(batch_results.get(title, 0.0) or 0.0)
                    # Build metrics record; keep additional fields for later processing
                    metrics = {
                        "timestamp": timestamp,
                        "title": title,
                        "metrics": {
                            "hype_score": round(hype_score, 3),
                            "imdb_rating": float(imdb_rating) if imdb_rating else None,
                            "netflix_hours": estimated_hours,
                            # placeholders - more fields can be added (cost, brand_equity, total_plays)
                            "brand_equity": None,
                            "total_plays": None
                        }
                    }
                    pipe.rpush(REDIS_QUEUE, json.dumps(metrics))
                pipe.execute()
                logging.info("Pushed %d events for batch (first title: %s)", len(batch), batch[0] if batch else "n/a")

        logging.info("Cycle complete - sleeping %d seconds before next update...", SLEEP_BETWEEN_CYCLES)
        time.sleep(SLEEP_BETWEEN_CYCLES)


if __name__ == "__main__":
    main_loop()