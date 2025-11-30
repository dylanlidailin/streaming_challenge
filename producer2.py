import os
import time
import json
import logging
from typing import List, Dict

import redis
import pandas as pd
from pytrends.request import TrendReq
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- CONFIGURATION (SAME AS PRODUCER 1) ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "franchise_queue")

DATA_DIR = os.getenv("DATA_DIR", "/data")
# We still need these to lookup metadata (Ratings/Votes) for the new shows
IMDB_RATINGS_FILE = os.getenv("IMDB_RATINGS_FILE", os.path.join(DATA_DIR, "title.ratings.tsv.gz"))
IMDB_BASICS_FILE = os.getenv("IMDB_BASICS_FILE", os.path.join(DATA_DIR, "title.basics.tsv.gz"))

# Rate limiting settings
SLEEP_BETWEEN_YEARS = 2

# --- MANUAL LIST INPUT ---
NEW_SHOWS = [
    "The Walking Dead", "Sherlock", "Death Note", "True Detective", 
    "Firefly", "Band of Brothers", "Chernobyl", "Dexter", "House of Cards", 
    "Prison Break", "Lost", "Vikings", "The Simpsons", "South Park", 
    "Twin Peaks", "House"
]

def load_imdb_metadata() -> dict:
    """
    Load IMDb Votes and Rating.
    We still run this so your new shows get 'brand_equity' and 'imdb_rating' data.
    """
    logging.info("Loading IMDb Metadata (This might take a moment)...")
    meta_map = {}
    if os.path.exists(IMDB_RATINGS_FILE) and os.path.exists(IMDB_BASICS_FILE):
        try:
            # Load Basics (Title -> ID)
            basics = pd.read_csv(IMDB_BASICS_FILE, sep="\t", compression="gzip", usecols=['tconst', 'primaryTitle'])
            # Load Ratings (ID -> Votes)
            ratings = pd.read_csv(IMDB_RATINGS_FILE, sep="\t", compression="gzip", usecols=['tconst', 'numVotes', 'averageRating'])
            
            # Merge
            merged = basics.merge(ratings, on='tconst')
            
            # Create Lookup Dictionary
            for _, row in merged.iterrows():
                title_key = str(row['primaryTitle']).lower().strip()
                # Store match
                meta_map[title_key] = {
                    "brand_equity": int(row['numVotes']),
                    "imdb_rating": float(row['averageRating'])
                }
        except Exception as e:
            logging.error(f"IMDb Load Failed: {e}")
    else:
        logging.warning("IMDb files not found. New shows will have 0 brand equity/rating.")
    return meta_map

class TrendsFetcher:
    def __init__(self):
        # hl='en-US' ensures we get English volume data
        self.pytrends = TrendReq(hl='en-US', tz=360, timeout=(10,25))

    def fetch_history(self, show_title):
        """Fetches 5 Years of Daily Data."""
        timeframes = [
            '2020-01-01 2020-12-31', 
            '2021-01-01 2021-12-31',
            '2022-01-01 2022-12-31', 
            '2023-01-01 2023-12-31',
            '2024-01-01 2024-12-31'
        ]
        
        all_data = []
        for period in timeframes:
            try:
                logging.info(f"   ...fetching {period}")
                self.pytrends.build_payload([show_title], cat=0, timeframe=period)
                data = self.pytrends.interest_over_time()
                if not data.empty:
                    for ts, row in data.iterrows():
                        all_data.append({
                            "timestamp": int(ts.timestamp()),
                            "hype_score": int(row[show_title])
                        })
                # Sleep to satisfy Google API limits
                time.sleep(SLEEP_BETWEEN_YEARS) 
            except Exception as e:
                logging.warning(f"   Failed for {period}: {e}")
                time.sleep(5) # Backoff slightly on error
        return all_data

def main_loop():
    # Connect to the SAME Redis instance
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    
    # Load metadata once
    imdb_map = load_imdb_metadata()
    fetcher = TrendsFetcher()

    logging.info(f"🚀 Starting SUPPLEMENTAL Producer for {len(NEW_SHOWS)} specific shows...")

    for i, title in enumerate(NEW_SHOWS):
        logging.info(f"[{i+1}/{len(NEW_SHOWS)}] Processing: {title}")
        
        # 1. Get Metadata (Votes/Rating)
        # Note: This relies on exact string matching with IMDb titles. 
        # e.g. "House" might need to be "House M.D." to match IMDb, 
        # but we use the list provided.
        meta = imdb_map.get(title.lower().strip(), {})
        brand_equity = meta.get("brand_equity", 0)
        rating = meta.get("imdb_rating", 0.0)

        # 2. Get History (Hype) from Google
        history = fetcher.fetch_history(title)
        
        if history:
            pipe = r.pipeline()
            for point in history:
                record = {
                    "timestamp": point['timestamp'],
                    "title": title,
                    "metrics": {
                        "hype_score": point['hype_score'],
                        "brand_equity": brand_equity,
                        "cost_basis": 1, 
                        "netflix_hours": 0,
                        "imdb_rating": rating
                    }
                }
                pipe.rpush(REDIS_QUEUE, json.dumps(record))
            pipe.execute()
            logging.info(f"   ✅ Pushed {len(history)} daily records for {title}")
        else:
            logging.warning(f"   ⚠️ No trend data found for {title}")

    logging.info("🎉 SUPPLEMENTAL JOB COMPLETE. Producer2 exiting.")

if __name__ == "__main__":
    main_loop()