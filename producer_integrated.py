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

# --- CONFIGURATION ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "franchise_queue")

DATA_DIR = os.getenv("DATA_DIR", "/data")
NETFLIX_FILE = os.getenv("NETFLIX_FILE", os.path.join(DATA_DIR, "netflix_titles.csv"))
IMDB_RATINGS_FILE = os.getenv("IMDB_RATINGS_FILE", os.path.join(DATA_DIR, "title.ratings.tsv.gz"))
IMDB_BASICS_FILE = os.getenv("IMDB_BASICS_FILE", os.path.join(DATA_DIR, "title.basics.tsv.gz"))

# Show selection configuration
NUM_NETFLIX_SHOWS = int(os.getenv("NUM_SHOWS", "200"))
# Increased default to 10s. MUST be set to 30s or more in .env for stability.
SLEEP_BETWEEN_YEARS = int(os.getenv("SLEEP_BETWEEN_YEARS", "10")) 

# --- RATE LIMIT FIX: Add a substantial delay between processing each show
RATE_LIMIT_DELAY_SECONDS = 15 

# Manual supplemental shows list
SUPPLEMENTAL_SHOWS = [
    "The Walking Dead", "Sherlock", "Death Note", "True Detective", 
    "Firefly", "Band of Brothers", "Chernobyl", "Dexter", "House of Cards", 
    "Prison Break", "Lost", "Vikings", "The Simpsons", "South Park", 
    "Twin Peaks", "House", "The Bear", "Succession", "Euphoria"
]

def load_shows_list() -> List[str]:
    """Load shows from Netflix dataset + supplemental list."""
    all_shows = []
    
    # Source 1: Netflix Dataset
    if os.path.exists(NETFLIX_FILE):
        try:
            df = pd.read_csv(NETFLIX_FILE)
            if "type" in df.columns:
                tv = df[df["type"].str.lower().str.contains("tv", na=False)]
                netflix_shows = tv["title"].dropna().unique().tolist()[:NUM_NETFLIX_SHOWS]
                all_shows.extend(netflix_shows)
                logging.info(f"Loaded {len(netflix_shows)} shows from Netflix dataset")
        except Exception as e:
            logging.error(f"Failed to load Netflix data: {e}")
    else:
        logging.warning(f"Netflix file not found, using supplemental shows only")
    
    # Source 2: Supplemental Shows
    all_shows.extend(SUPPLEMENTAL_SHOWS)
    logging.info(f"Added {len(SUPPLEMENTAL_SHOWS)} supplemental shows")
    
    # Deduplicate
    seen = set()
    unique_shows = []
    for show in all_shows:
        show_lower = show.lower().strip()
        if show_lower not in seen:
            seen.add(show_lower)
            unique_shows.append(show)
    
    logging.info(f"Total unique shows to process: {len(unique_shows)}")
    return unique_shows

def load_imdb_metadata() -> dict:
    """Load IMDb metadata."""
    logging.info("Loading IMDb Metadata...")
    meta_map = {}
    
    if os.path.exists(IMDB_RATINGS_FILE) and os.path.exists(IMDB_BASICS_FILE):
        try:
            basics = pd.read_csv(
                IMDB_BASICS_FILE, 
                sep="\t", 
                compression="gzip", 
                usecols=['tconst', 'primaryTitle']
            )
            ratings = pd.read_csv(
                IMDB_RATINGS_FILE, 
                sep="\t", 
                compression="gzip", 
                usecols=['tconst', 'numVotes', 'averageRating']
            )
            merged = basics.merge(ratings, on='tconst')
            
            for _, row in merged.iterrows():
                title_key = str(row['primaryTitle']).lower().strip()
                meta_map[title_key] = {
                    "brand_equity": int(row['numVotes']),
                    "imdb_rating": float(row['averageRating'])
                }
            
            logging.info(f"Loaded metadata for {len(meta_map)} titles from IMDb")
        except Exception as e:
            logging.error(f"IMDb Load Failed: {e}")
    else:
        logging.warning("IMDb files not found - using defaults")
    
    return meta_map

class TrendsFetcher:
    """Fetches historical Google Trends data."""
    
    def __init__(self):
        self.pytrends = TrendReq(hl='en-US', tz=360, timeout=(30, 60)) # Increased timeout for robustness

    def fetch_history(self, show_title: str) -> List[Dict]:
        """Fetches 5 years of daily data."""
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
                self.pytrends.build_payload([show_title], cat=0, timeframe=period)
                data = self.pytrends.interest_over_time()
                
                if not data.empty and show_title in data.columns:
                    for ts, row in data.iterrows():
                        all_data.append({
                            "timestamp": int(ts.timestamp()),
                            "hype_score": int(row[show_title])
                        })
                
                # Use the environment variable for sleep between years
                time.sleep(SLEEP_BETWEEN_YEARS)
                
            except Exception as e:
                logging.warning(f"   Failed to fetch {period} for {show_title}: {e}")
                # FIX: Increased backoff time on failure to 30 seconds
                logging.info(f"   ⚠️ Rate Limit hit. Waiting 30s to clear rate limit...")
                time.sleep(30) 
        
        return all_data

def process_show(title: str, imdb_map: dict, fetcher: TrendsFetcher, redis_conn: redis.Redis) -> bool:
    """Process a single show."""
    meta = imdb_map.get(title.lower().strip(), {})
    brand_equity = meta.get("brand_equity", 0)
    rating = meta.get("imdb_rating", 0.0)

    history = fetcher.fetch_history(title)
    
    if history:
        pipe = redis_conn.pipeline()
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
        
        logging.info(f"   ✅ Pushed {len(history)} daily records for '{title}'")
        return True
    else:
        logging.warning(f"   ⚠️  No trend data found for '{title}'")
        return False

def main_loop():
    """Main producer loop."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    shows = load_shows_list()
    imdb_map = load_imdb_metadata()
    fetcher = TrendsFetcher()

    logging.info(f"🚀 Starting PHASE 1: Historical Backfill for {len(shows)} shows...")
    logging.info(f"📊 This will take a while. Each show requires ~5 API calls.")
    logging.info(f"🐌 **RATE LIMITING FIX**: Sleeping for {RATE_LIMIT_DELAY_SECONDS}s between shows.")
    
    success_count = 0
    failure_count = 0
    
    for i, title in enumerate(shows):
        logging.info(f"[{i+1}/{len(shows)}] Processing: '{title}'")
        
        success = process_show(title, imdb_map, fetcher, r)
        if success:
            success_count += 1
        else:
            failure_count += 1

        # --- FIX: Add substantial delay between shows ---
        logging.info(f"    ...sleeping for {RATE_LIMIT_DELAY_SECONDS}s to respect Google Trends rate limits.")
        time.sleep(RATE_LIMIT_DELAY_SECONDS)
        # ----------------------------------------------
    
    logging.info("=" * 60)
    logging.info("🎉 PHASE 1 COMPLETE!")
    logging.info(f"✅ Successfully processed: {success_count} shows")
    logging.info(f"⚠️  Failed to process: {failure_count} shows")
    logging.info(f"📦 Total records queued: {r.llen(REDIS_QUEUE)}")
    logging.info("=" * 60)
    logging.info("NEXT STEP: Switch to producer_streaming.py for Phase 2")
    
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main_loop()