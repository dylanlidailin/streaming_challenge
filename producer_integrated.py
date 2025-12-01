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
IMDB_RATINGS_FILE = os.getenv("IMDB_RATINGS_FILE", os.path.join(DATA_DIR, "title.ratings.tsv.gz"))
IMDB_BASICS_FILE = os.getenv("IMDB_BASICS_FILE", os.path.join(DATA_DIR, "title.basics.tsv.gz"))

# Rate Limit Delay (Safe setting for 1 request per show)
RATE_LIMIT_DELAY_SECONDS = 12

# --- CURATED TRACKING LIST (200 Shows) ---
# Identical to producer_streaming.py for consistency
TRACKED_SHOWS = [
    # --- TIER 1: GLOBAL HITS & CULTURAL PHENOMENA ---
    "Stranger Things", "Squid Game", "The Crown", "Bridgerton", "The Witcher",
    "Money Heist", "Dark", "Ozark", "Black Mirror", "The Queen's Gambit",
    "House of Cards", "Mindhunter", "Narcos", "Peaky Blinders", "Better Call Saul",
    "Breaking Bad", "Friends", "The Office", "Seinfeld", "Community",
    "Gilmore Girls", "Grey's Anatomy", "Supernatural", "NCIS", "Shameless",
    "Attack on Titan", "Demon Slayer: Kimetsu no Yaiba", "One Piece", "Death Note",
    "Hunter X Hunter (2011)", "Avatar: The Last Airbender", "Arcane", "Rick and Morty",
    "BoJack Horseman", "Big Mouth", "Sex Education", "Emily in Paris", "Lupin",
    "Shadow and Bone", "Sweet Tooth", "Cobra Kai", "Lucifer", "Manifest",
    "You", "Ginny & Georgia", "Firefly Lane", "Outer Banks", "Virgin River",
    "The Umbrella Academy", "Locke & Key",
    
    # --- TIER 2: CRITICALLY ACCLAIMED & POPULAR (2015-2021) ---
    "Maid", "Midnight Mass", "Clickbait", "Sex/Life", "Sweet Magnolias",
    "Never Have I Ever", "The Chair", "Halston", "The Serpent", "Behind Her Eyes",
    "Fate: The Winx Saga", "Bling Empire", "Bridgerton", "Tiny Pretty Things",
    "Dash & Lily", "The Haunting of Bly Manor", "Ratched", "Away", "Cursed",
    "Warrior Nun", "Space Force", "Dead to Me", "Hollywood", "Into the Night",
    "Unorthodox", "Tiger King", "Love Is Blind", "Ragnarok", "I Am Not Okay with This",
    "Locke & Key", "The Stranger", "Dracula", "Messiah", "V Wars",
    "The Politician", "Unbelievable", "The Spy", "Criminal: UK", "The I-Land",
    "Wu Assassins", "Another Life", "Chambers", "Black Summer", "The Society",
    "Bonding", "Special", "Quicksand", "Osmosis", "Turn Up Charlie",
    "After Life", "Russian Doll", "Kingdom", "Tydying Up with Marie Kondo",
    "Perfume", "Dogs of Berlin", "The Kominsky Method", "Bodyguard", "Maniac",
    
    # --- TIER 3: INTERNATIONAL & GENRE HITS ---
    "Alice in Borderland", "Sweet Home", "The Uncanny Counter", "Vincenzo",
    "Hometown Cha-Cha-Cha", "Itaewon Class", "Crash Landing on You", "Kingdom",
    "Elite", "Cable Girls", "High Seas", "The House of Flowers", "Control Z",
    "Dark Desire", "Who Killed Sara?", "3%", "The Rain", "Dark",
    "Barbarians", "How to Sell Drugs Online (Fast)", "Biohackers", "Ragnarok",
    "The Valhalla Murders", "Caliphate", "Fauda", "Shtisel", "Sacred Games",
    "Delhi Crime", "Mirzapur", "Bard of Blood", "Betaal", "Jamtara",
    
    # --- TIER 4: HIGH VOLUME / LONG RUNNING ---
    "The Great British Baking Show", "Paul Hollywood's Big Continental Road Trip",
    "Comedians in Cars Getting Coffee", "My Next Guest Needs No Introduction",
    "Patriot Act with Hasan Minhaj", "The Chef Show", "Nailed It!", "Sugar Rush",
    "Queer Eye", "Selling Sunset", "Too Hot to Handle", "The Circle",
    "Floor Is Lava", "Rhythm + Flow", "Dream Home Makeover", "Get Organized",
    "Tiny House Nation", "Million Dollar Beach House", "Interior Design Masters",
    "Amazing Interiors", "Instant Hotel", "Stay Here", "Restaurants on the Edge",
    "Ugly Delicious", "Salt Fat Acid Heat", "Chef's Table", "Street Food",
    "Taco Chronicles", "Flavorful Origins", "The Final Table", "Million Pound Menu",
    
    # --- TIER 5: LEGACY & LICENSED FAVORITES ---
    "Downton Abbey", "Outlander", "The Good Place", "Schitt's Creek",
    "Kim's Convenience", "Workin' Moms", "Call the Midwife", "Sherlock",
    "Merlin", "The IT Crowd", "Broadchurch", "Happy Valley", "Luther",
    "Bodyguard", "Collateral", "Giri / Haji", "Marcella", "The Fall",
    "Top Boy", "Skins", "The Inbetweeners", "Derry Girls", "Crashing",
    "The End of the F***ing World", "Atypical", "Everything Sucks!",
    "I Am Not Okay With This", "Daybreak", "Insatiable", "The Order"
]

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
        # Increased timeout for robustness
        self.pytrends = TrendReq(hl='en-US', tz=360, timeout=(30, 60)) 

    def fetch_history(self, show_title: str) -> List[Dict]:
        """
        Fetches 5 years of data in ONE request.
        Returns WEEKLY data, which allows for consistent scaling across the full period.
        """
        # We start from 2021 to ensure the total range is <= 5 years, 
        # which guarantees Google returns WEEKLY data points instead of Monthly.
        full_timeframe = '2021-01-01 2025-12-31'
        
        all_data = []
        try:
            logging.info(f"   ...fetching full history ({full_timeframe})")
            self.pytrends.build_payload([show_title], cat=0, timeframe=full_timeframe)
            data = self.pytrends.interest_over_time()
            
            if not data.empty and show_title in data.columns:
                for ts, row in data.iterrows():
                    all_data.append({
                        "timestamp": int(ts.timestamp()),
                        "hype_score": int(row[show_title])
                    })
            
            # Since we only make 1 request per show, a short sleep is sufficient
            time.sleep(2)
            
        except Exception as e:
            logging.warning(f"   Failed to fetch history for {show_title}: {e}")
            logging.info(f"   ⚠️ Rate Limit hit. Waiting 30s...")
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
        
        logging.info(f"   ✅ Pushed {len(history)} weekly records for '{title}'")
        return True
    else:
        logging.warning(f"   ⚠️  No trend data found for '{title}'")
        return False

def main_loop():
    """Main producer loop."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    
    # We now use the hardcoded list directly instead of reading CSV
    shows = TRACKED_SHOWS
    imdb_map = load_imdb_metadata()
    fetcher = TrendsFetcher()

    logging.info(f"🚀 Starting PHASE 1: Historical Backfill for {len(shows)} shows...")
    logging.info(f"📊 Using curated list of 200 shows.")
    logging.info(f"🐌 Sleeping for {RATE_LIMIT_DELAY_SECONDS}s between shows.")
    
    success_count = 0
    failure_count = 0
    
    for i, title in enumerate(shows):
        logging.info(f"[{i+1}/{len(shows)}] Processing: '{title}'")
        
        success = process_show(title, imdb_map, fetcher, r)
        if success:
            success_count += 1
        else:
            failure_count += 1
        
        logging.info(f"    ...sleeping for {RATE_LIMIT_DELAY_SECONDS}s to respect Google Trends rate limits.")
        time.sleep(RATE_LIMIT_DELAY_SECONDS) 
    
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