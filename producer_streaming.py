import os
import time
import json
import logging
from typing import List, Dict
from datetime import datetime

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

# Streaming Configuration
STREAM_INTERVAL = int(os.getenv("STREAM_INTERVAL", "60"))  # Poll every 60 seconds

# --- CURATED TRACKING LIST (200 Shows) ---
# Replaces dynamic loading to ensure high-value targets are tracked.
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
    """Load IMDb metadata for vote counts and ratings."""
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
            
            logging.info(f"Loaded metadata for {len(meta_map)} titles")
        except Exception as e:
            logging.error(f"IMDb Load Failed: {e}")
    else:
        logging.warning(f"IMDb files not found at {IMDB_RATINGS_FILE} - using defaults")
    
    return meta_map

class StreamingTrendsFetcher:
    """
    Real-time streaming producer that polls Google Trends continuously.
    """
    
    def __init__(self, tracked_shows: List[str]):
        # Increased timeout for robustness
        self.pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        self.tracked_shows = tracked_shows
        self.last_values = {}
        
    def get_realtime_trends(self) -> List[str]:
        try:
            trending = self.pytrends.realtime_trending_searches(pn='US')
            if not trending.empty and 'title' in trending.columns:
                return trending['title'].head(20).tolist()
        except Exception as e:
            logging.warning(f"Failed to fetch real-time trends: {e}")
        return []
    
    def fetch_batch_interests(self, shows: List[str]) -> Dict[str, float]:
        """
        Fetch current interest for multiple shows efficiently.
        """
        results = {}
        # Batch size of 5 matches Google's comparison limit
        batch_size = 5
        
        for i in range(0, len(shows), batch_size):
            batch = shows[i:i + batch_size]
            try:
                # ... (existing fetching code) ...
                
                # --- CHANGE 1: Increase Success Sleep ---
                # Old: time.sleep(2)
                # New: Sleep 10s between batches to stay under radar
                time.sleep(10) 
                
            except Exception as e:
                logging.warning(f"Failed to fetch batch {batch}: {e}")
                # Use cached values on failure
                for show in batch:
                    results[show] = self.last_values.get(show, 0.0)
                
                # --- CHANGE 2: Increase Failure Backoff ---
                # Old: time.sleep(10)
                # New: Wait 30-60s if we get hit with a 429
                logging.info("   ⚠️ Rate limit hit (429). Cooling down for 30s...")
                time.sleep(30) 
        
        return results

def stream_loop(imdb_map: dict, redis_conn: redis.Redis, shows: List[str]):
    fetcher = StreamingTrendsFetcher(shows)
    iteration = 0
    
    logging.info(f"🌊 Starting STREAMING mode for {len(shows)} shows - polling every {STREAM_INTERVAL}s")
    
    while True:
        iteration += 1
        logging.info(f"\n{'='*40}")
        logging.info(f"🔄 Stream Iteration #{iteration} - {datetime.now().strftime('%H:%M:%S')}")
        
        try:
            # 1. Check what is trending globally (context)
            realtime_trends = fetcher.get_realtime_trends()
            
            # 2. Update scores for OUR shows
            current_interests = fetcher.fetch_batch_interests(shows)
            
            # 3. Push to Redis
            pipe = redis_conn.pipeline()
            timestamp = int(time.time())
            
            for show_title, hype_score in current_interests.items():
                meta = imdb_map.get(show_title.lower().strip(), {})
                brand_equity = meta.get("brand_equity", 0)
                rating = meta.get("imdb_rating", 0.0)
                is_trending = show_title in realtime_trends
                
                record = {
                    "timestamp": timestamp,
                    "title": show_title,
                    "metrics": {
                        "hype_score": float(hype_score),
                        "brand_equity": brand_equity,
                        "imdb_rating": rating,
                        "is_trending": is_trending,
                        "cost_basis": 1,
                        "netflix_hours": 0
                    }
                }
                pipe.rpush(REDIS_QUEUE, json.dumps(record))
            
            pipe.execute()
            logging.info(f"✅ Updated {len(current_interests)} records")
            
        except Exception as e:
            logging.error(f"Stream iteration failed: {e}")
        
        time.sleep(STREAM_INTERVAL)

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    
    # 1. Load Data
    imdb_map = load_imdb_metadata()
    
    # 2. Start Streaming with Curated List
    stream_loop(imdb_map, r, TRACKED_SHOWS)

if __name__ == "__main__":
    main()