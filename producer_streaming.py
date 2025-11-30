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
REALTIME_LOOKBACK = 7  # Days to look back for "recent" trends
TRACKED_SHOWS = [
    # Netflix Originals & Popular Series
    "Stranger Things", "The Crown", "Wednesday", "Squid Game", "Bridgerton",
    "The Witcher", "Money Heist", "Dark", "Ozark", "Black Mirror",
    # HBO/Max Hits
    "House of the Dragon", "The Last of Us", "Succession", "Euphoria", "White Lotus",
    # Amazon Prime
    "The Boys", "Rings of Power", "Jack Ryan", "Reacher", "The Marvelous Mrs. Maisel",
    # Disney+
    "The Mandalorian", "Loki", "WandaVision", "Andor", "Ahsoka",
    # Classic Must-Track
    "Breaking Bad", "Game of Thrones", "The Office", "Friends", "The Sopranos",
    # Recent Buzz Shows
    "The Bear", "Wednesday", "You", "Dahmer", "1899",
    # Anime
    "Attack on Titan", "Demon Slayer", "My Hero Academia", "One Piece", "Death Note",
    # Supplemental
    "The Walking Dead", "Sherlock", "True Detective", "Firefly", "Band of Brothers",
    "Chernobyl", "Dexter", "House of Cards", "Prison Break", "Lost", "Vikings",
    "The Simpsons", "South Park", "Twin Peaks", "House"
]

def load_imdb_metadata() -> dict:
    """
    Load IMDb metadata for vote counts and ratings.
    Returns dict mapping lowercase title to metadata.
    """
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
        logging.warning("IMDb files not found - using defaults")
    
    return meta_map

class StreamingTrendsFetcher:
    """
    Real-time streaming producer that polls Google Trends continuously.
    Combines real-time trending data with tracked show monitoring.
    """
    
    def __init__(self, tracked_shows: List[str]):
        self.pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        self.tracked_shows = tracked_shows
        self.last_values = {}  # Cache last known values for each show
        
    def get_realtime_trends(self) -> List[str]:
        """
        Fetch real-time trending searches from Google Trends.
        Returns list of currently trending terms.
        """
        try:
            trending = self.pytrends.realtime_trending_searches(pn='US')
            if not trending.empty and 'title' in trending.columns:
                trends = trending['title'].head(20).tolist()
                logging.info(f"Found {len(trends)} real-time trending searches")
                return trends
        except Exception as e:
            logging.warning(f"Failed to fetch real-time trends: {e}")
        return []
    
    def get_current_interest(self, show_title: str) -> float:
        """
        Get current search interest for a specific show.
        Uses last 7 days to get near-realtime data.
        """
        try:
            # Use "now 7-d" timeframe for near-realtime data
            self.pytrends.build_payload([show_title], cat=0, timeframe='now 7-d')
            data = self.pytrends.interest_over_time()
            
            if not data.empty and show_title in data.columns:
                # Get most recent value
                latest_value = float(data[show_title].iloc[-1])
                self.last_values[show_title] = latest_value
                return latest_value
            else:
                # Return cached value if available
                return self.last_values.get(show_title, 0.0)
        except Exception as e:
            logging.warning(f"Failed to fetch interest for {show_title}: {e}")
            return self.last_values.get(show_title, 0.0)
    
    def fetch_batch_interests(self, shows: List[str]) -> Dict[str, float]:
        """
        Fetch current interest for multiple shows efficiently.
        Returns dict mapping show title to current hype score.
        """
        results = {}
        # Process in small batches to avoid rate limits
        batch_size = 5
        
        for i in range(0, len(shows), batch_size):
            batch = shows[i:i + batch_size]
            try:
                # Fetch interest for batch
                self.pytrends.build_payload(batch, cat=0, timeframe='now 7-d')
                data = self.pytrends.interest_over_time()
                
                if not data.empty:
                    for show in batch:
                        if show in data.columns:
                            latest = float(data[show].iloc[-1])
                            results[show] = latest
                            self.last_values[show] = latest
                        else:
                            results[show] = self.last_values.get(show, 0.0)
                
                # Rate limiting
                time.sleep(2)
            except Exception as e:
                logging.warning(f"Failed to fetch batch {i}-{i+batch_size}: {e}")
                # Use cached values
                for show in batch:
                    results[show] = self.last_values.get(show, 0.0)
                time.sleep(5)
        
        return results

def stream_loop(imdb_map: dict, redis_conn: redis.Redis):
    """
    Main streaming loop - continuously polls Google Trends and pushes updates.
    """
    fetcher = StreamingTrendsFetcher(TRACKED_SHOWS)
    iteration = 0
    
    logging.info(f"🌊 Starting STREAMING mode - polling every {STREAM_INTERVAL}s")
    logging.info(f"📺 Tracking {len(TRACKED_SHOWS)} shows")
    
    while True:
        iteration += 1
        logging.info(f"\n{'='*60}")
        logging.info(f"🔄 Stream Iteration #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"{'='*60}")
        
        try:
            # 1. Get real-time trending topics
            realtime_trends = fetcher.get_realtime_trends()
            if realtime_trends:
                logging.info(f"📈 Top Trending Now: {', '.join(realtime_trends[:5])}")
            
            # 2. Fetch current interest for all tracked shows
            logging.info(f"🔍 Fetching interest for {len(TRACKED_SHOWS)} tracked shows...")
            current_interests = fetcher.fetch_batch_interests(TRACKED_SHOWS)
            
            # 3. Push updates to Redis queue
            pipe = redis_conn.pipeline()
            records_pushed = 0
            timestamp = int(time.time())
            
            for show_title, hype_score in current_interests.items():
                # Get metadata
                meta = imdb_map.get(show_title.lower().strip(), {})
                brand_equity = meta.get("brand_equity", 0)
                rating = meta.get("imdb_rating", 0.0)
                
                # Check if show is currently trending
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
                records_pushed += 1
                
                # Log high-interest shows
                if hype_score > 50:
                    status = "🔥 TRENDING" if is_trending else "📊 HIGH"
                    logging.info(f"   {status}: {show_title} = {hype_score:.1f}")
            
            pipe.execute()
            
            logging.info(f"✅ Pushed {records_pushed} records to queue")
            logging.info(f"💾 Queue depth: {redis_conn.llen(REDIS_QUEUE)}")
            
        except Exception as e:
            logging.error(f"Stream iteration failed: {e}")
        
        # Wait before next poll
        logging.info(f"⏳ Sleeping {STREAM_INTERVAL}s until next poll...")
        time.sleep(STREAM_INTERVAL)

def backfill_historical(imdb_map: dict, redis_conn: redis.Redis):
    """
    Optional: Backfill historical data for context before starting stream.
    Fetches last 90 days of daily data for each tracked show.
    """
    logging.info("🔙 Starting historical backfill (last 90 days)...")
    fetcher = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
    
    for i, show in enumerate(TRACKED_SHOWS):
        logging.info(f"[{i+1}/{len(TRACKED_SHOWS)}] Backfilling: {show}")
        
        try:
            # Fetch last 90 days
            fetcher.build_payload([show], cat=0, timeframe='today 3-m')
            data = fetcher.interest_over_time()
            
            if not data.empty and show in data.columns:
                meta = imdb_map.get(show.lower().strip(), {})
                brand_equity = meta.get("brand_equity", 0)
                rating = meta.get("imdb_rating", 0.0)
                
                pipe = redis_conn.pipeline()
                for ts, row in data.iterrows():
                    record = {
                        "timestamp": int(ts.timestamp()),
                        "title": show,
                        "metrics": {
                            "hype_score": float(row[show]),
                            "brand_equity": brand_equity,
                            "imdb_rating": rating,
                            "is_trending": False,
                            "cost_basis": 1,
                            "netflix_hours": 0
                        }
                    }
                    pipe.rpush(REDIS_QUEUE, json.dumps(record))
                pipe.execute()
                
                logging.info(f"   ✅ Backfilled {len(data)} days")
            
            time.sleep(2)  # Rate limiting
            
        except Exception as e:
            logging.warning(f"   ⚠️ Backfill failed for {show}: {e}")
            time.sleep(5)
    
    logging.info("✅ Historical backfill complete!")

def main():
    """
    Main entry point - runs backfill then starts streaming loop.
    """
    # Connect to Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    
    # Load IMDb metadata
    imdb_map = load_imdb_metadata()
    
    # Check if we should backfill
    backfill_mode = os.getenv("BACKFILL_HISTORICAL", "true").lower() == "true"
    
    if backfill_mode:
        backfill_historical(imdb_map, r)
    
    # Start streaming
    stream_loop(imdb_map, r)

if __name__ == "__main__":
    main()