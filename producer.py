import os
import time
import json
import requests
import pandas as pd
from quixstreams import Application
from dotenv import load_dotenv
from pytrends.request import TrendReq
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# --- CONFIGURATION ---
TRAKT_CLIENT_ID = os.getenv("TRAKT_CLIENT_ID")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")

if not TRAKT_CLIENT_ID or not TMDB_API_KEY:
    raise ValueError("❌ Missing API Keys! Check your .env file.")

# THE MEGA LIST (50+ Shows)
SHOW_NAMES = [
    "Stranger Things", "Wednesday", "The Last of Us", "Game of Thrones", "Breaking Bad",
    "Better Call Saul", "The Crown", "Bridgerton", "Squid Game", "Yellowstone",
    "The Mandalorian", "Andor", "Obi-Wan Kenobi", "The Witcher", "House of the Dragon",
    "The Rings of Power", "Westworld", "Black Mirror", "Severance", "Silo",
    "Fallout", "3 Body Problem", "Dark", "Arcane", "Cyberpunk: Edgerunners",
    "Succession", "The Bear", "Euphoria", "The White Lotus", "Peaky Blinders",
    "The Sopranos", "The Wire", "Mad Men", "Narcos", "Money Heist",
    "Mindhunter", "Reacher", "The Night Agent", "Ozark", "Shameless",
    "The Office", "Parks and Recreation", "Ted Lasso", "Friends", "Seinfeld",
    "Brooklyn Nine-Nine", "It's Always Sunny in Philadelphia", "Rick and Morty",
    "BoJack Horseman", "Fleabag", "Barry", "The Good Place", "Community"
]

class FranchiseStreamer:
    def __init__(self):
        self.app = Application(
            broker_address=KAFKA_BROKER,
            consumer_group="franchise-producer-v_final",
            producer_extra_config={"broker.address.family": "v4"}
        )
        self.topic = self.app.topic(name="franchise_data_stream", value_serializer="json")
        self.resolved_shows = [] 
        self.netflix_df = None 
        print(f"🚀 Connected to Redpanda at {KAFKA_BROKER}")

    def prefetch_netflix_data(self):
        url = "https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv"
        print("📥 Downloading Netflix Global Dataset...")
        try:
            self.netflix_df = pd.read_csv(url, sep='\t')
            print(f"   ✅ Downloaded {len(self.netflix_df)} rows.")
        except Exception as e:
            print(f"   ❌ Failed to download Netflix data: {e}")
            self.netflix_df = pd.DataFrame()

    def resolve_single_show(self, title):
        search_url = f"https://api.themoviedb.org/3/search/tv?api_key={TMDB_API_KEY}&query={title}"
        try:
            res = requests.get(search_url).json()
            if res.get('results'):
                top = res['results'][0]
                return {
                    "title": top['name'],
                    "tmdb_id": top['id'],
                    "slug": top['name'].lower().replace(' ', '-').replace(':', '').replace("'", "")
                }
        except: pass
        return None

    def backfill_google_trends(self, show_title):
        """Fetches daily data year-by-year to maximize volume."""
        print(f"   🔎 Trends: {show_title}...")
        timeframes = [
            '2020-01-01 2020-12-31', '2021-01-01 2021-12-31', 
            '2022-01-01 2022-12-31', '2023-01-01 2023-12-31', 
            '2024-01-01 2024-12-31'
        ]
        pytrends = TrendReq(hl='en-US', tz=360)
        total_daily = 0
        
        for period in timeframes:
            try:
                pytrends.build_payload([show_title], cat=0, timeframe=period)
                data = pytrends.interest_over_time()
                if not data.empty:
                    for index, row in data.iterrows():
                        event = {
                            "timestamp": index.timestamp(),
                            "title": show_title,
                            "metrics": {
                                "hype_score": int(row[show_title]), "active_watchers": 0,
                                "total_plays": 0, "brand_equity": 0, "cost_basis": 0, "netflix_hours": 0
                            }
                        }
                        self.publish(key=show_title, data=event)
                    total_daily += len(data)
                time.sleep(1.5)
            except: pass
            
        if total_daily > 0:
            print(f"      ✅ {show_title}: +{total_daily} daily records")

    def process_netflix_history(self, show_title):
        if self.netflix_df.empty: return
        show_data = self.netflix_df[self.netflix_df['show_title'].str.contains(show_title, case=False, na=False)]
        if not show_data.empty:
            for _, row in show_data.iterrows():
                event = {
                    "timestamp": pd.to_datetime(row['week']).timestamp(),
                    "title": show_title,
                    "metrics": {
                        "active_watchers": 0, "total_plays": 0, "hype_score": 0,
                        "brand_equity": 0, "cost_basis": 0,
                        "netflix_hours": int(row['weekly_hours_viewed'])
                    }
                }
                self.publish(key=show_title, data=event)
            print(f"      ✅ {show_title}: +{len(show_data)} Netflix records")

    def get_live_metrics(self, show):
        trakt_url = f"https://api.trakt.tv/shows/{show['slug']}/stats"
        trakt_headers = {'Content-Type': 'application/json', 'trakt-api-version': '2', 'trakt-api-key': TRAKT_CLIENT_ID}
        tmdb_url = f"https://api.themoviedb.org/3/tv/{show['tmdb_id']}?api_key={TMDB_API_KEY}"
        try:
            trakt = requests.get(trakt_url, headers=trakt_headers).json()
            tmdb = requests.get(tmdb_url).json()
            return {
                "title": show["title"],
                "active_watchers": trakt.get("watchers", 0),
                "total_plays": trakt.get("plays", 0),
                "hype_score": tmdb.get("popularity", 0),
                "brand_equity": tmdb.get("vote_count", 0),
                "cost_basis": tmdb.get("number_of_seasons", 1)
            }
        except: return None

    def publish(self, key, data):
        msg = self.topic.serialize(key=key, value=data)
        with self.app.get_producer() as producer:
            producer.produce(topic=self.topic.name, key=msg.key, value=msg.value)

    def run(self):
        print("📊 Starting ULTIMATE-STREAMER...")
        self.prefetch_netflix_data()

        print(f"\n--- 1. RESOLVING METADATA (Parallel) ---")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_show = {executor.submit(self.resolve_single_show, name): name for name in SHOW_NAMES}
            for future in as_completed(future_to_show):
                meta = future.result()
                if meta:
                    self.resolved_shows.append(meta)
                    print(f"   Found: {meta['title']}")

        print(f"\n--- 2. STARTING BACKFILL ---")
        for show in self.resolved_shows:
            self.process_netflix_history(show['title']) 
            self.backfill_google_trends(show['title']) 
        print("--- BACKFILL COMPLETE ---\n")

        print("🔴 Switching to Live Stream Mode...")
        while True:
            timestamp = time.time()
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(self.get_live_metrics, show) for show in self.resolved_shows]
                for future in as_completed(futures):
                    metrics = future.result()
                    if metrics:
                        event = {
                            "timestamp": timestamp, "title": metrics['title'],
                            "metrics": {
                                "active_watchers": metrics['active_watchers'], "total_plays": metrics['total_plays'],
                                "hype_score": metrics['hype_score'], "brand_equity": metrics['brand_equity'],
                                "cost_basis": metrics['cost_basis'], "netflix_hours": 0
                            }
                        }
                        slug = next(s['slug'] for s in self.resolved_shows if s['title'] == metrics['title'])
                        self.publish(key=slug, data=event)
            print(f"   ✓ Updated {len(self.resolved_shows)} shows.")
            time.sleep(60)

if __name__ == "__main__":
    streamer = FranchiseStreamer()
    streamer.run()