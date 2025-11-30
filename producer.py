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

# --- CONFIGURATION (PROJECT 1) ---
# Using Default Ports for Project 1
KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "stranger_things_data"
TRAKT_CLIENT_ID = os.getenv("TRAKT_CLIENT_ID")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

if not TRAKT_CLIENT_ID or not TMDB_API_KEY:
    raise ValueError("❌ Missing API Keys! Check your .env file.")

# Focused List for Deep Analysis
SHOW_NAMES = [
    "Stranger Things", 
    "Wednesday", 
    "The Last of Us", 
    "Bridgerton", 
    "Severance",
    "Squid Game",
    "The Crown",
    "Game of Thrones"
]

class StrangerThingsStreamer:
    def __init__(self):
        self.app = Application(
            broker_address=KAFKA_BROKER,
            consumer_group="project1-producer-turbo",
            producer_extra_config={"broker.address.family": "v4"}
        )
        self.topic = self.app.topic(name=TOPIC_NAME, value_serializer="json")
        self.resolved_shows = []
        self.netflix_df = None
        print(f"🚀 Project 1 Producer Connected to {KAFKA_BROKER}")

    def resolve_show_metadata(self, title):
        """Auto-finds TMDB ID, Trakt Slug, and IMDb ID."""
        search_url = f"https://api.themoviedb.org/3/search/tv?api_key={TMDB_API_KEY}&query={title}"
        try:
            res = requests.get(search_url).json()
            if res.get('results'):
                top = res['results'][0]
                tmdb_id = top['id']
                
                # Get External IDs (IMDb)
                ext_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
                ext_res = requests.get(ext_url).json()
                
                return {
                    "title": top['name'],
                    "tmdb_id": tmdb_id,
                    "imdb_id": ext_res.get('imdb_id'),
                    "slug": top['name'].lower().replace(' ', '-').replace(':', '').replace("'", "")
                }
        except: pass
        return None

    # --- DATA SOURCE 1: GOOGLE TRENDS (Smart Backfill) ---
    def backfill_google_trends(self, show_title):
        print(f"   🔎 Google Trends (Daily): {show_title}...")
        # Split into 6-month chunks to force Daily Data without hitting limits
        timeframes = [
            '2020-01-01 2020-06-30', '2020-07-01 2020-12-31',
            '2021-01-01 2021-06-30', '2021-07-01 2021-12-31',
            '2022-01-01 2022-06-30', '2022-07-01 2022-12-31',
            '2023-01-01 2023-06-30', '2023-07-01 2023-12-31',
            '2024-01-01 2024-06-30', '2024-07-01 2024-12-31'
        ]
        pytrends = TrendReq(hl='en-US', tz=360)
        
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
                                "hype_score": int(row[show_title]),
                                "active_watchers": 0, "total_plays": 0, 
                                "brand_equity": 0, "cost_basis": 0, "netflix_hours": 0
                            }
                        }
                        self.publish(key=show_title, data=event)
                time.sleep(2) # Polite sleep
            except: pass

    # --- DATA SOURCE 2: NETFLIX OFFICIAL (History) ---
    def prefetch_netflix_data(self):
        print("📥 Downloading Netflix Official Dataset...")
        try:
            url = "https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv"
            self.netflix_df = pd.read_csv(url, sep='\t')
            print(f"   ✅ Loaded {len(self.netflix_df)} rows.")
        except: self.netflix_df = pd.DataFrame()

    def backfill_netflix(self, show_title):
        if self.netflix_df is None or self.netflix_df.empty: return
        try:
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
                print(f"      ✅ Netflix History: +{len(show_data)} records")
        except: pass

    # --- DATA SOURCE 3: IMDb RATINGS (Massive Volume) ---
    def backfill_imdb_ratings(self):
        url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
        print(f"📥 Downloading IMDb Ratings (Compressed)...")
        imdb_map = {show['imdb_id']: show['title'] for show in self.resolved_shows if show.get('imdb_id')}
        
        try:
            df = pd.read_csv(url, sep='\t', compression='gzip')
            matched_df = df[df['tconst'].isin(imdb_map.keys())]
            
            if not matched_df.empty:
                for _, row in matched_df.iterrows():
                    show_title = imdb_map[row['tconst']]
                    event = {
                        "timestamp": time.time(),
                        "title": show_title,
                        "metrics": {
                            "active_watchers": 0, "total_plays": 0, "hype_score": 0,
                            "brand_equity": int(row['numVotes']), # <--- IMDb Votes
                            "cost_basis": 0, "netflix_hours": 0
                        }
                    }
                    self.publish(key=show_title, data=event)
                print(f"      ✅ IMDb: Matched {len(matched_df)} shows!")
        except: print("      ⚠️ IMDb Download Failed (or skipped)")

    # --- DATA SOURCE 4: LIVE API (Real-Time) ---
    def get_live_metrics(self, show):
        trakt_url = f"https://api.trakt.tv/shows/{show['slug']}/stats"
        headers = {'Content-Type': 'application/json', 'trakt-api-version': '2', 'trakt-api-key': TRAKT_CLIENT_ID}
        tmdb_url = f"https://api.themoviedb.org/3/tv/{show['tmdb_id']}?api_key={TMDB_API_KEY}"
        try:
            trakt = requests.get(trakt_url, headers=headers).json()
            tmdb = requests.get(tmdb_url).json()
            return {
                "title": show["title"],
                "active_watchers": trakt.get("watchers", 0),
                "total_plays": trakt.get("plays", 0),
                "hype_score": tmdb.get("popularity", 0),
                "brand_equity": tmdb.get("vote_count", 0),
                "cost_basis": tmdb.get("number_of_seasons", 1),
                "netflix_hours": 0
            }
        except: return None

    def publish(self, key, data):
        msg = self.topic.serialize(key=key, value=data)
        with self.app.get_producer() as producer:
            producer.produce(topic=self.topic.name, key=msg.key, value=msg.value)

    def run(self):
        print("📊 Starting Project 1 Turbo-Producer...")
        self.prefetch_netflix_data()
        
        # 1. Metadata
        print(f"\n--- 1. RESOLVING METADATA ---")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_show = {executor.submit(self.resolve_show_metadata, name): name for name in SHOW_NAMES}
            for future in as_completed(future_to_show):
                meta = future.result()
                if meta: self.resolved_shows.append(meta)
        print(f"   ✓ Resolved {len(self.resolved_shows)} shows.")

        # 2. Backfill
        print("--- STARTING BACKFILL ---")
        self.backfill_imdb_ratings() # Huge volume boost
        for show in self.resolved_shows:
            self.backfill_netflix(show['title'])
            self.backfill_google_trends(show['title'])
        print("--- BACKFILL COMPLETE ---\n")

        # 3. Live Stream
        print("🔴 Switching to Live Stream Mode...")
        while True:
            timestamp = time.time()
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(self.get_live_metrics, show) for show in self.resolved_shows]
                for future in as_completed(futures):
                    metrics = future.result()
                    if metrics:
                        event = {"timestamp": timestamp, "title": metrics['title'], "metrics": metrics}
                        self.publish(key=metrics['title'], data=event)
            
            print(f"   ✓ Updated {len(self.resolved_shows)} shows.")
            time.sleep(60)

if __name__ == "__main__":
    streamer = StrangerThingsStreamer()
    streamer.run()