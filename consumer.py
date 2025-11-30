"""
consumer.py - Updated to include brand_equity
"""

import os
import json
import time
import logging
from typing import Dict, Any, List
from dotenv import load_dotenv
import redis

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "franchise_queue")
REDIS_DATA_LIST = os.getenv("REDIS_DATA_LIST", "franchise_data")
POP_BATCH = int(os.getenv("POP_BATCH", "200"))
SNAPSHOT_EVERY = int(os.getenv("SNAPSHOT_EVERY", "5000")) 

DATA_SNAPSHOT_FILE = os.getenv("SNAPSHOT_FILE", "/data/franchise_data_snapshot.ndjson")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def safe_parse(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except Exception as e:
        logging.warning("Failed to parse JSON from raw: %s", e)
        return {}

def enrich_record(event: Dict[str, Any]) -> Dict[str, Any]:
    # Basic normalization and enrichment
    ts = event.get("timestamp") or int(time.time())
    title = event.get("title") or (event.get("metrics", {}).get("title")) or "Unknown"
    metrics = event.get("metrics", {})
    
    # Ensure numeric types and defaults
    hype = float(metrics.get("hype_score") or 0.0)
    
    # --- FIX START: Extract brand_equity ---
    brand_equity = int(metrics.get("brand_equity") or 0)
    # --- FIX END ---

    imdb_rating = metrics.get("imdb_rating")
    try:
        imdb_rating = float(imdb_rating) if imdb_rating is not None else None
    except Exception:
        imdb_rating = None
        
    netflix_hours = float(metrics.get("netflix_hours") or 0.0)
    
    # Compute engagement score
    engagement = hype * ((imdb_rating or 5.0) / 10.0)
    
    out = {
        "timestamp": int(ts),
        "title": title,
        "hype_score": round(hype, 3),
        "brand_equity": brand_equity,  # <--- Field added here
        "imdb_rating": imdb_rating,
        "netflix_hours": netflix_hours,
        "engagement_score": round(engagement, 4)
    }
    return out

def pop_batch(n: int) -> List[str]:
    items = []
    for _ in range(n):
        raw = r.lpop(REDIS_QUEUE)
        if raw is None:
            break
        items.append(raw)
    return items

def main_loop():
    total_processed = 0
    snapshot_accum = []
    logging.info("Starting consumer loop - reading from %s", REDIS_QUEUE)
    while True:
        items = pop_batch(POP_BATCH)
        if not items:
            time.sleep(1)
            continue
        pipe = r.pipeline()
        for raw in items:
            ev = safe_parse(raw)
            if not ev:
                continue
            rec = enrich_record(ev)
            pipe.rpush(REDIS_DATA_LIST, json.dumps(rec))
            snapshot_accum.append(rec)
            total_processed += 1
            if total_processed % 1000 == 0:
                logging.info("Total processed: %d", total_processed)
        pipe.execute()

        # persist snapshot periodically
        if len(snapshot_accum) >= SNAPSHOT_EVERY:
            try:
                os.makedirs(os.path.dirname(DATA_SNAPSHOT_FILE), exist_ok=True)
                with open(DATA_SNAPSHOT_FILE, "a", encoding="utf-8") as fh:
                    for rec in snapshot_accum:
                        fh.write(json.dumps(rec) + "\n")
                logging.info("Wrote %d records snapshot to disk (%s)", len(snapshot_accum), DATA_SNAPSHOT_FILE)
            except Exception as e:
                logging.exception("Failed to write snapshot: %s", e)
            snapshot_accum = []

if __name__ == "__main__":
    main_loop()