import json
import os
import redis
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

# 1. Setup Quixstreams
app = Application(
    broker_address=os.getenv("KAFKA_BROKER", "localhost:19092"),
    consumer_group="franchise-consumer-final", 
    auto_offset_reset="earliest",
    producer_extra_config={"broker.address.family": "v4"}
)

# 2. Setup Redis
r = redis.Redis(host='localhost', port=6379, db=0)

print("👷 Consumer Started... Writing to Redis List 'franchise_data'...")

# Flush DB on start to prevent duplicates from previous runs
r.delete("franchise_data") 

total_processed = 0
BATCH_SIZE = 2000

with app.get_consumer() as consumer:
    consumer.subscribe(["franchise_data_stream"])
    
    pipe = r.pipeline()

    while True:
        msg = consumer.poll(1)
        if msg is None or msg.error(): continue

        data = json.loads(msg.value())
        m = data.get('metrics', {})
        
        record = {
            "timestamp": data.get('timestamp'),
            "title": data.get('title'),
            "active_watchers": m.get('active_watchers', 0),
            "total_plays": m.get('total_plays', 0),
            "hype_score": m.get('hype_score', 0.0),
            "brand_equity": m.get('brand_equity', 0),
            "cost_basis": m.get('cost_basis', 1),
            "netflix_hours": m.get('netflix_hours', 0)
        }

        pipe.rpush("franchise_data", json.dumps(record))
        
        total_processed += 1
        if total_processed % BATCH_SIZE == 0:
            pipe.execute()
            print(f"   ⚡ Flushed {BATCH_SIZE} records (Total: {total_processed})")
            consumer.store_offsets(msg)