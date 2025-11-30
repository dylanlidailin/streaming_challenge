import json
import os
import redis
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

TOPIC = "final_project_stream"
KAFKA_BROKER = "localhost:19092"

app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="final-consumer-v1", 
    auto_offset_reset="earliest",
    producer_extra_config={"broker.address.family": "v4"}
)

r = redis.Redis(host='localhost', port=6379, db=0)
r.delete("franchise_data") # Clear old data on start

print(f"👷 Consumer Started... Listening to {TOPIC}")

total_processed = 0
pipe = r.pipeline()

with app.get_consumer() as consumer:
    consumer.subscribe([TOPIC])
    while True:
        msg = consumer.poll(1)
        if msg is None or msg.error(): continue

        data = json.loads(msg.value())
        m = data.get('metrics', {})
        
        record = {
            "timestamp": data.get('timestamp'), "title": data.get('title'),
            "active_watchers": m.get('active_watchers', 0), "total_plays": m.get('total_plays', 0),
            "hype_score": m.get('hype_score', 0.0), "brand_equity": m.get('brand_equity', 0),
            "cost_basis": m.get('cost_basis', 1), "netflix_hours": m.get('netflix_hours', 0)
        }
        pipe.rpush("franchise_data", json.dumps(record))
        
        total_processed += 1
        if total_processed % 2000 == 0:
            pipe.execute()
            print(f"   ⚡ Saved {total_processed} records...")
            consumer.store_offsets(msg)