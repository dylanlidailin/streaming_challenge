"""
app.py - Streamlit dashboard

- Reads processed records from Redis list 'franchise_data' (pushed by consumer).
- Builds a simple dashboard:
    - top titles by average hype_score
    - time-series of hype for selected titles (if timestamps present)
    - simple linear regression of hype -> netflix_hours if enough data.
- For production, connect to a proper DB. Here Redis list is quick for demo.

Run via `streamlit run app.py` or via docker-compose service.

Note: Streamlit caches data for a short TTL to avoid hammering Redis.
"""

import os
import json
import time
from datetime import datetime
import pandas as pd
import streamlit as st
import redis
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DATA_LIST = os.getenv("REDIS_DATA_LIST", "franchise_data")
MAX_READ = int(os.getenv("MAX_READ", "50000"))  # how many recent records to fetch (cap)

st.set_page_config(layout="wide", page_title="Streaming Wars HQ")
st.title("🎬 Franchise Valuation Engine")

@st.cache_data(ttl=10)
def load_from_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    # Use LRANGE to read last MAX_READ items
    try:
        total = r.llen(REDIS_DATA_LIST)
        start = max(0, total - MAX_READ)
        raw = r.lrange(REDIS_DATA_LIST, start, total)
        records = [json.loads(x) for x in raw]
        df = pd.DataFrame.from_records(records)
        if df.empty:
            return df
        # normalize timestamp and title
        if 'timestamp' in df.columns:
            df['time'] = pd.to_datetime(df['timestamp'], unit='s')
        else:
            df['time'] = pd.Timestamp.now()
        return df
    except Exception as e:
        st.error(f"Failed to read from Redis: {e}")
        return pd.DataFrame()

df = load_from_redis()

if df.empty:
    st.info("No data yet. Start the producer & consumer or check Redis connection.")
else:
    st.sidebar.header("Controls")
    titles = sorted(df['title'].unique().tolist())
    selected = st.sidebar.multiselect("Select titles for time series", titles[:20], default=titles[:5])
    top_n = st.sidebar.slider("Top N titles by avg hype", 5, 50, 10)

    st.subheader(f"Top {top_n} titles by average hype score (last {len(df)} records)")
    agg = df.groupby('title').agg({'hype_score':'mean', 'netflix_hours':'mean', 'engagement_score':'mean'}).reset_index()
    top = agg.sort_values('hype_score', ascending=False).head(top_n)
    st.table(top)

    if selected:
        st.subheader("Hype time series")
        sel_df = df[df['title'].isin(selected)].copy()
        # resample to weekly mean for clarity
        sel_df.set_index('time', inplace=True)
        plot_df = sel_df.groupby(['title']).resample('7D').mean().reset_index()
        # pivot for streamlit line_chart
        pivot = plot_df.pivot(index='time', columns='title', values='hype_score').fillna(0)
        st.line_chart(pivot)

    # small ML: linear regression hype -> netflix_hours (if enough points)
    st.subheader("Simple model: hype_score -> netflix_hours")
    ml_df = df[['title','time','hype_score','netflix_hours']].dropna()
    ml_df = ml_df.groupby(['title','time']).agg({'hype_score':'mean','netflix_hours':'max'}).reset_index()
    ml_df = ml_df[(ml_df['hype_score'] > 0) & (ml_df['netflix_hours'] > 0)]
    if len(ml_df) > 30:
        X = ml_df[['hype_score']].values
        y = ml_df['netflix_hours'].values
        model = LinearRegression().fit(X, y)
        preds = model.predict(X)
        r2 = r2_score(y, preds)
        st.write(f"Linear regression R^2 = {r2:.3f}; coef = {model.coef_[0]:.4f}, intercept = {model.intercept_:.2f}")
        # show scatter
        chart_df = pd.DataFrame({'hype_score': X.flatten(), 'actual_hours': y, 'predicted_hours': preds})
        st.altair_chart(
            ( (st.altair_chart)  if False else None)  # placeholder to avoid linting; real plotting below
        )
        st.write(chart_df.head(50))
    else:
        st.info("Not enough samples for ML (need >30 rows).")

    st.sidebar.write("Records loaded:", len(df))
    st.sidebar.write("Unique titles:", df['title'].nunique())