import streamlit as st
import pandas as pd
import redis
import json
import time
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

st.set_page_config(page_title="Streaming Wars HQ", layout="wide")
st.title("🎬 Franchise Valuation Engine")

@st.cache_data(ttl=5)
def load_data():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        raw_data = r.lrange("franchise_data", 0, -1)
        if not raw_data: return pd.DataFrame()
        records = [json.loads(x) for x in raw_data]
        df = pd.DataFrame(records)
        df['time'] = pd.to_datetime(df['timestamp'], unit='s')
        return df
    except: return pd.DataFrame()

df = load_data()

if not df.empty:
    st.caption(f"💾 Database Volume: {len(df):,} records processed.")

    all_shows = sorted(df['title'].unique())
    selected = st.multiselect("Select Franchises:", all_shows, default=all_shows[:3] if all_shows else [])
    
    if selected:
        filtered = df[df['title'].isin(selected)]
        
        # 1. History
        history = filtered[(filtered['active_watchers'] == 0) & (filtered['hype_score'] > 0)].sort_values('time')
        st.subheader("📅 5-Year Search History")
        st.line_chart(history, x='time', y='hype_score', color='title')

        # 2. ROI
        latest = filtered.sort_values('time').groupby('title').tail(1)
        st.subheader("💰 ROI Efficiency")
        latest['roi'] = latest['brand_equity'] / latest['cost_basis']
        st.bar_chart(latest.set_index('title')['roi'])

        # 3. ML
        st.markdown("---")
        st.subheader("🤖 AI Analyst")
        ml_df = filtered.set_index('time').groupby(['title', pd.Grouper(freq='W')]).agg({'hype_score': 'mean', 'netflix_hours': 'max'}).reset_index()
        ml_df = ml_df[(ml_df['hype_score'] > 0) & (ml_df['netflix_hours'] > 0)]

        if len(ml_df) > 5:
            X, y = ml_df[['hype_score']], ml_df['netflix_hours']
            model = LinearRegression().fit(X, y)
            preds = model.predict(X)
            st.line_chart(pd.DataFrame({'Week': ml_df['time'], 'Actual': y, 'Predicted': preds, 'Show': ml_df['title']}), x='Week', y=['Actual', 'Predicted'], color=['#ffaa00', '#0000ff'])
        else:
            st.info("Not enough history for ML.")
else:
    st.info("Waiting for data...")

time.sleep(5)
st.rerun()