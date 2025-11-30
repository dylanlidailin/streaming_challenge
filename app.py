import streamlit as st
import pandas as pd
import redis
import json
import time
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

st.set_page_config(page_title="Streaming Wars HQ", layout="wide")
st.title("🎬 Franchise Valuation Engine (Mega Edition)")

# CONFIG
REDIS_HOST = "redis" # Use 'localhost' if running locally outside docker
REDIS_PORT = 6379

@st.cache_data(ttl=5)
def load_data():
    try:
        # Connect to Redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        
        # Read processed data from the consumer's output list
        # Note: Ensure consumer.py is writing to 'franchise_data'
        raw_data = r.lrange("franchise_data", 0, -1)
        
        if not raw_data: return pd.DataFrame()
        
        records = [json.loads(x) for x in raw_data]
        df = pd.DataFrame(records)
        df['time'] = pd.to_datetime(df['timestamp'], unit='s')
        return df
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    # --- METRICS ---
    st.caption(f"💾 Database Volume (Redis): {len(df):,} records processed.")
    
    all_shows = sorted(df['title'].unique())
    selected_shows = st.multiselect(
        "Select Franchises:",
        options=all_shows,
        default=all_shows[:3] if len(all_shows) > 3 else all_shows
    )
    
    if not selected_shows: st.stop()
    
    # Filter Data
    filtered_df = df[df['title'].isin(selected_shows)]
    
    # A. History (Hype)
    history_df = filtered_df.sort_values('time')
    
    # B. ROI Snapshot (Latest value)
    latest_df = filtered_df.sort_values('time').groupby('title').tail(1)
    
    # C. Viral Impact (Peak / Avg)
    pbr_data = []
    for show in selected_shows:
        show_data = filtered_df[filtered_df['title'] == show]
        peak = show_data['hype_score'].max()
        avg = show_data['hype_score'].mean()
        if avg > 0: pbr_data.append({'title': show, 'pbr_score': peak / avg})
    viral_df = pd.DataFrame(pbr_data)

    # --- VISUALIZATIONS ---
    
    # Row 1: Search History
    st.subheader(f"📅 5-Year Search Volume History")
    st.line_chart(history_df, x='time', y='hype_score', color='title')

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💥 Viral Impact (Peak-to-Baseline)")
        if not viral_df.empty: 
            st.bar_chart(viral_df.set_index('title')['pbr_score'])
            
    with col2:
        st.subheader("💰 Brand Equity (Total IMDb Votes)")
        # Using 'brand_equity' (Votes) directly as the ROI metric
        if not latest_df.empty:
            st.bar_chart(latest_df.set_index('title')['brand_equity'])

    # Row 3: ML Analyst
    st.markdown("---")
    st.subheader(f"🤖 AI Analyst: Predicting Impact")
    
    # Prepare Data: Daily Hype vs Votes (Proxy for Success)
    # We aggregate by show to see if Average Hype correlates with Total Votes
    ml_df = df.groupby('title').agg({
        'hype_score': 'mean',
        'brand_equity': 'max' # Total Votes
    }).reset_index()
    
    if len(ml_df) > 5:
        X = ml_df[['hype_score']]
        y = ml_df['brand_equity']
        
        model = LinearRegression()
        model.fit(X, y)
        predictions = model.predict(X)
        r2 = r2_score(y, predictions)
        
        m1, m2 = st.columns(2)
        m1.metric("Model Accuracy (R²)", f"{r2:.2f}")
        m2.metric("Hype Multiplier", f"{int(model.coef_[0]):,}", help="Votes gained per 1 point of Avg Hype")
        
        chart_data = pd.DataFrame({
            'Avg Hype Score': ml_df['hype_score'],
            'Total Votes (Actual)': y,
            'Total Votes (Predicted)': predictions,
            'Show': ml_df['title']
        })
        
        st.scatter_chart(
            chart_data,
            x='Avg Hype Score',
            y=['Total Votes (Actual)', 'Total Votes (Predicted)'],
            color=['#ffaa00', '#0000ff']
        )
    else:
        st.info("Not enough data for ML yet. Let the producer run!")

else:
    st.info("Waiting for data... (Producer is backfilling 200+ shows)")
    
time.sleep(5)
st.rerun()