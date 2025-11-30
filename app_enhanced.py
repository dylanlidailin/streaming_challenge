import streamlit as st
import pandas as pd
import redis
import json
import time
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from datetime import datetime, timedelta

st.set_page_config(page_title="Streaming Wars HQ", layout="wide")
st.title("🎬 Franchise Valuation Engine (Real-Time Edition)")

# CONFIG
REDIS_HOST = "redis"
REDIS_PORT = 6379

@st.cache_data(ttl=5)
def load_data():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        raw_data = r.lrange("franchise_data", 0, -1)
        
        if not raw_data: return pd.DataFrame()
        
        records = [json.loads(x) for x in raw_data]
        df = pd.DataFrame(records)
        df['time'] = pd.to_datetime(df['timestamp'], unit='s')
        return df
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return pd.DataFrame()

def classify_peak_timing(show_df):
    """
    Classifies a show as 'Early Peaker' or 'Late Peaker' based on when
    maximum hype occurred in the time series.
    """
    if len(show_df) < 10:
        return "Insufficient Data"
    
    # Find the index of maximum hype
    peak_idx = show_df['hype_score'].idxmax()
    peak_position = show_df.index.get_loc(peak_idx)
    
    # Calculate relative position (0 = start, 1 = end)
    relative_pos = peak_position / len(show_df)
    
    if relative_pos < 0.33:
        return "Early Peaker"
    elif relative_pos > 0.67:
        return "Late Peaker"
    else:
        return "Mid-Run Peaker"

def calculate_engagement_metrics(show_df):
    """
    Calculates various engagement metrics for a show's hype trajectory.
    """
    if len(show_df) < 2:
        return {}
    
    hype_values = show_df['hype_score'].values
    
    metrics = {
        'peak_hype': float(hype_values.max()),
        'avg_hype': float(hype_values.mean()),
        'baseline_hype': float(np.percentile(hype_values, 25)),
        'peak_to_avg_ratio': float(hype_values.max() / (hype_values.mean() + 0.01)),
        'consistency': float(1 - (hype_values.std() / (hype_values.mean() + 0.01))),
        'total_records': len(show_df)
    }
    
    # Calculate sustain rate (% of values above 50% of peak)
    threshold = metrics['peak_hype'] * 0.5
    sustain_rate = (hype_values > threshold).sum() / len(hype_values)
    metrics['sustain_rate'] = float(sustain_rate)
    
    return metrics

df = load_data()

if not df.empty:
    # --- HEADER METRICS ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📦 Total Records", f"{len(df):,}")
    with col2:
        st.metric("🎭 Unique Shows", f"{df['title'].nunique()}")
    with col3:
        latest_time = df['time'].max()
        time_ago = datetime.now() - latest_time
        st.metric("🕐 Last Update", f"{int(time_ago.total_seconds() / 60)}m ago")
    with col4:
        avg_hype = df['hype_score'].mean()
        st.metric("📊 Avg Hype Score", f"{avg_hype:.1f}")
    
    st.markdown("---")
    
    # --- SHOW SELECTION ---
    all_shows = sorted(df['title'].unique())
    selected_shows = st.multiselect(
        "Select Franchises to Analyze:",
        options=all_shows,
        default=all_shows[:5] if len(all_shows) > 5 else all_shows
    )
    
    if not selected_shows:
        st.info("👆 Select at least one show to begin analysis")
        st.stop()
    
    filtered_df = df[df['title'].isin(selected_shows)]
    
    # --- REAL-TIME TRENDING INDICATOR ---
    if 'is_trending' in filtered_df.columns:
        trending_now = filtered_df[filtered_df['is_trending'] == True]['title'].unique()
        if len(trending_now) > 0:
            st.success(f"🔥 Currently Trending: {', '.join(trending_now)}")
    
    # --- ROW 1: HISTORICAL TRENDS ---
    st.subheader("📅 Search Volume History")
    history_df = filtered_df.sort_values('time')
    st.line_chart(history_df, x='time', y='hype_score', color='title')
    
    # --- ROW 2: COMPARATIVE ANALYSIS (Early vs Late Peakers) ---
    st.markdown("---")
    st.subheader("🎯 Comparative Analysis: Peak Timing Impact")
    
    # Classify all shows
    peak_analysis = []
    for show in selected_shows:
        show_data = filtered_df[filtered_df['title'] == show].sort_values('time')
        if len(show_data) >= 10:
            classification = classify_peak_timing(show_data)
            metrics = calculate_engagement_metrics(show_data)
            
            peak_analysis.append({
                'Show': show,
                'Peak Timing': classification,
                'Peak Hype': metrics['peak_hype'],
                'Avg Hype': metrics['avg_hype'],
                'Sustain Rate': metrics['sustain_rate'] * 100,
                'Consistency': metrics['consistency'],
                'IMDb Rating': show_data['imdb_rating'].iloc[0] if 'imdb_rating' in show_data.columns else None,
                'Brand Equity': show_data['brand_equity'].iloc[0] if 'brand_equity' in show_data.columns else 0
            })
    
    if peak_analysis:
        peak_df = pd.DataFrame(peak_analysis)
        
        # Display comparison table
        st.dataframe(
            peak_df.style.format({
                'Peak Hype': '{:.1f}',
                'Avg Hype': '{:.1f}',
                'Sustain Rate': '{:.1f}%',
                'Consistency': '{:.2f}',
                'IMDb Rating': '{:.1f}',
                'Brand Equity': '{:,.0f}'
            }),
            use_container_width=True
        )
        
        # Statistical Analysis
        st.markdown("#### 📊 Key Insights: Peak Timing vs. Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Group by peak timing
            timing_groups = peak_df.groupby('Peak Timing').agg({
                'Sustain Rate': 'mean',
                'Avg Hype': 'mean',
                'IMDb Rating': 'mean',
                'Show': 'count'
            }).reset_index()
            timing_groups.columns = ['Peak Timing', 'Avg Sustain Rate', 'Avg Hype', 'Avg IMDb Rating', 'Count']
            
            st.write("**Average Metrics by Peak Timing:**")
            st.dataframe(
                timing_groups.style.format({
                    'Avg Sustain Rate': '{:.1f}%',
                    'Avg Hype': '{:.1f}',
                    'Avg IMDb Rating': '{:.1f}'
                }),
                use_container_width=True
            )
            
            # Bar chart comparison
            st.bar_chart(timing_groups.set_index('Peak Timing')[['Avg Sustain Rate']])
        
        with col2:
            # Scatter plot: Sustain Rate vs IMDb Rating
            st.write("**Sustain Rate vs. Quality (IMDb):**")
            if 'IMDb Rating' in peak_df.columns and peak_df['IMDb Rating'].notna().any():
                scatter_data = peak_df[peak_df['IMDb Rating'].notna()].copy()
                st.scatter_chart(
                    scatter_data,
                    x='Sustain Rate',
                    y='IMDb Rating',
                    color='Peak Timing',
                    size='Brand Equity'
                )
            else:
                st.info("IMDb ratings not available for comparison")
        
        # Key Finding Card
        if len(timing_groups) > 1:
            early_peakers = timing_groups[timing_groups['Peak Timing'] == 'Early Peaker']
            late_peakers = timing_groups[timing_groups['Peak Timing'] == 'Late Peaker']
            
            if not early_peakers.empty and not late_peakers.empty:
                early_sustain = early_peakers['Avg Sustain Rate'].iloc[0]
                late_sustain = late_peakers['Avg Sustain Rate'].iloc[0]
                
                if late_sustain > early_sustain:
                    diff = late_sustain - early_sustain
                    st.success(f"💡 **Finding**: Late-peaking shows maintain {diff:.1f}% higher sustained engagement than early peakers, suggesting gradual build-up creates more durable audience interest.")
                else:
                    diff = early_sustain - late_sustain
                    st.info(f"💡 **Finding**: Early-peaking shows maintain {diff:.1f}% higher sustained engagement, indicating strong launches can carry momentum.")
    
    # --- ROW 3: STANDARD METRICS ---
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💥 Viral Impact (Peak-to-Baseline)")
        viral_data = []
        for show in selected_shows:
            show_data = filtered_df[filtered_df['title'] == show]
            if len(show_data) > 0:
                peak = show_data['hype_score'].max()
                avg = show_data['hype_score'].mean()
                if avg > 0:
                    viral_data.append({'title': show, 'viral_score': peak / avg})
        
        if viral_data:
            viral_df = pd.DataFrame(viral_data)
            st.bar_chart(viral_df.set_index('title')['viral_score'])
    
    with col2:
        st.subheader("💰 Brand Equity (Total IMDb Votes)")
        latest_df = filtered_df.sort_values('time').groupby('title').tail(1)
        if 'brand_equity' in latest_df.columns:
            st.bar_chart(latest_df.set_index('title')['brand_equity'])
    
    # --- ROW 4: ML PREDICTION MODEL ---
    st.markdown("---")
    st.subheader("🤖 AI Analyst: Predicting Success from Hype Patterns")
    
    ml_df = df.groupby('title').agg({
        'hype_score': ['mean', 'max', 'std'],
        'brand_equity': 'max',
        'imdb_rating': 'first'
    }).reset_index()
    
    ml_df.columns = ['title', 'avg_hype', 'peak_hype', 'hype_volatility', 'brand_equity', 'imdb_rating']
    ml_df = ml_df[ml_df['brand_equity'] > 0]
    
    if len(ml_df) > 5:
        # Feature engineering
        X = ml_df[['avg_hype', 'peak_hype', 'hype_volatility']]
        y = ml_df['brand_equity']
        
        model = LinearRegression()
        model.fit(X, y)
        predictions = model.predict(X)
        r2 = r2_score(y, predictions)
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Model Accuracy (R²)", f"{r2:.3f}")
        col2.metric("Avg Hype Coefficient", f"{int(model.coef_[0]):,}")
        col3.metric("Peak Hype Coefficient", f"{int(model.coef_[1]):,}")
        
        # Prediction vs Actual
        chart_data = pd.DataFrame({
            'Avg Hype': ml_df['avg_hype'],
            'Actual Votes': y,
            'Predicted Votes': predictions,
            'Show': ml_df['title']
        })
        
        st.scatter_chart(
            chart_data,
            x='Avg Hype',
            y=['Actual Votes', 'Predicted Votes'],
            color=['#ff6b6b', '#4ecdc4']
        )
        
        # Show over/under performers
        ml_df['prediction_error'] = predictions - y
        ml_df['error_pct'] = (ml_df['prediction_error'] / y) * 100
        
        st.write("**Over/Under Performers (Predicted vs Actual):**")
        performance_df = ml_df[['title', 'brand_equity', 'error_pct']].copy()
        performance_df['Status'] = performance_df['error_pct'].apply(
            lambda x: '🔴 Underperformer' if x < -20 else ('🟢 Overperformer' if x > 20 else '🟡 As Expected')
        )
        st.dataframe(
            performance_df.sort_values('error_pct', ascending=False).head(10).style.format({
                'brand_equity': '{:,.0f}',
                'error_pct': '{:.1f}%'
            }),
            use_container_width=True
        )
    else:
        st.info("Not enough data for ML predictions. Collecting more shows...")

else:
    st.info("⏳ Waiting for streaming data... Producer is initializing.")
    st.write("The system polls Google Trends in real-time. Data will appear shortly.")

# Auto-refresh
time.sleep(5)
st.rerun()