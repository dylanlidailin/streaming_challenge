import streamlit as st
import pandas as pd
import redis
import json
import time
import altair as alt
from datetime import datetime

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Franchise Valuation Report",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="collapsed"
)

# Custom CSS for Business Report Look
st.markdown("""
<style>
    .block-container {padding-top: 1rem; padding-bottom: 5rem;}
    h1 {font-size: 2.2rem; border-bottom: 2px solid #f0f2f6; padding-bottom: 10px;}
    .metric-card {background-color: #f9f9f9; padding: 15px; border-radius: 10px; border: 1px solid #ddd;}
</style>
""", unsafe_allow_html=True)

# --- CONFIGURATION ---
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_LIST = "franchise_data"

# --- HELPER FUNCTIONS ---
def classify_peaker(row, start_ts, end_ts):
    """Classifies a show based on trend shape."""
    if row['hype_score'] == 0: return "N/A"
    duration = end_ts - start_ts
    if duration == 0: return "Instant"
    
    # Relative position of peak (0.0=Start, 1.0=End)
    peak_pos = (row['peak_timestamp'] - start_ts) / duration
    
    if peak_pos < 0.33: return "Early Peaker"
    elif peak_pos < 0.66: return "Mid Peaker"
    else: return "Late Peaker"

@st.cache_data(ttl=2)
def load_data():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        raw_data = r.lrange(REDIS_LIST, 0, -1)
        if not raw_data: return pd.DataFrame()
        records = [json.loads(x) for x in raw_data]
        df = pd.DataFrame(records)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        return df
    except Exception as e:
        return pd.DataFrame()

df = load_data()

# --- HEADER SECTION ---
st.title("📊 Franchise Valuation & Market Intelligence")
st.markdown("**Executive Summary:** Real-time analysis of content performance combining Search Volume (Demand) and IMDb Votes (Brand Equity).")
st.write("")

if not df.empty:
    # --- DATA ENRICHMENT (Peaker Logic) ---
    stats_df = df.groupby('title').agg(
        start_ts=('timestamp', 'min'),
        end_ts=('timestamp', 'max')
    ).reset_index()
    
    # Find peak timestamps
    peak_times = df.loc[df.groupby('title')['hype_score'].idxmax()][['title', 'timestamp']]
    peak_times = peak_times.rename(columns={'timestamp': 'peak_timestamp'})
    
    stats_df = stats_df.merge(peak_times, on='title')
    stats_df = stats_df.merge(df[['title', 'hype_score']].groupby('title').max(), on='title') 
    
    # Apply Classification
    stats_df['Lifecycle'] = stats_df.apply(lambda x: classify_peaker(x, x['start_ts'], x['end_ts']), axis=1)
    
    # Merge back to main dataframe
    df = df.merge(stats_df[['title', 'Lifecycle']], on='title', how='left')

    # --- SECTION 1: KPIS ---
    total_points = len(df)
    unique_shows = df['title'].nunique()
    
    # Calculate Last Update Time
    last_refresh_dt = df['datetime'].max()
    last_refresh_str = last_refresh_dt.strftime('%H:%M:%S')

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📡 Data Points Processed", f"{total_points:,}")
    m2.metric("🎬 Franchises Tracked", f"{unique_shows}")
    
    # --- UPDATED METRIC HERE ---
    m3.metric("⏱️ Last Updated", last_refresh_str, "Live Stream")
    
    m4.metric("🧠 Dominant Type", f"{stats_df['Lifecycle'].mode()[0] if not stats_df.empty else 'N/A'}")
    
    st.markdown("---")

    # --- SECTION 2: CONTROLS ---
    with st.container():
        c_filter, c_info = st.columns([3, 1])
        with c_filter:
            all_shows = sorted(df['title'].unique())
            # Safety Check
            preferred = ['Stranger Things', 'Breaking Bad', 'Money Heist', 'The Witcher', 'The Crown']
            valid_defaults = [s for s in preferred if s in all_shows]
            
            selected_shows = st.multiselect(
                "Filter Analysis by Franchise:", 
                all_shows, 
                default=valid_defaults,
                help="Select specific shows to compare."
            )
        with c_info:
            st.caption("ℹ️ **Analyst Note:** Default view shows top 5 franchises. Add/Remove items to customize.")

    if selected_shows:
        df_filtered = df[df['title'].isin(selected_shows)]
    else:
        df_filtered = df

    st.write("")

    # --- SECTION 3: TREND VELOCITY (Full Width) ---
    st.subheader("1. Market Demand Velocity")
    
    line_chart = alt.Chart(df_filtered).mark_line(strokeWidth=3).encode(
        x=alt.X('datetime', title='Timeline'), 
        y=alt.Y('hype_score', title='Search Volume Index (0-100)'),
        color=alt.Color('title', legend=alt.Legend(title="Franchise")),
        tooltip=['title', 'datetime', 'hype_score', 'Lifecycle']
    ).properties(height=400).interactive()
    
    st.altair_chart(line_chart, use_container_width=True)
    
    with st.expander("💡 View Lifecycle Definitions"):
        st.markdown("""
        * **Early Peaker:** Peak popularity in first 1/3 of history (e.g. Pilot Viral).
        * **Mid Peaker:** Peaked mid-run (e.g. Season 3).
        * **Late Peaker:** Peak is recent (Current Viral Hit).
        """)

    st.markdown("---")

    # --- SECTION 4: FRANCHISE LEADERBOARD (Bar Chart) ---
    st.subheader("2. Franchise Performance Leaderboard")
    
    r2_1, r2_2 = st.columns([1, 3])
    with r2_1:
        st.success("📊 **Analyst Insight**")
        st.markdown("""
        **Performance Rankings:**
        
        This chart ranks the selected franchises by their **Average Hype Score** over the tracked period.
        
        * **Higher Bars:** More consistent sustained interest.
        * **Colors:** Indicate the lifecycle phase of the show.
        """)
        
    with r2_2:
        # Aggregation for Leaderboard
        bar_df = df_filtered.groupby(['title', 'Lifecycle']).agg({
            'hype_score': 'mean',
            'brand_equity': 'max'
        }).reset_index().sort_values(by='hype_score', ascending=False)
        
        # Horizontal Bar Chart
        bar_chart = alt.Chart(bar_df).mark_bar().encode(
            x=alt.X('hype_score', title='Average Hype Score'),
            y=alt.Y('title', sort='-x', title=None), # Sort descending by x
            color=alt.Color('Lifecycle', legend=alt.Legend(title="Lifecycle Phase")),
            tooltip=['title', 'hype_score', 'brand_equity', 'Lifecycle']
        ).properties(height=350).interactive()
        
        st.altair_chart(bar_chart, use_container_width=True)

    st.markdown("---")

    # --- SECTION 5: VOLATILITY ANALYSIS ---
    st.subheader("3. Franchise Stability (Volatility Index)")
    
    r3_1, r3_2 = st.columns([3, 1])
    with r3_1:
        box_chart = alt.Chart(df_filtered).mark_boxplot(extent='min-max').encode(
            x=alt.X('title', title='Franchise'),
            y=alt.Y('hype_score', title='Hype Distribution'),
            color='Lifecycle'
        ).properties(height=300).interactive()
        st.altair_chart(box_chart, use_container_width=True)
        
    with r3_2:
        st.warning("📉 **How to read:**")
        st.markdown("""
        **Tall Box:** High Volatility. Interest spikes and crashes (Event TV).
        
        **Short Box:** High Stability. Consistent daily interest (Comfort TV).
        """)

    # --- RAW DATA ---
    with st.expander("📂 View Source Data Table"):
        st.dataframe(
            df_filtered[['datetime', 'title', 'hype_score', 'Lifecycle', 'brand_equity']].sort_values(by='datetime', ascending=False),
            use_container_width=True
        )

else:
    st.warning("Waiting for data stream... Please ensure the Producer service is active.")
    time.sleep(2)
    st.rerun()