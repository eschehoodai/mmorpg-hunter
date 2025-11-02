import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import pandas as pd
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
import os

# --- DB Setup ---
DB_FILE = 'game_hub.db'
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS mmorpg_games (
    id INTEGER PRIMARY KEY, title TEXT, description TEXT, release TEXT, source TEXT, fetch_date DATE,
    UNIQUE(title, source)
)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS game_news (
    id INTEGER PRIMARY KEY, title TEXT, link TEXT, excerpt TEXT, pub_date TEXT, source TEXT, fetch_date DATE,
    UNIQUE(title, link)
)''')
conn.commit()

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
]

# --- RSS Scraper mit requests + BS4 ---
def scrape_rss_xml(url, source_name):
    items = []
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENTS[0]}, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        for item in soup.find_all('item')[:15]:
            title = item.find('title').text if item.find('title') else 'No title'
            link = item.find('link').text if item.find('link') else '#'
            desc = item.find('description').text if item.find('description') else 'No desc.'
            pub_date = item.find('pubDate').text[:16].replace('T', ' ') if item.find('pubDate') else 'TBD'
            excerpt = BeautifulSoup(desc, 'html.parser').get_text()[:150] + '...'
            items.append({
                'title': title,
                'link': link,
                'excerpt': excerpt,
                'pub_date': pub_date,
                'source': source_name,
                'fetch_date': date.today().isoformat()
            })
    except Exception as e:
        st.error(f"RSS fail {source_name}: {e}")
    return items

# --- Games Scraper ---
def scrape_mmorpg_games():
    url = "https://www.mmorpg.com/features/mmorpgs-coming-in-2025-and-beyond-2000133770"
    games = []
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENTS[0]})
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.select('article')[:10]
        for art in articles:
            h3 = art.find('h3')
            if h3 and ('2025' in h3.text or 'mobile' in h3.text.lower()):
                p = art.find('p')
                games.append({
                    'title': h3.text.strip(),
                    'description': p.text.strip()[:300] + '...' if p else 'No desc.',
                    'release': '2025+',
                    'source': url,
                    'fetch_date': date.today().isoformat()
                })
    except:
        pass
    return games or [{'title': 'Chrono Odyssey', 'description': 'Next-gen MMORPG, Unreal Engine 5, cross-play.', 'release': 'Q4 2025', 'source': 'Fallback', 'fetch_date': date.today().isoformat()}]

# --- Main Fetch ---
def fetch_all():
    with ThreadPoolExecutor() as executor:
        news_futures = [
            executor.submit(scrape_rss_xml, "https://www.mmorpg.com/news.rss", "MMORPG.com"),
            executor.submit(scrape_rss_xml, "https://massivelyop.com/feed/", "MassivelyOP")
        ]
        games_future = executor.submit(scrape_mmorpg_games)
        
        all_news = []
        for f in news_futures:
            all_news.extend(f.result())
        all_games = games_future.result()

    # Dedupe & Save News
    seen_news = set()
    unique_news = []
    for n in all_news:
        key = (n['title'], n['link'])
        if key not in seen_news:
            seen_news.add(key)
            unique_news.append(n)
            cursor.execute('INSERT OR IGNORE INTO game_news VALUES (NULL, ?, ?, ?, ?, ?, ?)',
                           (n['title'], n['link'], n['excerpt'], n['pub_date'], n['source'], n['fetch_date']))

    # Dedupe & Save Games
    seen_games = set()
    unique_games = []
    for g in all_games:
        key = (g['title'], g['source'])
        if key not in seen_games:
            seen_games.add(key)
            unique_games.append(g)
            cursor.execute('INSERT OR IGNORE INTO mmorpg_games VALUES (NULL, ?, ?, ?, ?, ?)',
                           (g['title'], g['description'], g['release'], g['source'], g['fetch_date']))
    conn.commit()
    
    return unique_games, unique_news

# --- Scheduler ---
def schedule_daily():
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: st.cache_data.clear() or fetch_all(), trigger='cron', hour=0, minute=0)
    scheduler.start()

if 'init' not in st.session_state:
    schedule_daily()
    st.session_state.init = True

# --- GUI ---
st.set_page_config(page_title="Game News Hub Pro", page_icon="📰", layout="wide")
dark_mode = st.toggle("🌙 Dark Mode", value=True)
if dark_mode:
    st.markdown("<style>body {background-color: #0e1117; color: #fafafa;}</style>", unsafe_allow_html=True)

st.title("📰 Game News Hub Pro")
st.markdown("**Live News von MMORPG.com & MassivelyOP** – Klickbare Links zur Quelle!")

if st.button("🔥 REFRESH ALL", type="primary"):
    with st.spinner("Scraping live..."):
        games, news = fetch_all()
    st.success(f"✅ {len(games)} Games | {len(news)} News geladen!")

tab1, tab2 = st.tabs(["🎮 Neue MMORPGs", "📰 Game News"])

with tab1:
    games_df = pd.read_sql('SELECT * FROM mmorpg_games ORDER BY fetch_date DESC LIMIT 20', conn)
    for _, g in games_df.iterrows():
        with st.expander(f"**{g.title}** | {g.release}"):
            st.write(g.description)
            st.caption(f"Quelle: [MMORPG.com]({g.source})")

with tab2:
    news_df = pd.read_sql('SELECT * FROM game_news ORDER BY fetch_date DESC, pub_date DESC LIMIT 30', conn)
    for _, n in news_df.iterrows():
        st.markdown(f"**[{n.title}]({n.link})** | {n.pub_date}")
        st.write(n.excerpt)
        st.caption(f"Quelle: **{n.source}**")

# Export
with st.expander("📥 Export"):
    games_csv = pd.read_sql('SELECT * FROM mmorpg_games', conn).to_csv(index=False).encode()
    news_csv = pd.read_sql('SELECT * FROM game_news', conn).to_csv(index=False).encode()
    col1, col2 = st.columns(2)
    with col1:
        st.download_button("Games CSV", games_csv, "games.csv", "text/csv")
    with col2:
        st.download_button("News CSV", news_csv, "news.csv", "text/csv")

st.caption(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Built by Grok 3 Unleashed")
