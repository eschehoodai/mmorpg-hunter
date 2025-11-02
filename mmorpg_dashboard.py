import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import pandas as pd
import sqlite3
import time
import re
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

USER_AGENTS = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36']

# --- RSS FIXXED: Nur MassivelyOP + robust pubDate parsing ---
def scrape_rss_xml(url, source_name):
    items = []
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENTS[0]}, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        for item in soup.find_all('item')[:15]:
            title = item.find('title').text.strip() if item.find('title') else 'No title'
            link = item.find('link').text.strip() if item.find('link') else '#'
            desc = item.find('description').text if item.find('description') else 'No desc.'
            pubdate_raw = item.find('pubDate').text if item.find('pubDate') else 'TBD'
            # Robust parsing: Extract date like 'Sun, 02 Nov 2025'
            pub_date = re.search(r'(\w{3}, \d{2} \w{3} \d{4})', pubdate_raw).group(1) if re.search(r'(\w{3}, \d{2} \w{3} \d{4})', pubdate_raw) else 'TBD'
            excerpt = BeautifulSoup(desc, 'html.parser').get_text(strip=True)[:150] + '...'
            items.append({
                'title': title, 'link': link, 'excerpt': excerpt, 'pub_date': pub_date,
                'source': source_name, 'fetch_date': date.today().isoformat()
            })
        st.success(f"✅ {source_name}: {len(items)} News geladen!")
    except Exception as e:
        st.error(f"RSS fail {source_name}: {e}")
    return items

# --- Games FIXXED: Alle h3 + next p's ---
def scrape_mmorpg_games():
    url = "https://www.mmorpg.com/features/mmorpgs-coming-in-2025-and-beyond-2000133770"
    games = []
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENTS[0]}, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        h3s = soup.find_all('h3')[:20]  # Alle h3
        for h3 in h3s:
            title = h3.get_text(strip=True)
            if any(kw in title.lower() for kw in ['2025', 'mobile', 'odyssey', 'dune', 'aion']):
                # Nächste p's als desc
                desc_parts = []
                for p in h3.find_next_siblings('p')[:8]:  # 8 p's pro Game
                    desc_parts.append(p.get_text(strip=True))
                desc = ' '.join(desc_parts)[:300] + '...'
                games.append({
                    'title': title, 'description': desc or 'No desc.',
                    'release': '2025+', 'source': url, 'fetch_date': date.today().isoformat()
                })
        st.success(f"✅ MMORPG.com: {len(games)} Games gefunden!")
    except Exception as e:
        st.error(f"Games fail: {e}")
    return games or [
        {'title': 'Chrono Odyssey', 'description': 'Next-gen MMORPG Q4 2025.', 'release': 'Q4 2025', 'source': 'Fallback', 'fetch_date': date.today().isoformat()},
        {'title': 'Dune: Awakening', 'description': 'Survival MMO Early 2025.', 'release': 'Early 2025', 'source': 'Fallback', 'fetch_date': date.today().isoformat()}
    ]

# --- Main Fetch ---
@st.cache_data(ttl=3600)  # Cache 1h
def fetch_all():
    with ThreadPoolExecutor(max_workers=3) as executor:
        news_future = executor.submit(scrape_rss_xml, "https://massivelyop.com/feed/", "MassivelyOP")
        games_future = executor.submit(scrape_mmorpg_games)
        
        all_news = news_future.result()
        all_games = games_future.result()

    # Save News
    seen_news = set()
    for n in all_news:
        key = (n['title'], n['link'])
        if key not in seen_news:
            seen_news.add(key)
            cursor.execute('INSERT OR IGNORE INTO game_news VALUES (NULL, ?, ?, ?, ?, ?, ?)',
                           (n['title'], n['link'], n['excerpt'], n['pub_date'], n['source'], n['fetch_date']))

    # Save Games
    seen_games = set()
    for g in all_games:
        key = (g['title'], g['source'])
        if key not in seen_games:
            seen_games.add(key)
            cursor.execute('INSERT OR IGNORE INTO mmorpg_games VALUES (NULL, ?, ?, ?, ?, ?)',
                           (g['title'], g['description'], g['release'], g['source'], g['fetch_date']))
    conn.commit()
    
    return all_games, all_news

# --- Scheduler ---
def schedule_daily():
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: st.cache_data.clear() and fetch_all(), trigger='cron', hour=0, minute=0)
    scheduler.start()

if 'init' not in st.session_state:
    schedule_daily()
    st.session_state.init = True

# --- GUI ---
st.set_page_config(page_title="Game News Hub Pro", page_icon="📰", layout="wide")
dark_mode = st.toggle("🌙 Dark Mode", value=True)
if dark_mode:
    st.markdown("<style>body {background-color: #0e1117; color: #fafafa;}</style>", unsafe_allow_html=True)

st.title("📰 Game News Hub Pro – MMORPG News & Upcoming")
st.markdown("**MassivelyOP RSS + MMORPG.com 2025 Games** – Vollautomatisch täglich!")

if st.button("🔥 REFRESH ALL (Live Scrape!)", type="primary"):
    with st.spinner("🔥 Scraping News & Games..."):
        games, news = fetch_all()
    st.balloons()
    st.success(f"✅ {len(games)} Games + {len(news)} News GELADEN! 🎉")

tab1, tab2 = st.tabs(["🎮 Neue MMORPGs (2025+)", "📰 Frische News"])

with tab1:
    games_df = pd.read_sql('SELECT * FROM mmorpg_games ORDER BY fetch_date DESC, title LIMIT 20', conn)
    if not games_df.empty:
        for _, g in games_df.iterrows():
            with st.expander(f"**{g.title}** | {g.release}"):
                st.write(g.description)
                st.caption(f"[Quelle]({g.source}) | {g.fetch_date}")
    else:
        st.info("🔄 Scrape erstmal – klick REFRESH!")

with tab2:
    news_df = pd.read_sql('SELECT * FROM game_news ORDER BY fetch_date DESC LIMIT 30', conn)
    if not news_df.empty:
        for _, n in news_df.iterrows():
            st.markdown(f"**[{n.title}]({n.link})**  _{n.pub_date}_")
            st.caption(n.excerpt)
            st.divider()
    else:
        st.info("🔄 News laden – REFRESH!")

# Export
with st.expander("📥 Export CSV"):
    col1, col2 = st.columns(2)
    with col1:
        games_csv = pd.read_sql('SELECT * FROM mmorpg_games', conn).to_csv(index=False).encode('utf-8')
        st.download_button("🎮 Games CSV", games_csv, "mmorpg_games.csv", "text/csv")
    with col2:
        news_csv = pd.read_sql('SELECT * FROM game_news', conn).to_csv(index=False).encode('utf-8')
        st.download_button("📰 News CSV", news_csv, "game_news.csv", "text/csv")

st.caption(f"🕐 Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Built by Grok 3 Unleashed – NOW FULL POWER! 🚀")
