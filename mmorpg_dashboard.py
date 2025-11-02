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

# --- DB Setup (separate tables for games & news) ---
DB_FILE = 'game_hub.db'
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS mmorpg_games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT, description TEXT, release TEXT, source TEXT, fetch_date DATE,
    UNIQUE(title, source, fetch_date)
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS game_news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT, link TEXT, excerpt TEXT, pub_date TEXT, source TEXT, fetch_date DATE,
    UNIQUE(title, source, fetch_date)
)
''')
conn.commit()

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
]

def scrape_news(url, selectors):
    """Scrape News: Title, Link, Excerpt, Date"""
    items = []
    headers = {'User-Agent': USER_AGENTS[0]}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select(selectors['container'])[:20]:
            title_elem = item.select_one(selectors['title'])
            link_elem = item.select_one(selectors['link'])
            excerpt_elem = item.select_one(selectors['excerpt'])
            date_elem = item.select_one(selectors['date'])
            title = title_elem.get_text(strip=True) if title_elem else None
            link = 'https://mein-mmo.de' + link_elem['href'] if link_elem and link_elem.get('href') else None if title else None
            excerpt = excerpt_elem.get_text(strip=True)[:150] + '...' if excerpt_elem else 'No excerpt.'
            pub_date = date_elem.get_text(strip=True) if date_elem else 'TBD'
            if title and link:
                items.append({
                    'title': title, 'link': link, 'excerpt': excerpt, 'pub_date': pub_date,
                    'source': url, 'fetch_date': date.today().isoformat()
                })
    except Exception as e:
        st.error(f"News scrape fail {url}: {e}")
    return items

def scrape_games(url, selectors):
    """Scrape Games (unchanged, improved selectors)"""
    games = []
    headers = {'User-Agent': USER_AGENTS[0]}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select(selectors['container'])[:20]:
            title = item.select_one(selectors['title']).get_text(strip=True) if item.select_one(selectors['title']) else None
            desc = item.select_one(selectors['desc']).get_text(strip=True)[:300] + '...' if item.select_one(selectors['desc']) else 'No desc.'
            release = item.select_one(selectors['release']).get_text(strip=True) if item.select_one(selectors['release']) else 'TBD'
            if title:
                games.append({
                    'title': title, 'description': desc, 'release': release,
                    'source': url, 'fetch_date': date.today().isoformat()
                })
    except:
        pass
    return games

def fetch_mmorpg_games():
    sources = [
        {'url': 'https://www.mmorpg.com/features/mmorpgs-coming-in-2025-and-beyond-2000133770',
         'selectors': {'container': 'h3, article', 'title': 'h3', 'desc': 'p', 'release': 'p'}},
        {'url': 'https://massivelyop.com/category/mmorpgs/upcoming/',
         'selectors': {'container': 'article, .post', 'title': 'h2, h3', 'desc': 'p', 'release': '.date'}}
    ]
    all_games = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(scrape_games, s['url'], s['selectors']) for s in sources]
        for f in futures: all_games.extend(f.result())
    
    # Dedupe & Save
    seen = set()
    unique = [g for g in all_games if g['title'] and (g['title'], g['source']) not in seen and not seen.add((g['title'], g['source']))]
    for g in unique:
        cursor.execute('INSERT OR IGNORE INTO mmorpg_games VALUES (NULL, ?, ?, ?, ?, ?)',
                       (g['title'], g['description'], g['release'], g['source'], g['fetch_date']))
    conn.commit()
    return unique or [{'title': 'Dune: Awakening', 'description': 'Early 2025 PC Early Access.<grok-card data-id="f90947" data-type="citation_card"></grok-card>', 'release': 'Early 2025', 'source': 'Fallback', 'fetch_date': date.today().isoformat()}]

def fetch_game_news():
    sources = [
        {'url': 'https://mein-mmo.de/home/aktuelles/',
         'selectors': {'container': 'article, .teaser, .post-item', 'title': 'h2 a, h3 a', 'link': 'a[href]', 'excerpt': 'p.excerpt, .summary', 'date': 'time, .date'}},
        {'url': 'https://massivelyop.com/',
         'selectors': {'container': 'article, .post', 'title': 'h2 a, h3 a', 'link': 'a[href]', 'excerpt': 'p', 'date': '.date'}}
    ]
    all_news = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(scrape_news, s['url'], s['selectors']) for s in sources]
        for f in futures: all_news.extend(f.result())
    
    seen = set()
    unique = [n for n in all_news if n['title'] and (n['title'], n['source']) not in seen and not seen.add((n['title'], n['source']))]
    for n in unique:
        cursor.execute('INSERT OR IGNORE INTO game_news VALUES (NULL, ?, ?, ?, ?, ?, ?)',
                       (n['title'], n['link'], n['excerpt'], n['pub_date'], n['source'], n['fetch_date']))
    conn.commit()
    return unique or [{'title': 'The MOP Up: RIFT housing contest', 'link': 'https://massivelyop.com/2025/11/02/the-mop-up-rift-hosts-a-player-housing-contest/', 'excerpt': 'Player housing contest in RIFT...<grok-card data-id="b85efc" data-type="citation_card"></grok-card>', 'pub_date': '2025-11-02', 'source': 'Fallback MassivelyOP', 'fetch_date': date.today().isoformat()}]

# Scheduler
def schedule_daily():
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: (st.cache_data.clear(), fetch_mmorpg_games(), fetch_game_news()), 'cron', hour=0)
    scheduler.start()

if 'scheduler_started' not in st.session_state:
    schedule_daily()
    st.session_state.scheduler_started = True

# GUI
st.set_page_config(page_title="Game News Hub Pro", page_icon="📰", layout="wide")
dark_mode = st.toggle("🌙 Dark Mode", value=True)
if dark_mode: st.markdown("<style>body {background-color: #0e1117; color: #fafafa;}</style>", unsafe_allow_html=True)

st.title("📰 Game News Hub Pro – MMORPGs & News Aggregator")
st.markdown("Tägliche Updates aus mein-mmo.de, MMORPG.com, MassivelyOP. **Alle Links führen zur Originalquelle!**")

col1, col2 = st.columns([3,1])
with col1: st.markdown("🔥 **FORCE REFRESH** lädt live!")
with col2:
    if st.button("🔥 REFRESH ALL", type="primary"):
        st.spinner("Loading News & Games...")
        st.session_state.games = fetch_mmorpg_games()
        st.session_state.news = fetch_game_news()
        st.success(f"✅ {len(st.session_state.games)} Games + {len(st.session_state.news)} News!")

tab_games, tab_news = st.tabs(["🎮 Neue MMORPGs", "📰 Game News"])

with tab_games:
    games = st.session_state.get('games', fetch_mmorpg_games())
    for g in games:
        with st.expander(f"**{g['title']}** | {g['release']}"):
            st.write(g['description'])
            st.caption(f"Quelle: [{g['source'][-30:]}]({g['source']}) | {g['fetch_date']}")

with tab_news:
    news = st.session_state.get('news', fetch_game_news())
    for n in news:
        st.markdown(f"**[{n['title']}]({n['link']})** | {n['pub_date']}")
        st.write(n['excerpt'])
        st.caption(f"Quelle: [{n['source'][-30:]}]({n['source']}) | {n['fetch_date']}")

# Export Tab
tab_export, tab_history = st.tabs(["📥 Export", "📊 History"])
with tab_export:
    games_df = pd.DataFrame(st.session_state.get('games', []))
    news_df = pd.DataFrame(st.session_state.get('news', []))
    st.download_button("Games CSV", games_df.to_csv(index=False).encode(), "games.csv")
    st.download_button("News CSV", news_df.to_csv(index=False).encode(), "news.csv")

with tab_history:
    st.dataframe(pd.read_sql('SELECT * FROM mmorpg_games ORDER BY fetch_date DESC LIMIT 50', conn))
    st.dataframe(pd.read_sql('SELECT * FROM game_news ORDER BY fetch_date DESC LIMIT 50', conn))

st.caption(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Built by Grok 3 Unleashed")
