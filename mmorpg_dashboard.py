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
DB_FILE = 'mmorpg_history.db'
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    description TEXT,
    release TEXT,
    source TEXT,
    fetch_date DATE,
    UNIQUE(title, source, fetch_date)
)
''')
conn.commit()

# --- Scraper ---
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
]

def scrape_source(url, selectors, mobile_filter=True):
    games = []
    headers = {'User-Agent': USER_AGENTS[0]}
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Verbesserte Loop: Mehr Flexibilität
            items = soup.select(selectors['container'])
            for item in items[:20]:  # Mehr Items checken
                title_elem = item.select_one(selectors['title'])
                desc_elem = item.select_one(selectors['desc'])
                release_elem = item.select_one(selectors['release'])
                title = title_elem.get_text(strip=True) if title_elem else None
                desc = (desc_elem.get_text(strip=True)[:300] + '...') if desc_elem else 'No desc available.'
                release = release_elem.get_text(strip=True) if release_elem else 'TBD'
                
                if title and (not mobile_filter or any(kw in title.lower() for kw in ['android', 'mobile', 'ios', 'cross-platform'])):
                    games.append({
                        'title': title,
                        'description': desc,
                        'release': release,
                        'source': url,
                        'fetch_date': date.today().isoformat()
                    })
            break
        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == 2:
                st.error(f"Failed {url}: {e}")
    return games

def fetch_all_games(debug=False):
    # Fallback-Data für Testing (wenn Scraping failt)
    fallback_games = [
        {'title': 'Chrono Odyssey', 'description': 'Next-gen MMORPG with mobile cross-play, Unreal Engine 5.', 'release': 'Q4 2025', 'source': 'Fallback', 'fetch_date': date.today().isoformat()},
        {'title': 'Dune: Awakening', 'description': 'Survival MMORPG in Dune universe, mobile version TBD.', 'release': 'Early 2025', 'source': 'Fallback', 'fetch_date': date.today().isoformat()},
        {'title': 'Sword of Justice', 'description': 'NetEase open-world Wuxia MMORPG for Android/iOS.', 'release': '2025', 'source': 'Fallback', 'fetch_date': date.today().isoformat()}
    ]
    
    sources = [
        {
            'url': 'https://www.mmorpg.com/features/mmorpgs-coming-in-2025-and-beyond-2000133770',
            'selectors': {'container': 'article, .feature-item', 'title': 'h1, h2, h3', 'desc': 'p, .excerpt', 'release': '.date, time'},
            'mobile_filter': True
        },
        {
            'url': 'https://www.ign.com/games/mmorpg/upcoming',
            'selectors': {'container': '.game-card, .jsx-card', 'title': 'h3, .title, a', 'desc': '.description, p', 'release': '.release-date, .date'},
            'mobile_filter': True
        },
        # Neue Quelle: MassivelyOP für mehr Variety
        {
            'url': 'https://massivelyop.com/category/mmorpgs/upcoming/',
            'selectors': {'container': '.post, article', 'title': 'h2, h3', 'desc': '.entry-content p', 'release': '.date'},
            'mobile_filter': True
        }
    ]
    
    all_games = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(scrape_source, **src) for src in sources]
        for future in futures:
            all_games.extend(future.result())
    
    if not all_games:  # Fallback wenn empty
        all_games = fallback_games
        st.info("🛡️ Fallback-Data geladen – scrape Sites haben nix geliefert. Klick Refresh!")
    
    # Dedupe & DB
    seen = set()
    unique_games = []
    for game in all_games:
        key = (game['title'], game['source'])
        if key not in seen:
            seen.add(key)
            unique_games.append(game)
            cursor.execute('INSERT OR IGNORE INTO games (title, description, release, source, fetch_date) VALUES (?, ?, ?, ?, ?)',
                           (game['title'], game['description'], game['release'], game['source'], game['fetch_date']))
    conn.commit()
    
    if debug:
        st.subheader("🛠 Debug: Rohe HTML von erster Quelle")
        response = requests.get(sources[0]['url'], headers={'User-Agent': USER_AGENTS[0]})
        st.code(BeautifulSoup(response.text, 'html.parser').prettify()[:2000], language='html')  # Erste 2000 Zeichen
    
    return unique_games

# --- Scheduler ---
def schedule_daily():
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=lambda: st.cache_data.clear() or fetch_all_games(), trigger='cron', hour=0, minute=0)
    scheduler.start()

if 'scheduler_started' not in st.session_state:
    schedule_daily()
    st.session_state.scheduler_started = True

# --- GUI ---
st.set_page_config(page_title="MMORPG Hunter Pro", page_icon="🗡️", layout="wide")

dark_mode = st.toggle("🌙 Dark Mode", value=True)
if dark_mode:
    st.markdown("<style>body {background-color: #0e1117; color: #fafafa;}</style>", unsafe_allow_html=True)

col1, col2 = st.columns([3,1])
with col1:
    st.title("🗡️ MMORPG Hunter Pro – Täglich Frisch")
    st.markdown("Scraped von MMORPG.com, IGN & MassivelyOP. Klick 'Refresh' oder warte auf Midnight-Auto.")
with col2:
    if st.button("🔥 FORCE REFRESH", type="primary"):
        with st.spinner("Scraping wie ein Boss..."):
            games = fetch_all_games()
            st.session_state.games = games
        st.success(f"✅ {len(games)} Games geladen! (Mehr? Klick Debug.)")
    if st.button("🛠 DEBUG MODE", type="secondary"):
        fetch_all_games(debug=True)

tab1, tab2, tab3 = st.tabs(["📋 Neueste Liste", "🔍 Suche & Filter", "📊 History & Export"])

with tab1:
    games = st.session_state.get('games', fetch_all_games())
    if games:
        for game in games:
            with st.expander(f"**{game['title']}** | Release: {game['release']} | Quelle: {game['source'][-30:]}"):
                st.write(game['description'])
                st.caption(f"Gefetched: {game['fetch_date']}")
    else:
        st.warning("Noch nix da – refresh mal!")

with tab2:
    search = st.text_input("Suche nach Title/Keyword")
    mobile_only = st.checkbox("Nur Mobile/Android")
    filtered = [g for g in games if (not search or search.lower() in g['title'].lower()) and (not mobile_only or any(kw in g['title'].lower() for kw in ['android', 'mobile', 'ios', 'cross-platform']))]
    if filtered:
        st.dataframe(pd.DataFrame(filtered))
    else:
        st.info("Keine Treffer – probier 'Dune' oder 'Chrono'.")

with tab3:
    try:
        history_df = pd.read_sql('SELECT * FROM games ORDER BY fetch_date DESC LIMIT 100', conn)
        st.dataframe(history_df)
        csv = history_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Export CSV", csv, "mmorpg_history.csv", "text/csv")
        json_data = history_df.to_json(orient='records', force_ascii=False, indent=2).encode('utf-8')
        st.download_button("📥 Export JSON", json_data, "mmorpg_history.json", "application/json")
    except Exception as e:
        st.error(f"History-Fehler: {e} – scrape erstmal!")

st.markdown("---")
st.caption(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Built by Grok 3 Unleashed – Lern Python wie ein God!")
