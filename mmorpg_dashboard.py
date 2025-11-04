import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd

# Dein Published Doc-Link (aktualisiert mit deinem!)
DOC_URL = "https://docs.google.com/document/d/e/2PACX-1vQWBQ8aViVqo5N0T7BvZdd2nGXrnmjvcyfUyZot2wZcCyuBgB-TgYLaBbOVId_027LA4tDJZ6fWG3ry/pub?output=html"

@st.cache_data(ttl=1800)  # Cache 30min
def fetch_games_from_doc():
    try:
        response = requests.get(DOC_URL, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        text = BeautifulSoup(response.text, 'html.parser').get_text()
        
        # Clean: Remove header crap
        text = re.sub(r'Neueste Online-PC-Spiele.*?(?=##)', '', text, flags=re.DOTALL | re.IGNORECASE)
        
        # Split auf Game-Sections (## Header als Trennung)
        sections = re.split(r'\n##\s*', text)[1:]  # Skip first empty
        games = []
        seen_titles = set()
        
        for section in sections:
            lines = [line.strip() for line in section.split('\n') if line.strip()]
            if not lines:
                continue
            
            # Parse colon-style: Name: ..., Erscheinungsdatum: ..., Genre: ..., Zusammenfassung: ..., Quelle: ...
            name = re.search(r'Name:\s*(.+?)(?=\n|$)', section)
            release = re.search(r'Erscheinungsdatum:\s*(.+?)(?=\n|$)', section)
            genre = re.search(r'Genre:\s*(.+?)(?=\n|$)', section)
            desc = re.search(r'Zusammenfassung:\s*(.+?)(?=\nQuelle:|$)', section, re.DOTALL)
            quelle = re.search(r'Quelle:\s*(.+?)(?=\n|$)', section)
            
            title = name.group(1).strip() if name else lines[0].strip()
            if title in seen_titles:
                continue  # Dedupe
            seen_titles.add(title)
            
            games.append({
                'name': title,
                'release': release.group(1).strip() if release else 'TBD',
                'genre': genre.group(1).strip() if genre else 'Unknown',
                'desc': (desc.group(1).strip()[:300] + '...') if desc else 'No desc.',
                'quelle': quelle.group(1).strip() if quelle else 'Unknown'
            })
        
        st.success(f"✅ {len(games)} Games aus Doc geparsed!")  # Debug
        return games[:20]
    except Exception as e:
        st.error(f"Doc fail: {e}")
        return []  # Fallback

st.set_page_config(page_title="PC Game Releases Hub", page_icon="🎮", layout="wide")
st.title("🎮 PC Game Releases – Täglich Frisch aus Pokee.ai!")

if st.button("🔥 REFRESH FROM DOC", type="primary"):
    with st.spinner("Lade aus Google Docs..."):
        games = fetch_games_from_doc()
        st.session_state.games = games
    st.success(f"✅ {len(games)} Games geladen! (z.B. {games[0]['name'] if games else 'Noch nix'})")

games = st.session_state.get('games', fetch_games_from_doc())

if games:
    df = pd.DataFrame(games)
    st.subheader("📊 Übersicht Tabelle")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Pretty Cards
    st.subheader("🎯 Game Cards")
    cols = st.columns(3)
    for i, game in enumerate(games):
        with cols[i % 3]:
            with st.container():
                st.markdown(f"**{game['name']}**")
                st.caption(f"📅 {game['release']} | 🎭 {game['genre']}")
                st.write(game['desc'])
                st.caption(f"🔗 [Quelle]({game['quelle']})")

    # Filter
    st.subheader("🔍 Filter & Suche")
    genre = st.selectbox("Genre filtern", options=['All'] + sorted(set(g['genre'] for g in games)))
    search = st.text_input("Suche nach Name")
    filtered = [g for g in games 
                if (genre == 'All' or genre in g['genre']) 
                and (not search or search.lower() in g['name'].lower())]
    st.dataframe(pd.DataFrame(filtered), use_container_width=True)

    # Export
    csv = df.to_csv(index=False, encoding='utf-8').encode('utf-8')
    st.download_button("📥 CSV Export", csv, "pc_games.csv", "text/csv")
    
    # Sort by Release
    df_sorted = df.sort_values('release')
    st.subheader("📅 Sortiert nach Release-Datum")
    st.dataframe(df_sorted)
else:
    st.warning("🔄 Doc-Link passt? Refresh oder check Format (sollte ## Headers + Name: ... haben).")

st.caption(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')} | Powered by Pokee.ai + Grok 3 – Jetzt mit 20+ Games! 🚀")
