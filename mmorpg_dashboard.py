import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd

# Dein Published Doc-Link (ersetze ABC123!)
DOC_URL = "https://docs.google.com/document/d/e/2PACX-1vQWBQ8aViVqo5N0T7BvZdd2nGXrnmjvcyfUyZot2wZcCyuBgB-TgYLaBbOVId_027LA4tDJZ6fWG3ry/pub"  # Plain text oder HTML

@st.cache_data(ttl=1800)  # Cache 30min
def fetch_games_from_doc():
    try:
        response = requests.get(DOC_URL)
        text = BeautifulSoup(response.text, 'html.parser').get_text()  # Clean text
        # Split auf Games (jeder Block: 1.\n2.\n3.\n4.\n5.)
        blocks = re.split(r'\n\s*\d+\.\s*(?=\d+\.)', text)  # Regex für numbered blocks
        games = []
        for block in blocks:
            lines = [line.strip() for line in block.split('\n') if line.strip()]
            if len(lines) >= 5:
                games.append({
                    'name': lines[0].replace('1. ', ''),
                    'release': lines[1].replace('2. ', ''),
                    'genre': lines[2].replace('3. ', ''),
                    'desc': ' '.join(lines[3].replace('4. ', '').split()[:50]),  # 2 Sätze kürzen
                    'quelle': lines[4].replace('5. ', '')
                })
        return games[:20]  # Top 20
    except Exception as e:
        st.error(f"Doc fail: {e}")
        return []  # Fallback empty

st.set_page_config(page_title="PC Game Releases Hub", page_icon="🎮", layout="wide")
st.title("🎮 PC Game Releases – Täglich Frisch aus Pokee.ai!")

if st.button("🔥 REFRESH FROM DOC", type="primary"):
    with st.spinner("Lade aus Google Docs..."):
        games = fetch_games_from_doc()
        st.session_state.games = games
    st.success(f"✅ {len(games)} Games geladen!")

games = st.session_state.get('games', fetch_games_from_doc())

if games:
    df = pd.DataFrame(games)
    st.dataframe(df, use_container_width=True)

    # Pretty Cards
    cols = st.columns(3)
    for i, game in enumerate(games):
        with cols[i % 3]:
            with st.expander(f"**{game['name']}** | {game['release']} | {game['genre']}"):
                st.write(game['desc'])
                st.caption(f"Quelle: [Link]({game['quelle']})")

    # Filter
    genre = st.selectbox("Filter Genre", options=['All'] + list(set(g['genre'] for g in games)))
    filtered = [g for g in games if genre == 'All' or genre in g['genre']]
    st.subheader(f"{len(filtered)} Games ({genre})")
    st.dataframe(pd.DataFrame(filtered))

    # Export
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 CSV Export", csv, "pc_games.csv", "text/csv")
else:
    st.warning("🔄 Paste deinen Published Doc-Link oben & refresh!")

st.caption(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')} | Powered by Pokee.ai + Grok 3")
