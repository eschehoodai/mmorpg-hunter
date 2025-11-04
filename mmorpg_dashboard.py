import os
import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd

# ----------------------------
# Konfiguration: setze hier deine signierte GCS-URL oder eine andere öffentlich erreichbare HTML-Datei
# Beispiel: DOC_URL = "https://storage.googleapis.com/....html?X-Goog-Algorithm=..."
# Wenn du stattdessen eine lokale Datei verwenden willst, lasse DOC_URL leer und kopiere die Datei
# published_doc.html in das gleiche Verzeichnis wie dieses Script.
# ----------------------------
DOC_URL = "https://storage.googleapis.com/pokee-api-bucket/user_350nB0KCk3rbjsg6f3VE8EMa63v/021c9724-294c-4c28-9de4-938f11c8ae8b/html_report.html?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=pokee-storage-access%40verdant-option-419105.iam.gserviceaccount.com%2F20251104%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20251104T230505Z&X-Goog-Expires=604799&X-Goog-SignedHeaders=host&X-Goog-Signature=67951e345bfcf3f96ce6d56502a969e467b255ff08c4205b3b596b98047a33c2a107b23d2780a264d8215c50e2e4a70eb740a96be303829be010375badf161d46c425157a118a686e58eca1806c6992012e09c34b9cd9c370a3651ff8521d118a83b01d1e0965e7785037d59e44e1dceae8048bb22e3e806bce9f705e63b457ec0a327f902d8903a9c38cfcd39355f796d52b34a0c199c7bec5d4230b92653491dbc89653b39901818b9cde279ae078b531730e850190ecc65aab8a13b5d485c0aa7af835c06fac34a3cd44a15241d707b841540ee3c391514095b807af6296cd70beea2531c864f0cb2b91acf2800589a075b3965f87bef90131650de256a95"  # <-- PASTE_HIER_DEINEN_SIGNED_URL (oder leer lassen, wenn du published_doc.html lokal ablegst)
LOCAL_CACHE = "published_doc.html"
# ----------------------------

def fetch_remote_and_cache(url: str, local_path: str = LOCAL_CACHE, force_download: bool = False, timeout: int = 15):
    """
    Lädt die remote HTML-Datei herunter (wenn URL angegeben) und speichert sie lokal als Cache.
    Falls die lokale Datei vorhanden ist und force_download == False, wird die lokale Datei verwendet.
    Rückgabe: (html_text, status_code)
    """
    # Wenn keine URL gesetzt ist, versuche lokale Datei zu lesen
    if not url:
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8") as f:
                return f.read(), 200
        raise RuntimeError("Keine DOC_URL gesetzt und lokale Datei not found: " + local_path)

    # Falls lokale Cache existiert und keine Forcierung, nutze sie
    if os.path.exists(local_path) and not force_download:
        try:
            with open(local_path, "r", encoding="utf-8") as f:
                return f.read(), 200
        except Exception:
            # Falls Lesen fehlschlägt, fahre fort und lade neu
            pass

    # Lade remote
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
    r.raise_for_status()
    html = r.text
    # Schreibe Cache
    try:
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        # Falls Schreiben fehlschlägt, trotzdem HTML zurückgeben
        pass
    return html, r.status_code

@st.cache_data(ttl=1800)
def fetch_games_from_doc(force_download: bool = False):
    """
    Lädt (ggf. aus Cache) das HTML herunter/liest aus LOCAL_CACHE und parsed die Spiele.
    force_download True -> zwingt erneutes Herunterladen (bypass local cache).
    """
    try:
        html, status = fetch_remote_and_cache(DOC_URL, LOCAL_CACHE, force_download=force_download)
        # Speichere zuletzt ermittelten HTTP-Status für Debug-UI
        try:
            st.session_state['_last_fetch_status'] = status
        except Exception:
            pass

        soup = BeautifulSoup(html, 'html.parser')

        # Priorisiere Google-Docs-artige Struktur: Suche nach #contents div und h2-Elementen
        contents = soup.find('div', id='contents') or soup
        headers = contents.find_all('h2')
        games = []
        seen = set()

        if headers:
            for h in headers:
                title = h.get_text(" ", strip=True)
                # Sammle folgende <p> bis zur nächsten h2
                field_paragraphs = []
                for sib in h.next_siblings:
                    if getattr(sib, 'name', None) == 'h2':
                        break
                    if getattr(sib, 'name', None) == 'p':
                        text = sib.get_text(" ", strip=True)
                        if text:
                            field_paragraphs.append(sib)

                # Parse Felder aus den <p> (häufig: <span class="c1">Name:</span><span> Wert</span>)
                parsed = {}
                for p in field_paragraphs:
                    spans = p.find_all('span')
                    if len(spans) >= 2:
                        key = spans[0].get_text(" ", strip=True).rstrip(':').strip()
                        val = " ".join(s.get_text(" ", strip=True) for s in spans[1:]).strip()
                    else:
                        full = p.get_text(" ", strip=True)
                        m = re.match(r'^(.*?):\s*(.+)$', full)
                        if m:
                            key, val = m.group(1).strip(), m.group(2).strip()
                        else:
                            continue
                    parsed[key.lower()] = val

                # Name fallback: parsed name or h2 title
                name = parsed.get('name') or title or parsed.get('titel')
                if not name:
                    continue
                if name in seen:
                    continue
                seen.add(name)

                desc_text = parsed.get('zusammenfassung') or parsed.get('beschreibung') or ''
                if desc_text and len(desc_text) > 400:
                    desc_text = desc_text[:400].rstrip() + '...'

                game = {
                    'name': name,
                    'release': parsed.get('erscheinungsdatum') or parsed.get('release') or 'TBD',
                    'genre': parsed.get('genre') or 'Unknown',
                    'desc': desc_text or 'No desc.',
                    'quelle': parsed.get('quelle') or 'Unknown'
                }
                games.append(game)

        # Fallback: falls keine h2 gefunden oder kein Ergebnis -> altes Text-Fallback (## Split)
        if not games:
            text = soup.get_text('\n')
            text = re.sub(r'Neueste Online-PC-Spiele.*?(?=##)', '', text, flags=re.DOTALL | re.IGNORECASE)
            sections = re.split(r'\n##\s*', text)[1:]
            seen_titles = set()
            for section in sections:
                lines = [l.strip() for l in section.split('\n') if l.strip()]
                if not lines:
                    continue
                name = re.search(r'Name:\s*(.+?)(?=\n|$)', section, flags=re.IGNORECASE)
                release = re.search(r'Erscheinungsdatum:\s*(.+?)(?=\n|$)', section, flags=re.IGNORECASE)
                genre = re.search(r'Genre:\s*(.+?)(?=\n|$)', section, flags=re.IGNORECASE)
                desc = re.search(r'Zusammenfassung:\s*(.+?)(?=\nQuelle:|$)', section, re.DOTALL | re.IGNORECASE)
                quelle = re.search(r'Quelle:\s*(.+?)(?=\n|$)', section, flags=re.IGNORECASE)
                title = name.group(1).strip() if name else lines[0].strip()
                if title in seen_titles:
                    continue
                seen_titles.add(title)
                desc_val = (desc.group(1).strip()[:400] + '...') if desc else 'No desc.'
                games.append({
                    'name': title,
                    'release': release.group(1).strip() if release else 'TBD',
                    'genre': genre.group(1).strip() if genre else 'Unknown',
                    'desc': desc_val,
                    'quelle': quelle.group(1).strip() if quelle else 'Unknown'
                })

        # Optional: limitieren, falls zu viele Einträge
        return games[:200]
    except Exception as e:
        # Wir geben eine klare Fehlermeldung zurück, damit die UI sie anzeigt.
        raise RuntimeError(f"Fehler beim Abrufen/Parsen des Dokuments: {e}") from e

# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="PC Game Releases Hub", page_icon="🎮", layout="wide")
st.title("🎮 PC Game Releases – Täglich Frisch aus Pokee.ai!")

# Debug-Optionen in Sidebar
show_debug = st.sidebar.checkbox("🔧 Debug: Roh-HTML & Status anzeigen", value=False)
st.sidebar.caption("Wenn nichts geladen wird, aktiviere Debug. Du kannst auch eine lokale published_doc.html verwenden.")

# Button: Refresh from remote (force download)
if st.button("🔥 REFRESH FROM DOC", type="primary"):
    with st.spinner("Lade aus URL / Cache..."):
        try:
            games = fetch_games_from_doc(force_download=True)
            st.session_state.games = games
            st.success(f"✅ {len(games)} Games geladen!")
        except Exception as e:
            st.error(str(e))

# Erstlade-Logik: falls keine games in session_state, lade (aus evtl. lokalem Cache)
if 'games' not in st.session_state:
    try:
        st.session_state.games = fetch_games_from_doc(force_download=False)
    except Exception as e:
        st.session_state.games = []
        if show_debug:
            st.error(str(e))

# Debug: zeige HTTP-Status + Ausschnitt des lokalen Caches / Remote-Inhalts
if show_debug:
    status = st.session_state.get('_last_fetch_status', None)
    if status is not None:
        st.write(f"HTTP Status der letzten Abfrage: {status}")
    else:
        st.write("Kein HTTP-Status verfügbar (noch nicht heruntergeladen).")

    # Versuche den lokalen Cache zu zeigen (besser für Signed-URL-Verfall)
    if os.path.exists(LOCAL_CACHE):
        try:
            with open(LOCAL_CACHE, "r", encoding="utf-8") as f:
                html_sample = f.read(4000)
            st.code(html_sample, language='html')
        except Exception as e:
            st.error(f"Fehler beim Lesen des lokalen Caches: {e}")
    else:
        # Falls kein lokaler Cache vorhanden ist, versuche eine direkte fetch (nur zur Ansicht)
        if DOC_URL:
            try:
                r = requests.get(DOC_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
                st.write(f"Direkter Fetch HTTP Status: {r.status_code}")
                st.code(r.text[:4000], language='html')
            except Exception as e:
                st.error(f"Direkter Fetch fehlgeschlagen: {e}")
        else:
            st.info("Keine DOC_URL gesetzt und kein lokaler Cache vorhanden.")

games = st.session_state.get('games', [])

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
                st.caption(f"🔗 {game['quelle']}")

    # Filter
    st.subheader("🔍 Filter & Suche")
    genre_options = ['All'] + sorted({g['genre'] for g in games})
    genre = st.selectbox("Genre filtern", options=genre_options)
    search = st.text_input("Suche nach Name")
    filtered = [g for g in games
                if (genre == 'All' or genre in g['genre'])
                and (not search or search.lower() in g['name'].lower())]
    st.dataframe(pd.DataFrame(filtered), use_container_width=True)

    # Export
    csv = df.to_csv(index=False, encoding='utf-8').encode('utf-8')
    st.download_button("📥 CSV Export", csv, "pc_games.csv", "text/csv")

    # Sort by Release (TBD entries bleiben am Ende)
    try:
        df_sorted = df.sort_values('release')
    except Exception:
        df_sorted = df
    st.subheader("📅 Sortiert nach Release-Datum")
    st.dataframe(df_sorted)
else:
    st.warning("🔄 Doc nicht gefunden oder keine Spiele geparst. Prüfe DOC_URL oder lege published_doc.html lokal ab.")

st.caption(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')} | Powered by Pokee.ai + Grok 3 – Jetzt mit aktuellen Releases! 🚀")
