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

# --- RSS Scraper mit requests + BS4 (KEIN feedparser!) ---
def scrape_rss_xml(url, source_name):
    items = []
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENTS[0]}, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')  # XML-Parser!
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

# --- Games Scraper (MMORPG.com Features) ---
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
       
