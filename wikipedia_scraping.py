import re
import sqlite3
import requests
from bs4 import BeautifulSoup

# Step 5 — connect
connection = sqlite3.connect("movies.db")

# Step 6 — cursor
cursor = connection.cursor()

# Step 7 — drop existing table to avoid duplicates
cursor.execute("""DROP TABLE IF EXISTS movies;""")

# Step 8 — create the movies table per ERD (id PK, title, worldwide_gross, year)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        worldwide_gross INTEGER,
        year INTEGER
    );
""")

# Step 11 — scraper that returns a list of dicts with keys: title, worldwide_gross, year
def scrape_wikipedia():
    url = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the 'Highest-grossing films' table by caption, else fall back to first wikitable
    tables = soup.find_all("table", class_="wikitable")
    target = None
    for t in tables:
        cap = t.caption.get_text(strip=True).lower() if t.caption else ""
        if "highest-grossing films" in cap:
            target = t
            break
    if target is None and tables:
        target = tables[0]
    if target is None:
        return []

    rows = target.find_all("tr")
    if not rows:
        return []

    # Map headers to indexes to be robust to small layout changes
    header_cells = rows[0].find_all(["th", "td"])
    headers_norm = [hc.get_text(strip=True).lower() for hc in header_cells]

    def find_col(match):
        for i, h in enumerate(headers_norm):
            if match(h):
                return i
        return None

    idx_title = find_col(lambda h: "title" in h)
    idx_year  = find_col(lambda h: "year" in h)
    idx_gross = find_col(lambda h: "worldwide" in h and "gross" in h)

    # Typical fallback if headers weren’t found exactly
    if idx_title is None or idx_year is None or idx_gross is None:
        # Common layout: Rank | Title | Year | Worldwide gross | Ref(s)
        idx_title = 1 if idx_title is None else idx_title
        idx_year  = 2 if idx_year  is None else idx_year
        idx_gross = 3 if idx_gross is None else idx_gross

    only_digits = re.compile(r"\D+")

    results = []
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= max(idx_title, idx_year, idx_gross):
            continue

        # Title
        title = cells[idx_title].get_text(strip=True)

        # Year: first 4-digit number
        year_text = cells[idx_year].get_text(strip=True)
        m = re.search(r"\b(19|20)\d{2}\b", year_text)
        year = m.group(0) if m else ""   # <-- string

        # Worldwide gross: keep digits only, stripping $, commas, and stray letters like F/F8/T
        gross_text = cells[idx_gross].get_text(" ", strip=True)
        gross_digits = only_digits.sub("", gross_text)
        worldwide_gross = int(gross_digits) if gross_digits else 0

        if title and year and worldwide_gross:
            results.append({
                "title": title,
                "worldwide_gross": worldwide_gross,
                "year": year
            })

    return results

# Step 12 — insert scraped rows into the DB
movies = scrape_wikipedia()
for m in movies:
    cursor.execute(
        "INSERT INTO movies (title, worldwide_gross, year) VALUES (?, ?, ?)",
        (m["title"], m["worldwide_gross"], int(m["year"]))
    )

# Keep this as the very last line
connection.commit()
