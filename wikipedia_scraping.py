import requests
from bs4 import BeautifulSoup
import re
import sqlite3

# Step 5 — connect
connection = sqlite3.connect("movies.db")

# Step 6 — cursor
cursor = connection.cursor()

# Step 7 — drop existing table to avoid duplicates
cursor.execute("""DROP TABLE IF EXISTS movies;""")

# Step 8 — create the movies table per ERD (id PK, title, worldwide_gross, year)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id INTEGER PRIMARY KEY,
        title TEXT,
        worldwide_gross INTEGER,
        year INTEGER
    );
""")

# >>> any further steps (scraping/inserts) go here, still above commit <<<

# keep this as the LAST line in the file
connection.commit()

