"""
CLEANING FILE
"""

import pandas as pd
import os
import re
from prefect import get_run_logger, flow, task
from datetime import datetime
import json

with open("config.json") as f:
    config = json.load(f)

def clean_title(text):
    """
    Purpose is to remove likely scam postings and standardise the job title delimiter (e.g. / or ,) to faciliate splitting of roles downstream
    """
    text = str(text)

    # =========================
    # 1. Remove emojis / weird symbols
    # =========================
    text = re.sub(r"[^\x00-\x7F]+", " ", text)

    # =========================
    # 2. Remove text inside brackets
    # =========================
    text = re.sub(r"\(.*?\)", " ", text)
    text = re.sub(r"\[.*?\]", " ", text)

    # =========================
    # 3. Remove code-like prefixes only
    # Examples:
    # AB03 - Admin Assistant
    # 11/ HR NO INTERVIEW ...
    # =========================
    text = re.sub(r"^[A-Z]{1,5}\d{1,5}\s*-\s*", "", text)
    text = re.sub(r"^\d+\s*/\s*", "", text)

    # =========================
    # 4. Remove hashtags like #HYTT
    # =========================
    text = re.sub(r"#\w+", " ", text)
    text = re.sub(r"^[^\w]+", "", text)   # remove leading junk like !, !!
    text = re.sub(r"[-\s]+$", "", text)   # remove trailing '-' and spaces

    # =========================
    # 5. Remove obvious promo phrases
    # =========================
    remove_phrases = [
        "no interview",
        "fresh grad",
        "entry level",
        "travel opportunities",
        "travelling opportunities",
        "immediate hiring",
        "urgent hiring",
        "urgent",
        "hiring now",
        'fast hire'
    ]

    for phrase in remove_phrases:
        text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text, flags=re.IGNORECASE)

    # =========================
    # 6. Remove suffixes like "5.5 days"
    # =========================
    text = re.sub(r"\b\d+(\.\d+)?\s*days?\b", " ", text, flags=re.IGNORECASE)

    # =========================
    # 7. Remove asterisks
    # =========================
    text = text.replace("*", " ")

    # =========================
    # 8. Normalize separators carefully
    # =========================
    text = text.replace("|", " ")

    # =========================
    # 9. Normalize spaces
    # =========================
    text = re.sub(r"\s+", " ", text)

    return text.strip()


@task
def file_setup():
    """
    Load scraped job file and apply clean title function
    """
    os.makedirs(config['paths']['output'], exist_ok=True)

    df = pd.read_csv(f'{config['paths']['output']}scraped_jobs.csv')

    # 1. Fill empty titles with empty text first to prevent errors
    df["title"] = df["title"].fillna("")

    # 2. Run your cleaning function
    df["title_clean"] = df["title"].apply(clean_title)

    # 3. Capitalize the first letter of every word
    df["title_clean"] = df["title_clean"].str.title()

    return df


@task
def entry_filter(df):
    """
    Filter out jobs that are junior based on the `position` column and split the job title based on the delimiter
    """
    # Entry level mask
    entry_lvl = df['position'].isin(['Fresh/entry level', 'Junior Executive', 'Executive'])

    # Salary mid-point
    df = df.rename(columns={'salary minimum':'salary_minimum','salary maximum': 'salary_maximum'})
    df['salary_midpoint'] = ((df['salary_minimum'] + df['salary_maximum']) / 2).astype(int)

    # Sort values
    df = df.dropna(subset='salary_midpoint')
    sorted = df[entry_lvl].sort_values('salary_midpoint', ascending=False)

    # Role column creation
    sorted['role'] = sorted['title_clean'].str.split('/')

    return sorted


@task
def remove_senior(df):
    """
    Removed any mention of `senior` from the splitted role column to reduce errors in grouping downstream
    """
    senior = (df['role'].str.len() == 1) & (df['role'].str[0].fillna('').str.contains('senior', case=False))

    kept = df[~senior].copy()

    kept['role']= kept['role'].apply(lambda x: [item.strip() for item in x if 'senior' not in item.lower()])

    # Checking of senior roles
    # mask = kept[kept['title_clean'].str.contains('senior',case=False)]

    kept['role'] = kept['role'].apply(
    lambda x: [
        item.strip()
        for item in x
        if re.match(r'^[A-Za-z\s&/+-]+$', item.strip())
    ]
    )

    kept = kept[kept['role'].str.len() > 0]

    return kept


@task
def trim_df(df):
    """
    Remove the jobs that are very low paying or charge hourly
    """
    df['newPostingDate'] = pd.to_datetime(df['newPostingDate'], errors='coerce')
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], format='%Y-%m-%d_%H-%M-%S', errors='coerce')

    df['newPostingDate'] = df['newPostingDate'].dt.strftime('%Y-%m-%d')
    df['scrape_date'] = df['scrape_date'].dt.strftime('%Y-%m-%d')

    clean = df.dropna(subset=['newPostingDate', 'scrape_date']).copy()

    clean = clean.explode('role').reset_index(drop=True)
    clean = clean[clean['type'] == 'Monthly'] 
    clean = clean[clean['salary_maximum'] >= 2000]
    clean = clean.dropna()

    return clean


@task
def create_snapshot(file):
    """
    Generate a history snapshot of all scraped jobs for more compact storage every run cycle
    """
    logger = get_run_logger()
    
    if not os.path.exists(file):
        return None
    else:
        os.makedirs(f"{config['paths']['snapshot']}", exist_ok=True)

    curr_ver = pd.read_csv(file)
    file_path = f"snapshot_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet"

    curr_ver.to_parquet(f"{config['paths']['snapshot']}{file_path}", engine="pyarrow", index=False)
    os.remove(file)

    logger.info(f"Removed `{file}` from `output`")

    return file_path


@flow
def job_clean_flow():
    logger = get_run_logger()

    setup = file_setup()
    logger.info("Title cleaning complete")

    entry_level = entry_filter(setup)
    remove_rows = remove_senior(entry_level)
    trim = trim_df(remove_rows)
    logger.info("Filtering complete")

    file = f'{config['paths']['output']}clean_jobs.csv'
    file_path = create_snapshot(file)
    if file_path:
        logger.info(f"Snapshot saved as {file_path}")
    else:
        logger.info("No previous clean file found, skipping snapshot")

    # exploded = explode_date_range(trim)
    trim.to_csv(f'{config['paths']['output']}clean_jobs.csv', index=False)
    logger.info(f"File saved in `output` folder")

    # return exploded
    return trim