"""
WEBSITE JOB SCRAPER
"""
# - 200 = OK ✅
# - 404 = page not found ❌
# - 403 = blocked 🚫
# - 429 = rate-limit reached ❌

import pandas as pd
import time
from datetime import datetime
import requests
from prefect import task, flow, get_run_logger
import os
from dotenv import load_dotenv
import json

with open("config.json") as f:
    config = json.load(f)

@task(retries=3, retry_delay_seconds=5)
def job_portal_access(url):
    logger = get_run_logger()

    job_list = []
    MAX_PAGES = 5

    for page_num in range(0, MAX_PAGES):
        page_url = f"{url}?limit=20&page={page_num}"
        response = requests.post(page_url)

        logger.info(f"Processing page {page_num + 1}")

        if response.status_code == 429:
            logger.info("Hit rate limit inside loop, waiting...")
            time.sleep(10)
            continue
        elif response.status_code != 200:
            logger.info(response.text[:200])
            raise Exception(f"API failed on page {page_num + 1} with status {response.status_code}")

        data = response.json()

        job_list.extend(data['results'])

        if not data['results']:
            logger.info("No more results, stopping early.")
            break

        # print("Request URL:", page_url)
        # print("First job on this page:", data['results'][0]['metadata']['jobPostId'])

        time.sleep(1)

    return job_list


# ==============
# PART 2
# ==============
@task
def parse_jobs(job_list):
    rows = []

    for job in job_list:
        job_type = []
        for item in job['employmentTypes']:
            job_type.append(item['employmentType'])

        skills = []
        for item in job['skills']:
            skills.append(item['skill'])

        rows.append({
            "uuid": job['uuid'],
            "position": job['positionLevels'][0]['position'],
            'title': job['title'],
            "salary minimum": job['salary']['minimum'],
            "salary maximum": job['salary']['maximum'],
            "type" : job['salary']['type']['salaryType'],
            "employmentType": job_type,
            "categories": job['categories'][0]['category'],
            "updatedAt": job['metadata']['updatedAt'],
            "newPostingDate": job['metadata']['newPostingDate'],
            "skills": skills
        })

    return rows

@task
def build_dataframe(rows):
    df = pd.DataFrame(rows)
    current = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') 
    df['scrape_date'] = current

    df = df.dropna(subset='uuid')
    df = df[df['salary maximum'] > df['salary minimum']]

    return df

@task
def save_outputs(df):
    logger = get_run_logger()
    os.makedirs(config["paths"]["output"], exist_ok=True)
    os.makedirs(config["paths"]["batch"], exist_ok=True)

    current = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    df.to_csv(f'{config['paths']['batch']}batch_{current}.csv', index=False) # index=False prevents extra columns later
    logger.info(f"Created `batch_{current}.csv`")

    if not os.path.exists(f'{config['paths']['output']}scraped_jobs.csv'):
        df.to_csv(f'{config['paths']['output']}scraped_jobs.csv', index=False)
        
    master_df = pd.read_csv(f'{config['paths']['output']}scraped_jobs.csv')
    batch_df = pd.read_csv(f'{config['paths']['batch']}batch_{current}.csv')

    output = pd.concat([master_df, batch_df])
    output = output.drop_duplicates(subset='uuid', keep='last')

    output = output.reset_index(drop=True)

    output.to_csv(f"{config['paths']['output']}scraped_jobs.csv", index=False)
    logger.info(f"Appended to `scraped_jobs.csv`")

    # if os.path.exists(f'{config['paths']['batch']}scraped_jobs.csv'):
    #     os.remove(f'{config['paths']['batch']}scraped_jobs.csv')

    # output.to_csv(f'{config['paths']['batch']}scraped_jobs.csv', index=False)

    return output, master_df


@flow
def job_scrape_flow():
    logger = get_run_logger()
    load_dotenv()

    url = os.getenv("API_URL_1")
    logger.info('URL loaded')

    jobs = job_portal_access(url)
    logger.info(f"Fetched {len(jobs)} jobs")

    rows = parse_jobs(jobs)
    logger.info(f"Parsed {len(rows)} rows")

    df = build_dataframe(rows)
    logger.info(f"Dataframe has {len(df)} rows")

    output, master_df = save_outputs(df)
    logger.info(f"Added {len(output) - len(master_df)} jobs to `scraped_jobs.csv`")
    
    return df, output