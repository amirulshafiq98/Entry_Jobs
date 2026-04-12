from scraper import job_scrape_flow
from cleaning import job_clean_flow
from duckdb_layer import run_duckdb_layer
from prefect import flow

@flow
def full_pipeline():
    print("Starting scrape...")
    job_scrape_flow()

    print("Starting cleaning...")
    job_clean_flow()

    print("Starting SQL database...")
    run_duckdb_layer()

    print("Pipeline complete.")


if __name__ == "__main__":
    full_pipeline()

# if __name__ == "__main__":
#     full_pipeline.serve(
#         name="job-pipeline",
#         cron="0 9 * * *",
#         timezone="Asia/Singapore"
#     )