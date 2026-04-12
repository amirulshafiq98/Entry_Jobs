import duckdb
import json
import os

with open("config.json") as f:
    config = json.load(f)


def create_schemas(conn):
    """
    Creating medallion schemas (bronze, silver, gold) at set up using config for file paths.
    """

    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # print(conn.execute("""
    # SELECT *
    # FROM information_schema.tables
    # WHERE table_schema = 'main'
    # """).df())

    # conn.execute("""
    # DROP TABLE table_name
    # """)

def upload_raw_tables(conn):
    """
    CSV dump of files into SQL database from cleaned jobs files
    """

    conn.execute(f"""\
    CREATE OR REPLACE TABLE bronze.clean_jobs AS
    SELECT * EXCLUDE (salary_midpoint),
    CAST(salary_midpoint AS DOUBLE) AS salary_midpoint 
    FROM read_csv_auto("{config['paths']['output']}clean_jobs.csv")
    """)

def exploded_skills_table(conn):
    """
    Create exploded skills table from cleaned jobs file so that only 1 skill is in 1 row.
    """

    conn.execute("""
    CREATE OR REPLACE TABLE silver.role_skills AS
    WITH exploded AS (
        SELECT
            role,
            UNNEST(
                string_split(
                    replace(
                        replace(
                            replace(skills, '[', ''),
                        ']', ''),
                    '''', ''),
                    ', '
                )
            ) AS skill
        FROM bronze.clean_jobs
    )

    SELECT
        role, skill,
        COUNT(*) as freq
    FROM exploded
    GROUP BY role, skill
    ORDER BY role ASC, freq DESC
    """)


def top5_skills_table(conn):
    """
    Group the top 5 most common skills from each role into 1 row.
    """

    conn.execute("""
    CREATE OR REPLACE TABLE silver.top5_skills AS
    WITH exploded AS (
        SELECT
            role,
            UNNEST(
                string_split(
                    replace(
                        replace(
                            replace(skills, '[', ''),
                        ']', ''),
                    '''', ''),
                    ', '
                )
            ) AS skill
        FROM bronze.clean_jobs
    ),

    freq_count AS (
    SELECT
        role, skill,
        COUNT(*) as freq
    FROM exploded
    GROUP BY role, skill
    ORDER BY role ASC, freq DESC
    ),
                    
    ranking AS (
    SELECT *,
    ROW_NUMBER() over (PARTITION BY role ORDER BY freq DESC) as rnk
    FROM freq_count
    )

    SELECT role,
    string_agg(skill, ', ' ORDER BY rnk) AS top_5_skills
    FROM ranking
    WHERE rnk <= 5
    GROUP BY role
    ORDER BY role ASC
    """)

def date_splicing_table(conn):
    """
    Slicing months based on posting and scraped date to determine number of months that jobs were up on the portal.
    """
    print("Creating timeframe tables...")

    conn.execute("""
    CREATE OR REPLACE TABLE silver.job_months AS
    SELECT *,
        strftime(
            UNNEST(
                generate_series(
                    date_trunc('month', CAST(newPostingDate AS DATE)),
                    date_trunc('month', CAST(scrape_date AS DATE)),
                    interval 1 month
                )
            ), '%Y-%m'
        ) AS active_month
    FROM bronze.clean_jobs
    """)

def date_categorisation_table(conn):
    """
    Group dates based on their preset timeframes (i.e. YTD, last 6 months, last year, last month).
    """

    conn.execute("""
    CREATE OR REPLACE TABLE silver.timeframe_table AS
    WITH ytd AS (
        SELECT
            'YTD' AS timeframe,
            role,
            AVG(salary_midpoint) AS avg_salary,
            MEDIAN(salary_midpoint) AS median_salary,
            COUNT(DISTINCT uuid) AS job_count
        FROM silver.job_months
        WHERE active_month >= strftime(date_trunc('year', current_date), '%Y-%m')
        AND active_month <= strftime(date_trunc('month', current_date), '%Y-%m')
        GROUP BY role
    ),

    last_month AS (
        SELECT
            'Last Month' AS timeframe,
            role,
            AVG(salary_midpoint) AS avg_salary,
            MEDIAN(salary_midpoint) AS median_salary,
            COUNT(DISTINCT uuid) AS job_count
        FROM silver.job_months
        WHERE active_month = strftime(
            date_trunc('month', current_date - INTERVAL 1 MONTH),
            '%Y-%m'
        )
        GROUP BY role
    ),

    last_year AS (
        SELECT
            'Last Year' AS timeframe,
            role,
            AVG(salary_midpoint) AS avg_salary,
            MEDIAN(salary_midpoint) AS median_salary,
            COUNT(DISTINCT uuid) AS job_count
        FROM silver.job_months
        WHERE active_month >= strftime(date_trunc('year', current_date - INTERVAL 1 YEAR), '%Y-%m')
        AND active_month <  strftime(date_trunc('year', current_date), '%Y-%m')
        GROUP BY role
    ),

    last_six AS (
        SELECT
            'Last 6 Months' AS timeframe,
            role,
            AVG(salary_midpoint) AS avg_salary,
            MEDIAN(salary_midpoint) AS median_salary,
            COUNT(DISTINCT uuid) AS job_count
        FROM silver.job_months
        WHERE active_month >= strftime(date_trunc('month', current_date - INTERVAL 5 MONTH), '%Y-%m')
        AND active_month <= strftime(date_trunc('month', current_date), '%Y-%m')
        GROUP BY role
    )

    SELECT * FROM ytd
    UNION ALL
    SELECT * FROM last_month
    UNION ALL
    SELECT * FROM last_year
    UNION ALL
    SELECT * FROM last_six
    ORDER BY role, timeframe;
    """)

def collated_output_tables(conn):       
    """
    TABLE 1: Filtered collated tables by job counts to minimise roles that paid outlier amounts for fresh/entry level roles. 
    TABLE 2: Created a separate table that segregates median and average salary by individual months to faciliate deeper level insights.
    """ 

    conn.execute("""
    CREATE OR REPLACE TABLE gold.combined_output AS                
    SELECT t.role, CAST(t.avg_salary AS INTEGER) AS avg_salary, CAST(t.median_salary AS INTEGER) as median_salary, s.top_5_skills, t.job_count, t.timeframe
    FROM silver.timeframe_table t
    LEFT JOIN silver.top5_skills s
    ON s.role = t.role
    WHERE t.job_count >= 5
    ORDER BY t.avg_salary DESC
    """)
  
    conn.execute("""
    CREATE OR REPLACE TABLE gold.monthly_output AS
    SELECT t.role,
    CAST(AVG(t.salary_midpoint) AS INTEGER) AS avg_salary,
    CAST(MEDIAN(t.salary_midpoint) AS INTEGER) AS median_salary,
    s.top_5_skills, 
    COUNT(DISTINCT t.uuid) AS job_count, 
    t.active_month
    FROM silver.job_months t
    LEFT JOIN silver.top5_skills s
    ON s.role = t.role
    GROUP BY t.active_month, t.role, s.top_5_skills
    HAVING COUNT(DISTINCT t.uuid) >= 5
    ORDER BY avg_salary DESC 
    """)

def convert_to_csv(conn):
    """
    Exports final gold table to CSV for downstream visualisation.
    """

    file_path = f"{config['paths']['output']}graph_ready.csv"

    file_ver = 'gold.combined_output'
    # file_ver = 'gold.monthly_output'

    conn.execute(f"""
    COPY {file_ver} 
    TO '{file_path}' 
    (HEADER, DELIMITER ',')
    """)

def run_duckdb_layer():
    try: 
        db_path = config["paths"]["db"]
        db_folder = os.path.dirname(db_path)
        os.makedirs(db_folder, exist_ok=True)
        conn = duckdb.connect(db_path)

        print("Creating medallion schemas...")
        create_schemas(conn)

        print("Uploading CSVs into SQL database...")
        upload_raw_tables(conn)
        
        print("Creating skills table...")
        exploded_skills_table(conn)
        top5_skills_table(conn)

        print("Spilcing dates...")
        date_splicing_table(conn)
        date_categorisation_table(conn)

        print("Creating gold table...")
        collated_output_tables(conn)

        convert_to_csv(conn)
        print("Analysis-ready CSV created in `output`")

    finally:
        conn.close()