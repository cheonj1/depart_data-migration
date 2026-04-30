import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.sql")

# FK 의존성 역순으로 드랍
DROP_SQL = """
DROP TABLE IF EXISTS
    client_sprint_notes,
    ig_content_insights,
    ig_contents,
    ig_insights_total,
    ig_insights_demographics,
    ig_organic_insights,
    ad_keywords,
    ad_performance_daily,
    ads,
    ad_sets,
    campaigns,
    ad_accounts,
    ig_accounts,
    business_portfolios,
    client_members,
    client_info,
    clients
CASCADE;
"""


def create_schema():
    url = os.getenv("depart_data_URL")
    if not url:
        raise ValueError("depart_data_URL is not set in .env")

    with open(SCHEMA_FILE, "r") as f:
        schema_sql = f.read()

    conn = psycopg2.connect(url)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            print("Dropping existing tables...")
            cur.execute(DROP_SQL)
            print("Creating schema...")
            cur.execute(schema_sql)
        conn.commit()
        print("Schema created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    create_schema()
