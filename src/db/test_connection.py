import os
import pyodbc
from dotenv import load_dotenv
from pathlib import Path

# Load .env relative to this file
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")

if not SERVER or not DATABASE:
    raise ValueError("Missing SQL_SERVER or SQL_DATABASE in .env")

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

def main():
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DB_NAME();")
            print("Connected to:", cur.fetchone()[0])

            cur.execute("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE='BASE TABLE'
                ORDER BY TABLE_NAME;
            """)
            tables = [row[0] for row in cur.fetchall()]
            print("Tables:", tables)

if __name__ == "__main__":
    main()
