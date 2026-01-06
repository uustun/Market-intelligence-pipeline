import os
import pyodbc
from dotenv import load_dotenv
from pathlib import Path

# (repo-safe)
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

def get_conn():
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    if not server or not database:
        raise ValueError("Missing SQL_SERVER or SQL_DATABASE in .env")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)
