import os
import pyodbc
from dotenv import load_dotenv
from src.ingest.ch_client import company_profile

load_dotenv()

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

def upsert_company(cur, c: dict) -> None:
    company_number = c.get("company_number")
    company_name = c.get("company_name")
    status = c.get("company_status")
    inc_date = c.get("date_of_creation")
    company_type = c.get("type")

    cur.execute("""
        MERGE companies AS tgt
        USING (SELECT ? AS company_number) AS src
        ON tgt.company_number = src.company_number
        WHEN MATCHED THEN
            UPDATE SET
                company_name = ?,
                company_status = ?,
                incorporation_date = ?,
                company_type = ?
        WHEN NOT MATCHED THEN
            INSERT (company_number, company_name, company_status, incorporation_date, company_type)
            VALUES (?, ?, ?, ?, ?);
    """, company_number, company_name, status, inc_date, company_type,
         company_number, company_name, status, inc_date, company_type)

def insert_address(cur, company_number: str, addr: dict) -> None:
    locality = addr.get("locality")
    region = addr.get("region")
    postal_code = addr.get("postal_code")
    country = addr.get("country")

    # Simple approach for now: delete existing address rows for the company then insert 1 current row
    cur.execute("DELETE FROM company_addresses WHERE company_number = ?;", company_number)

    cur.execute("""
        INSERT INTO company_addresses (company_number, locality, region, postal_code, country)
        VALUES (?, ?, ?, ?, ?);
    """, company_number, locality, region, postal_code, country)

def upsert_sic(cur, company_number: str, sic_list: list[str]) -> None:
    # Ensure join table stays clean
    cur.execute("DELETE FROM company_sic WHERE company_number = ?;", company_number)

    for sic in sic_list:
        # We don’t have official descriptions from company profile; keep description NULL for now
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sic_codes WHERE sic_code = ?)
                INSERT INTO sic_codes (sic_code, description) VALUES (?, NULL);
        """, sic, sic)

        cur.execute("""
            INSERT INTO company_sic (company_number, sic_code)
            VALUES (?, ?);
        """, company_number, sic)

def log_run(cur, inserted: int, status: str) -> None:
    cur.execute("""
        INSERT INTO ingestion_log (records_inserted, source, status)
        VALUES (?, ?, ?);
    """, inserted, "Companies House API", status)

def main():
    # A known test company number (you can change later)
    company_number = "00006400"  # Example: should exist

    c = company_profile(company_number)

    with pyodbc.connect(conn_str) as conn:
        try:
            cur = conn.cursor()
            upsert_company(cur, c)

            addr = c.get("registered_office_address", {}) or {}
            insert_address(cur, company_number, addr)

            sic_list = c.get("sic_codes", []) or []
            upsert_sic(cur, company_number, sic_list)

            log_run(cur, inserted=1, status="success")
            conn.commit()
            print(f"Inserted/updated company {company_number} successfully.")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            log_run(cur, inserted=0, status="failure")
            conn.commit()
            raise

if __name__ == "__main__":
    main()
