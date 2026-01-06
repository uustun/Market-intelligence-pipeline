from __future__ import annotations

import os
import time
from typing import Optional

from src.db.connection import get_conn
from src.ingest.ch_client import advanced_search_companies

# --- Target universe (Luton radius + Milton Keynes) ---
LOCATIONS = [
    "Luton",
    "Dunstable",
    "St Albans",
    "Hemel Hempstead",
    "Stevenage",
    "Hitchin",
    "Harpenden",
    "Leighton Buzzard",
    "Milton Keynes",
]

# --- Filters ---
SIC_CODES = ["62020", "62012"]


# Default to 2018..2025 unless overridden by env vars
MIN_YEAR = int(os.getenv("MIN_YEAR", "2018"))
MAX_YEAR = int(os.getenv("MAX_YEAR", "2025"))  # inclusive

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "500"))  # total cap across all locations

# --- DB / resilience knobs ---
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "50"))
MAX_CONSECUTIVE_PAGE_ERRORS = int(os.getenv("MAX_CONSECUTIVE_PAGE_ERRORS", "3"))
SLEEP_ON_ERROR_SECONDS = float(os.getenv("SLEEP_ON_ERROR_SECONDS", "1.0"))


def parse_year(d: Optional[str]) -> Optional[int]:
    """Parse 'YYYY-MM-DD' -> YYYY. Returns None if missing/invalid."""
    if not d or len(d) < 4:
        return None
    try:
        return int(d[:4])
    except ValueError:
        return None


def upsert_company_from_item(cur, it: dict) -> None:
    company_number = it.get("company_number")
    company_name = it.get("company_name") or it.get("title")
    status = it.get("company_status")
    inc_date = it.get("date_of_creation")
    company_type = it.get("company_type")

    cur.execute(
        """
        MERGE dbo.companies AS tgt
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
        """,
        company_number,
        company_name,
        status,
        inc_date,
        company_type,
        company_number,
        company_name,
        status,
        inc_date,
        company_type,
    )


def replace_address_from_item(cur, company_number: str, it: dict) -> None:
    addr = it.get("registered_office_address") or {}

    cur.execute("DELETE FROM dbo.company_addresses WHERE company_number = ?;", company_number)

    cur.execute(
        """
        INSERT INTO dbo.company_addresses (company_number, locality, region, postal_code, country)
        VALUES (?, ?, ?, ?, ?);
        """,
        company_number,
        addr.get("locality"),
        addr.get("region"),
        addr.get("postal_code"),
        addr.get("country"),
    )


def replace_sic(cur, company_number: str, sic_list: list[str], existing_sic: set[str]) -> None:
    cur.execute("DELETE FROM dbo.company_sic WHERE company_number = ?;", company_number)

    for sic in (sic_list or []):
        if sic not in existing_sic:
            continue
        cur.execute(
            "INSERT INTO dbo.company_sic (company_number, sic_code) VALUES (?, ?);",
            company_number,
            sic,
        )


def log_run(cur, inserted: int, status: str, note: str) -> None:
    cur.execute(
        """
        INSERT INTO dbo.ingestion_log (records_inserted, source, status)
        VALUES (?, ?, ?);
        """,
        inserted,
        f"Companies House API (advanced search) - {note}",
        status,
    )


def main() -> None:
    inserted_total = 0
    scanned_total = 0

    incorporated_from = f"{MIN_YEAR}-01-01"
    incorporated_to = f"{MAX_YEAR}-12-31"

    note = (
        f"locations={len(LOCATIONS)} | sic={','.join(SIC_CODES)} | "
        f"incorporated {incorporated_from}..{incorporated_to} | cap {MAX_RECORDS}"
    )

    with get_conn() as conn:
        cur = conn.cursor()

        # Load SIC dimension once
        cur.execute("SELECT sic_code FROM dbo.sic_codes;")
        existing_sic = {row[0] for row in cur.fetchall()}

        try:
            for loc in LOCATIONS:
                if inserted_total >= MAX_RECORDS:
                    break

                print(f"\n=== Location: {loc} ===")
                start_index = 0
                consecutive_page_errors = 0

                while inserted_total < MAX_RECORDS:
                    try:
                        data = advanced_search_companies(
                            location=loc,
                            sic_codes=SIC_CODES,
                            start_index=start_index,
                            size=PAGE_SIZE,
                            company_status="active",
                            incorporated_from=incorporated_from,
                            incorporated_to=incorporated_to,
                        )
                    except Exception as e:
                        consecutive_page_errors += 1
                        print(
                            f"API error at {loc} start_index={start_index} "
                            f"(consecutive={consecutive_page_errors}). Error: {e}"
                        )

                        if consecutive_page_errors >= MAX_CONSECUTIVE_PAGE_ERRORS:
                            print(f"Too many API failures for {loc}. Moving to next location.")
                            break

                        time.sleep(SLEEP_ON_ERROR_SECONDS)
                        start_index += PAGE_SIZE
                        continue

                    consecutive_page_errors = 0

                    items = data.get("items", []) or []
                    hits = data.get("hits")
                    print(f"Fetched page start_index={start_index} | items={len(items)} | hits={hits}")

                    if not items:
                        break

                    last_page = len(items) < PAGE_SIZE

                    for it in items:
                        scanned_total += 1
                        if inserted_total >= MAX_RECORDS:
                            break

                        # Defensive check (API should already enforce)
                        y = parse_year(it.get("date_of_creation"))
                        if y is None or y < MIN_YEAR or y > MAX_YEAR:
                            continue

                        number = it.get("company_number")
                        if not number:
                            continue

                        if inserted_total == 0:
                            print("Starting first insert...")

                        upsert_company_from_item(cur, it)
                        replace_address_from_item(cur, number, it)
                        replace_sic(cur, number, it.get("sic_codes", []) or [], existing_sic)

                        inserted_total += 1

                        if inserted_total % COMMIT_EVERY == 0:
                            conn.commit()
                            print(f"Committed {inserted_total} records so far")

                        if inserted_total % 50 == 0:
                            print(f"Inserted {inserted_total}/{MAX_RECORDS} (scanned {scanned_total})")

                    if last_page:
                        break

                    if isinstance(hits, int) and (start_index + len(items) >= hits):
                        break

                    start_index += PAGE_SIZE

            log_run(cur, inserted=inserted_total, status="success", note=note)
            conn.commit()
            print(f"\nDone. Inserted/updated: {inserted_total} (scanned: {scanned_total})")

        except Exception:
            conn.rollback()
            cur = conn.cursor()
            log_run(cur, inserted=inserted_total, status="failure", note=note)
            conn.commit()
            raise


if __name__ == "__main__":
    main()
