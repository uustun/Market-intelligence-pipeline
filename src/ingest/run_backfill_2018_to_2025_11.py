from __future__ import annotations

import os
from datetime import date
from typing import Optional

from src.db.connection import get_conn
from src.ingest.ch_client import advanced_search_companies

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

# Recommend: keep it tight but meaningful
SIC_CODES = ["62020", "62012", "62090"]

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "200000"))
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "200"))

BACKFILL_FROM = date(2018, 1, 1)
BACKFILL_TO = date(2025, 11, 30)  # inclusive


def start_run(cur, note: str) -> int:
    cur.execute(
        """
        INSERT INTO dbo.ingestion_log (records_inserted, source, status)
        OUTPUT INSERTED.run_id
        VALUES (?, ?, ?);
        """,
        0,
        note,
        "running",
    )
    row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("start_run(): could not retrieve run_id from ingestion_log insert")
    return int(row[0])


def finish_run(cur, run_id: int, status: str, records_inserted: int) -> None:
    cur.execute(
        """
        UPDATE dbo.ingestion_log
        SET status = ?, records_inserted = ?
        WHERE run_id = ?;
        """,
        status,
        records_inserted,
        run_id,
    )


def upsert_company(cur, it: dict, run_id: int) -> None:
    number = it.get("company_number")
    name = it.get("company_name") or it.get("title")
    status = it.get("company_status")
    inc_date = it.get("date_of_creation")
    ctype = it.get("company_type")

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
                company_type = ?,
                last_seen_run_id = ?,
                last_seen_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                company_number, company_name, company_status, incorporation_date, company_type,
                first_seen_run_id, last_seen_run_id, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """,
        number, name, status, inc_date, ctype, run_id,
        number, name, status, inc_date, ctype, run_id, run_id,
    )


def replace_address(cur, number: str, it: dict) -> None:
    addr = it.get("registered_office_address") or {}

    cur.execute("DELETE FROM dbo.company_addresses WHERE company_number = ?;", number)
    cur.execute(
        """
        INSERT INTO dbo.company_addresses (company_number, locality, region, postal_code, country)
        VALUES (?, ?, ?, ?, ?);
        """,
        number,
        addr.get("locality"),
        addr.get("region"),
        addr.get("postal_code"),
        addr.get("country"),
    )


def replace_sic(cur, number: str, sic_list: list[str], existing_sic: set[str]) -> None:
    cur.execute("DELETE FROM dbo.company_sic WHERE company_number = ?;", number)
    for sic in sorted(set(sic_list or [])):  # dedupe
        if sic not in existing_sic:
            continue
        cur.execute(
            "INSERT INTO dbo.company_sic (company_number, sic_code) VALUES (?, ?);",
            number,
            sic,
        )


def main() -> None:
    note = (
        f"BACKFILL {BACKFILL_FROM}..{BACKFILL_TO} "
        f"locations={len(LOCATIONS)} sic={','.join(SIC_CODES)} cap={MAX_RECORDS}"
    )

    inserted_total = 0
    scanned_total = 0

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT sic_code FROM dbo.sic_codes;")
        existing_sic = {row[0] for row in cur.fetchall()}

        run_id = start_run(cur, note=note)
        conn.commit()

        try:
            for loc in LOCATIONS:
                if inserted_total >= MAX_RECORDS:
                    break

                print(f"\n=== Backfill location: {loc} ===")
                start_index = 0

                while inserted_total < MAX_RECORDS:
                    data = advanced_search_companies(
                        location=loc,
                        sic_codes=SIC_CODES,
                        start_index=start_index,
                        size=PAGE_SIZE,
                        company_status="active",
                        incorporated_from=str(BACKFILL_FROM),
                        incorporated_to=str(BACKFILL_TO),
                    )

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

                        number = it.get("company_number")
                        if not number:
                            continue

                        if inserted_total == 0:
                            print("Starting first insert...")

                        upsert_company(cur, it, run_id=run_id)
                        replace_address(cur, number, it)
                        replace_sic(cur, number, it.get("sic_codes", []) or [], existing_sic)

                        inserted_total += 1

                        if inserted_total % COMMIT_EVERY == 0:
                            conn.commit()
                            print(f"Committed {inserted_total} records so far")

                    if last_page:
                        break
                    if isinstance(hits, int) and (start_index + len(items) >= hits):
                        break

                    start_index += PAGE_SIZE

            finish_run(cur, run_id, status="success", records_inserted=inserted_total)
            conn.commit()
            print(f"\nBACKFILL DONE. run_id={run_id} inserted/updated={inserted_total} scanned={scanned_total}")

        except Exception:
            conn.rollback()
            cur = conn.cursor()
            finish_run(cur, run_id, status="failure", records_inserted=inserted_total)
            conn.commit()
            raise


if __name__ == "__main__":
    main()
