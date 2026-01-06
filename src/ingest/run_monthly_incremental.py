from __future__ import annotations
import os
import csv
from datetime import date
from typing import Tuple, Optional
from src.db.connection import get_conn
from src.ingest.ch_client import advanced_search_companies

# (Luton to MK corridor
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

DEFAULT_SIC_CODES = ["62020", "62012"]

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "200"))

# Optional: user can set TARGET_MONTH=YYYY-MM
TARGET_MONTH_ENV = os.getenv("TARGET_MONTH", "").strip()


def previous_month_yyyy_mm(today: Optional[date] = None) -> str:
    today = today or date.today()
    y, m = today.year, today.month
    if m == 1:
        return f"{y - 1}-12"
    return f"{y}-{m - 1:02d}"


def normalize_target_month(raw: str) -> str:
    """Return YYYY-MM. If invalid/blank, default to previous month."""
    raw = (raw or "").strip()
    if not raw:
        return previous_month_yyyy_mm()

    parts = raw.split("-")
    if len(parts) != 2:
        return previous_month_yyyy_mm()

    try:
        y = int(parts[0])
        m = int(parts[1])
        if m < 1 or m > 12:
            return previous_month_yyyy_mm()
        return f"{y}-{m:02d}"
    except ValueError:
        return previous_month_yyyy_mm()


def month_range(yyyy_mm: str) -> Tuple[date, date]:
    """Returns (start_inclusive, end_exclusive) for the month."""
    y, m = map(int, yyyy_mm.split("-"))
    start = date(y, m, 1)
    if m == 12:
        end = date(y + 1, 1, 1)
    else:
        end = date(y, m + 1, 1)
    return start, end


def parse_sic_codes() -> list[str]:
    raw = os.getenv("SIC_CODES", "").strip()
    if not raw:
        return DEFAULT_SIC_CODES
    return [x.strip() for x in raw.split(",") if x.strip()]


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
        raise RuntimeError("Could not retrieve run_id from ingestion_log insert")
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
        number,
        name,
        status,
        inc_date,
        ctype,
        run_id,
        number,
        name,
        status,
        inc_date,
        ctype,
        run_id,
        run_id,
    )


def replace_address(cur, company_number: str, it: dict) -> None:
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

    for sic in sorted(set(sic_list or [])):  # dedupe
        if sic not in existing_sic:
            continue
        cur.execute(
            "INSERT INTO dbo.company_sic (company_number, sic_code) VALUES (?, ?);",
            company_number,
            sic,
        )


def export_new_companies_csv(conn, run_id: int, out_path: str) -> int:
    sql = """
        SELECT
            c.company_number,
            c.company_name,
            c.company_status,
            c.incorporation_date,
            a.locality,
            a.region,
            a.postal_code,
            a.country
        FROM dbo.companies c
        LEFT JOIN dbo.company_addresses a ON a.company_number = c.company_number
        WHERE c.first_seen_run_id = ?
        ORDER BY c.incorporation_date DESC;
    """

    cur = conn.cursor()
    cur.execute(sql, run_id)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    return len(rows)


def main() -> None:
    sic_codes = parse_sic_codes()
    target_month = normalize_target_month(TARGET_MONTH_ENV)

    start_date, end_date = month_range(target_month)
    incorporated_from = str(start_date)
    incorporated_to = str(end_date)  # end_exclusive

    note = (
        f"INCREMENTAL month={target_month} "
        f"incorporated {incorporated_from}..{incorporated_to} "
        f"locations={len(LOCATIONS)} sic={','.join(sic_codes)}"
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
                print(f"\n=== Incremental location: {loc} ===")
                start_index = 0

                while True:
                    data = advanced_search_companies(
                        location=loc,
                        sic_codes=sic_codes,
                        start_index=start_index,
                        size=PAGE_SIZE,
                        company_status="active",
                        incorporated_from=incorporated_from,
                        incorporated_to=incorporated_to,
                    )

                    items = data.get("items", []) or []
                    hits = data.get("hits")
                    print(f"Fetched page start_index={start_index} | items={len(items)} | hits={hits}")

                    if not items:
                        break

                    last_page = len(items) < PAGE_SIZE

                    for it in items:
                        scanned_total += 1
                        number = it.get("company_number")
                        if not number:
                            continue

                        upsert_company(cur, it, run_id=run_id)
                        replace_address(cur, number, it)
                        replace_sic(cur, number, it.get("sic_codes", []) or [], existing_sic)

                        inserted_total += 1
                        if inserted_total % COMMIT_EVERY == 0:
                            conn.commit()
                            print(f"Committed {inserted_total} rows so far")

                    if last_page:
                        break
                    if isinstance(hits, int) and (start_index + len(items) >= hits):
                        break

                    start_index += PAGE_SIZE

            out_path = os.path.join("data", "exports", f"new_companies_{target_month}_run_{run_id}.csv")

            conn.commit()
            new_count = export_new_companies_csv(conn, run_id, out_path)

            finish_run(cur, run_id, status="success", records_inserted=inserted_total)
            conn.commit()

            print(
                f"\nINCREMENTAL DONE. run_id={run_id} "
                f"inserted/updated={inserted_total} scanned={scanned_total}"
            )
            print(f"New companies exported: {new_count}")
            print(f"CSV written to: {out_path}")

            #  send email with attachment
            if os.getenv("SEND_EMAIL", "0") == "1":
                from src.notifications.send_email import send_csv_email

                send_csv_email(
                    csv_path=out_path,
                    subject=f"Companies House MI - New companies ({target_month})",
                    body=(
                        f"Attached: new companies for {target_month}\n"
                        f"run_id={run_id}\n"
                        f"locations={len(LOCATIONS)} | sic={','.join(sic_codes)}\n"
                        f"rows_in_csv={new_count}\n"
                    ),
                )
                print("Email sent.")

        except Exception:
            conn.rollback()
            cur = conn.cursor()
            finish_run(cur, run_id, status="failure", records_inserted=inserted_total)
            conn.commit()
            raise


if __name__ == "__main__":
    main()
