from __future__ import annotations

import os
import csv
from datetime import date
from pathlib import Path
from typing import Tuple, Optional

from src.db.connection import get_conn

EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "data/exports"))
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_SIC_CODES = ["62020", "62012"]


def previous_month_yyyy_mm(today: Optional[date] = None) -> str:
    today = today or date.today()
    y, m = today.year, today.month
    if m == 1:
        return f"{y-1}-12"
    return f"{y}-{m-1:02d}"


def normalize_target_month(raw: str) -> str:
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
    """Returns (start_inclusive, end_exclusive)."""
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


def get_latest_success_run_id(cur, only_incremental: bool = True) -> int:
    """
    Gets the latest successful run_id from dbo.ingestion_log.
    If only_incremental=True, it tries to pick runs whose source contains 'INCREMENTAL'.
    """
    if only_incremental:
        cur.execute(
            """
            SELECT TOP 1 run_id
            FROM dbo.ingestion_log
            WHERE status = 'success'
              AND source LIKE '%INCREMENTAL%'
            ORDER BY run_id DESC;
            """
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])

    # Fallback: latest success of any type (backfill, etc.)
    cur.execute(
        """
        SELECT TOP 1 run_id
        FROM dbo.ingestion_log
        WHERE status = 'success'
        ORDER BY run_id DESC;
        """
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        raise RuntimeError("No successful runs found in dbo.ingestion_log.")
    return int(row[0])


def export_month_companies_csv(
    conn,
    start_date: str,
    end_date: str,
    sic_codes: list[str],
    out_path: Path,
) -> int:
    """
    Export companies incorporated within [start_date, end_date) AND matching SIC codes.
    """
    if not sic_codes:
        raise ValueError("sic_codes is empty")

    placeholders = ",".join(["?"] * len(sic_codes))

    sql = f"""
        SELECT DISTINCT
            c.company_number,
            c.company_name,
            c.company_status,
            c.incorporation_date,
            a.locality,
            a.region,
            a.postal_code,
            a.country
        FROM dbo.companies c
        LEFT JOIN dbo.company_addresses a
            ON a.company_number = c.company_number
        INNER JOIN dbo.company_sic cs
            ON cs.company_number = c.company_number
        WHERE
            c.incorporation_date >= ?
            AND c.incorporation_date < ?
            AND cs.sic_code IN ({placeholders})
        ORDER BY c.incorporation_date DESC;
    """

    params = [start_date, end_date] + sic_codes

    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    return len(rows)


def main() -> None:
    target_month = normalize_target_month(os.getenv("TARGET_MONTH", ""))
    sic_codes = parse_sic_codes()

    # Optional toggle: export based on "latest incremental run"
    only_incremental = os.getenv("ONLY_INCREMENTAL_RUNS", "1") == "1"

    start_dt, end_dt = month_range(target_month)

    with get_conn() as conn:
        cur = conn.cursor()
        run_id = get_latest_success_run_id(cur, only_incremental=only_incremental)

        out_path = EXPORT_DIR / f"companies_incorp_{target_month}_run_{run_id}.csv"
        count = export_month_companies_csv(
            conn=conn,
            start_date=str(start_dt),
            end_date=str(end_dt),
            sic_codes=sic_codes,
            out_path=out_path,
        )

    print(f"Run id used: {run_id}")
    print(f"Target month: {target_month}")
    print(f"SIC codes: {','.join(sic_codes)}")
    print(f"Exported rows: {count}")
    print(f"CSV written to: {out_path}")


if __name__ == "__main__":
    main()
