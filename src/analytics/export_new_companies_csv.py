from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd

from src.db.connection import get_conn

EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "data/exports"))
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def get_latest_success_run_id(cur) -> int:
    cur.execute("""
        SELECT TOP 1 run_id
        FROM dbo.pipeline_runs
        WHERE status = 'success'
        ORDER BY run_id DESC;
    """)
    row = cur.fetchone()
    if not row:
        raise RuntimeError("No successful runs found in dbo.pipeline_runs.")
    return int(row[0])


def export_new_companies(run_id: int) -> Path:
    sql = """
    SELECT
        c.company_number,
        c.company_name,
        c.company_status,
        c.incorporation_date,
        c.company_type,
        c.first_seen_run_id,
        c.last_seen_run_id,
        c.last_seen_at,
        ca.locality,
        ca.region,
        ca.postal_code,
        ca.country,
        STRING_AGG(cs.sic_code, ',') AS sic_codes
    FROM dbo.companies c
    LEFT JOIN dbo.company_addresses ca
        ON ca.company_number = c.company_number
    LEFT JOIN dbo.company_sic cs
        ON cs.company_number = c.company_number
    WHERE c.first_seen_run_id = ?
    GROUP BY
        c.company_number, c.company_name, c.company_status, c.incorporation_date, c.company_type,
        c.first_seen_run_id, c.last_seen_run_id, c.last_seen_at,
        ca.locality, ca.region, ca.postal_code, ca.country
    ORDER BY c.incorporation_date DESC;
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=[run_id])

    out = EXPORT_DIR / f"new_companies_run_{run_id}_{date.today().isoformat()}.csv"
    df.to_csv(out, index=False)
    return out


def main() -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        run_id = get_latest_success_run_id(cur)

    out_path = export_new_companies(run_id)
    print(f"Exported: {out_path}")


if __name__ == "__main__":
    main()
