from __future__ import annotations

from src.ingest.run_monthly_incremental import main as ingest_main
from src.analytics.export_new_companies_csv import main as export_main


def main() -> None:
    ingest_main()
    export_main()


if __name__ == "__main__":
    main()
