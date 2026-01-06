from __future__ import annotations

import os
import time
import random
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.company-information.service.gov.uk"
CH_API_KEY = os.getenv("CH_API_KEY")

if not CH_API_KEY:
    raise RuntimeError(
        "CH_API_KEY is missing. Add it to your .env like:\n"
        "CH_API_KEY=your_companies_house_api_key"
    )

RETRY_STATUS = {500, 502, 503, 504}
SESSION = requests.Session()


def advanced_search_companies(
    *,
    location: str,
    sic_codes: List[str],
    start_index: int,
    size: int,
    company_status: str = "active",
    incorporated_from: Optional[str] = None,  # 'YYYY-MM-DD'
    incorporated_to: Optional[str] = None,    # 'YYYY-MM-DD'
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Companies House advanced search: /advanced-search/companies

    Retries transient 5xx with exponential backoff + jitter.
    Raises a RuntimeError with useful context after final failure.
    """
    params: Dict[str, Any] = {
        "location": location,
        "sic_codes": ",".join(sic_codes),
        "start_index": start_index,
        "size": size,
        "company_status": company_status,
    }
    if incorporated_from:
        params["incorporated_from"] = incorporated_from
    if incorporated_to:
        params["incorporated_to"] = incorporated_to

    url = f"{BASE_URL}/advanced-search/companies"

    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = SESSION.get(url, params=params, auth=(CH_API_KEY, ""), timeout=30)

            if resp.status_code in RETRY_STATUS:
                # exponential backoff + jitter
                sleep_s = (2 ** attempt) + random.uniform(0.0, 0.75)
                time.sleep(sleep_s)
                last_exc = RuntimeError(
                    f"HTTP {resp.status_code} | {resp.url} | {resp.text[:300]}"
                )
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            last_exc = e
            sleep_s = (2 ** attempt) + random.uniform(0.0, 0.75)
            time.sleep(sleep_s)

    # failed
    raise RuntimeError(
        f"Companies House API failed after retries. Last error: {last_exc}"
    )
