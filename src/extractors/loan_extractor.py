"""Loan data extractor from proprietary mortgage LOS systems."""
import logging
import os
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class LoanRecord:
    loan_id: str
    borrower_id: str
    loan_amount: float
    interest_rate: float
    loan_status: str
    origination_date: str
    property_zip: Optional[str] = None


class LOSExtractor:
    """Extracts loan records from a Loan Origination System (LOS) REST API."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def fetch_loans(self, batch_date: str, page_size: int = 500) -> list:
        """Fetches all loans originated on batch_date with pagination."""
        records = []
        page = 1
        while True:
            resp = self.session.get(
                f"{self.base_url}/loans",
                params={"date": batch_date, "page": page, "size": page_size},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            loans = data.get("loans", [])
            if not loans:
                break
            for loan in loans:
                records.append(
                    LoanRecord(
                        loan_id=loan["loan_id"],
                        borrower_id=loan["borrower_id"],
                        loan_amount=float(loan["amount"]),
                        interest_rate=float(loan["rate"]),
                        loan_status=loan["status"],
                        origination_date=loan["origination_date"],
                        property_zip=loan.get("property_zip"),
                    )
                )
            page += 1
            logger.info("Fetched page %d, total so far: %d", page - 1, len(records))
        logger.info("Extraction complete for %s: %d records", batch_date, len(records))
        return records


def get_extractor() -> LOSExtractor:
    return LOSExtractor(
        base_url=os.environ["LOS_BASE_URL"],
        api_key=os.environ["LOS_API_KEY"],
    )
