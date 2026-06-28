"""Unit tests for LOSExtractor."""
import unittest
from unittest.mock import MagicMock, patch

from src.extractors.loan_extractor import LoanRecord, LOSExtractor


class TestLoanRecord(unittest.TestCase):
    def test_loan_record_creation(self):
        record = LoanRecord(
            loan_id="LN-001",
            borrower_id="B-001",
            loan_amount=350000.0,
            interest_rate=6.75,
            loan_status="ACTIVE",
            origination_date="2024-01-15",
        )
        self.assertEqual(record.loan_id, "LN-001")
        self.assertIsNone(record.property_zip)

    def test_loan_record_with_zip(self):
        record = LoanRecord(
            loan_id="LN-002",
            borrower_id="B-002",
            loan_amount=275000.0,
            interest_rate=7.1,
            loan_status="CLOSED",
            origination_date="2024-03-01",
            property_zip="90210",
        )
        self.assertEqual(record.property_zip, "90210")


class TestLOSExtractor(unittest.TestCase):
    def setUp(self):
        self.extractor = LOSExtractor(
            base_url="https://los-api.example.com",
            api_key="test-api-key",
        )

    @patch("src.extractors.loan_extractor.requests.Session")
    def test_fetch_loans_single_page(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_resp = MagicMock()
        mock_resp.json.side_effect = [
            {"loans": [{"loan_id": "LN-001", "borrower_id": "B-001",
                        "amount": "350000", "rate": "6.75",
                        "status": "ACTIVE", "origination_date": "2024-01-15"}]},
            {"loans": []},
        ]
        mock_session.get.return_value = mock_resp
        extractor = LOSExtractor("https://test.com", "key")
        extractor.session = mock_session
        result = extractor.fetch_loans("2024-01-15")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].loan_id, "LN-001")


if __name__ == "__main__":
    unittest.main()
