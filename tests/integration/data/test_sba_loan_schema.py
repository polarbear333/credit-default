# tests/integration/data/test_sba_loan_schema.py
import pytest
import pandas as pd
from pandera.errors import SchemaErrors

from core.schemas.sba_loans import LoanDataSmokeSchema


@pytest.fixture
def valid_loan_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "LoanNumber": [1001, 1002, 1003],
            "LoanStatus": ["P I F", "CHGOFF", "P I F"],
            "GrossApproval": [10000.0, 75000.0, 250000.0],
            "TermInMonths": [120, 84, 240],
            "ExtraColumn": ["A", "B", "C"],
        }
    )


@pytest.fixture
def invalid_loan_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "LoanNumber": [1001, 1002, 1001],
            "LoanStatus": ["P I F", None, "INVALID_STATUS"],
            "GrossApproval": [10000.0, -500.0, 250000.0],
            # TermInMonths is missing
        }
    )


def test_schema_validates_correct_data(valid_loan_data):
    """
    Verify that the schema successfully validates a conforming DataFrame.
    """
    # Act & Assert
    try:
        validated_df = LoanDataSmokeSchema.validate(valid_loan_data)
        assert "ExtraColumn" not in validated_df.columns  # strict="filter" removes it
        assert len(validated_df) == 3
    except SchemaErrors as e:
        pytest.fail(f"Validation failed unexpectedly: {e}")


def test_schema_fails_on_invalid_data(invalid_loan_data):
    """Verify that the schema correctly identifies multiple validation failures."""
    with pytest.raises(SchemaErrors) as excinfo:
        LoanDataSmokeSchema.validate(invalid_loan_data, lazy=True)

    failure_reasons = excinfo.value.failure_cases["check"].tolist()

    # Check for specific failures based on the schema
    assert "column_in_dataframe" in failure_reasons
    assert "not_nullable" in failure_reasons
    assert any("isin" in reason for reason in failure_reasons)
    assert any("greater_than" in reason for reason in failure_reasons)
