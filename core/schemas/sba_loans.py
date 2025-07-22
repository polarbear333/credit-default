import pandera.pandas as pa
import pandas as pd
from pandera import Column, Check, Index

# This schema is for "smoke testing" the final, combined raw dataset.
# It ensures critical columns are present and have the correct basic types and constraints
# before the data is handed off to the feature engineering stage.

LoanDataSmokeSchema = pa.DataFrameSchema(
    {
        "AsOfDate": Column(pa.String, nullable=True, coerce=True),
        "Program": Column(pa.String, nullable=True, coerce=True),
        "BorrName": Column(pa.String, nullable=True, coerce=True),
        "BorrStreet": Column(pa.String, nullable=True, coerce=True),
        "BorrCity": Column(pa.String, nullable=True, coerce=True),
        "BorrState": Column(pa.String, nullable=True, coerce=True),
        "BorrZip": Column(pa.Int, nullable=True, coerce=True),
        "LocationID": Column(pa.Float, nullable=True, coerce=True),
        "CDC_Name": Column(pa.String, nullable=True, coerce=True),
        "CDC_Street": Column(pa.String, nullable=True, coerce=True),
        "CDC_City": Column(pa.String, nullable=True, coerce=True),
        "CDC_State": Column(pa.String, nullable=True, coerce=True),
        "CDC_Zip": Column(pa.Float, nullable=True, coerce=True),
        "ThirdPartyLender_Name": Column(
            pa.String, nullable=True, coerce=True
        ),  # Possibly bad data
        "ThirdPartyLender_City": Column(pa.String, nullable=True, coerce=True),
        "ThirdPartyLender_State": Column(pa.String, nullable=True, coerce=True),
        "ThirdPartyDollars": Column(pa.Float, nullable=True, coerce=True),
        "GrossApproval": Column(
            pa.Float, Check.greater_than(0), nullable=False, coerce=True
        ),
        "ApprovalDate": Column(pa.String, nullable=True, coerce=True),
        "ApprovalFiscalYear": Column(pa.Int, nullable=True, coerce=True),
        "FirstDisbursementDate": Column(pa.String, nullable=True, coerce=True),
        "ProcessingMethod": Column(pa.String, nullable=True, coerce=True),
        "Subprogram": Column(pa.String, nullable=True, coerce=True),
        "TermInMonths": Column(pa.Int, Check.ge(0), nullable=False, coerce=True),
        "NaicsCode": Column(pa.Float, nullable=True, coerce=True),
        "NaicsDescription": Column(pa.String, nullable=True, coerce=True),
        "FranchiseCode": Column(pa.String, nullable=True, coerce=True),
        "FranchiseName": Column(pa.String, nullable=True, coerce=True),
        "ProjectCounty": Column(pa.String, nullable=True, coerce=True),
        "ProjectState": Column(pa.String, nullable=True, coerce=True),
        "SBADistrictOffice": Column(pa.String, nullable=True, coerce=True),
        "CongressionalDistrict": Column(pa.Float, nullable=True, coerce=True),
        "BusinessType": Column(pa.String, nullable=True, coerce=True),
        "BusinessAge": Column(pa.String, nullable=True, coerce=True),
        "LoanStatus": Column(
            pa.String,
            Check.isin(["PIF", "CHGOFF", "CANCLD", "EXEMPT", "NOT FUNDED", "COMMIT"]),
            nullable=True,
        ),
        "PaidInFullDate": Column(pa.String, nullable=True, coerce=True),
        "ChargeOffDate": Column(pa.String, nullable=True, coerce=True),
        "GrossChargeOffAmount": Column(pa.Int, nullable=True, coerce=True),
        "JobsSupported": Column(pa.Int, nullable=True, coerce=True),
        "CollateralInd": Column(pa.String, nullable=True, coerce=True),
    },
    strict="filter",
    index=Index(pa.Int),
    ordered=False,
)


def filter_schema_for_df(
    schema: pa.DataFrameSchema, df: pd.DataFrame
) -> pa.DataFrameSchema:
    matched_columns = {k: v for k, v in schema.columns.items() if k in df.columns}
    return pa.DataFrameSchema(
        matched_columns, strict=False, index=schema.index, ordered=schema.ordered
    )
