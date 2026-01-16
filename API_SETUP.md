# USITC DataWeb API Setup Guide

## The Problem: Invalid dataToReport Field Names

You're getting this error:

```
Invalid dataToReport object selected: CONS_VAL_MO
```

This means the data measure field names in the code don't match what the USITC DataWeb API expects. The API has **not publicly documented the exact field names**, which is why trial-and-error keeps failing.

## Solution: Find the Correct Field Names

### Method 1: Browser Network Inspector (RECOMMENDED)

1. Go to https://www.usitc.gov/applications/dataweb/
2. Open **Developer Tools** (F12 or Right-click → Inspect)
3. Go to the **Network** tab
4. Create a manual query with:
   - **Imports for Consumption** (already selected)
   - **HTS Items** (already selected)
   - **Your desired metrics** (e.g., "Customs Value", "Quantity", "Duties")
   - **Countries**: China
   - **Years**: 1996-2005
   - **Granularity**: 10-digit HTS
5. Click **Run Report**
6. In the Network tab, find the POST request to `/dataweb/api/v2/report2/runReport`
7. Click on it and look at the **Request → Payload**
8. Find the `"dataToReport"` array - the values there are the correct field names!

Example of what you might see:

```json
{
  "searchOptions": {
    "componentSettings": {
      "dataToReport": ["IMPT_VAL_MO", "IMPT_UNIT_QTY", "IMPT_DUTY_MO"]
    }
  }
}
```

### Method 2: Contact USITC Support

- Email: dataweb@usitc.gov
- Ask for: "Official API documentation for valid dataToReport field identifiers"

## After Finding the Correct Field Names

Update `api.py` in the `query_china_imports()` function:

```python
def query_china_imports(years: list) -> Optional[pd.DataFrame]:
    data_measures = [
        "ACTUAL_FIELD_1",    # Replace with correct name
        "ACTUAL_FIELD_2",    # Replace with correct name
        "ACTUAL_FIELD_3",    # Replace with correct name
    ]
    # ... rest of function
```

Then test:

```bash
python3 api.py
```

## Setting Your API Token

Make sure your token is set:

```bash
export TRADE_API_KEY='your_actual_token_here'
```

Get your token from the API tab at: https://www.usitc.gov/applications/dataweb/
