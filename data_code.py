import pandas as pd
import requests
import json
import urllib3
import os

api_key = os.getenv("TRADE_API_KEY")

# 1. SETUP
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- PASTE YOUR TOKEN HERE ---

token = api_key

base_url = "https://datawebws.usitc.gov/dataweb"
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorwization": "Bearer " + token,
}

# 2. THE ORIGINAL QUERY (As provided in your snippet)
requestData = {
    "savedQueryType": "",
    "isOwner": True,
    "unitConversion": "0",
    "manualConversions": [],
    "reportOptions": {"tradeType": "Import", "classificationSystem": "HTS"},
    "searchOptions": {
        "MiscGroup": {
            "districts": {
                "aggregation": "Aggregate District",
                "districtGroups": {},
                "districts": [],
                "districtsExpanded": [{"name": "All Districts", "value": "all"}],
                "districtsSelectType": "all",
            },
            "importPrograms": {
                "aggregation": None,
                "importPrograms": [],
                "programsSelectType": "all",
            },
            "extImportPrograms": {
                "aggregation": "Aggregate CSC",
                "extImportPrograms": [],
                "extImportProgramsExpanded": [],
                "programsSelectType": "all",
            },
            "provisionCodes": {
                "aggregation": "Aggregate RPCODE",
                "provisionCodesSelectType": "all",
                "rateProvisionCodes": [],
                "rateProvisionCodesExpanded": [],
                "rateProvisionGroups": {"systemGroups": []},
            },
        },
        "commodities": {
            "aggregation": "Break Out Commodities",
            "codeDisplayFormat": "NO",
            "commodities": [],
            "commoditiesExpanded": [],
            "commoditiesManual": "",
            "commodityGroups": {"systemGroups": [], "userGroups": []},
            "commoditySelectType": "all",
            "granularity": "10",
            "groupGranularity": None,
            "searchGranularity": None,
            "showHTSValidDetails": "",
        },
        "componentSettings": {
            "dataToReport": [
                # "CONS_CUSTOMS_VALUE",
                # "CONS_FIR_UNIT_QUANT",
                # "CONS_CALC_DUTY",
            ],
            "scale": "1",
            "timeframeSelectType": "fullYears",
            "years": [
                "1995",
            ],
            "startDate": None,
            "endDate": None,
            "startMonth": None,
            "endMonth": None,
            "yearsTimeline": "Monthly",
        },
        "countries": {
            "aggregation": "Break Out Countries",
            "countries": ["5700"],
            "countriesExpanded": [{"name": "China - CN - CHN", "value": "5700"}],
            "countriesSelectType": "list",
            "countryGroups": {"systemGroups": [], "userGroups": []},
        },
    },
    "sortingAndDataFormat": {
        "DataSort": {
            "columnOrder": ["COUNTRY", "YEAR"],
            "fullColumnOrder": [],
            "sortOrder": [
                {"sortData": "Countries", "orderBy": "asc"},
                {"sortData": "Year", "orderBy": "asc"},
            ],
        },
        "reportCustomizations": {
            "exportCombineTables": False,
            "totalRecords": "20000",
            "exportRawData": True,
        },
    },
    "deletedCountryUserGroups": [],
    "deletedCommodityUserGroups": [],
    "deletedDistrictUserGroups": [],
}

# 3. EXECUTE REQUEST
print("Sending ORIGINAL request...")
try:
    response = requests.post(
        base_url + "/api/v2/report2/runReport",
        headers=headers,
        json=requestData,
        verify=False,
    )

    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        resp_json = response.json()

        # 4. PARSE DATA (Using the complex loop from the snippet)
        if resp_json.get("dto") and resp_json["dto"].get("tables"):
            tables = resp_json["dto"]["tables"]

            # Map table index to metric name
            metric_names = {
                0: "Customs Value",
                1: "First Unit of Quantity",
                2: "Calculated Duties",
            }

            all_rows = []

            for i, table in enumerate(tables):
                metric = metric_names.get(i, f"Metric_{i}")

                # Column labels
                col_labels = []
                if table.get("column_groups"):
                    for col_group in table.get("column_groups", []):
                        for col in col_group.get("columns", []):
                            col_labels.append(col.get("label", ""))

                # Process rows
                if table.get("row_groups"):
                    for row_group in table.get("row_groups", []):
                        for row in row_group.get("rowsNew", []):
                            row_entries = row.get("rowEntries", [])
                            row_values = []
                            for entry in row_entries:
                                val = entry.get("value", None)
                                row_values.append(val)
                            all_rows.append([metric] + row_values)

            # Create DataFrame
            if all_rows:
                # Add Data Type as first column
                # Note: col_labels might be empty if the table structure varies,
                # but this matches the provided logic.
                final_columns = ["Data Type"] + col_labels

                # Safety check: ensure columns match row length
                if len(final_columns) != len(all_rows[0]):
                    print(
                        "Warning: Column headers length matches rows mismatch. Using generic headers."
                    )
                    final_columns = ["Data Type"] + [
                        f"Col_{x}" for x in range(len(all_rows[0]) - 1)
                    ]

                df_combined = pd.DataFrame(all_rows, columns=final_columns)

                print(f"Success! Retrieved {len(df_combined)} rows.")
                print(df_combined.head())

                df_combined.to_csv("China_Original_Test.csv", index=False)
                print("Saved to China_Original_Test.csv")
            else:
                print("Parsed successfully, but result is empty.")
        else:
            print("Response structure unexpected (No tables found in 'dto').")
    else:
        print("Request failed.")
        print(response.text)

except Exception as e:
    print(f"An error occurred: {e}")
