"""
USITC DataWeb API Query Script
For W26 Causal Inference Economics Project

Data Requirements:
- Imports for Consumption, HTS Items
- Customs value, first unit of quantity, calculated duties
- Years 1996-2005, monthly aggregation
- China vs. Rest of World
- HTS 10-digit level, broken out by commodity and country
"""

import pandas as pd
import requests
import json
import time
from typing import Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

TOKEN = '[YOUR_API_TOKEN_HERE]'  # Get from API tab in DataWeb (requires login)
BASE_URL = 'https://datawebws.usitc.gov/dataweb'
HEADERS = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorization": "Bearer " + TOKEN
}

# Disable SSL warnings (DataWeb uses verify=False in their examples)
requests.packages.urllib3.disable_warnings()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_columns(column_groups, prev_cols=None):
    """Extract column names from API response."""
    if prev_cols is None:
        columns = []
    else:
        columns = prev_cols
    for group in column_groups:
        if isinstance(group, dict) and 'columns' in group.keys():
            get_columns(group['columns'], columns)
        elif isinstance(group, dict) and 'label' in group.keys():
            columns.append(group['label'])
        elif isinstance(group, list):
            get_columns(group, columns)
    return columns


def get_data(data_groups):
    """Extract data values from API response JSON."""
    data = []
    for row in data_groups:
        row_data = []
        for field in row['rowEntries']:
            row_data.append(field['value'])
        data.append(row_data)
    return data


def run_query(query_data: dict) -> Optional[pd.DataFrame]:
    """Execute query and return DataFrame."""
    try:
        response = requests.post(
            f"{BASE_URL}/api/v2/report2/runReport",
            headers=HEADERS,
            json=query_data,
            verify=False
        )
        response.raise_for_status()
        
        result = response.json()
        if 'dto' not in result or 'tables' not in result['dto']:
            print("Unexpected response structure")
            print(json.dumps(result, indent=2)[:500])
            return None
            
        columns = get_columns(result['dto']['tables'][0]['column_groups'])
        data = get_data(result['dto']['tables'][0]['row_groups'][0]['rowsNew'])
        return pd.DataFrame(data, columns=columns)
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"Error parsing response: {e}")
        return None


def get_all_countries() -> list:
    """Retrieve list of all available countries."""
    response = requests.get(
        f"{BASE_URL}/api/v2/country/getAllCountries",
        headers=HEADERS,
        verify=False
    )
    return response.json().get('options', [])


# =============================================================================
# QUERY TEMPLATES
# =============================================================================

def build_base_query(
    years: list,
    countries: list,
    data_to_report: list,
    country_aggregation: str = "Break Out Countries",
    commodity_aggregation: str = "Break Out Commodities",
    granularity: str = "10"  # HTS 10-digit
) -> dict:
    """
    Build query for USITC DataWeb API.
    
    Args:
        years: List of years as strings, e.g. ['1996', '1997', ...]
        countries: List of country codes, e.g. ['5700'] for China
        data_to_report: List of data measures to retrieve
        country_aggregation: "Aggregate Countries" or "Break Out Countries"
        commodity_aggregation: "Aggregate Commodities" or "Break Out Commodities"
        granularity: HTS digit level ("2", "4", "6", "8", "10")
    """
    return {
        "savedQueryName": "",
        "savedQueryDesc": "",
        "isOwner": True,
        "runMonthly": False,
        "reportOptions": {
            "tradeType": "Import",  # Imports for Consumption
            "classificationSystem": "HTS"  # HTS Items
        },
        "searchOptions": {
            "MiscGroup": {
                "districts": {
                    "aggregation": "Aggregate District",
                    "districtGroups": {"userGroups": []},
                    "districts": [],
                    "districtsExpanded": [{"name": "All Districts", "value": "all"}],
                    "districtsSelectType": "all"
                },
                "importPrograms": {
                    "aggregation": None,
                    "importPrograms": [],
                    "programsSelectType": "all"
                },
                "extImportPrograms": {
                    "aggregation": "Aggregate CSC",
                    "extImportPrograms": [],
                    "extImportProgramsExpanded": [],
                    "programsSelectType": "all"
                },
                "provisionCodes": {
                    "aggregation": "Aggregate RPCODE",
                    "provisionCodesSelectType": "all",
                    "rateProvisionCodes": [],
                    "rateProvisionCodesExpanded": []
                }
            },
            "commodities": {
                "aggregation": commodity_aggregation,
                "codeDisplayFormat": "YES",
                "commodities": [],
                "commoditiesExpanded": [],
                "commoditiesManual": "",
                "commodityGroups": {"systemGroups": [], "userGroups": []},
                "commoditySelectType": "all",
                "granularity": granularity,
                "groupGranularity": None,
                "searchGranularity": None
            },
            "componentSettings": {
                "dataToReport": data_to_report,
                "scale": "1",  # No scaling
                "timeframeSelectType": "fullYears",
                "years": years,
                "startDate": None,
                "endDate": None,
                "startMonth": None,
                "endMonth": None,
                "yearsTimeline": "Monthly"  # Monthly aggregation
            },
            "countries": {
                "aggregation": country_aggregation,
                "countries": countries,
                "countriesExpanded": [],
                "countriesSelectType": "list" if countries else "all",
                "countryGroups": {"systemGroups": [], "userGroups": []}
            }
        },
        "sortingAndDataFormat": {
            "DataSort": {
                "columnOrder": [],
                "fullColumnOrder": [],
                "sortOrder": []
            },
            "reportCustomizations": {
                "exportCombineTables": False,
                "showAllSubtotal": True,
                "subtotalRecords": "",
                "totalRecords": "50000",  # Increased for large dataset
                "exportRawData": False
            }
        }
    }


# =============================================================================
# MAIN QUERY FUNCTIONS
# =============================================================================

def query_china_imports(years: list) -> Optional[pd.DataFrame]:
    """
    Query imports from China.
    China country code in DataWeb is typically '5700'.
    """
    # Data measures for imports:
    # CONS_VAL_MO = Customs Value
    # CONS_FIR_UNIT_QUANT = First Unit of Quantity
    # CONS_DUTY_MO = Calculated Duties
    data_measures = [
        "CONS_VAL_MO",           # Customs Value
        "CONS_FIR_UNIT_QUANT",   # First Unit of Quantity
        "CONS_DUTY_MO"           # Calculated Duties
    ]
    
    query = build_base_query(
        years=years,
        countries=["5700"],  # China
        data_to_report=data_measures,
        country_aggregation="Break Out Countries",
        commodity_aggregation="Break Out Commodities",
        granularity="10"
    )
    
    print("Querying China imports...")
    return run_query(query)


def query_row_imports(years: list, exclude_china: bool = True) -> Optional[pd.DataFrame]:
    """
    Query imports from Rest of World (excluding China if specified).
    This may need to be split into multiple queries due to size.
    """
    data_measures = [
        "CONS_VAL_MO",
        "CONS_FIR_UNIT_QUANT",
        "CONS_DUTY_MO"
    ]
    
    # Get all countries first
    all_countries = get_all_countries()
    
    if exclude_china:
        # Filter out China (5700)
        country_codes = [c['value'] for c in all_countries 
                        if c['value'] != '5700' and c['value'] != 'all']
    else:
        country_codes = [c['value'] for c in all_countries if c['value'] != 'all']
    
    query = build_base_query(
        years=years,
        countries=country_codes,
        data_to_report=data_measures,
        country_aggregation="Break Out Countries",
        commodity_aggregation="Break Out Commodities",
        granularity="10"
    )
    
    print(f"Querying Rest of World imports ({len(country_codes)} countries)...")
    return run_query(query)


def query_by_year(year: str, countries: list = None) -> Optional[pd.DataFrame]:
    """
    Query single year to handle large datasets.
    Useful if full query times out or exceeds row limits.
    """
    data_measures = [
        "CONS_VAL_MO",
        "CONS_FIR_UNIT_QUANT", 
        "CONS_DUTY_MO"
    ]
    
    query = build_base_query(
        years=[year],
        countries=countries if countries else [],
        data_to_report=data_measures,
        country_aggregation="Break Out Countries",
        commodity_aggregation="Break Out Commodities",
        granularity="10"
    )
    
    if not countries:
        query["searchOptions"]["countries"]["countriesSelectType"] = "all"
        query["searchOptions"]["countries"]["countriesExpanded"] = [
            {"name": "All Countries", "value": "all"}
        ]
    
    print(f"Querying year {year}...")
    return run_query(query)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function."""
    
    # Define study period
    years = [str(y) for y in range(1996, 2006)]  # 1996-2005
    
    print("=" * 60)
    print("USITC DataWeb Query - Causal Inference Project")
    print("=" * 60)
    
    # Option 1: Query China and ROW separately
    print("\n[Option 1] Querying China imports...")
    df_china = query_china_imports(years)
    
    if df_china is not None:
        print(f"China data shape: {df_china.shape}")
        df_china.to_csv("china_imports_1996_2005.csv", index=False)
        print("Saved to china_imports_1996_2005.csv")
    
    # Option 2: Query year by year (if full query is too large)
    print("\n[Option 2] Querying year by year...")
    all_data = []
    
    for year in years:
        df_year = query_by_year(year, countries=["5700"])  # China only
        if df_year is not None:
            df_year['query_year'] = year
            all_data.append(df_year)
            print(f"  Year {year}: {len(df_year)} rows")
        time.sleep(1)  # Rate limiting
    
    if all_data:
        df_combined = pd.concat(all_data, ignore_index=True)
        df_combined.to_csv("china_imports_by_year.csv", index=False)
        print(f"\nCombined data shape: {df_combined.shape}")
        print("Saved to china_imports_by_year.csv")
    
    print("\n" + "=" * 60)
    print("Query complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()


# =============================================================================
# ADDITIONAL UTILITIES
# =============================================================================

def explore_api():
    """Utility function to explore available API options."""
    
    print("Fetching available countries...")
    countries = get_all_countries()
    df_countries = pd.DataFrame(countries)
    print(df_countries.head(20))
    
    # Find China's code
    china = [c for c in countries if 'china' in c.get('name', '').lower()]
    print(f"\nChina entry: {china}")
    
    return df_countries


def check_saved_queries():
    """Check user's saved queries for reference."""
    response = requests.get(
        f"{BASE_URL}/api/v2/savedQuery/getAllSavedQueries",
        headers=HEADERS,
        verify=False
    )
    return response.json()