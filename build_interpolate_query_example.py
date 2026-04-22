"""
Build interpolate query with real-world parameters
"""

import sys

sys.path.insert(0, "/Users/amber.rigg/Projects/core/src/sdk/python")

from rtdip_sdk.queries.time_series._time_series_query_builder import _query_builder

# Real-world parameters
params = {
    "business_unit": "integratedgas",
    "region": "americas",
    "asset": "lngc",
    "data_security_level": "confidential",
    "data_type": "float",
    "tag_names": ["CA:KIT:211FI0011.PV"],
    "include_bad_data": False,
    "start_date": "2026-01-01T00:00:00+00:00",
    "end_date": "2026-01-15T23:59:59+00:00",
    "time_interval_rate": "1",
    "time_interval_unit": "second",
    "time_zone": "UTC",
    "agg_method": None,
    "suppress_warning": True,
}

print("=" * 100)
print("BUILDING INTERPOLATE QUERY WITH REAL-WORLD PARAMETERS")
print("=" * 100)
print()
print("Parameters:")
for key, value in params.items():
    print(f"  {key}: {value}")
print()

try:
    query = _query_builder(params, "interpolate")

    print("=" * 100)
    print("GENERATED INTERPOLATE QUERY:")
    print("=" * 100)
    print()
    print(query)
    print()

    # Pretty print the query with line breaks at CTEs
    print("=" * 100)
    print("FORMATTED QUERY (WITH READABLE CTE BREAKS):")
    print("=" * 100)
    print()

    formatted_query = query.replace(", ", ",\n")
    print(formatted_query)

except Exception as e:
    print(f"Error building query: {e}")
    import traceback

    traceback.print_exc()
