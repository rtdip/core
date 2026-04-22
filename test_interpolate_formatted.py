"""
Formatted output of the complete interpolate query with mocked data
"""

import sys

sys.path.insert(0, "/Users/amber.rigg/Projects/core/src/sdk/python")

from rtdip_sdk.queries.time_series._time_series_query_builder import (
    _build_raw_query_for_interpolate,
    _build_interpolate_intervals_cte,
    _build_interpolate_fill_intervals_cte,
    _build_interpolate_calculate_cte,
    _build_interpolate_interpolate_cte,
)

# Mock parameters
timestamp_column = "EventTime"
tagname_column = "TagName"
value_column = "Value"
status_column = "Status"
tag_names = ["QSGTL_111TI32064.DACA.PV"]
start_date = "2026-01-20T00:00:00+00:00"
end_date = "2026-01-21T23:59:59+00:00"
time_zone = "+0000"
time_interval_rate = "1"
time_interval_unit = "minute"
business_unit = "integratedgas"
asset = "qsgtl"
data_security_level = "confidential"
data_type = "float"

# Build CTEs
raw_query = _build_raw_query_for_interpolate(
    sql_query_name="raw",
    timestamp_column=timestamp_column,
    tagname_column=tagname_column,
    status_column=status_column,
    value_column=value_column,
    start_date=start_date,
    end_date=end_date,
    time_zone=time_zone,
    time_interval_unit=time_interval_unit,
    agg_method=None,
    deduplicate=True,
    source=None,
    business_unit=business_unit,
    asset=asset,
    data_security_level=data_security_level,
    data_type=data_type,
    tag_names=tag_names,
    include_status=False,
    include_bad_data=True,
    case_insensitivity_tag_search=False,
    sort=True,
)

intervals_query = _build_interpolate_intervals_cte(
    timestamp_column=timestamp_column,
    tagname_column=tagname_column,
    tag_names=tag_names,
    start_date=start_date,
    end_date=end_date,
    time_zone=time_zone,
    time_interval_rate=time_interval_rate,
    time_interval_unit=time_interval_unit,
    case_insensitivity_tag_search=False,
)

fill_intervals_query = _build_interpolate_fill_intervals_cte(
    timestamp_column=timestamp_column,
    tagname_column=tagname_column,
    value_column=value_column,
)

interpolate_calculate_query = _build_interpolate_calculate_cte(
    timestamp_column=timestamp_column,
    tagname_column=tagname_column,
    value_column=value_column,
)

interpolate_query = _build_interpolate_interpolate_cte(
    timestamp_column=timestamp_column,
    tagname_column=tagname_column,
    value_column=value_column,
)

# Format and print
formatted_query = f"""WITH {raw_query},
{intervals_query},
{fill_intervals_query},
{interpolate_calculate_query},
{interpolate_query}
SELECT `{timestamp_column}`, `{tagname_column}`, `{value_column}`
FROM interpolate
"""

# Write to file
with open("/tmp/interpolate_query_formatted.sql", "w") as f:
    f.write(formatted_query)

print(formatted_query)
print("\n✓ Query saved to /tmp/interpolate_query_formatted.sql")
