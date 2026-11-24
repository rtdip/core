import json

import pandas as pd


def extract_boundary_search_status(result, marker_column):
    if isinstance(result, pd.DataFrame):
        if marker_column in result.columns:
            incomplete = (
                pd.to_numeric(result[marker_column], errors="coerce")
                .fillna(0)
                .gt(0)
                .any()
            )
            return result.drop(columns=[marker_column]), bool(incomplete)

        if "Value" not in result.columns:
            return result, False

        values = result["Value"].tolist()
        decoded = [_decode_envelope(value, marker_column) for value in values]
        if not any(found_marker for _, found_marker, _ in decoded):
            return result, False

        cleaned = result.copy()
        cleaned["Value"] = [value for value, _, _ in decoded]
        return cleaned, any(incomplete for _, _, incomplete in decoded)

    if not isinstance(result, dict) or not result.get("data"):
        return result, False

    rows = json.loads(f"[{result['data']}]")
    decoded_rows = [_decode_envelope(row, marker_column) for row in rows]
    if not any(found_marker for _, found_marker, _ in decoded_rows):
        return result, False

    cleaned = result.copy()
    row_values = [value for value, _, _ in decoded_rows]
    cleaned["data"] = ",".join(
        json.dumps(value, separators=(",", ":")) for value in row_values
    )
    cleaned["sample_row"] = (
        json.dumps(row_values[0], separators=(",", ":")) if row_values else None
    )
    return cleaned, any(incomplete for _, _, incomplete in decoded_rows)


def _decode_envelope(value, marker_column):
    was_string = isinstance(value, str)
    try:
        row = json.loads(value) if was_string else value
    except (TypeError, ValueError):
        return value, False, False

    if not isinstance(row, dict) or marker_column not in row:
        return value, False, False

    incomplete = bool(pd.to_numeric(pd.Series([row.pop(marker_column)]), errors="coerce").fillna(0).gt(0).iloc[0])
    row = row.pop("data", row)
    cleaned = json.dumps(row, separators=(",", ":")) if was_string else row
    return cleaned, True, incomplete