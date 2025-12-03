from typing import Any, Dict, List, Optional, Tuple
from io import StringIO
import math
import sys

import duckdb


MAX_VALUE_COUNT_ROWS = 200
MAX_VALUE_DISPLAY_CHARS = 80
NONE_ROW_HIGHLIGHT_STYLE = "background-color:#ffecec;display:block"


def render_df_info(duckdf: duckdb.DuckDBPyRelation) -> str:
    """returns md like formatted df.info"""
    shape = duckdf.shape

    output_buffer = StringIO()
    sys.stdout = output_buffer
    duckdf.describe().show(max_width=10**19)
    sys.stdout = sys.__stdout__
    descr = output_buffer.getvalue()

    h = f"### Rows: {shape[0]}, Columns: {shape[1]}\n{'-'*50}\n"
    try:
        lines = descr.strip().split("\n")
        headers = lines[1].strip("│").split("│")
        headers = [header.strip() for header in headers]
        types = lines[2].strip("│").split("│")
        types = [t.strip() for t in types]
        data_lines = lines[4:-1]
        data = []
        for line in data_lines:
            row = line.strip("│").split("│")
            row = [item.strip() for item in row]
            data.append(row)

        stat_names = [row[0] for row in data]

        markdown_table = "| Column | Type | " + " | ".join(stat_names) + " |\n"
        markdown_table += "|-" + "-|-".join(["-"] * (len(stat_names) + 2)) + "-|\n"

        for col_idx in range(1, len(headers)):
            row_values = [headers[col_idx], types[col_idx]]
            for row in data:
                row_values.append(row[col_idx] if col_idx < len(row) else "")
            markdown_table += "| " + " | ".join(row_values) + " |\n"

        return h + markdown_table

    except Exception as e:
        return h + "\n" + descr


def _escape_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _collect_value_counts(
    relation: duckdb.DuckDBPyRelation,
    column: str,
) -> Tuple[List[Tuple[Any, int]], int]:
    escaped_column = _escape_identifier(column)
    counts_relation = relation.aggregate(
        f"{escaped_column} AS value, COUNT(*) AS value_count GROUP BY 1"
    ).order("value_count DESC")
    rows = counts_relation.fetchall()
    normalized: List[Tuple[Any, int]] = []
    total = 0
    for raw_value, raw_count in rows:
        count = 0
        if raw_count is not None:
            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                count = 0
        normalized.append((raw_value, count))
        total += count
    return normalized, total


def _format_value_cell(value: Any) -> str:
    if value is None:
        return "None"

    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"

    if isinstance(value, bytes):
        try:
            display = value.decode("utf-8")
        except UnicodeDecodeError:
            display = value.decode("utf-8", errors="replace")
    else:
        display = str(value)

    display = display.replace("|", "\\|").replace("\n", " ").strip()
    if not display:
        display = "(empty)"

    if len(display) > MAX_VALUE_DISPLAY_CHARS:
        display = display[: MAX_VALUE_DISPLAY_CHARS - 3] + "..."
    return display


def _apply_none_row_highlight(value: Any, cells: List[str]) -> List[str]:
    if value is None:
        return [
            f'<span style="{NONE_ROW_HIGHLIGHT_STYLE}">{cell}</span>' for cell in cells
        ]
    return cells


def _format_count_cell(count: Optional[int]) -> str:
    if count is None:
        return ""
    return f"{count:,}"


def _format_count_cell_with_percentage(
    count: Optional[int], total: Optional[int]
) -> str:
    if count is None or total is None:
        return ""
    return f"{count:,}  ({count/total:03.0%})"


def render_column_value_counts(
    current_view: Optional[duckdb.DuckDBPyRelation],
    column: str,
    full_view: Optional[duckdb.DuckDBPyRelation] = None,
    max_rows: int = MAX_VALUE_COUNT_ROWS,
) -> str:
    title_line = f"### Value counts for `{column}`"
    separator_line = "-" * 50

    if current_view is None:
        return f"{title_line}\nNo active view provided.\n{separator_line}"

    if not column:
        return f"{title_line}\nColumn name must not be empty.\n{separator_line}"

    assert current_view is not None
    if full_view is None:
        full_view = current_view
    assert full_view is not None

    current_columns = getattr(current_view, "columns", None)
    if current_columns and column not in current_columns:
        return (
            f"{title_line}\nColumn `{column}` is not available in the current view.\n"
            f"{separator_line}"
        )

    full_columns = getattr(full_view, "columns", None)
    full_column_available = full_columns is None or column in full_columns

    try:
        current_counts, current_total = _collect_value_counts(current_view, column)
    except Exception as exc:
        return (
            f"{title_line}\nFailed to compute value counts for the current view.\n\n"
            f"{exc}"
        )

    all_counts: List[Tuple[Any, int]] = []
    all_total: Optional[int] = None
    all_error: Optional[str] = None

    if full_column_available:
        try:
            all_counts, all_total = _collect_value_counts(full_view, column)
        except Exception as exc:
            all_error = f"Unable to compute counts for the original data: {exc}"
    else:
        all_error = "Column not present in the original dataset."

    summary_parts = [f"Rows (current view): {current_total:,}"]
    if all_total is not None:
        summary_parts.append(f"Rows (all): {all_total:,}")
    elif all_error:
        summary_parts.append("Rows (all): n/a")

    header = f"{title_line}\n{' | '.join(summary_parts)}\n{separator_line}\n"

    current_dict: Dict[Any, int] = {value: count for value, count in current_counts}
    all_dict: Dict[Any, int] = {value: count for value, count in all_counts}
    combined_values = set(current_dict.keys()) | set(all_dict.keys())

    if not combined_values:
        note_text = f"{all_error}\n" if all_error else ""
        return header + "No values found for this column.\n" + note_text

    value_order = sorted(
        combined_values,
        key=lambda val: (
            current_dict.get(val, -1),
            all_dict.get(val, -1),
            _format_value_cell(val),
        ),
        reverse=True,
    )

    truncated = False
    if len(value_order) > max_rows:
        value_order = value_order[:max_rows]
        truncated = True

    notes: List[str] = []
    lines = ["<br/><br/><br/>", "#### Order by counts", ""]
    lines += ["| Value | Current view | All |", "| --- | ---: | ---: |"]
    for value in value_order:
        cells = [
            _format_value_cell(value),
            _format_count_cell_with_percentage(current_dict.get(value), current_total),
            _format_count_cell_with_percentage(all_dict.get(value), all_total),
        ]
        highlighted_cells = _apply_none_row_highlight(value, cells)
        lines.append("| " + " | ".join(highlighted_cells) + " |")
    if truncated:
        lines.append(
            "| "
            + " | ".join([f"{len(combined_values) - max_rows:,} more...", "-", "-"])
            + " |"
        )

    lines.append("")
    lines.append("<br/><br/><br/>")
    lines.append("")
    lines.append("#### Order by value name")
    lines += ["| Value | Current view | All |", "| --- | ---: | ---: |"]
    order_values = sorted(
        combined_values, key=lambda val: val if isinstance(val, str) else "None"
    )
    if len(order_values) > max_rows:
        order_values = order_values[:max_rows]
        truncated = True
    for value in order_values:
        cells = [
            _format_value_cell(value),
            _format_count_cell_with_percentage(current_dict.get(value), current_total),
            _format_count_cell_with_percentage(all_dict.get(value), all_total),
        ]
        highlighted_cells = _apply_none_row_highlight(value, cells)
        lines.append("| " + " | ".join(highlighted_cells) + " |")
    if truncated:
        lines.append(
            "| "
            + " | ".join([f"{len(combined_values) - max_rows:,} more...", "-", "-"])
            + " |"
        )

    if all_error:
        notes.append(all_error)
    notes_block = ("\n\n\n" + "\n".join(notes) + "") if notes else ""
    # `` show all lines
    additional_lines = []
    if truncated:
        additional_lines.append("\n\n")
        additional_lines.append("<br/><br/><br/>")
        additional_lines.append("")
        additional_lines.append("#### Order by value name (current view only)")
        additional_lines += ["| Value | Current view |", "| --- | ---: |"]
        order_values = sorted(
            combined_values, key=lambda val: val if isinstance(val, str) else "None"
        )
        for value in order_values:
            if value not in current_dict:
                continue
            cells = [
                _format_value_cell(value),
                _format_count_cell_with_percentage(
                    current_dict.get(value), current_total
                ),
            ]
            highlighted_cells = _apply_none_row_highlight(value, cells)
            additional_lines.append("| " + " | ".join(highlighted_cells) + " |")

    return header + "\n".join(lines) + notes_block + "\n".join(additional_lines)
