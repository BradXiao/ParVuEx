from typing import Any, TYPE_CHECKING
from io import StringIO
import math
import sys
from PyQt5.QtGui import QFont, QTextDocument
import re
from PyQt5.QtWidgets import QWidget
import duckdb
import json

if TYPE_CHECKING:
    from src.schemas import Settings


MAX_VALUE_COUNT_ROWS = 200
MAX_VALUE_DISPLAY_CHARS = 80
NONE_ROW_HIGHLIGHT_STYLE = "background-color:#fad2e1;display:block"

TABLE_MARKDOWN_STYLESHEET = """
table {
    border-collapse: collapse;
}
th, td {
    padding: 5px;
}
""".strip()

ZEBRA_ODD_BG = "#f0efeb"


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
        data: list[list[str]] = []
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

    except:
        return h + "\n" + descr


def _escape_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _collect_value_counts(
    relation: duckdb.DuckDBPyRelation,
    column: str,
) -> tuple[list[tuple[Any, int]], int, int]:
    escaped_column = _escape_identifier(column)
    counts_relation = relation.aggregate(
        f"{escaped_column} AS value, COUNT(*) AS value_count GROUP BY 1"
    ).order("value_count DESC")
    rows = counts_relation.fetchall()
    normalized: list[tuple[Any, int]] = []
    total = 0
    distinct_items: set[Any] = set()
    for raw_value, raw_count in rows:
        count = 0
        distinct_items.add(raw_value)
        if raw_count is not None:
            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                count = 0
        normalized.append((raw_value, count))
        total += count
    return normalized, total, len(distinct_items)


def _format_value_cell(
    value: Any, limit_max_chars: bool = True, json_format: bool = False
) -> str:
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

    if json_format and (display.startswith("{") or display.startswith("[")):
        try:
            display = json.dumps(json.loads(display), indent="\t", ensure_ascii=False)
            display = display.replace("\n", "%%BR%%")
            display = display.replace("\t", "%%TAB%%")
            display = f"<div>{display}</div>"
        except:
            pass

    display = display.replace("|", "\\|").replace("\n", " ").strip()

    if not display:
        display = "(empty)"

    if limit_max_chars and len(display) > MAX_VALUE_DISPLAY_CHARS:
        display = display[: MAX_VALUE_DISPLAY_CHARS - 3] + "..."
    return display


def _apply_none_row_highlight(value: Any, cells: list[str]) -> list[str]:
    if value is None:
        return [
            f'<span style="{NONE_ROW_HIGHLIGHT_STYLE}">{cell}</span>' for cell in cells
        ]
    return cells


def _format_count_cell_with_percentage(count: int | None, total: int | None) -> str:
    if count is None or total is None:
        return ""
    return f"{count:,}  ({count/total:03.0%})"


def render_column_value_counts(
    current_view: duckdb.DuckDBPyRelation | None,
    column: str,
    full_view: duckdb.DuckDBPyRelation | None = None,
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
        current_counts, current_total, current_distinct = _collect_value_counts(
            current_view, column
        )
        current_distinct_text = f"{current_distinct:,}"
    except Exception as exc:
        return (
            f"{title_line}\nFailed to compute value counts for the current view.\n\n"
            f"{exc}"
        )

    all_counts: list[tuple[Any, int]] = []
    all_total: int | None = None
    all_error: str | None = None

    if full_column_available:
        try:
            all_counts, all_total, all_distinct = _collect_value_counts(
                full_view, column
            )
            all_distinct_text = f"{all_distinct:,}"
        except Exception as exc:
            all_error = f"Unable to compute counts for the original data: {exc}"
    else:
        all_error = "Column not present in the original dataset."
        all_distinct_text = f"n/a"

    summary_parts = [f"Rows (current view): {current_total:,}"]
    if all_total is not None:
        summary_parts.append(f"Rows (all): {all_total:,}")
    elif all_error:
        summary_parts.append("Rows (all): n/a")

    header = f"{title_line}\n{' | '.join(summary_parts)}\n{separator_line}\n"

    current_dict: dict[Any, int] = {value: count for value, count in current_counts}
    all_dict: dict[Any, int] = {value: count for value, count in all_counts}
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

    notes: list[str] = []
    lines = ["<br/><br/><br/>", "#### Values info", ""]
    lines += ["| Item | Current view | All |", "| --- | ---: | ---: |"]
    lines += [f"| Distinct | {current_distinct_text} | {all_distinct_text} |"]

    lines.append("")
    lines.append("<br/><br/><br/>")
    lines.append("")
    lines += ["#### Order by counts", ""]
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
    additional_lines: list[str] = []
    if truncated:
        additional_lines.append("\n\n")
        additional_lines.append("<br/><br/><br/>")
        additional_lines.append("")
        additional_lines.append(
            "#### Order by value name (with values in current view)"
        )
        additional_lines += ["| Value | Current view |", "| --- | ---: |"]
        order_values = sorted(
            combined_values, key=lambda val: val if isinstance(val, str) else "None"
        )
        counter = 0
        for value in order_values:
            if value not in current_dict:
                continue
            counter += 1
            if counter > max_rows:
                truncated = True
                break
            cells = [
                _format_value_cell(value),
                _format_count_cell_with_percentage(
                    current_dict.get(value), current_total
                ),
            ]
            highlighted_cells = _apply_none_row_highlight(value, cells)
            additional_lines.append("| " + " | ".join(highlighted_cells) + " |")

        if truncated:
            additional_lines.append("| " + " | ".join([f"skipped...", "-", "-"]) + " |")

    return header + "\n".join(lines) + notes_block + "\n".join(additional_lines)


def render_row_values(
    row_values: dict[str, Any],
) -> str:
    title_line = f"### Values for selected row"
    separator_line = "-" * 50

    lines = [
        title_line,
        separator_line,
        "",
        "| Column | Value |",
        "| --- | --- |",
    ]

    for column_name, value in row_values.items():
        column_display = column_name.replace("|", "\\|").replace("\n", " ").strip()
        cells = [
            column_display,
            _format_value_cell(
                value,
                limit_max_chars=False,
                json_format=False if isinstance(value, str) == False else True,
            ),
        ]
        highlighted_cells = _apply_none_row_highlight(value, cells)
        lines.append("| " + " | ".join(highlighted_cells) + " |")

    return "\n".join(lines)


def _apply_zebra_striping(html: str) -> str:
    """Add inline background color to odd-numbered table rows."""

    def replace_tr(match: re.Match[str]) -> str:
        before_table = match.group(1)
        table_content = match.group(2)
        after_table = match.group(3)

        row_idx = 0

        def style_row(row_match: re.Match[str]) -> str:
            nonlocal row_idx
            row_idx += 1
            tag = row_match.group(0)
            # Skip header rows (first row or rows with <th>)
            if row_idx == 1:
                return tag
            # Odd data rows (row_idx 2, 4, 6... are actually rows 1, 3, 5... after header)
            if row_idx % 2 == 0:
                if 'style="' in tag:
                    return tag.replace(
                        'style="', f'style="background-color:{ZEBRA_ODD_BG};'
                    )
                return tag.replace(
                    "<tr", f'<tr style="background-color:{ZEBRA_ODD_BG};"'
                )
            return tag

        styled_content = re.sub(r"<tr[^>]*>", style_row, table_content)
        return before_table + styled_content + after_table

    return re.sub(r"(<table[^>]*>)(.*?)(</table>)", replace_tr, html, flags=re.DOTALL)


def markdown_to_html_with_table_styles(markdown_text: str, table_font: QFont) -> str:
    doc = QTextDocument()
    doc.setDefaultFont(table_font)
    doc.setMarkdown(markdown_text)
    html = doc.toHtml()
    html = html.replace("%%BR%%", "<br>")
    html = html.replace("%%TAB%%", "&nbsp;&nbsp;&nbsp;&nbsp;")
    html = _apply_zebra_striping(html)
    style_block = f"<style>{TABLE_MARKDOWN_STYLESHEET}</style>"
    if "<head>" in html:
        return html.replace("<head>", f"<head>{style_block}", 1)
    return f"{style_block}{html}"


def normalize_instance_mode_value(value: str | None) -> str:
    _MULTI_MODE_TOKENS = {
        "multi",
        "multi_instance",
        "multi-instance",
        "multiwindow",
        "multi_window",
        "multiwindows",
        "multiple",
        "windows",
        "true",
        "yes",
        "on",
    }
    _SINGLE_MODE_TOKENS = {
        "single",
        "single_instance",
        "single-instance",
        "singlewindow",
        "single_window",
        "one",
        "1",
        "false",
        "no",
        "off",
    }
    if value is None:
        return "single"
    normalized = str(value).strip().lower()
    if normalized in ("single", "multi_window"):
        return normalized
    if normalized in _MULTI_MODE_TOKENS:
        return "multi_window"
    if normalized in _SINGLE_MODE_TOKENS:
        return "single"
    raise ValueError(f"Unsupported instance mode value: {value}")


def get_instance_mode(settings: Settings) -> str:
    raw_mode = getattr(settings, "instance_mode", "single")
    try:
        return normalize_instance_mode_value(raw_mode)
    except ValueError:
        return "single"


def is_multi_window_mode(settings: Settings) -> bool:
    return get_instance_mode(settings) == "multi_window"


def change_font_size(settings: Settings, component: QWidget):
    font = component.font()
    font.setPointSize(int(settings.default_ui_font_size))
    component.setFont(font)
