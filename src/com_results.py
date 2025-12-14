import json
import pandas as pd
from typing import TYPE_CHECKING, Any, cast
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QAction,
    QHBoxLayout,
    QLabel,
    QMenu,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
)
from PyQt5.QtCore import (
    QPoint,
    QTimer,
    Qt,
)
from PyQt5.QtWidgets import QApplication
from gui_tools import (
    change_font_size,
    render_column_value_counts,
    render_row_values,
)
from components import AutoWrapDelegate

if TYPE_CHECKING:
    from main import ParquetSQLApp
    from schemas import History, Settings
    from components import DataContainer
    from com_dialog import DialogController


class ResultsTable(QTableWidget):
    def __init__(
        self, settings: Settings, history: History, data_container: DataContainer
    ):
        super().__init__()
        self._settings = settings
        self._history = history
        self._data_container = data_container
        self._column_names: list[str] = []
        self._column_resize_connected = False
        self._total_pages = None
        self._page = 1
        self._page_df: pd.DataFrame | None = None
        self._total_view_row_count: int | None = None
        self._total_row_count: int | None = None
        self._rows_per_page: int = 0
        self._is_applying_column_widths = False
        self._zebra_striping_enabled = False
        self._deleyed_column_saving = QTimer()
        self._deleyed_column_saving.setSingleShot(True)
        self._deleyed_column_saving.timeout.connect(self._save_column_widths)
        self.last_column_widths: list[tuple[str, int]] | None = None
        self.is_error = False
        # init ui
        self.setWordWrap(True)
        self._wrap_delegate = AutoWrapDelegate(self, min_wrapped_lines=2)
        self.setItemDelegate(self._wrap_delegate)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.installEventFilter(self)
        self.viewport().installEventFilter(self)
        self.apply_row_colors()

    def toggle_zebra_striping(self):
        self._zebra_striping_enabled = not self._zebra_striping_enabled
        self.apply_row_colors()

    def is_zebra_striping_enabled(self) -> bool:
        return self._zebra_striping_enabled

    def apply_row_colors(self):
        base_color = QColor(self._settings.colour_resultTable)
        if not base_color.isValid():
            base_color = QColor("#ffffff")
        lightness = base_color.lightness()
        if lightness < 128:
            alternate_color = base_color.lighter(115)
        else:
            alternate_color = base_color.darker(110)
        if self._zebra_striping_enabled:
            self.setAlternatingRowColors(True)
            self.setStyleSheet(
                (
                    "QTableWidget {"
                    f"background-color: {base_color.name()};"
                    f"alternate-background-color: {alternate_color.name()};"
                    "}"
                    "QTableWidget::item:selected { background-color: palette(highlight); }"
                )
            )
        else:
            self.setAlternatingRowColors(False)
            self.setStyleSheet(f"background-color: {base_color.name()}")

    def reset_table_size(self):
        if not self._data_container.is_file_open():
            return
        if self.columnCount() == 0:
            return

        self.resizeColumnsToContents()
        self._limit_max_column_widths()
        self.apply_row_height()
        self._history.add_col_width(str(self._data_container.get_file_path()), None)

    def set_page(self, page: int):
        self._page = page

    def apply_settings(self):
        self._rows_per_page = int(
            self._settings.render_vars(self._settings.result_pagination_rows_per_page)
        )

    def get_page_row_offset(self) -> int:
        page_index = max(self._page - 1, 0)
        return page_index * self._rows_per_page

    def get_total_row_count(self) -> int | None:
        return self._total_row_count

    def get_total_view_row_count(self) -> int | None:
        return self._total_view_row_count

    def get_total_pages(self) -> int | None:
        return self._total_pages

    def set_page_df(self, df: pd.DataFrame | None):
        self._page_df = df
        if df is None:
            self._page = 1
            self.clear()
            self.setRowCount(0)
            self.setColumnCount(0)
            return

        self.setColumnCount(len(df.columns))
        self.setRowCount(len(df.index))

        self._column_names = [str(col) for col in df.columns]  # type: ignore
        header_labels = [
            f"{idx + 1}\n{name}" for idx, name in enumerate(self._column_names)
        ]
        if header_labels:
            self.setHorizontalHeaderLabels(header_labels)
        else:
            self.setHorizontalHeaderLabels([])
        header = self.horizontalHeader()
        if header:
            header.setDefaultAlignment(Qt.AlignCenter)
            if not self._column_resize_connected:
                header.sectionResized.connect(self._on_column_section_resized)
                self._column_resize_connected = True

        # Fill the table with the DataFrame data
        for i in range(len(df.index)):
            for j in range(len(df.columns)):
                item = QTableWidgetItem(str(df.iat[i, j]))
                self.setItem(i, j, item)

        page_rows = len(df.index)
        start_index = self.get_page_row_offset()
        if page_rows:
            row_labels = [str(start_index + idx + 1) for idx in range(page_rows)]
            self.setVerticalHeaderLabels(row_labels)
        else:
            self.setVerticalHeaderLabels([])

        self.apply_row_height()

        if len(df.index) and len(df.columns):
            self.setCurrentCell(0, 0)

        self._is_applying_column_widths = True
        self.resizeColumnsToContents()
        self._limit_max_column_widths()
        self._restore_column_widths()
        self._collect_current_column_widths()
        self._is_applying_column_widths = False

    def get_column_names(self) -> list[str]:
        return self._column_names

    def get_page_df(self) -> pd.DataFrame | None:
        return self._page_df

    def release_resources(self):
        self._page_df = None
        self._column_names = []
        self._total_pages = None
        self._total_row_count = None
        self._total_view_row_count = None
        self.clear()
        self.setRowCount(0)
        self.setColumnCount(0)

    def update_page_row_info(self):
        total_pages, total_view_row_count, total_row_count = (
            self._data_container.get_page_row_info()
        )
        self._total_pages = total_pages
        self._total_row_count = total_row_count
        self._total_view_row_count = total_view_row_count

    def apply_row_height(self):
        """Ensure row height stays compact even after data refreshes."""
        vertical_header = self.verticalHeader()
        if vertical_header:
            vertical_header.setDefaultSectionSize(
                self._settings.RESULT_TABLE_ROW_HEIGHT
            )
            vertical_header.setMinimumSectionSize(
                self._settings.RESULT_TABLE_ROW_HEIGHT
            )
            vertical_font = vertical_header.font()
            vertical_font.setPointSize(
                max(1, int(self._settings.default_result_font_size) - 2)
            )

    def get_column_name(self, column_i: int) -> str:
        if 0 <= column_i < len(self._column_names):
            return self._column_names[column_i]

        if self._page_df is not None and 0 <= column_i < len(self._page_df.columns):
            return str(self._page_df.columns[column_i])

        header_item = self.horizontalHeaderItem(column_i)
        if header_item:
            parts = header_item.text().splitlines()
            return parts[-1].strip()
        return ""

    def first_page(self):
        if not self._data_container.data or self._page == 1:
            return False
        self._data_container.load_page(page=1)
        return True

    def prev_page(self):
        if not self._data_container.data or self._page <= 1:
            return False
        self._page -= 1
        self._data_container.load_page(page=self._page)
        return True

    def next_page(self):
        if not self._data_container.data:
            return False
        if self._total_pages is None:
            return False
        if self._page >= self._total_pages:
            return False
        self._page += 1
        self._data_container.load_page(page=self._page)
        return True

    def last_page(self):
        if not self._data_container.data:
            return False
        if self._total_pages is None:
            return False
        self._page = self._total_pages
        self._data_container.load_page(page=self._page)
        return True

    def get_page(self) -> int:
        return self._page

    def _save_column_widths(self):
        current_widths = self._collect_current_column_widths()
        file_path = self._data_container.get_file_path()
        if not current_widths or file_path is None:
            return
        self._history.add_col_width(str(file_path), current_widths)
        self._deleyed_column_saving.stop()

    def _collect_current_column_widths(self) -> dict[str, int]:
        widths: dict[str, int] = {}
        if not self._column_names:
            return widths
        column_count = min(len(self._column_names), self.columnCount())
        if self.last_column_widths is None:
            self.last_column_widths = []
            for idx in range(column_count):
                width = self.columnWidth(idx)
                self.last_column_widths.append((self._column_names[idx], width))

        for idx in range(column_count):
            last_name, last_width = self.last_column_widths[idx]
            width = self.columnWidth(idx)
            if (
                width > 0
                and last_name == self._column_names[idx]
                and last_width != width
            ):
                widths[self._column_names[idx]] = width
        return widths

    def _limit_max_column_widths(self):
        for idx in range(self.columnCount()):
            width = self.columnWidth(idx)
            if width > self._settings.MAX_COLUMN_WIDTH:
                self.setColumnWidth(idx, self._settings.MAX_COLUMN_WIDTH)

    def _restore_column_widths(self) -> bool:
        """Apply persisted column widths for the current file, if any."""
        file_path = self._data_container.get_file_path()
        if not file_path or not self._column_names:
            return False
        saved_widths = self._history.get_col_widths(str(file_path))
        if not saved_widths:
            return False
        column_count = min(len(self._column_names), self.columnCount())
        for idx in range(column_count):
            column_name = self._column_names[idx]
            width = saved_widths.get(column_name)
            if isinstance(width, int) and width > 0:
                width = min(width, self._settings.MAX_COLUMN_WIDTH)
                self.setColumnWidth(idx, width)

        return True

    def _on_column_section_resized(
        self, _logical_index: int, _old_size: int, _new_size: int
    ):
        """Defer persistence when the user adjusts a column width."""
        if self._page_df is None or self._is_applying_column_widths:
            return
        if self._deleyed_column_saving.isActive() == False:
            self._deleyed_column_saving.start(1000)


class ResultsController:
    def __init__(
        self,
        parent: ParquetSQLApp,
        settings: Settings,
        history: History,
        dialog_controller: DialogController,
    ):
        super().__init__()
        self._parent = parent
        self._settings = settings
        self._parent.data_container.bind_methods(
            self._data_prepared,
            self._handle_error,
            self._query_finished,
        )
        # init ui
        self.result_label = QLabel()
        self.result_table = ResultsTable(settings, history, self._parent.data_container)
        self.result_table.customContextMenuRequested.connect(self._show_context_menu)
        self.result_table.currentCellChanged.connect(self._on_current_row_changed)

        self.pagination_layout = QHBoxLayout()
        self.pagination_layout.setSpacing(8)

        self.first_button = QPushButton("1")
        self.first_button.clicked.connect(self.first_page)
        self.pagination_layout.addWidget(self.first_button)

        self.prev_button = QPushButton("<")
        self.prev_button.clicked.connect(self.prev_page)
        self.pagination_layout.addWidget(self.prev_button)

        self.page_label = QLabel()
        self.page_label.setAlignment(Qt.AlignCenter)
        self.pagination_layout.addWidget(self.page_label, stretch=1)

        self.next_button = QPushButton(">")
        self.next_button.clicked.connect(self.next_page)
        self.pagination_layout.addWidget(self.next_button)

        self.last_button = QPushButton("1")
        self.last_button.clicked.connect(self.last_page)
        self.pagination_layout.addWidget(self.last_button)

        self.dialog_controller = dialog_controller

    def execute(self):
        self._parent.data_container.load_page(page=1)
        self.update_page_text()
        self.result_table.last_column_widths = None

    def apply_styles(self):
        self.result_table.apply_row_colors()
        header = self.result_table.horizontalHeader()
        if header:
            header.setStyleSheet("QHeaderView::section { padding: 6px 4px; }")
        change_font_size(self._settings, self.result_label)
        change_font_size(self._settings, self.first_button)
        change_font_size(self._settings, self.prev_button)
        change_font_size(self._settings, self.page_label)
        change_font_size(self._settings, self.next_button)
        change_font_size(self._settings, self.last_button)
        table_font = self.result_table.font()
        table_font.setFamily(self._settings.default_result_font)
        table_font.setPointSize(int(self._settings.default_result_font_size))
        self.result_table.setFont(table_font)

        header = self.result_table.horizontalHeader()
        header_font = header.font()
        header_font.setFamily(self._settings.default_result_font)
        header_font.setPointSize(
            max(1, int(self._settings.default_result_font_size) - 1)
        )
        header.setFont(header_font)

        vertical_header = self.result_table.verticalHeader()
        if vertical_header:
            vertical_font = vertical_header.font()
            vertical_font.setFamily(self._settings.default_result_font)
            vertical_font.setPointSize(
                max(1, int(self._settings.default_result_font_size) - 1)
            )
            vertical_header.setFont(vertical_font)
            self.result_table.apply_row_height()
        self.update_page_text()
        self.result_table.apply_settings()

    def update_result_label(self, row: int | None = None, column: int | None = None):
        df = self.result_table.get_page_df()
        if df is None:
            if not self.result_table.is_error:
                self.result_label.setText("No data loaded")
            return
        page_rows = len(df.index)
        total_cols = len(df.columns)
        valid_row = row if isinstance(row, int) and row >= 0 else None
        valid_col = column if isinstance(column, int) and column >= 0 else None
        start_offset = self.result_table.get_page_row_offset()
        row_text = f"{start_offset + valid_row + 1}" if valid_row is not None else ""
        col_text = f"{valid_col + 1}" if valid_col is not None else ""
        if page_rows:
            visible_range = f"Range: {start_offset + 1}~{start_offset + page_rows}"
        else:
            visible_range = ""

        total_row_count = self.result_table.get_total_row_count()
        total_view_row_count = self.result_table.get_total_view_row_count()
        if total_view_row_count is not None:
            total_rows_text = f"{total_row_count:,}"
            total_view_rows_text = f"{total_view_row_count:,}"
            select_text = (
                f"Select: {row_text}×{col_text}"
                if (row_text + col_text).strip()
                else ""
            )
            row_stats_text = f"{total_view_rows_text}"
            if total_view_rows_text != total_rows_text:
                row_stats_text += f" of {total_rows_text}"
            self.result_label.setText(
                f"Rows: {row_stats_text}   Page Rows: {str(page_rows)}    {visible_range}   Cols: {total_cols}   {select_text}"
            )
            total_pages = self.result_table.get_total_pages()
            self.last_button.setText(str(total_pages))
        else:
            if total_row_count is not None:
                total_rows_text = f"{total_row_count:,}"
                self.result_label.setText(f"Rows: 0 of {total_rows_text}")
            else:
                self.result_label.setText("No data loaded")
            self.last_button.setText("")

    def update_page_text(self):
        """set next / prev button text"""
        data = self._parent.data_container.data
        total_pages_value = self.result_table.get_total_pages()
        if data is not None and total_pages_value is not None:
            page = self.result_table.get_page()
            can_go_prev = page > 1
            page_str = f"Page {page}"
            assert total_pages_value is not None
            can_go_next = page < total_pages_value
        else:
            can_go_prev = False
            can_go_next = False
            page_str = ""

        self.prev_button.setEnabled(can_go_prev)
        self.first_button.setEnabled(can_go_prev)
        self.next_button.setEnabled(can_go_next)
        self.last_button.setEnabled(can_go_next)
        self.page_label.setText(page_str)

    def release_resources(self):
        self.result_table.is_error = False
        self.result_table.release_resources()
        self.update_result_label()
        self.update_page_text()

    def first_page(self):
        if self.result_table.first_page():
            self.update_page_text()

    def prev_page(self):
        if self.result_table.prev_page():
            self.update_page_text()

    def next_page(self):
        if self.result_table.next_page():
            self.update_page_text()

    def last_page(self):
        if self.result_table.last_page():
            self.update_page_text()

    def reset_table_size(self):
        self.result_table.reset_table_size()

    def toggle_zebra_striping(self):
        self.result_table.toggle_zebra_striping()
        if self._parent and hasattr(self._parent, "menu_controller"):
            action = getattr(
                self._parent.menu_controller, "toggle_zebra_striping_action", None
            )
            if action is not None:
                action.setChecked(self.result_table.is_zebra_striping_enabled())

    def handle_page_hotkeys(self, key: int):
        match key:
            case Qt.Key_Left:
                self.prev_page()
                return True
            case Qt.Key_Right:
                self.next_page()
                return True
            case _:
                return False

    def _handle_error(self, error: str):
        self._parent.stop_loading_animation()
        self.result_table.set_page_df(None)
        self.result_label.setText(f"Error: {error}")
        self.result_table.is_error = True

    def _data_prepared(self, df: pd.DataFrame, query: str, page: int):
        self.result_table.is_error = False
        self._parent.data_container.exit_query_thread()
        self.result_table.set_page(page)
        self.result_table.update_page_row_info()
        self.result_table.set_page_df(df)
        if self._parent.data_container.data:
            self._parent.sql_edit_controller.update_highlighter_columns(
                self._parent.data_container.data.columns,
            )
        if query.strip():
            self._parent.data_container.queried = query
        self._parent.sql_edit_controller.handle_edit_check()

    def _query_finished(self):
        self.update_result_label()
        self.update_page_text()
        self.result_table.last_column_widths = None
        self._parent.update_window_title()
        self._parent.menu_controller.update_action_states()
        self._parent.stop_loading_animation()

    def _on_current_row_changed(
        self,
        current_row: int | None,
        current_column: int | None,
        previous_row: int | None,
        previous_column: int | None,
    ):
        if current_row is not None and current_row >= 0:
            self.update_result_label(current_row, current_column)
        else:
            self.update_result_label()

    def _show_context_menu(self, pos: QPoint):
        contextMenu = QMenu(self._parent)

        header = self.result_table.horizontalHeader()
        column = header.logicalIndexAt(pos.x())
        row = self.result_table.indexAt(pos).row()

        if column >= 0:
            column_name = self.result_table.get_column_name(column)
            value_counts = QAction("Show Value Counts", self._parent)
            value_counts.triggered.connect(
                lambda: self._show_column_value_counts(column_name)
            )
            contextMenu.addAction(value_counts)
            row_values = QAction("Show This Row", self._parent)
            row_values.triggered.connect(lambda: self._show_row_values(row))
            contextMenu.addAction(row_values)
            contextMenu.addSeparator()

            # Create Copy Submenu
            copy_column_action = QAction("Copy Column Name", self._parent)
            copy_column_action.triggered.connect(lambda: self._copy_column_name(column))
            contextMenu.addAction(copy_column_action)

            copy_column_values_action = QAction("Copy Whole Column", self._parent)
            copy_column_values_action.triggered.connect(
                lambda: self._copy_column_values(column)
            )
            contextMenu.addAction(copy_column_values_action)

            if row >= 0:
                copy_row_values_action = QAction("Copy Whole Row", self._parent)
                copy_row_values_action.triggered.connect(
                    lambda: self._copy_row_values(row)
                )
                contextMenu.addAction(copy_row_values_action)
                copy_row_values_action = QAction("Copy Whole Row as Dict", self._parent)
                copy_row_values_action.triggered.connect(
                    lambda: self._copy_row_values(row, as_dict=True)
                )
                contextMenu.addAction(copy_row_values_action)

        contextMenu.exec_(self.result_table.mapToGlobal(pos))

    def _copy_column_name(self, column: int):
        column_name = self.result_table.get_column_name(column)
        if not column_name:
            return
        clipboard = QApplication.clipboard()
        clipboard.setText(column_name)

    def _copy_column_values(self, column: int):
        df = self.result_table.get_page_df()
        if df is None:
            return
        values = cast(list[Any], df.iloc[:, column].tolist())
        clipboard = QApplication.clipboard()
        clipboard.setText(str(values))

    def _copy_row_values(self, row: int, as_dict: bool = False):
        df = self.result_table.get_page_df()
        if df is None:
            return
        if not as_dict:
            values = cast(list[Any], df.iloc[row, :].tolist())
        else:
            txt = cast(dict[str, Any], df.iloc[row, :].to_dict())
            for key in list(txt.keys()):
                if txt[key] is None or isinstance(txt[key], (int, float, bool, str)):
                    continue
                txt[key] = str(txt[key])  # try to convert to string

            values = json.dumps(txt, indent=4, ensure_ascii=False)
        clipboard = QApplication.clipboard()
        clipboard.setText(str(values))

    def _show_column_value_counts(self, column: str):
        data = self._parent.data_container.data
        if not self._parent.data_container.is_file_open() or not data:
            return
        table_info = render_column_value_counts(
            data.reader.duckdf_query,
            column,
            data.reader.duckdf,
            max_rows=50,
        )
        return self.dialog_controller.show_table_dialog(
            f"Value Counts for {column}", table_info, 1
        )

    def _show_row_values(self, row: int):
        df = self.result_table.get_page_df()
        if not self._parent.data_container.is_file_open() or df is None:
            return
        dict_values = cast(dict[str, Any], df.iloc[row, :].to_dict())
        table_info = render_row_values(dict_values)
        return self.dialog_controller.show_table_dialog(
            f"Values for Row {row}", table_info
        )
