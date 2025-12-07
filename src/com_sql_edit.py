from typing import TYPE_CHECKING
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import (
    QPushButton,
    QTextEdit,
)
from gui_tools import (
    change_font_size,
    render_df_info,
)
from components import SQLHighlighter

if TYPE_CHECKING:
    from schemas import History, Settings
    from com_dialog import DialogController
    from components import DataContainer


class SqlEditController:
    def __init__(
        self,
        settings: Settings,
        history: History,
        data_container: DataContainer,
        dialog_controller: DialogController,
    ):
        self._settings = settings
        self._data_container = data_container
        self._dialog_controller = dialog_controller
        self._history = history
        self._sql_edit_dirty: bool = False
        self._history_index: int | None = None
        self._history_snapshot: str | None = None
        self._highlighter = None
        # init ui
        self.sql_edit = QTextEdit()
        self.sql_edit.setAcceptRichText(False)
        self.sql_edit.setPlainText(settings.render_vars(settings.default_sql_query))
        self.sql_edit.setMaximumHeight(90)

        self.execute_button = QPushButton("Execute")
        self.execute_button.setFixedSize(120, 25)
        self.execute_button.setStyleSheet(
            f"background-color: {settings.colour_executeButton}"
        )
        self.execute_button.clicked.connect(self.execute_query)

        self.default_button = QPushButton("Default SQL")
        self.default_button.setFixedSize(120, 25)
        self.default_button.setStyleSheet(f"background-color: #bc4749; color: white;")
        self.default_button.clicked.connect(self._clear_query)

        self.table_info_button = QPushButton("Table Info")
        self.table_info_button.setFixedSize(120, 25)
        self.table_info_button.setStyleSheet(
            f"background-color: {settings.colour_tableInfoButton}"
        )
        self.table_info_button.clicked.connect(self.toggle_table_info)

    def update_highlighter_columns(self, columns: list[str]):
        if self._highlighter is not None:
            self._highlighter.update_columns(columns)

    def toggle_table_info(self):
        data = self._data_container.data

        if not self._data_container.is_file_open() or not data:
            return

        table_info = render_df_info(data.reader.duckdf_query)

        return self._dialog_controller.show_table_dialog("Table Info", table_info)

    def reset_history_navigation(self):
        self._history_index = None
        self._history_snapshot = None
        self.execute_button.setText("Execute")

    def execute_query(self, add_to_history: bool = True):
        query_text = self.sql_edit.toPlainText()

        if add_to_history:
            self._add_query_to_history(query_text)
        self._data_container.load_page(page=1, query=query_text)
        self._mark_sql_edit_dirty(False)

    def apply_styles(self):
        """Configure SQL editor colours and border state."""

        self._apply_edit_styles()
        self.execute_button.setStyleSheet(
            f"background-color: {self._settings.colour_executeButton}"
        )
        self.table_info_button.setStyleSheet(
            f"background-color: {self._settings.colour_tableInfoButton}"
        )
        change_font_size(self._settings, self.execute_button)
        change_font_size(self._settings, self.default_button)
        change_font_size(self._settings, self.table_info_button)
        font = self.sql_edit.font()
        font.setFamily(self._settings.default_sql_font)
        font.setPointSize(int(self._settings.default_sql_font_size))
        self.sql_edit.setFont(font)
        self._highlighter = SQLHighlighter(self.sql_edit.document(), self._settings)

    def handle_history_hotkeys(self, key: int):
        match key:
            case Qt.Key_Up:
                if self._show_previous_history_entry():
                    return True
                return False
            case Qt.Key_Down:
                if self._show_next_history_entry():
                    return True
                return False
            case _:
                return False

    def load_history_query(self):
        if self.execute_button.text() != "Execute":
            return False
        return self._show_previous_history_entry()

    def handle_edit_check(self, handle_history: bool = True):
        def delayed_handle_edit_check():
            queried = self._data_container.queried
            if queried is None:
                return
            text_changed = queried.strip() != self.sql_edit.toPlainText().strip()
            if handle_history and self._history_index is not None and text_changed:
                self.reset_history_navigation()

            if text_changed:
                self._mark_sql_edit_dirty(True)
            else:
                self._mark_sql_edit_dirty(False)

        QTimer.singleShot(50, delayed_handle_edit_check)

    def _mark_sql_edit_dirty(self, dirty: bool):
        if self._sql_edit_dirty == dirty:
            return
        self._sql_edit_dirty = dirty
        self._apply_edit_styles()

    def _clear_query(self):
        self.sql_edit.clear()
        self.sql_edit.setPlainText(
            self._settings.render_vars(self._settings.default_sql_query)
        )
        self.reset_history_navigation()
        self.execute_query(add_to_history=False)

    def _add_query_to_history(self, query_text: str):
        file_path = self._data_container.get_file_path()
        if file_path is None:
            return

        query = query_text.strip()
        file_path_str = str(file_path)

        if (
            self._history_index is not None
            and file_path_str in self._history.queries
            and self._history.queries[file_path_str][self._history_index] == query
        ):
            return
        if query:
            self._history.add_query(file_path_str, query_text)
        self.reset_history_navigation()

    def _begin_history_navigation(self) -> bool:
        file_path = self._data_container.get_file_path()
        if file_path is None:
            return False

        file_path_str = str(file_path)

        if (
            file_path_str not in self._history.queries
            or not self._history.queries[file_path_str]
        ):
            return False
        return True

    def _apply_edit_styles(self):
        background_colour = self._settings.colour_sqlEdit
        border_style = (
            self._settings.SQL_EDIT_DIRTY_BORDER
            if self._sql_edit_dirty
            else self._settings.SQL_EDIT_CLEAN_BORDER
        )
        self.sql_edit.setStyleSheet(
            f"background-color: {background_colour}; border: {border_style};"
        )

    def _apply_history_entry(self, text: str):
        previous_state = self.sql_edit.blockSignals(True)
        try:
            self.sql_edit.setPlainText(text)
            cursor = self.sql_edit.textCursor()
            cursor.movePosition(QTextCursor.End)
            self.sql_edit.setTextCursor(cursor)
        finally:
            self.sql_edit.blockSignals(previous_state)

    def _show_previous_history_entry(self) -> bool:
        if not self._begin_history_navigation():
            return False
        file_path = self._data_container.get_file_path()
        if file_path is None:
            return False
        file_path_str = str(file_path)
        if self._history_index is None:
            self._history_snapshot = self.sql_edit.toPlainText()
            self._history_index = 0
        elif self._history_index + 1 < len(self._history.queries[file_path_str]):
            self._history_index += 1
        entries = self._history.queries[file_path_str]
        entry = entries[self._history_index]
        self._apply_history_entry(entry)
        self.execute_button.setText(
            f"Execute (-{self._history_index + 1}/{len(entries)})"
        )
        self.handle_edit_check(handle_history=False)
        return True

    def _show_next_history_entry(self) -> bool:
        file_path = self._data_container.get_file_path()
        if file_path is None:
            return False
        file_path_str = str(file_path)
        if (
            file_path_str not in self._history.queries
            or not self._history.queries[file_path_str]
        ):
            return False
        if self._history_index is None:
            return False
        assert self._history_index is not None
        if self._history_index > 0:
            self._history_index -= 1
            entries = self._history.queries[file_path_str]
            entry = entries[self._history_index]
            self._apply_history_entry(entry)
            self.execute_button.setText(
                f"Execute (-{self._history_index + 1}/{len(entries)})"
            )
            self.handle_edit_check(handle_history=False)
            return True

        if self._history_snapshot is not None:
            snapshot = self._history_snapshot
            self.reset_history_navigation()
            self._apply_history_entry(snapshot)
            self.handle_edit_check()
            return True
        return False
