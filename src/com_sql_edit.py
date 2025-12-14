from typing import TYPE_CHECKING
from PyQt5.QtCore import QPoint, QTimer, Qt, QStringListModel, pyqtSignal
from PyQt5.QtGui import QKeyEvent, QTextCursor
from PyQt5.QtWidgets import (
    QCompleter,
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
    from com_results import ResultsController


class AutoCompleteTextEdit(QTextEdit):
    execute_requested = pyqtSignal()

    def __init__(self):
        super().__init__()
        self._completer: QCompleter | None = None
        self._string_list_model: QStringListModel | None = None
        self._all_completions: list[str] = []
        self._recent_completions: list[str] = []

    def set_completer(self, completer: QCompleter):
        if self._completer is not None:
            try:
                self._completer.activated.disconnect(self.insert_completion)
            except TypeError:
                pass
        self._completer = completer
        self._completer.setWidget(self)
        self._completer.activated.connect(self.insert_completion)
        model = completer.model()
        self._string_list_model = model if isinstance(model, QStringListModel) else None

    def insert_completion(self, completion: str):
        if not self._completer:
            return
        cursor = self.textCursor()
        cursor.select(QTextCursor.WordUnderCursor)
        cursor.insertText(completion)
        self.setTextCursor(cursor)
        self._record_completion_use(completion)

    def _record_completion_use(self, completion: str):
        trimmed = completion.strip()
        if not trimmed:
            return
        if trimmed in self._recent_completions:
            self._recent_completions.remove(trimmed)
        self._recent_completions.insert(0, trimmed)
        if len(self._recent_completions) > 5:
            self._recent_completions = self._recent_completions[:5]

    @staticmethod
    def _normalize_for_match(value: str) -> str:
        return value.replace("_", "").replace(" ", "").lower()

    @staticmethod
    def _is_subsequence(query: str, target: str) -> bool:
        if not query:
            return True
        qi = 0
        for ch in target:
            if ch == query[qi]:
                qi += 1
                if qi == len(query):
                    return True
        return False

    def set_completion_words(self, words: list[str]):
        self._all_completions = list(words)

    def is_completion_visible(self) -> bool:
        return bool(self._completer and self._completer.popup().isVisible())

    def hide_completion_popup(self):
        if self._completer:
            self._completer.popup().hide()

    def _text_under_cursor(self) -> str:
        cursor = self.textCursor()
        block = cursor.block()
        text = block.text()
        block_pos = cursor.position() - block.position()

        def is_word_char(ch: str) -> bool:
            return ch.isalnum() or ch == "_"

        start = block_pos
        end = block_pos

        while start > 0 and is_word_char(text[start - 1]):
            start -= 1
        while end < len(text) and is_word_char(text[end]):
            end += 1

        if start == end and end < len(text) and is_word_char(text[end]):
            while end < len(text) and is_word_char(text[end]):
                end += 1

        return text[start:end]

    def keyPressEvent(self, event: QKeyEvent):
        print(event.key())
        if self._completer and self._completer.popup().isVisible():
            match event.key():
                case Qt.Key_Enter | Qt.Key_Return:
                    popup = self._completer.popup()
                    current_index = popup.currentIndex()
                    if current_index.isValid():
                        completion = current_index.data(Qt.DisplayRole)
                        if completion:
                            self.insert_completion(completion)
                        self._completer.popup().hide()
                        return
                    else:
                        self._completer.popup().hide()
                        self.execute_requested.emit()
                        return
                case Qt.Key_Escape | Qt.Key_Tab | Qt.Key_Backtab:
                    event.ignore()
                    return
                case _:
                    pass

        super().keyPressEvent(event)

        if not self._completer:
            return

        match event.key():
            case (
                Qt.Key_Left
                | Qt.Key_Right
                | Qt.Key_Up
                | Qt.Key_Down
                | Qt.Key_Home
                | Qt.Key_End
                | Qt.Key_PageUp
                | Qt.Key_PageDown
            ):
                self._completer.popup().hide()
                return
            case _:
                pass

        if not event.text() and event.key() not in (Qt.Key_Backspace, Qt.Key_Delete):
            self._completer.popup().hide()
            return

        completion_prefix = self._text_under_cursor()
        if not completion_prefix:
            self._completer.popup().hide()
            return

        prefix_normalized = self._normalize_for_match(completion_prefix)
        source_words = self._all_completions or (
            self._string_list_model.stringList()
            if self._string_list_model is not None
            else []
        )
        matches = [
            word
            for word in source_words
            if self._is_subsequence(prefix_normalized, self._normalize_for_match(word))
        ]
        matches = self._sort_matches_by_recent_usage(matches)
        if (
            len(matches) == 1
            and self._normalize_for_match(matches[0]) == prefix_normalized
        ):
            self._completer.popup().hide()
            return

        if not matches:
            self._completer.popup().hide()
            return

        if self._string_list_model is not None:
            self._string_list_model.setStringList(matches)
        self._completer.setCompletionPrefix("")  # avoid Qt re-filtering by underscores
        popup = self._completer.popup()
        popup.setCurrentIndex(self._completer.completionModel().index(0, 0))

        cursor_rect = self.cursorRect()
        cursor_rect.setWidth(
            self._completer.popup().sizeHintForColumn(0)
            + self._completer.popup().verticalScrollBar().sizeHint().width()
        )
        self._completer.complete(cursor_rect)

    def _sort_matches_by_recent_usage(self, matches: list[str]) -> list[str]:
        if not self._recent_completions:
            return matches
        prioritized = []
        seen: set[str] = set()
        for recent in self._recent_completions:
            if recent in matches and recent not in seen:
                prioritized.append(recent)
                seen.add(recent)
        prioritized.extend([m for m in matches if m not in seen])
        return prioritized


class SqlEditController:
    def __init__(
        self,
        settings: Settings,
        history: History,
        data_container: DataContainer,
        dialog_controller: DialogController,
        results_controller: ResultsController,
    ):
        self._settings = settings
        self._data_container = data_container
        self._dialog_controller = dialog_controller
        self._results_controller = results_controller
        self._history = history
        self._sql_edit_dirty: bool = False
        self._history_index: int | None = None
        self._history_snapshot: str | None = None
        self._highlighter = None
        self._auto_complete_model = QStringListModel()
        self._auto_complete_completer = QCompleter(self._auto_complete_model)
        self._auto_complete_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._auto_complete_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._auto_complete_completer.setFilterMode(Qt.MatchContains)
        # init ui
        self.sql_edit = AutoCompleteTextEdit()
        self.sql_edit.setAcceptRichText(False)
        self.sql_edit.setPlainText(settings.render_vars(settings.default_sql_query))
        self.sql_edit.setMaximumHeight(90)
        self.sql_edit.setContextMenuPolicy(Qt.CustomContextMenu)
        self.sql_edit.customContextMenuRequested.connect(
            self._show_sql_edit_context_menu
        )
        self.sql_edit.set_completer(self._auto_complete_completer)
        self.sql_edit.execute_requested.connect(self.execute_query)
        self.update_auto_complete_words([])

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

    def update_auto_complete_words(self, words: list[str]):
        base_words = [
            self._settings.render_vars(self._settings.default_data_var_name),
            *self._settings.sql_keywords,
        ]
        merged_words = base_words + words
        deduped: list[str] = []
        seen: set[str] = set()
        for word in merged_words:
            if not word:
                continue
            lowered = word.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(word)
        self._auto_complete_model.setStringList(deduped)
        self.sql_edit.set_completion_words(deduped)

    def is_completion_visible(self) -> bool:
        return self.sql_edit.is_completion_visible()

    def hide_completion_popup(self):
        self.sql_edit.hide_completion_popup()

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

    def _insert_columns(self):
        if not self._data_container.data:
            return
        columns = self._data_container.data.columns
        cursor = self.sql_edit.textCursor()
        cursor.insertText(f"({','.join(columns)})")

    def _insert_selected_columns(self):
        table = self._results_controller.result_table
        selected_column_names: list[str] = []
        selection_model = table.selectionModel()
        selection_columns = selection_model.selectedColumns()
        for column_info in selection_columns:
            selected_column_names.append(table.get_column_name(column_info.column()))

        if not selected_column_names:
            return
        cursor = self.sql_edit.textCursor()
        cursor.insertText(f"({','.join(selected_column_names)})")

    def _show_sql_edit_context_menu(self, pos: QPoint):
        menu = self.sql_edit.createStandardContextMenu()
        menu.addSeparator()

        insert_columns_action = menu.addAction("Insert All Columns")
        insert_columns_action.triggered.connect(self._insert_columns)
        insert_selected_columns_action = menu.addAction("Insert Selected Columns")
        insert_selected_columns_action.triggered.connect(self._insert_selected_columns)
        menu.addSeparator()

        execute_action = menu.addAction("Execute Query")
        execute_action.triggered.connect(lambda: self.execute_query())

        default_action = menu.addAction("Reset to Default SQL")
        default_action.triggered.connect(lambda: self._clear_query())

        table_info_action = menu.addAction("Table Info")
        table_info_action.triggered.connect(lambda: self.toggle_table_info())
        table_info_action.setEnabled(
            self._data_container.is_file_open() and bool(self._data_container.data)
        )

        menu.addSeparator()
        has_history = self._begin_history_navigation()
        previous_action = menu.addAction("History - Previous")
        previous_action.triggered.connect(lambda: self._show_previous_history_entry())
        previous_action.setEnabled(has_history)

        next_action = menu.addAction("History - Next")
        next_action.triggered.connect(lambda: self._show_next_history_entry())
        next_action.setEnabled(self._history_index is not None)

        menu.exec_(self.sql_edit.mapToGlobal(pos))
