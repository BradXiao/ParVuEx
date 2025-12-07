from typing import TYPE_CHECKING, Any, Callable
from pathlib import Path
from PyQt5.QtWidgets import (
    QLabel,
    QMessageBox,
    QStyleOptionViewItem,
    QStyledItemDelegate,
    QWidget,
    QTextBrowser,
    QFrame,
)
from PyQt5.QtGui import (
    QColor,
    QFont,
    QMovie,
    QPixmap,
    QResizeEvent,
    QSyntaxHighlighter,
    QTextCharFormat,
    QKeyEvent,
    QTextCursor,
)
from PyQt5.QtCore import (
    QModelIndex,
    QRegExp,
    QSize,
    QThread,
    Qt,
    pyqtSignal,
)
import pandas as pd
from query_revisor import Revisor, BadQueryException
from schemas import Settings
from core import Data
from PyQt5.QtWidgets import QDialog, QApplication
from PyQt5.QtCore import QEvent
from utils import get_resource_path

if TYPE_CHECKING:
    from main import ParquetSQLApp
    from PyQt5.QtGui import QShowEvent, QTextDocument


class QueryThread(QThread):
    result_ready = pyqtSignal(pd.DataFrame, str, int)
    error_occurred = pyqtSignal(str)

    def __init__(
        self,
        data: Data,
        nth_batch: int,
        app: ParquetSQLApp,
        query: str | None = None,
    ):
        super().__init__()
        self.query = query
        self.nth_batch = nth_batch
        self.data = data
        self.app = app

    def query_revisor(self, query: str) -> str | BadQueryException | None:
        """do checking and changes in query before it goes to run"""
        rev_res = Revisor(query).run()
        if rev_res is True:
            return query

        elif isinstance(rev_res, BadQueryException):
            return rev_res

    def run(self):

        try:
            if self.query and self.query.strip():
                query = self.query_revisor(self.query)
                if isinstance(query, BadQueryException):
                    raise Exception(query.name + ": " + query.message)

                if isinstance(query, str):
                    self.data.execute_query(query, as_df=False)

            df = self.data.get_nth_batch(n=self.nth_batch, as_df=True)
            self.result_ready.emit(df, self.query, self.nth_batch)

        except Exception as e:
            err_message = f"""
                            An error occurred while executing the query: '{self.query}'\n
                            Error: '{str(e)}'
                        """
            self.error_occurred.emit(err_message)


class AnimationWidget(QWidget):
    def __init__(self, parent: ParquetSQLApp | None = None):
        super(AnimationWidget, self).__init__(parent)
        self.parent_window = parent
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        animation_path = get_resource_path("static/loading-thinking.gif")
        if not animation_path.exists():
            raise FileNotFoundError(
                f"Loading animation file not found: {animation_path}"
            )

        pixmap = QPixmap(str(animation_path))
        self._img_wh = (pixmap.width(), pixmap.height())
        self.movie = QMovie(str(animation_path))
        self.label = QLabel(self)
        self.label.setScaledContents(True)
        self.label.setMovie(self.movie)

    def showEvent(self, event: QShowEvent):
        self.start()
        super().showEvent(event)

    def resizeEvent(self, event: QResizeEvent) -> None:
        new_size = event.size()
        self._apply_size(new_size.width(), new_size.height())
        super().resizeEvent(event)

    def start(self):
        self.movie.start()
        self._center_on_parent()

    def stop(self):
        self.movie.stop()
        self.close()

    def _apply_size(self, width: int, height: int) -> None:
        width = max(1, width)
        height = max(1, height)
        self.movie.setScaledSize(QSize(width, height))
        self.label.setGeometry(0, 0, width, height)

    def _center_on_parent(self):
        if self.parent_window:
            parent_geom = self.parent_window.geometry()
            if not parent_geom.isValid():
                parent_geom = self.parent_window.frameGeometry()

            parent_width = max(1, parent_geom.width())
            parent_height = max(1, parent_geom.height())

            dialog_width = max(1, int(parent_width * 0.3))
            dialog_height = max(1, int(parent_height * 0.3))

            r = min(dialog_width / self._img_wh[0], dialog_height / self._img_wh[1])
            new_width = int(self._img_wh[0] * r)
            new_height = int(self._img_wh[1] * r)
            self.setFixedSize(new_width, new_height)
            target_x = (parent_width - new_width) // 2
            target_y = (parent_height - new_height) // 2
            self.move(target_x, target_y)


class SQLHighlighter(QSyntaxHighlighter):
    def __init__(self, parent: QTextDocument, settings: Settings):
        super(SQLHighlighter, self).__init__(parent)
        self._keyword_rules: list[tuple[QRegExp, QTextCharFormat]] = []
        self._init_keyword_rules(settings)

        self._column_rules: list[tuple[QRegExp, QTextCharFormat]] = []

        self._predefined_rules: list[tuple[QRegExp, QTextCharFormat]] = []
        self._init_predefined_rules()

    def update_columns(self, columns: list[str]):
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("#00a5d6"))
        keyword_format.setFontWeight(QFont.Bold)
        self._column_rules = []
        for column in columns:
            pattern = QRegExp(f"\\b{column}\\b", Qt.CaseInsensitive)
            self._column_rules.append((pattern, keyword_format))

        self.rehighlight()

    def highlightBlock(self, text: str):
        for pattern, format in (
            self._keyword_rules + self._column_rules + self._predefined_rules
        ):
            index = pattern.indexIn(text)
            while index >= 0:
                length = pattern.matchedLength()
                self.setFormat(index, length, format)
                index = pattern.indexIn(text, index + length)
        self.setCurrentBlockState(0)

    def _init_keyword_rules(self, settings: Settings):
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("blue"))
        keyword_format.setFontWeight(QFont.Bold)
        keywords = settings.sql_keywords + [
            settings.render_vars(settings.default_data_var_name)
        ]

        for keyword in keywords:
            pattern = QRegExp(f"\\b{keyword}\\b", Qt.CaseInsensitive)
            self._keyword_rules.append((pattern, keyword_format))

    def _init_predefined_rules(self):
        string_format = QTextCharFormat()
        string_format.setForeground(QColor("#5e503f"))
        val_format = QTextCharFormat()
        val_format.setForeground(QColor("#007f5f"))
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor("#919090"))
        comment_format.setFontItalic(True)
        multiline_comment_format = QTextCharFormat()
        multiline_comment_format.setForeground(QColor("#919090"))
        multiline_comment_format.setFontItalic(True)
        self._predefined_rules += [
            (
                QRegExp("\\b\\d+\\b", Qt.CaseInsensitive),
                val_format,
            ),
            (
                QRegExp("'[^']+'", Qt.CaseInsensitive),
                string_format,
            ),
            (
                QRegExp("--.*", Qt.CaseInsensitive),
                comment_format,
            ),
            (
                QRegExp("/\\*.*\\*/", Qt.CaseInsensitive),
                multiline_comment_format,
            ),
        ]


class DataLoaderThread(QThread):
    data_ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)

    def __init__(self, file_path: str, virtual_table_name: str, batchsize: int):
        super().__init__()
        self.file_path = Path(file_path)
        self.virtual_table_name = virtual_table_name
        self.batchsize = batchsize

    def run(self):
        try:
            data = Data(
                path=self.file_path,
                virtual_table_name=self.virtual_table_name,
                batchsize=self.batchsize,
            )
            self.data_ready.emit(data)
        except Exception as exc:
            self.error_occurred.emit(str(exc))


class Popup(QDialog):
    def __init__(self, parent_window: QWidget, title: str):
        super().__init__(parent_window)
        self.setWindowTitle(title)
        self.setWindowFlags(Qt.Dialog | Qt.FramelessWindowHint)
        self.setModal(False)
        self.parent_window = parent_window

    def event(self, event: QEvent):
        if event.type() == QEvent.WindowDeactivate:
            app = QApplication.instance()
            if (
                isinstance(app, QApplication)
                and app.applicationState() == Qt.ApplicationActive
                and QApplication.activeWindow() is self.parent_window
            ):
                self.close()
                return True
        return super().event(event)


class SearchIndicator(QFrame):
    """Floating search indicator showing search term and match count."""

    def __init__(self, parent: QWidget):
        super().__init__(parent)
        self.setObjectName("SearchIndicator")
        self.setStyleSheet(
            """
            #SearchIndicator {
                background-color: #2d2d30;
                border: 1px solid #007acc;
                border-radius: 4px;
                padding: 4px 8px;
            }
        """
        )
        self._label = QLabel(self)
        self._label.setStyleSheet(
            """
            QLabel {
                color: #e0e0e0;
                font-family: Consolas, 'Courier New', monospace;
                font-size: 12px;
                background: transparent;
                border: none;
            }
        """
        )
        self.hide()

    def update_text(self, search_term: str, count: int):
        if not search_term:
            self.hide()
            return
        text = f'"{search_term}" ({count})'
        self._label.setText(text)
        self._label.adjustSize()
        # Add padding
        self.setFixedSize(self._label.width() + 16, self._label.height() + 8)
        self._label.move(8, 4)
        self.reposition()
        self.show()
        self.raise_()

    def reposition(self):
        """Position in top-right corner of parent."""
        if self.parent():
            parent = self.parent()
            x = parent.width() - self.width() - 30  # type: ignore
            y = 10
            self.move(x, y)


class SearchableTextBrowser(QTextBrowser):
    """QTextBrowser with incremental search-as-you-type highlighting."""

    HIGHLIGHT_BG = QColor("#ffea00")  # Yellow highlight
    HIGHLIGHT_FG = QColor("#000000")  # Black text

    def __init__(self, parent: QWidget):
        super().__init__(parent)
        self._search_term = ""
        self._match_positions: list[tuple[int, int]] = []  # [(start, length), ...]
        self._indicator = SearchIndicator(self)
        self.setFocusPolicy(Qt.StrongFocus)
        self.parent_window = parent

    def resizeEvent(self, event: QResizeEvent):
        super().resizeEvent(event)
        if self._indicator.isVisible():
            self._indicator.reposition()

    def keyPressEvent(self, event: QKeyEvent):
        key = event.key()
        text = event.text()

        # Handle Escape - clear search
        if key == Qt.Key_Escape:
            if self._search_term == "":
                self.parent_window.close()
            self._clear_search()
            return

        # Handle Backspace - remove last character from search
        if key == Qt.Key_Backspace:
            if self._search_term:
                self._search_term = self._search_term[:-1]
                self._update_highlights()
            return

        # Handle printable characters - add to search term
        if text and text.isprintable() and not event.modifiers() & Qt.ControlModifier:  # type: ignore
            self._search_term += text
            self._update_highlights()
            return

        # For navigation keys, pass to parent but keep search active
        if key in (
            Qt.Key_Up,
            Qt.Key_Down,
            Qt.Key_Left,
            Qt.Key_Right,
            Qt.Key_PageUp,
            Qt.Key_PageDown,
            Qt.Key_Home,
            Qt.Key_End,
        ):
            super().keyPressEvent(event)
            return

        # Let other keys pass through
        super().keyPressEvent(event)

    def setHtml(self, text: str):
        """Override to clear search on content change."""
        self._clear_search()
        super().setHtml(text)

    def setMarkdown(self, text: str):
        """Override to clear search on content change."""
        self._clear_search()
        super().setMarkdown(text)

    def _update_highlights(self):
        """Find all matches and highlight them."""
        self._match_positions.clear()
        extra_selections = []

        if not self._search_term:
            self.setExtraSelections([])
            self._indicator.update_text("", 0)
            return

        # Get plain text content for searching
        document = self.document()
        content = document.toPlainText()
        search_lower = self._search_term.lower()
        content_lower = content.lower()

        # Find all occurrences (case-insensitive)
        pos = 0
        while True:
            idx = content_lower.find(search_lower, pos)
            if idx == -1:
                break
            self._match_positions.append((idx, len(self._search_term)))
            pos = idx + 1

        # Create extra selections for highlights
        fmt = QTextCharFormat()
        fmt.setBackground(self.HIGHLIGHT_BG)
        fmt.setForeground(self.HIGHLIGHT_FG)

        for start, length in self._match_positions:
            cursor = self.textCursor()
            cursor.setPosition(start)
            cursor.movePosition(QTextCursor.Right, QTextCursor.KeepAnchor, length)
            selection = QTextBrowser.ExtraSelection()
            selection.cursor = cursor
            selection.format = fmt
            extra_selections.append(selection)

        self.setExtraSelections(extra_selections)
        self._indicator.update_text(self._search_term, len(self._match_positions))

        # Scroll to first match if any
        if self._match_positions:
            cursor = self.textCursor()
            cursor.setPosition(self._match_positions[0][0])
            self.setTextCursor(cursor)
            self.ensureCursorVisible()

    def _clear_search(self):
        """Clear search term and highlights."""
        self._search_term = ""
        self._match_positions.clear()
        self.setExtraSelections([])
        self._indicator.hide()


class AutoWrapDelegate(QStyledItemDelegate):
    """Delegate that wraps text when the row is tall enough but still elides overflow."""

    def __init__(
        self,
        parent: QWidget | None = None,
        min_wrapped_lines: int = 2,
    ):
        super().__init__(parent)
        self.min_wrapped_lines = max(1, min_wrapped_lines)

    def initStyleOption(self, option: QStyleOptionViewItem, index: QModelIndex):
        super().initStyleOption(option, index)
        line_height = option.fontMetrics.lineSpacing()
        wrap_threshold = line_height * self.min_wrapped_lines
        option.textElideMode = Qt.ElideRight
        if option.rect.height() >= wrap_threshold:
            option.features |= QStyleOptionViewItem.WrapText
            option.displayAlignment = Qt.AlignLeft | Qt.AlignVCenter
        else:
            option.features &= ~QStyleOptionViewItem.WrapText  # type: ignore
            option.displayAlignment = Qt.AlignLeft | Qt.AlignVCenter


class DataContainer:
    def __init__(
        self,
        parent: ParquetSQLApp,
        settings: Settings,
    ):
        self._settings = settings
        self._parent = parent

        def _not_bound_fn(*_: Any, **__: Any) -> None:
            raise RuntimeError("method not bound")

        self._error_fn = _not_bound_fn
        self._query_finished_fn = _not_bound_fn
        self._data_prepared_fn = _not_bound_fn
        self._data_loader = None
        self._query_thread = None
        self._file_path = None
        self.data: Data | None = None
        self._pending_query: str | None = None
        self.queried: str | None = None

    def bind_methods(
        self,
        data_prepared_fn: Callable[[pd.DataFrame, str, int], None],
        error_fn: Callable[[str], None],
        query_finished_fn: Callable[[], None],
    ):
        self._data_prepared_fn = data_prepared_fn
        self._error_fn = error_fn
        self._query_finished_fn = query_finished_fn

    def exit_query_thread(self):
        if self._query_thread:
            self._query_thread.quit()
            self._query_thread.wait()
            self._query_thread = None

    def load_page(self, page: int, query: str | None = None):
        if not self._file_path:
            self._parent.result_controller.result_label.setText("Browse file first...")
            return
        if not self._file_path.exists():
            self._parent.result_controller.result_label.setText(
                f"File not found: {self._file_path}"
            )
            return
        self._parent.start_loading_animation()
        self._pending_query = query

        if self.data is None:
            self.start_data_loader(str(self._file_path))
        else:
            self._start_query_thread(page, query)

    def reload_file(self):
        if not self._file_path:
            return
        self.open_file_path(
            self._file_path, add_to_recents=False, load_prev_history=False
        )

    def open_file_path(
        self,
        file_path: str | Path,
        add_to_recents: bool = False,
        auto_execute: bool = True,
        load_prev_history: bool = True,
    ) -> bool:
        path = Path(file_path)
        if not path.exists():
            self._error_fn(f"File not found: {path}")
            QMessageBox.warning(
                self._parent, "File Not Found", f"File not found: {path}"
            )
            return False

        self._file_path = path
        self._parent.update_window_title()
        self._parent.menu_controller.update_action_states()
        self.release_resources()
        self._parent.result_controller.release_resources()

        if add_to_recents:
            self._parent.menu_controller.add_recent(path)

        history_loaded = False
        if load_prev_history:
            history_loaded = self._parent.sql_edit_controller.load_history_query()

        if auto_execute:
            if not history_loaded:
                self._parent.sql_edit_controller.default_button.click()
            else:
                self._parent.sql_edit_controller.execute_query(add_to_history=False)

        return True

    def close_file(self):
        if not self._file_path:
            return
        self._file_path = None
        self.release_resources()

    def release_resources(self):
        if self._query_thread and self._query_thread.isRunning():
            self._query_thread.quit()
            self._query_thread.wait()
        self._query_thread = None

        if self._data_loader and self._data_loader.isRunning():
            self._data_loader.quit()
            self._data_loader.wait()
        self._data_loader = None

        data = self.data
        self.data = None
        if data is not None:
            del data

    def is_file_open(self) -> bool:
        return self._file_path is not None and Path(self._file_path).exists()

    def get_file_path(self) -> Path | None:
        return self._file_path

    def start_data_loader(self, file_path: str):
        if self._data_loader and self._data_loader.isRunning():
            return

        rows_per_page = int(
            self._settings.render_vars(self._settings.result_pagination_rows_per_page)
        )
        self._data_loader = DataLoaderThread(
            file_path=file_path,
            virtual_table_name=self._settings.render_vars(
                self._settings.default_data_var_name
            ),
            batchsize=rows_per_page,
        )
        self._data_loader.data_ready.connect(self._on_data_ready)
        self._data_loader.error_occurred.connect(self._handle_error)
        self._data_loader.start()

    def get_page_row_info(self) -> tuple[int, int, int]:
        total_pages, total_view_row_count, total_row_count = 0, 0, 0
        if self.data:
            total_pages = self.data.calc_n_batches()
            total_view_row_count = self.data.get_total_view_rows()
            total_row_count = self.data.get_total_rows()

        return total_pages, total_view_row_count, total_row_count

    def _on_data_ready(self, data: Data):
        self.data = data
        loader = self._data_loader
        if loader is not None:
            loader.wait()
            loader.deleteLater()
        self._data_loader = None
        self._start_query_thread(1, self._pending_query)

    def _start_query_thread(self, page: int, query: str | None):
        if not self.data:
            return

        if self._query_thread and self._query_thread.isRunning():
            self._query_thread.quit()
            self._query_thread.wait()

        self._query_thread = QueryThread(
            data=self.data, query=query, nth_batch=page, app=self._parent
        )
        self._query_thread.result_ready.connect(self._data_prepared_fn)
        self._query_thread.error_occurred.connect(self._handle_error)
        self._query_thread.finished.connect(self._query_finished_fn)
        self._query_thread.start()

    def _handle_error(self, error: str):
        if self._query_thread and self._query_thread.isRunning():
            self._query_thread.quit()
            self._query_thread.wait()
            self._query_thread = None

        if self._data_loader and self._data_loader.isRunning():
            self._data_loader.quit()
            self._data_loader.wait()
        self._data_loader = None

        self._error_fn(error)
