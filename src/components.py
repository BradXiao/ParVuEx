import sys
from typing import TYPE_CHECKING
from pathlib import Path
from PyQt5.QtWidgets import QLabel, QWidget, QTextBrowser, QFrame
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
from PyQt5.QtCore import QRegExp, QSize, QThread, Qt, pyqtSignal, QTimer
import pandas as pd
from query_revisor import Revisor, BadQueryException
from schemas import Settings
from core import Data
from PyQt5.QtWidgets import QDialog, QApplication
from PyQt5.QtCore import QEvent

if TYPE_CHECKING:
    from main import ParquetSQLApp
    from PyQt5.QtGui import QShowEvent, QTextDocument


class QueryThread(QThread):
    result_ready = pyqtSignal(pd.DataFrame)
    error_occurred = pyqtSignal(str)

    def __init__(
        self,
        data: Data,
        nth_batch: int,
        app: "ParquetSQLApp",
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
            self.result_ready.emit(df)

        except Exception as e:
            err_message = f"""
                            An error occurred while executing the query: '{self.query}'\n
                            Error: '{str(e)}'
                        """
            self.error_occurred.emit(err_message)


def get_resource_path(relative_path: str) -> Path:
    """
    Get absolute path to resource, works for both development and PyInstaller bundle.

    Args:
        relative_path: Path relative to the application root (e.g., "static/logo.jpg")

    Returns:
        Path object pointing to the resource
    """
    if hasattr(sys, "_MEIPASS"):
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = Path(sys._MEIPASS)  # type: ignore
    else:
        # Development mode - resources are in src/ directory
        base_path = Path(__file__).parent

    return base_path / relative_path


class AnimationWidget(QWidget):
    def __init__(self, parent: ParquetSQLApp | None = None):
        super(AnimationWidget, self).__init__(parent)
        self.win = parent
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

    def _apply_size(self, width: int, height: int) -> None:
        width = max(1, width)
        height = max(1, height)
        self.movie.setScaledSize(QSize(width, height))
        self.label.setGeometry(0, 0, width, height)

    def start(self):
        self.movie.start()
        if self.win:
            parent_geom = self.win.geometry()
            if not parent_geom.isValid():
                parent_geom = self.win.frameGeometry()

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

    def stop(self):
        self.movie.stop()
        self.close()


class SQLHighlighter(QSyntaxHighlighter):
    def __init__(self, parent: QTextDocument, settings: Settings):
        super(SQLHighlighter, self).__init__(parent)
        self._highlighting_rules: list[tuple[QRegExp, QTextCharFormat]] = []

        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("blue"))
        keyword_format.setFontWeight(QFont.Bold)
        keywords = settings.sql_keywords + [
            settings.render_vars(settings.default_data_var_name)
        ]

        for keyword in keywords:
            pattern = QRegExp(f"\\b{keyword}\\b", Qt.CaseInsensitive)
            self._highlighting_rules.append((pattern, keyword_format))

        self._column_rules: list[tuple[QRegExp, QTextCharFormat]] = []

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
        self._predefined: list[tuple[QRegExp, QTextCharFormat]] = [
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
            self._highlighting_rules + self._column_rules + self._predefined
        ):
            index = pattern.indexIn(text)
            while index >= 0:
                length = pattern.matchedLength()
                self.setFormat(index, length, format)
                index = pattern.indexIn(text, index + length)
        self.setCurrentBlockState(0)


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
                app is not None
                and app.applicationState() == Qt.ApplicationActive  # type: ignore
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
        self._clear_timer = QTimer(self)
        self._clear_timer.setSingleShot(True)
        self._clear_timer.timeout.connect(self._clear_search)
        self.setFocusPolicy(Qt.StrongFocus)
        self.parent_window = parent

    def resizeEvent(self, event: QResizeEvent):
        super().resizeEvent(event)
        if self._indicator.isVisible():
            self._indicator.reposition()

    def keyPressEvent(self, event: QKeyEvent):
        key = event.key()
        text = event.text()

        # Reset clear timer on any relevant key
        self._clear_timer.stop()

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
                # if self._search_term:
                #     self._clear_timer.start(3000)  # Auto-clear after 3s idle
            return

        # Handle printable characters - add to search term
        if text and text.isprintable() and not event.modifiers() & Qt.ControlModifier:  # type: ignore
            self._search_term += text
            self._update_highlights()
            # self._clear_timer.start(3000)  # Auto-clear after 3s idle
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
            # if self._search_term:
            #     self._clear_timer.start(3000)
            return

        # Let other keys pass through
        super().keyPressEvent(event)

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

    def setHtml(self, text: str):
        """Override to clear search on content change."""
        self._clear_search()
        super().setHtml(text)

    def setMarkdown(self, text: str):
        """Override to clear search on content change."""
        self._clear_search()
        super().setMarkdown(text)
