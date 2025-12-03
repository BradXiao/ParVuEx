import sys
from typing import Union, Optional, TYPE_CHECKING
from pathlib import Path
from PyQt5.QtWidgets import QLabel, QWidget
from PyQt5.QtGui import (
    QColor,
    QFont,
    QMovie,
    QPixmap,
    QResizeEvent,
    QSyntaxHighlighter,
    QTextCharFormat,
)
from PyQt5.QtCore import QRegExp, QSize, QThread, Qt, pyqtSignal
import pandas as pd
from query_revisor import Revisor, BadQueryException
from schemas import Settings
from core import Data
from PyQt5.QtWidgets import QDialog, QApplication
from PyQt5.QtCore import QEvent

if TYPE_CHECKING:
    from main import ParquetSQLApp


class QueryThread(QThread):
    resultReady = pyqtSignal(pd.DataFrame)
    errorOccurred = pyqtSignal(str)

    def __init__(
        self,
        DATA: Data,
        nth_batch: int,
        app: "ParquetSQLApp",
        query: Optional[str] = None,
    ):
        super().__init__()
        self.query = query
        self.nth_batch = nth_batch
        self.DATA = DATA
        self.app = app

    def queryRevisor(self, query: str) -> Union[str, BadQueryException, None]:
        """do checking and changes in query before it goes to run"""
        rev_res = Revisor(query).run()
        if rev_res is True:
            return query

        elif isinstance(rev_res, BadQueryException):
            return rev_res

    def run(self):

        try:
            if self.query and isinstance(self.query, str) and self.query.strip():
                query = self.queryRevisor(self.query)
                if isinstance(query, BadQueryException):
                    raise Exception(query.name + ": " + query.message)

                if isinstance(query, str):
                    self.DATA.execute_query(query, as_df=False)

            df = self.DATA.get_nth_batch(n=self.nth_batch, as_df=True)
            self.resultReady.emit(df)

        except Exception as e:
            err_message = f"""
                            An error occurred while executing the query: '{self.query}'\n
                            Error: '{str(e)}'
                        """
            # raise e
            self.errorOccurred.emit(err_message)


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
        base_path = Path(sys._MEIPASS)
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

    def showEvent(self, event):
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
    def __init__(self, parent, settings: Settings):
        super(SQLHighlighter, self).__init__(parent)
        self._highlighting_rules = []

        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("blue"))
        keyword_format.setFontWeight(QFont.Bold)
        keywords = settings.sql_keywords + [
            settings.render_vars(settings.default_data_var_name)
        ]

        for keyword in keywords:
            pattern = QRegExp(f"\\b{keyword}\\b", Qt.CaseInsensitive)
            self._highlighting_rules.append((pattern, keyword_format))

    def highlightBlock(self, text):
        for pattern, format in self._highlighting_rules:
            index = pattern.indexIn(text)
            while index >= 0:
                length = pattern.matchedLength()
                self.setFormat(index, length, format)
                index = pattern.indexIn(text, index + length)
        self.setCurrentBlockState(0)


class DataLoaderThread(QThread):
    dataReady = pyqtSignal(object)
    errorOccurred = pyqtSignal(str)

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
            self.dataReady.emit(data)
        except Exception as exc:
            self.errorOccurred.emit(str(exc))


class Popup(QDialog):
    def __init__(self, parent_window: QWidget, title: str):
        super().__init__(parent_window)
        self.setWindowTitle(title)
        self.setWindowFlags(Qt.Dialog | Qt.FramelessWindowHint)
        self.setModal(False)
        self.parent_window = parent_window

    def event(self, event):
        if event.type() == QEvent.WindowDeactivate:
            app = QApplication.instance()
            if (
                app is not None
                and app.applicationState() == Qt.ApplicationActive
                and QApplication.activeWindow() is self.parent_window
            ):
                self.close()
                return True
        return super().event(event)
