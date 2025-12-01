from __future__ import annotations
from typing import Union, Optional, List, ClassVar, Dict, cast
from pathlib import Path
import json
from PyQt5.QtWidgets import QMainWindow, QApplication

from PyQt5.QtWidgets import QWidget, QMainWindow, QApplication
from PyQt5.QtCore import QLockFile
from PyQt5.QtNetwork import QLocalServer, QLocalSocket
import pandas as pd

from PyQt5.QtWidgets import (
    QVBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTextEdit,
    QFileDialog,
    QTableWidget,
    QTableWidgetItem,
    QHBoxLayout,
    QMenu,
    QAction,
    QMessageBox,
    QFormLayout,
    QDialog,
    QTextBrowser,
    QSystemTrayIcon,
    QShortcut,
    QGraphicsOpacityEffect,
    QSizePolicy,
    QToolTip,
)
from PyQt5.QtGui import (
    QFont,
    QIcon,
    QKeySequence,
    QTextCursor,
    QHelpEvent,
)
from PyQt5.QtCore import (
    Qt,
    QTimer,
    QEvent,
)

from schemas import settings, Settings, recents, history
from gui_tools import render_df_info, render_column_value_counts
from core import Data
from components import (
    get_resource_path,
    QueryThread,
    DataLoaderThread,
    AnimationWidget,
    SQLHighlighter,
)

INSTANCE_MESSAGE_KEY = "file"

INSTANCE_MODE_SINGLE = "single"
INSTANCE_MODE_MULTI_WINDOW = "multi_window"
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


def normalize_instance_mode_value(value: Optional[str]) -> str:
    if value is None:
        return INSTANCE_MODE_SINGLE
    normalized = str(value).strip().lower()
    if normalized in (INSTANCE_MODE_SINGLE, INSTANCE_MODE_MULTI_WINDOW):
        return normalized
    if normalized in _MULTI_MODE_TOKENS:
        return INSTANCE_MODE_MULTI_WINDOW
    if normalized in _SINGLE_MODE_TOKENS:
        return INSTANCE_MODE_SINGLE
    raise ValueError(f"Unsupported instance mode value: {value}")


def get_instance_mode() -> str:
    raw_mode = getattr(settings, "instance_mode", INSTANCE_MODE_SINGLE)
    try:
        return normalize_instance_mode_value(raw_mode)
    except ValueError:
        return INSTANCE_MODE_SINGLE


def is_multi_window_mode() -> bool:
    return get_instance_mode() == INSTANCE_MODE_MULTI_WINDOW


class ParquetSQLApp(QMainWindow):
    open_windows: ClassVar[List["ParquetSQLApp"]] = []
    RESULT_TABLE_ROW_HEIGHT: ClassVar[int] = 25
    MAX_COLUMN_WIDTH: ClassVar[int] = 600

    @classmethod
    def _open_window_count(cls) -> int:
        return len(cls.open_windows)

    def _is_last_open_window(self) -> bool:
        return len(ParquetSQLApp.open_windows) <= 1

    @classmethod
    def find_window_by_file(cls, file_path: str) -> Optional["ParquetSQLApp"]:
        """Find an open window that has the specified file loaded."""
        target_path = Path(file_path).resolve()
        for window in cls.open_windows:
            if window.file_path and window.file_path.resolve() == target_path:
                return window
        return None

    @classmethod
    def focus_window(cls, window: "ParquetSQLApp"):
        """Bring the specified window to the front and activate it."""
        window.setWindowState(Qt.WindowNoState)
        window.show()
        window.showNormal()
        window.raise_()
        window.activateWindow()

        # Force the window to come to front on Windows
        # current_flags = window.windowFlags()
        # window.setWindowFlags(current_flags | Qt.WindowStaysOnTopHint)
        # window.show()
        # QTimer.singleShot(1000, lambda: cls._removeFocusStayOnTop(window, current_flags))

    @classmethod
    def _removeFocusStayOnTop(
        cls, window: "ParquetSQLApp", original_flags: Qt.WindowFlags
    ):
        """Helper to remove the WindowStaysOnTopHint flag from focused window"""
        window.setWindowFlags(original_flags)
        window.show()
        window.raise_()
        window.activateWindow()

    def __init__(
        self,
        file_path: str | None = None,
        enable_tray: bool = True,
        launch_minimized: bool = True,
        is_secondary: bool = False,
    ):
        super().__init__()
        self.base_title = "ParVuEx v1"
        self.setWindowTitle(self.base_title)
        logo_path = get_resource_path("static/logo.jpg")
        if not logo_path.exists():
            raise FileNotFoundError(f"Logo file not found: {logo_path}")
        self.setWindowIcon(QIcon(str(logo_path)))

        self.page = 1
        self.total_pages = None
        self.total_row_count: Optional[int] = None
        self.rows_per_page = int(
            settings.render_vars(settings.result_pagination_rows_per_page)
        )
        self.df = pd.DataFrame()
        self.DATA: Optional[Data] = None
        self.queryThread: Optional[QueryThread] = None
        self.dataLoader: Optional[DataLoaderThread] = None
        self.loading: Optional[AnimationWidget] = None
        self.pending_query: Optional[str] = None
        self._column_names: List[str] = []
        self._last_query: Optional[str] = None
        self._last_query_file: Optional[Path] = None
        self._history_index: Optional[int] = None
        self._history_snapshot: Optional[str] = None
        self.trayIcon: Optional[QSystemTrayIcon] = None
        self._hinted_tray_icon: bool = False
        self.newWindowAction: Optional[QAction] = None
        self._new_window_separator: Optional[QAction] = None
        self._recents_separator: Optional[QAction] = None
        self._recent_actions: List[QAction] = []
        self._table_effect: Optional[QGraphicsOpacityEffect] = None
        self._force_close = False
        self.single_instance_server: Optional[QLocalServer] = None
        self.instance_lock: Optional[QLockFile] = None
        self.launch_minimized = launch_minimized
        self.enable_tray = enable_tray
        self._deleyed_column_saving = QTimer()
        self._deleyed_column_saving.setSingleShot(True)
        self._deleyed_column_saving.timeout.connect(self._save_column_widths)
        self._is_applying_column_widths = False
        self.is_secondary = is_secondary
        # use this variable to store opened files path
        self.file_path: Optional[Path] = Path(file_path) if file_path else None

        self.initUI()
        self.applySettingsToUi()
        if self.enable_tray:
            self.initTrayIcon()

        self.updateWindowTitle()

        if self.launch_minimized and self.trayIcon:
            screen = QApplication.desktop().screenGeometry()
            window_width = int(screen.width() * 0.6)
            window_height = int(screen.height() * 0.6)
            x = (screen.width() - window_width) // 2
            y = (screen.height() - window_height) // 2
            self.setGeometry(x, y, window_width, window_height)
            QTimer.singleShot(0, self.minimizeOnLaunch)
        else:
            # Set window size to 60% of screen dimensions and center it
            screen = QApplication.desktop().screenGeometry()
            window_width = int(screen.width() * 0.6)
            window_height = int(screen.height() * 0.6)
            x = (screen.width() - window_width) // 2
            y = (screen.height() - window_height) // 2
            self.setGeometry(x, y, window_width, window_height)
            self.show()

        ParquetSQLApp.open_windows.append(self)
        if self.file_path:
            self.openFilePath(self.file_path, add_to_recents=True)

    def initUI(self):
        layout = QVBoxLayout()

        # SQL Edit
        # self.sqlLabel = QLabel(f'Data Query - AS {settings.render_vars(settings.default_data_var_name)}:')
        # self.sqlLabel.setFont(QFont("Courier", 8))
        # layout.addWidget(self.sqlLabel)

        self.sqlEdit = QTextEdit()
        self.sqlEdit.setPlainText(settings.render_vars(settings.default_sql_query))
        self.sqlEdit.setMaximumHeight(80)
        self.sqlEdit.setStyleSheet(f"background-color: {settings.colour_sqlEdit}")
        self.sqlEdit.installEventFilter(self)
        editorLayout = QHBoxLayout()
        editorLayout.addWidget(self.sqlEdit)

        controlLayout = QVBoxLayout()
        controlLayout.setSpacing(8)

        self.executeButton = QPushButton("Execute")
        self.executeButton.setFixedSize(110, 32)
        self.executeButton.setStyleSheet(
            f"background-color: {settings.colour_executeButton}"
        )
        self.executeButton.clicked.connect(self.executeQuery)
        controlLayout.addWidget(self.executeButton)

        # meta info
        self.tableInfoButton = QPushButton("Table Info")
        self.tableInfoButton.setFixedSize(110, 32)
        self.tableInfoButton.setStyleSheet(
            f"background-color: {settings.colour_tableInfoButton}"
        )
        self.tableInfoButton.clicked.connect(self.toggleTableInfo)
        controlLayout.addWidget(self.tableInfoButton)
        controlLayout.addStretch()

        editorLayout.addLayout(controlLayout)
        layout.addLayout(editorLayout)

        self.resultLabel = QLabel()
        self.updateResultLabel()
        layout.addWidget(self.resultLabel)

        self.resultTable = QTableWidget()
        self.resultTable.setStyleSheet(
            f"background-color: {settings.colour_resultTable}"
        )
        self.resultTable.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.resultTable.setContextMenuPolicy(Qt.CustomContextMenu)
        self.resultTable.installEventFilter(self)
        self.resultTable.viewport().installEventFilter(self)
        self.resultTable.customContextMenuRequested.connect(self.showContextMenu)
        self.resultTable.currentCellChanged.connect(self.onCurrentRowChanged)
        self._column_resize_connected = False
        self.configureResultTableFont()
        layout.addWidget(self.resultTable, stretch=1)

        # pagination
        self.paginationLayout = QHBoxLayout()
        self.paginationLayout.setSpacing(8)

        self.firstButton = QPushButton("1")
        self.firstButton.clicked.connect(self.firstPage)
        self.paginationLayout.addWidget(self.firstButton)

        self.prevButton = QPushButton("<")
        self.prevButton.clicked.connect(self.prevPage)
        self.paginationLayout.addWidget(self.prevButton)

        self.pageLabel = QLabel()
        self.pageLabel.setAlignment(Qt.AlignCenter)
        self.paginationLayout.addWidget(self.pageLabel, stretch=1)

        self.nextButton = QPushButton(">")
        self.nextButton.clicked.connect(self.nextPage)
        self.paginationLayout.addWidget(self.nextButton)

        self.lastButton = QPushButton("1")
        self.lastButton.clicked.connect(self.lastPage)
        self.paginationLayout.addWidget(self.lastButton)

        layout.addLayout(self.paginationLayout)

        self.nextPageShortcut = QShortcut(QKeySequence("Ctrl+Right"), self)
        self.nextPageShortcut.activated.connect(self.nextPage)
        self.prevPageShortcut = QShortcut(QKeySequence("Ctrl+Left"), self)
        self.prevPageShortcut.activated.connect(self.prevPage)

        layout.setStretch(0, 0)  # editor + controls
        layout.setStretch(1, 0)  # result label
        layout.setStretch(2, 1)  # result table grows with window
        layout.setStretch(3, 0)  # pagination
        layout.setStretch(4, 0)  # loading label

        central_widget = QWidget()
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

        # Create menu bar
        self.createMenuBar()
        self.update_page_text()

    def setupSqlEdit(self):
        # Set monospace font for consistency
        font = self.sqlEdit.font()
        font.setFamily(settings.default_sql_font)
        font.setPointSize(int(settings.default_sql_font_size))
        self.sqlEdit.setFont(font)

        # Apply syntax highlighting
        self.highlighter = SQLHighlighter(self.sqlEdit.document(), settings)

    def configureResultTableFont(self):
        table_font = self.resultTable.font()
        table_font.setFamily(settings.default_result_font)
        table_font.setPointSize(int(settings.default_result_font_size))
        self.resultTable.setFont(table_font)

        header = self.resultTable.horizontalHeader()
        header_font = header.font()
        header_font.setFamily(settings.default_result_font)
        header_font.setPointSize(max(1, int(settings.default_result_font_size) - 1))
        header.setFont(header_font)

        vertical_header = self.resultTable.verticalHeader()
        if vertical_header:
            vertical_font = vertical_header.font()
            vertical_font.setFamily(settings.default_result_font)
            vertical_font.setPointSize(
                max(1, int(settings.default_result_font_size) - 1)
            )
            vertical_header.setFont(vertical_font)
            self._applyRowHeight()

    def _applyRowHeight(self):
        """Ensure row height stays compact even after data refreshes."""
        vertical_header = self.resultTable.verticalHeader()
        if vertical_header:
            vertical_header.setDefaultSectionSize(self.RESULT_TABLE_ROW_HEIGHT)
            vertical_header.setMinimumSectionSize(self.RESULT_TABLE_ROW_HEIGHT)
            vertical_font = vertical_header.font()
            vertical_font.setPointSize(
                max(1, int(settings.default_result_font_size) - 2)
            )

    def _limit_max_column_widths(self):
        for idx in range(self.resultTable.columnCount()):
            width = self.resultTable.columnWidth(idx)
            if width > self.MAX_COLUMN_WIDTH:
                self.resultTable.setColumnWidth(idx, self.MAX_COLUMN_WIDTH)

    def _restore_column_widths(self) -> bool:
        """Apply persisted column widths for the current file, if any."""
        if not self.file_path or not self._column_names:
            return False
        saved_widths = history.get_col_widths(str(self.file_path))
        if not saved_widths:
            return False
        column_count = min(len(self._column_names), self.resultTable.columnCount())
        for idx in range(column_count):
            column_name = self._column_names[idx]
            width = saved_widths.get(column_name)
            if isinstance(width, int) and width > 0:
                width = min(width, self.MAX_COLUMN_WIDTH)
                self.resultTable.setColumnWidth(idx, width)

        return True

    def _collect_current_column_widths(self) -> Dict[str, int]:
        """Capture the current visible widths for all known columns."""
        widths: Dict[str, int] = {}
        if not self._column_names or not hasattr(self, "resultTable"):
            return widths
        column_count = min(len(self._column_names), self.resultTable.columnCount())
        for idx in range(column_count):
            width = self.resultTable.columnWidth(idx)
            if width > 0:
                widths[self._column_names[idx]] = width
        return widths

    def _on_column_section_resized(
        self, _logical_index: int, _old_size: int, _new_size: int
    ):
        """Defer persistence when the user adjusts a column width."""
        if not self.file_path or self._is_applying_column_widths:
            return
        if self._deleyed_column_saving.isActive() == False:
            self._deleyed_column_saving.start(1000)

    def _save_column_widths(self):

        current_widths = self._collect_current_column_widths()
        if not current_widths:
            return
        history.add_col_width(str(self.file_path), current_widths)
        self._deleyed_column_saving.stop()

    def applySettingsToUi(self):
        """Re-apply dynamic settings such as fonts, colours, and pagination."""
        self.rows_per_page = int(
            settings.render_vars(settings.result_pagination_rows_per_page)
        )
        self.sqlEdit.setStyleSheet(f"background-color: {settings.colour_sqlEdit}")
        self.executeButton.setStyleSheet(
            f"background-color: {settings.colour_executeButton}"
        )
        self.tableInfoButton.setStyleSheet(
            f"background-color: {settings.colour_tableInfoButton}"
        )
        self.resultTable.setStyleSheet(
            f"background-color: {settings.colour_resultTable}"
        )
        header = self.resultTable.horizontalHeader()
        if header:
            header.setStyleSheet("QHeaderView::section { padding: 6px 4px; }")
        self.setupSqlEdit()
        self.configureResultTableFont()
        self.updateResultLabel()
        self.update_page_text()

    def _current_page_row_offset(self) -> int:
        page_index = max(self.page - 1, 0)
        return page_index * self.rows_per_page

    def updateResultLabel(
        self, row: Optional[int] = None, column: Optional[int] = None
    ):
        page_rows = len(self.df.index) if isinstance(self.df, pd.DataFrame) else 0
        total_cols = len(self.df.columns) if isinstance(self.df, pd.DataFrame) else 0
        valid_row = row if isinstance(row, int) and row >= 0 else None
        valid_col = column if isinstance(column, int) and column >= 0 else None
        start_offset = self._current_page_row_offset()
        row_text = f"{start_offset + valid_row + 1}" if valid_row is not None else ""
        col_text = f"{valid_col + 1}" if valid_col is not None else ""
        if page_rows:
            visible_range = f"Range: {start_offset + 1}~{start_offset + page_rows}"
        else:
            visible_range = ""

        if self.total_row_count is not None:
            total_rows_text = (
                f"{self.total_row_count:,}"
                if isinstance(self.total_row_count, int)
                else "???"
            )
            select_text = (
                f"Select: {row_text}×{col_text}"
                if (row_text + col_text).strip()
                else ""
            )
            self.resultLabel.setText(
                f"Rows: {total_rows_text}   Page Rows: {str(page_rows)}    {visible_range}   Cols: {total_cols}   {select_text}"
            )
            if hasattr(self, "lastButton"):
                self.lastButton.setText(str(self.total_pages))
        else:
            self.resultLabel.setText("No data loaded")
            if hasattr(self, "lastButton"):
                self.lastButton.setText("")

    def onCurrentRowChanged(
        self, currentRow, currentColumn, previousRow, previousColumn
    ):
        if currentRow is not None and currentRow >= 0:
            self.updateResultLabel(currentRow, currentColumn)
        else:
            self.updateResultLabel()

    def updateWindowTitle(self):
        if self.file_path:
            self.setWindowTitle(
                f"{self.base_title} ({self.file_path.name} @ {self.file_path.parent})"
            )
        else:
            self.setWindowTitle(self.base_title)

    def updateActionStates(self):
        has_file = self.file_path is not None
        if hasattr(self, "viewAction"):
            self.viewAction.setEnabled(has_file)
        if hasattr(self, "closeFileAction"):
            self.closeFileAction.setEnabled(has_file)
        if hasattr(self, "exportAction"):
            self.exportAction.setEnabled(has_file)
        if hasattr(self, "resetTableSizeAction"):
            self.resetTableSizeAction.setEnabled(has_file)
        if hasattr(self, "reloadAction"):
            self.reloadAction.setEnabled(has_file)
        self.updateInstanceActions()

    def updateInstanceActions(self):
        multi_mode = is_multi_window_mode()
        if self.newWindowAction:
            self.newWindowAction.setVisible(multi_mode)
            self.newWindowAction.setEnabled(multi_mode)
        if self._new_window_separator:
            self._new_window_separator.setVisible(multi_mode)

    @classmethod
    def refresh_all_instance_actions(cls):
        for window in list(cls.open_windows):
            window.updateInstanceActions()

    @classmethod
    def refresh_all_recents_menus(cls):
        for window in list(cls.open_windows):
            if hasattr(window, "fileMenu"):
                window.updateRecentsMenu()

    def attachInstanceServer(self, server: Optional[QLocalServer]):
        """Register the local server used to communicate with secondary launches."""
        self.single_instance_server = server
        if server:
            server.setParent(self)
            server.newConnection.connect(self._handleIncomingInstanceRequest)

    def attachInstanceLock(self, lock: Optional[QLockFile]):
        """Store the lock so it remains held until the primary instance quits."""
        self.instance_lock = lock

    def _handleIncomingInstanceRequest(self):
        if not self.single_instance_server:
            return
        socket = self.single_instance_server.nextPendingConnection()
        if not socket:
            return
        socket.readyRead.connect(self._handleInstanceSocketData)

    def _handleInstanceSocketData(self):
        socket = self.sender()
        if not isinstance(socket, QLocalSocket):
            return
        data = bytes(socket.readAll()).decode("utf-8").strip()
        socket.disconnectFromServer()
        socket.deleteLater()
        self._handleInstanceMessage(data)

    def _handleInstanceMessage(self, payload: str):
        multi_mode = is_multi_window_mode()
        file_to_open: Optional[str] = None
        message = None
        if payload:
            try:
                message = json.loads(payload)
            except json.JSONDecodeError:
                message = None
        if isinstance(message, dict):
            file_candidate = message.get(INSTANCE_MESSAGE_KEY)
            if isinstance(file_candidate, str):
                file_candidate = file_candidate.strip()
            file_to_open = file_candidate or None

        if multi_mode:
            self._open_additional_window(file_to_open)
            return

        self.restoreFromTray()
        if file_to_open:
            self.openFilePath(file_to_open, add_to_recents=True)

    def _open_additional_window(self, file_to_open: Optional[str]):
        if file_to_open:
            existing_window = ParquetSQLApp.find_window_by_file(file_to_open)
            if existing_window:
                ParquetSQLApp.focus_window(existing_window)
                return
            first_window = (
                ParquetSQLApp.open_windows[0] if ParquetSQLApp.open_windows else None
            )
            if first_window and not first_window.file_path:
                first_window.openFilePath(file_to_open, add_to_recents=True)
                ParquetSQLApp.focus_window(first_window)
                return
        new_window = ParquetSQLApp.spawn_additional_window(file_to_open)
        ParquetSQLApp.focus_window(new_window)

    @classmethod
    def spawn_additional_window(cls, file_to_open: Optional[str]):
        window = cls(
            file_path=None, enable_tray=False, launch_minimized=False, is_secondary=True
        )
        if file_to_open:
            window.openFilePath(file_to_open, add_to_recents=True)
        return window

    def _closeInstanceServer(self):
        if self.single_instance_server:
            self.single_instance_server.close()
            self.single_instance_server.deleteLater()
            self.single_instance_server = None

    def _releaseInstanceLock(self):
        if self.instance_lock:
            self.instance_lock.unlock()
            self.instance_lock = None

    def closeFile(self):
        if not self.file_path:
            return
        self.releaseResources()
        self.file_path = None
        self._last_query = None
        self._last_query_file = None
        self.updateWindowTitle()
        self.updateActionStates()

    def resetTableSize(self):
        if not self.file_path:
            return
        if not hasattr(self, "resultTable") or self.resultTable.columnCount() == 0:
            return

        self.resultTable.resizeColumnsToContents()
        self._limit_max_column_widths()
        self._applyRowHeight()
        history.add_col_width(str(self.file_path), None)

    def exitApplication(self):
        confirm = QMessageBox.question(
            self,
            "Exit ParVuEx",
            "Are you sure you want to exit ParVuEx?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return

        self._force_close = True
        self.releaseResources()
        self._closeInstanceServer()
        self._releaseInstanceLock()
        QApplication.instance().quit()

    def createMenuBar(self):
        menubar = self.menuBar()
        # app menu
        sysMenu = menubar.addMenu("App")
        self.newWindowAction = QAction("New Window", self)
        self.newWindowAction.triggered.connect(self.openNewWindowInstance)
        sysMenu.addAction(self.newWindowAction)
        self._new_window_separator = sysMenu.addSeparator()
        settingsAction = QAction("Settings", self)
        settingsAction.triggered.connect(self.editSettings)
        sysMenu.addAction(settingsAction)
        # quit app
        sysMenu.addSeparator()
        exitMenuAction = QAction("Quit App", self)
        exitMenuAction.triggered.connect(self.exitApplication)
        sysMenu.addAction(exitMenuAction)

        # file menu
        self.fileMenu = menubar.addMenu("File")
        browseAction = QAction("Open...", self)
        browseAction.triggered.connect(self.browseFile)
        self.fileMenu.addAction(browseAction)
        self.reloadAction = QAction("Reload", self)
        self.reloadAction.triggered.connect(self.reloadFile)
        self.fileMenu.addAction(self.reloadAction)
        self.closeFileAction = QAction("Close", self)
        self.closeFileAction.triggered.connect(self.closeFile)
        self.fileMenu.addAction(self.closeFileAction)
        self.fileMenu.addSeparator()
        self.exportAction = QAction("Export...", self)
        self.exportAction.triggered.connect(self.exportResults)
        self.fileMenu.addAction(self.exportAction)
        self.updateRecentsMenu()

        # view file
        actionMenu = menubar.addMenu("Actions")
        self.viewAction = QAction("Show all data", self)
        self.viewAction.triggered.connect(self.ViewFile)
        actionMenu.addAction(self.viewAction)
        actionMenu.addSeparator()
        self.resetTableSizeAction = QAction("Reset row/col height/width", self)
        self.resetTableSizeAction.triggered.connect(self.resetTableSize)
        actionMenu.addAction(self.resetTableSizeAction)

        # help
        helpMenu = menubar.addMenu("Help")
        helpAction = QAction("Help/Info", self)
        helpAction.triggered.connect(self.showHelpDialog)
        helpMenu.addAction(helpAction)
        self.updateActionStates()

    def initTrayIcon(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return

        logo_path = get_resource_path("static/logo.jpg")
        if not logo_path.exists():
            raise FileNotFoundError(f"Logo file not found: {logo_path}")
        tray_icon = QSystemTrayIcon(QIcon(str(logo_path)), self)
        tray_menu = QMenu(self)

        restore_action = QAction("Restore", self)
        restore_action.triggered.connect(self.restoreFromTray)
        newWindowAction = QAction("New Window", self)
        newWindowAction.triggered.connect(self.openNewWindowInstance)
        exit_action = QAction("Exit App", self)
        exit_action.triggered.connect(self.exitFromTray)

        tray_menu.addAction(restore_action)
        tray_menu.addAction(newWindowAction)
        tray_menu.addSeparator()
        tray_menu.addAction(exit_action)

        tray_icon.setContextMenu(tray_menu)
        tray_icon.activated.connect(self.handleTrayActivation)
        tray_icon.show()

        self.trayIcon = tray_icon
        self.trayMenu = tray_menu
        self.restoreAction = restore_action
        self.exitAction = exit_action

    def ensureTrayIcon(self) -> bool:
        if self.trayIcon:
            return True
        self.initTrayIcon()
        return self.trayIcon is not None

    def handleTrayActivation(self, reason):
        activation_reason = getattr(QSystemTrayIcon, "ActivationReason", None)
        valid_reasons = []
        if activation_reason:
            trigger = getattr(activation_reason, "Trigger", None)
            double_click = getattr(activation_reason, "DoubleClick", None)
            valid_reasons = [val for val in (trigger, double_click) if val is not None]
        else:
            # fallback for PyQt versions without ActivationReason helper
            trigger = getattr(QSystemTrayIcon, "Trigger", None)
            double_click = getattr(QSystemTrayIcon, "DoubleClick", None)
            valid_reasons = [val for val in (trigger, double_click) if val is not None]

        if not valid_reasons or reason in valid_reasons:
            self.restoreFromTray()

    def restoreFromTray(self):
        # Ensure proper window state
        self.setWindowState(Qt.WindowNoState)

        # Show and raise the window
        self.show()
        self.showNormal()
        self.raise_()  # Bring to top of window stack
        self.activateWindow()  # Request focus

        # # Force the window to come to front on Windows
        # # This sets the window to stay on top temporarily
        # current_flags = self.windowFlags()
        # self.setWindowFlags(current_flags | Qt.WindowStaysOnTopHint)
        # self.show()
        # QTimer.singleShot(100, lambda: self._removeStayOnTop(current_flags))

        if self.DATA is None and self.file_path:
            self.execute()

    def _removeStayOnTop(self, original_flags: Qt.WindowFlags):
        """Helper to remove the WindowStaysOnTopHint flag"""
        self.setWindowFlags(original_flags)
        self.show()
        self.raise_()
        self.activateWindow()

    def hintTrayIcon(self):
        if self._hinted_tray_icon or not self.trayIcon:
            return
        self.trayIcon.showMessage(
            "ParVuEx", "ParVuEx is running in the tray.", msecs=3000
        )
        self._hinted_tray_icon = True

    def minimizeOnLaunch(self):
        if self.trayIcon:
            self.hide()
            self.hintTrayIcon()
        else:
            self.showMinimized()

    def minimizeToTray(self):
        if self.trayIcon:
            self.hide()
            self.hintTrayIcon()
        else:
            self.showMinimized()

    def exitFromTray(self):
        self._force_close = True
        self.releaseResources()
        self._closeInstanceServer()
        self._releaseInstanceLock()
        QApplication.instance().quit()

    def releaseResources(self):
        if self.queryThread and self.queryThread.isRunning():
            self.queryThread.quit()
            self.queryThread.wait()
        self.queryThread = None

        if self.dataLoader and self.dataLoader.isRunning():
            self.dataLoader.quit()
            self.dataLoader.wait()
        self.dataLoader = None

        self.stopLoadingAnimation()
        data = self.DATA
        self.DATA = None
        if data is not None:
            del data
        self.df = pd.DataFrame()
        self._column_names = []
        self.total_pages = None
        self.total_row_count = None
        self.resultTable.clear()
        self.resultTable.setRowCount(0)
        self.resultTable.setColumnCount(0)
        self.updateResultLabel()
        self.update_page_text()

    def closeEvent(self, event):
        if not self._force_close and not self.trayIcon and self._is_last_open_window():
            self.ensureTrayIcon()

        if not self._force_close and self.trayIcon:
            event.ignore()
            self.releaseResources()
            self.closeFileAction.triggered.emit()
            self.minimizeToTray()
            return

        self.releaseResources()
        self._closeInstanceServer()
        self._releaseInstanceLock()
        super().closeEvent(event)
        if event.isAccepted() and self in ParquetSQLApp.open_windows:
            ParquetSQLApp.open_windows.remove(self)

    def _center_dialog_relative_to_window(
        self, dialog: QDialog, width_ratio: float = 0.8, height_ratio: float = 0.8
    ):
        """Resize dialog relative to the main window and center it."""
        parent_geom = self.geometry()
        if not parent_geom.isValid():
            parent_geom = self.frameGeometry()

        parent_width = max(1, parent_geom.width())
        parent_height = max(1, parent_geom.height())

        dialog_width = max(1, int(parent_width * width_ratio))
        dialog_height = max(1, int(parent_height * height_ratio))
        dialog.resize(dialog_width, dialog_height)

        target_x = parent_geom.x() + (parent_width - dialog_width) // 2
        target_y = parent_geom.y() + (parent_height - dialog_height) // 2
        dialog.move(target_x, target_y)

    def showHelpDialog(self):
        with open(settings.static_dir / "help.md", "r", encoding="utf-8") as f:
            help_text = f.read()

        dialog = QDialog(self, Qt.WindowTitleHint | Qt.WindowCloseButtonHint)
        dialog.setWindowTitle("Help/Info")

        text_browser = QTextBrowser(dialog)
        text_browser.setMarkdown(help_text)
        text_browser.setReadOnly(True)

        layout = QVBoxLayout()
        layout.addWidget(text_browser)
        dialog.setLayout(layout)

        # dialog.setWindowFlags(Qt.Dialog | Qt.FramelessWindowHint)
        dialog.setWindowFlags(Qt.Popup | Qt.FramelessWindowHint)
        self._center_dialog_relative_to_window(dialog)
        dialog.exec_()

    def openFilePath(
        self,
        file_path: Union[str, Path],
        add_to_recents: bool = False,
        auto_execute: bool = True,
    ) -> bool:
        path = Path(file_path)
        if not path.exists():
            self.resultLabel.setText(f"File not found: {path}")
            QMessageBox.warning(self, "File Not Found", f"File not found: {path}")
            return False

        self.file_path = path
        self.updateWindowTitle()
        self.updateActionStates()
        self.releaseResources()
        self._last_query = None
        self._last_query_file = None

        if add_to_recents and settings.save_file_history in (
            "True",
            "true",
            "1",
            True,
            1,
        ):
            recents.add_recent(str(path))
            ParquetSQLApp.refresh_all_recents_menus()
        if auto_execute:
            self.execute()

        return True

    def reloadFile(self):
        if not self.file_path:
            return
        self.openFilePath(self.file_path, add_to_recents=False)

    def browseFile(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getOpenFileName(
            self,
            "Open Parquet File",
            "",
            "Parquet Files (*.parquet);;All Files (*)",
            options=options,
        )
        if fileName:
            existing_window = ParquetSQLApp.find_window_by_file(fileName)
            if existing_window:
                ParquetSQLApp.focus_window(existing_window)
                return False
            self.openFilePath(fileName, add_to_recents=True, auto_execute=False)
            self.viewAction.triggered.emit()

    def openNewWindowInstance(self):
        if not is_multi_window_mode():
            QMessageBox.information(
                self,
                "Multi-Window Disabled",
                "Enable multi-window mode in Settings to open additional windows.",
            )
            return
        self._open_additional_window(None)

    def ViewFile(self):
        if not self.file_path:
            self.resultLabel.setText("Browse file first...")
            return

        if not self.file_path.exists():
            self.resultLabel.setText(f"File not found: {self.file_path}")
            return

        self.releaseResources()
        self.execute()

    def execute(self):
        self._last_query = None
        self._last_query_file = self.file_path
        self.page = 1
        self.loadPage()
        self.update_page_text()

    def executeQuery(self):
        query_text = self.sqlEdit.toPlainText()
        self._add_query_to_history(query_text)
        self.page = 1
        self.loadPage(query=query_text)
        self.update_page_text()

    def startLoadingAnimation(self):
        if self.loading:
            self.loading.stop()
        if self._table_effect is None:
            self._table_effect = QGraphicsOpacityEffect(self.resultTable)
        self._table_effect.setOpacity(0.35)
        self.resultTable.setGraphicsEffect(self._table_effect)
        self.resultTable.setDisabled(True)
        if not self.isHidden():
            self.loading = AnimationWidget(self)
            self.loading.show()

    def stopLoadingAnimation(self):
        self.resultTable.setEnabled(True)
        self.resultTable.setGraphicsEffect(None)
        self._table_effect = None
        if self.loading:
            self.loading.stop()
            self.loading = None

    def startDataLoader(self, file_path: str):
        if self.dataLoader and self.dataLoader.isRunning():
            return

        self.dataLoader = DataLoaderThread(
            file_path=file_path,
            virtual_table_name=settings.render_vars(settings.default_data_var_name),
            batchsize=self.rows_per_page,
        )
        self.dataLoader.dataReady.connect(self.onDataReady)
        self.dataLoader.errorOccurred.connect(self.handleError)
        self.dataLoader.start()

    def onDataReady(self, data):
        self.DATA = data
        loader = self.dataLoader
        if loader is not None:
            loader.wait()
            loader.deleteLater()
        self.dataLoader = None
        self.calcTotalPages(force=True)
        self._startQueryThread(self.pending_query)

    def _startQueryThread(self, query: Optional[str]):
        if not self.DATA:
            return

        if self.queryThread and self.queryThread.isRunning():
            self.queryThread.quit()
            self.queryThread.wait()

        self.queryThread = QueryThread(
            DATA=self.DATA, query=query, nth_batch=self.page, app=self
        )
        self.queryThread.resultReady.connect(self.handleResults)
        self.queryThread.errorOccurred.connect(self.handleError)
        self.queryThread.finished.connect(self.update_page_text)
        self.queryThread.start()

    def loadPage(self, query: Optional[str] = None):
        if not self.file_path:
            self.resultLabel.setText("Browse file first...")
            return
        if not self.file_path.exists():
            self.resultLabel.setText(f"File not found: {self.file_path}")
            return
        if isinstance(query, str) and query.strip():
            self._last_query = query
            self._last_query_file = self.file_path
        self.pending_query = query
        self.startLoadingAnimation()

        if self.DATA is None:
            self.startDataLoader(str(self.file_path))
        else:
            self._startQueryThread(query)

    def handleResults(self, df):
        if self.queryThread:
            self.queryThread.quit()
            self.queryThread.wait()
            self.queryThread = None
        self.df = df
        self.calcTotalPages(force=True)
        self.displayResults(df)
        self.stopLoadingAnimation()

    def handleError(self, error):
        if self.queryThread and self.queryThread.isRunning():
            self.queryThread.quit()
            self.queryThread.wait()
            self.queryThread = None

        if self.dataLoader and self.dataLoader.isRunning():
            self.dataLoader.quit()
            self.dataLoader.wait()
        self.dataLoader = None

        self.resultLabel.setText(f"Error: {error}")
        self.stopLoadingAnimation()

    def displayResults(self, df):
        self.updateResultLabel()
        # Set the table dimensions
        self.resultTable.setColumnCount(len(df.columns))
        self.resultTable.setRowCount(len(df.index))

        # Set the column headers
        self._column_names = [str(col) for col in df.columns]
        header_labels = [
            f"{idx + 1}\n{name}" for idx, name in enumerate(self._column_names)
        ]
        if header_labels:
            self.resultTable.setHorizontalHeaderLabels(header_labels)
        else:
            self.resultTable.setHorizontalHeaderLabels([])
        header = self.resultTable.horizontalHeader()
        if header:
            header.setDefaultAlignment(Qt.AlignCenter)
            # Connect column resize signal once the table has columns
            if not self._column_resize_connected:
                header.sectionResized.connect(self._on_column_section_resized)
                self._column_resize_connected = True

        # Fill the table with the DataFrame data
        for i in range(len(df.index)):
            for j in range(len(df.columns)):
                self.resultTable.setItem(i, j, QTableWidgetItem(str(df.iat[i, j])))

        page_rows = len(df.index)
        start_index = self._current_page_row_offset()
        if page_rows:
            row_labels = [str(start_index + idx + 1) for idx in range(page_rows)]
            self.resultTable.setVerticalHeaderLabels(row_labels)
        else:
            self.resultTable.setVerticalHeaderLabels([])

        self._applyRowHeight()

        if len(df.index) and len(df.columns):
            self.resultTable.setCurrentCell(0, 0)

        self._is_applying_column_widths = True
        if not self._restore_column_widths():
            self.resultTable.resizeColumnsToContents()
            self._limit_max_column_widths()
        self._is_applying_column_widths = False

    def update_page_text(self):
        """set next / prev button text"""
        total_pages_value = (
            self.total_pages if isinstance(self.total_pages, int) else None
        )
        total_display = (
            total_pages_value if isinstance(total_pages_value, int) else "???"
        )
        has_data = self.DATA is not None
        if hasattr(self, "pageLabel"):
            self.pageLabel.setText(f"Page {self.page}")
        can_go_prev = has_data and self.page > 1
        at_last_page = (
            isinstance(total_pages_value, int) and self.page >= total_pages_value
        )
        can_go_next = has_data and (
            not isinstance(total_pages_value, int) or not at_last_page
        )

        if hasattr(self, "prevButton"):
            self.prevButton.setEnabled(can_go_prev)
        if hasattr(self, "firstButton"):
            self.firstButton.setEnabled(can_go_prev)
        if hasattr(self, "nextButton"):
            self.nextButton.setEnabled(can_go_next)
        if hasattr(self, "lastButton"):
            self.lastButton.setEnabled(
                has_data and isinstance(total_pages_value, int) and not at_last_page
            )

    def firstPage(self):
        if not self.DATA or self.page == 1:
            return
        self.page = 1
        self.loadPage()
        self.update_page_text()

    def prevPage(self):
        if not self.DATA:
            return
        if self.page > 1:
            self.page -= 1
            self.loadPage()
            self.update_page_text()

    def nextPage(self):
        # if is the last page, do nothing
        if not self.DATA:
            return
        total_batches = (
            self.DATA.total_batches
            if isinstance(self.DATA.total_batches, int)
            else self.total_pages
        )
        if isinstance(total_batches, int) and self.page >= total_batches:
            return
        self.page += 1
        self.loadPage()
        self.update_page_text()

    def lastPage(self):
        if not self.DATA:
            return
        self.calcTotalPages(force=True)
        if not isinstance(self.total_pages, int) or self.page >= self.total_pages:
            return
        self.page = self.total_pages
        self.loadPage()
        self.update_page_text()

    def calcTotalPages(self, force: bool = False):
        """calculate how many pages data will have, if `force` is False then won't recalculate it"""
        if self.DATA:
            if force or self.total_pages is None:
                self.total_pages = self.DATA.calc_n_batches()
                self.total_row_count = self.DATA.calc_total_rows()
                self.update_page_text()
                self.updateResultLabel()

    def showContextMenu(self, pos):
        contextMenu = QMenu(self)

        header = self.resultTable.horizontalHeader()
        column = header.logicalIndexAt(pos.x())
        row = self.resultTable.indexAt(pos).row()

        if column >= 0:
            column_name = self._get_column_name(column)
            value_counts = QAction("Value Counts", self)
            value_counts.triggered.connect(
                lambda: self.showColumnValueCounts(column_name)
            )
            contextMenu.addAction(value_counts)
            contextMenu.addSeparator()

            # Create Copy Submenu
            copy_column_action = QAction("Copy Column Name", self)
            copy_column_action.triggered.connect(lambda: self.copyColumnName(column))
            contextMenu.addAction(copy_column_action)

            copy_column_values_action = QAction("Copy Whole Column", self)
            copy_column_values_action.triggered.connect(
                lambda: self.copyColumnValues(column)
            )
            contextMenu.addAction(copy_column_values_action)

            if row >= 0:
                copy_row_values_action = QAction("Copy Whole Row", self)
                copy_row_values_action.triggered.connect(
                    lambda: self.copyRowValues(row)
                )
                contextMenu.addAction(copy_row_values_action)
                copy_row_values_action = QAction("Copy Whole Row as Dict", self)
                copy_row_values_action.triggered.connect(
                    lambda: self.copyRowValues(row, as_dict=True)
                )
                contextMenu.addAction(copy_row_values_action)

        contextMenu.exec_(self.resultTable.mapToGlobal(pos))

    def showColumnValueCounts(self, column: str):
        table_info = render_column_value_counts(
            self.DATA.reader.duckdf_query,
            column,
            self.DATA.reader.duckdf,
            max_rows=50,
        )

        dialog = QDialog(self)
        dialog.setWindowTitle(f"Value Counts for {column}")
        dialog.setWindowFlags(Qt.Popup | Qt.FramelessWindowHint)

        text_browser = QTextBrowser(dialog)
        table_font = QFont(
            settings.default_result_font, int(settings.default_result_font_size)
        )
        text_browser.setFont(table_font)
        text_browser.setMarkdown(table_info)
        text_browser.setReadOnly(True)
        text_browser.setOpenExternalLinks(True)

        layout = QVBoxLayout()
        layout.addWidget(text_browser)
        dialog.setLayout(layout)
        self._center_dialog_relative_to_window(dialog)
        dialog.exec_()

    def copyColumnName(self, column):
        column_name = self._get_column_name(column)
        if not column_name:
            return
        clipboard = QApplication.clipboard()
        clipboard.setText(column_name)

    def copyColumnValues(self, column):
        values = self.df.iloc[:, column].tolist()
        clipboard = QApplication.clipboard()
        clipboard.setText(str(values))

    def copyRowValues(self, row: int, as_dict: bool = False):
        if not as_dict:
            values = self.df.iloc[row, :].tolist()
        else:
            values = json.dumps(
                self.df.iloc[row, :].to_dict(), indent=4, ensure_ascii=False
            )
        clipboard = QApplication.clipboard()
        clipboard.setText(str(values))

    def _get_column_name(self, column: int) -> str:
        if 0 <= column < len(self._column_names):
            return self._column_names[column]
        if isinstance(self.df, pd.DataFrame) and 0 <= column < len(self.df.columns):
            return str(self.df.columns[column])
        header_item = self.resultTable.horizontalHeaderItem(column)
        if header_item:
            parts = header_item.text().splitlines()
            return parts[-1].strip()
        return ""

    def eventFilter(self, obj, event):
        table_viewport = (
            self.resultTable.viewport() if hasattr(self, "resultTable") else None
        )
        if obj in (self.resultTable, table_viewport):
            if event.type() == QEvent.Wheel and event.modifiers() & Qt.ShiftModifier:
                scrollbar = self.resultTable.horizontalScrollBar()
                delta_point = event.pixelDelta()
                if not delta_point.isNull():
                    scroll_delta = delta_point.x() or delta_point.y()
                    if scroll_delta:
                        scrollbar.setValue(scrollbar.value() - scroll_delta)
                        return True

                angle_delta = event.angleDelta()
                scroll_delta = angle_delta.x() or angle_delta.y()
                if scroll_delta:
                    single_step = max(1, scrollbar.singleStep())
                    steps = scroll_delta / 120
                    scrollbar.setValue(int(scrollbar.value() - steps * single_step))
                    return True

            if event.type() == QEvent.ToolTip:
                help_event = cast(QHelpEvent, event)
                viewport = self.resultTable.viewport()
                viewport_pos = viewport.mapFromGlobal(help_event.globalPos())
                index = self.resultTable.indexAt(viewport_pos)
                if index.isValid():
                    item = self.resultTable.item(index.row(), index.column())
                    if item is not None:
                        QToolTip.showText(
                            help_event.globalPos(),
                            item.text(),
                            self.resultTable,
                        )
                        return True
                QToolTip.hideText()
                event.ignore()
                return True

        if obj is self.sqlEdit and event.type() == QEvent.KeyPress:
            key = event.key()
            modifiers = event.modifiers()
            ctrl_only = bool(modifiers & Qt.ControlModifier) and not (
                modifiers & (Qt.ShiftModifier | Qt.AltModifier)
            )
            if ctrl_only:
                if key == Qt.Key_Up:
                    if self.showPreviousHistoryEntry():
                        return True
                    return super().eventFilter(obj, event)
                if key == Qt.Key_Down:
                    if self.showNextHistoryEntry():
                        return True
                    return super().eventFilter(obj, event)
                if key == Qt.Key_Left:
                    self.prevPage()
                    return True
                if key == Qt.Key_Right:
                    self.nextPage()
                    return True

            if key in (Qt.Key_Return, Qt.Key_Enter):
                if modifiers & Qt.ShiftModifier:
                    if self._history_index is not None:
                        self._reset_history_navigation()
                    return False
                self.executeQuery()
                return True

            if self._history_index is not None and self._key_changes_text(event):
                self._reset_history_navigation()

        return super().eventFilter(obj, event)

    def _add_query_to_history(self, query_text: str):
        q = query_text.strip()
        if (
            self._history_index is not None
            and str(self.file_path) in history.queries
            and history.queries[str(self.file_path)][self._history_index] == q
        ):
            return
        if q:
            history.add_query(str(self.file_path), query_text)
        self._reset_history_navigation()

    def _reset_history_navigation(self):
        self._history_index = None
        self._history_snapshot = None
        self.executeButton.setText("Execute")

    def _begin_history_navigation(self) -> bool:
        if (
            str(self.file_path) not in history.queries
            or not history.queries[str(self.file_path)]
        ):
            return False
        return True

    def _apply_history_entry(self, text: str):
        previous_state = self.sqlEdit.blockSignals(True)
        try:
            self.sqlEdit.setPlainText(text)
            cursor = self.sqlEdit.textCursor()
            cursor.movePosition(QTextCursor.End)
            self.sqlEdit.setTextCursor(cursor)
        finally:
            self.sqlEdit.blockSignals(previous_state)

    def _key_changes_text(self, event) -> bool:
        key = event.key()
        modifiers = event.modifiers()
        text = event.text()
        if key in (Qt.Key_Backspace, Qt.Key_Delete):
            return True
        if key in (Qt.Key_Return, Qt.Key_Enter, Qt.Key_Tab) and not (
            modifiers & Qt.ControlModifier
        ):
            return True
        if text and not (modifiers & Qt.ControlModifier):
            return True
        if modifiers & Qt.ControlModifier and key in (
            Qt.Key_V,
            Qt.Key_X,
            Qt.Key_Z,
            Qt.Key_Y,
        ):
            return True
        if modifiers & Qt.ShiftModifier and key == Qt.Key_Insert:
            return True
        return False

    def showPreviousHistoryEntry(self) -> bool:
        if not self._begin_history_navigation():
            return False
        if self._history_index is None:
            self._history_snapshot = self.sqlEdit.toPlainText()
            self._history_index = 0
        elif self._history_index + 1 < len(history.queries[str(self.file_path)]):
            self._history_index += 1
        entry = history.queries[str(self.file_path)][self._history_index]
        self._apply_history_entry(entry)
        self.executeButton.setText(f"Execute (-{self._history_index + 1})")
        return True

    def showNextHistoryEntry(self) -> bool:
        if (
            str(self.file_path) not in history.queries
            or not history.queries[str(self.file_path)]
        ):
            return False
        if self._history_index is None:
            return False
        assert self._history_index is not None
        if self._history_index > 0:
            self._history_index -= 1
            entry = history.queries[str(self.file_path)][self._history_index]
            self._apply_history_entry(entry)
            self.executeButton.setText(f"Execute (-{self._history_index + 1})")
            return True

        if self._history_snapshot is not None:
            snapshot = self._history_snapshot
            self._reset_history_navigation()
            self._apply_history_entry(snapshot)
            return True

        return False

    def exportResults(self):
        if not self.DATA:
            self.resultLabel.setText("No data to export")
            return
        options = QFileDialog.Options()
        filePath, _ = QFileDialog.getSaveFileName(
            self,
            "Export Results",
            "",
            "CSV Files (*.csv);;Parquet Files (*.parquet);;All Files (*)",
            options=options,
        )
        if filePath:
            if filePath.endswith(".csv"):
                self.DATA.reader.duckdf_query.to_csv(filePath)
            # elif filePath.endswith('.xlsx'):
            # # todo: add support for xlsx(https://duckdb.org/docs/guides/file_formats/excel_export.html)
            #     self.DATA.reader.duckdf_query.to(filePath, index=False)

            elif filePath.endswith(".parquet"):
                self.DATA.reader.duckdf_query.to_parquet(filePath)
            else:
                QMessageBox.warning(
                    self,
                    "Invalid File Type",
                    "Please select a valid file type (CSV or XLSX).",
                )

    def toggleTableInfo(self):
        if self.file_path and self.file_path.exists():
            if not self.DATA:
                return

            table_info = render_df_info(self.DATA.reader.duckdf_query)

            dialog = QDialog(self)
            dialog.setWindowTitle("Table Info")
            dialog.setWindowFlags(Qt.Popup | Qt.FramelessWindowHint)

            text_browser = QTextBrowser(dialog)
            table_font = QFont(
                settings.default_result_font, int(settings.default_result_font_size)
            )
            text_browser.setFont(table_font)
            text_browser.setMarkdown(table_info)
            text_browser.setReadOnly(True)
            text_browser.setOpenExternalLinks(True)

            layout = QVBoxLayout()
            layout.addWidget(text_browser)
            dialog.setLayout(layout)

            self._center_dialog_relative_to_window(dialog)
            dialog.exec_()

    def editSettings(self):
        settings_file = settings.usr_settings_file
        default_settings_file = settings.default_settings_file
        if not Path(settings_file).exists():
            QMessageBox.critical(
                self, "Error", f"Settings file '{settings_file}' does not exist."
            )
            return

        class SettingsDialog(QDialog):
            # these settings won't be editable
            read_only_fields = [
                "recents_file",
                "settings_file",
                "default_settings_file",
                "static_dir",
                "usr_recents_file",
                "usr_settings_file",
                "user_app_settings_dir",
            ]

            help_text = (
                "Did you know:\nYou can use field names inside string as `$(field_name)` for render it."
                "\nSet 'instance_mode' to 'single' or 'multi_window' to control multi-window mode."
            )

            def __init__(self, settings: Settings, default_settings_file: Path):
                super().__init__()
                self.settings = settings
                self.default_settings_file = default_settings_file
                self.initUI()

            def validateSettings(self):
                for field, line_edit in self.fields.items():
                    if field in self.read_only_fields:
                        continue

                    if field == "default_data_var_name":
                        if line_edit.text().upper() in settings.sql_keywords:
                            QMessageBox.critical(
                                self,
                                "Error",
                                "The data variable name cannot be a SQL keyword.",
                            )
                            return False
                    if field == "result_pagination_rows_per_page":
                        if not line_edit.text().isdigit() or int(line_edit.text()) < 1:
                            QMessageBox.critical(
                                self,
                                "Error",
                                "The result pagination rows per page must be a positive integer.",
                            )
                            return False
                        if not (10 <= int(line_edit.text()) <= 1000):
                            QMessageBox.critical(
                                self,
                                "Error",
                                "The result pagination rows per page must be between 10 and 1000.",
                            )
                            return False
                    if field == "instance_mode":
                        try:
                            normalize_instance_mode_value(line_edit.text())
                        except ValueError:
                            QMessageBox.critical(
                                self,
                                "Error",
                                "Invalid instance_mode. Use 'single' or 'multi_window'.",
                            )
                            return False

                return True

            def initUI(self):
                layout = QFormLayout()

                self.fields = {}
                for field, value in self.settings.model_dump().items():
                    if field in self.read_only_fields:
                        continue

                    line_edit = QLineEdit()
                    # line_edit.setPlaceholderText(str(value))
                    line_edit.setText(str(value))
                    self.fields[field] = line_edit
                    layout.addRow(QLabel(field), line_edit)

                help_text = QLabel(self.help_text)
                help_text.setFont(QFont("Courier", 9, weight=QFont.Bold))
                layout.addRow(help_text)

                button_layout = QHBoxLayout()

                save_button = QPushButton("Save")
                save_button.clicked.connect(self.saveSettings)
                button_layout.addWidget(save_button)

                reset_button = QPushButton("Reset to Default")
                reset_button.clicked.connect(self.resetSettings)
                button_layout.addWidget(reset_button)

                layout.addRow(button_layout)

                self.setLayout(layout)
                self.setWindowTitle("Edit Settings")
                self.resize(400, 300)

            def saveSettings(self):
                if not self.validateSettings():
                    QMessageBox.critical(
                        self, "Error", "Please fix the errors before saving."
                    )
                    return
                for field, line_edit in self.fields.items():
                    if line_edit.text():
                        if field == "sql_keywords":
                            # replace stringed list into list[str]
                            kws = line_edit.text()
                            kws = [
                                i.strip().replace("'", "") for i in kws[1:-1].split(",")
                            ]
                            setattr(self.settings, field, kws)
                        elif field == "instance_mode":
                            try:
                                normalized_mode = normalize_instance_mode_value(
                                    line_edit.text()
                                )
                            except ValueError:
                                QMessageBox.critical(
                                    self, "Error", "Invalid instance_mode value."
                                )
                                return
                            setattr(self.settings, field, normalized_mode)
                        else:
                            setattr(self.settings, field, line_edit.text())

                self.settings.save_settings()
                self.accept()

            def resetSettings(self):
                default_settings_file = (
                    Path(__file__).parent / "settings" / "default_settings.json"
                )
                with open(default_settings_file.as_posix(), "r") as f:
                    default_settings_data = f.read()
                with settings_file.open("w") as f:
                    f.write(default_settings_data)
                self.settings = Settings.load_settings()
                QMessageBox.information(
                    self,
                    "Settings Reset",
                    "Settings have been reset to default values. Please restart the application for changes to take effect.",
                )
                self.accept()

        dialog = SettingsDialog(settings, default_settings_file)
        if dialog.exec_() == QDialog.Accepted:
            self.handleSettingsChanged()

    def handleSettingsChanged(self):
        self._reloadSettingsModel()
        self.applySettingsToUi()
        ParquetSQLApp.refresh_all_instance_actions()
        self._refreshDataAfterSettingsChange()

    def _reloadSettingsModel(self):
        refreshed_settings = Settings.load_settings()
        for field_name in Settings.model_fields:
            setattr(settings, field_name, getattr(refreshed_settings, field_name))

    def _refreshDataAfterSettingsChange(self):
        if not self.file_path:
            return
        if self.DATA is None and self.dataLoader is None:
            return
        has_query = isinstance(self._last_query, str) and self._last_query.strip()
        query_to_run = (
            self._last_query
            if has_query and self._last_query_file == self.file_path
            else None
        )
        self.releaseResources()
        self.page = 1
        if query_to_run:
            self.loadPage(query=query_to_run)
        else:
            self._last_query = None
            self._last_query_file = self.file_path
            self.loadPage()

    def updateRecentsMenu(self):
        """Refresh the File menu to show the latest recents list."""
        if not hasattr(self, "fileMenu"):
            return

        for action in self._recent_actions:
            self.fileMenu.removeAction(action)
            action.deleteLater()
        self._recent_actions = []
        self._recents_separator = None

        if not recents.recents:
            return

        separator = cast(QAction, self.fileMenu.addSeparator())
        self._recents_separator = separator
        self._recent_actions.append(separator)

        for recent in recents.recents:
            filename = Path(recent).name
            name = f"{filename} @ {Path(recent).parent}"
            recent_action = QAction(name, self)
            recent_action.triggered.connect(
                lambda checked, path=recent: self.openRecentFile(path)
            )
            self.fileMenu.addAction(recent_action)
            self._recent_actions.append(recent_action)

        clear_action = QAction("Clear List", self)
        clear_action.setFont(QFont("Courier", 9, weight=QFont.Bold))
        clear_action.triggered.connect(self.clearRecents)
        self.fileMenu.addAction(clear_action)
        self._recent_actions.append(clear_action)

    def clearRecents(self):
        recents.recents = []
        recents.save_recents()
        ParquetSQLApp.refresh_all_recents_menus()

    def openRecentFile(self, file_path):
        if not Path(file_path).exists():
            reply = QMessageBox.question(
                self,
                "File Not Found",
                f"The file {file_path} does not exist. Do you want to remove it from recents?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                recents.recents.remove(file_path)
                recents.save_recents()
                ParquetSQLApp.refresh_all_recents_menus()
            return
        print(file_path)
        existing_window = ParquetSQLApp.find_window_by_file(file_path)
        if existing_window:
            ParquetSQLApp.focus_window(existing_window)
            return False
        self.openFilePath(file_path)
