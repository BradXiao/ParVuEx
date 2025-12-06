from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar, cast
from pathlib import Path
import json
import sys
import ctypes
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
    QSystemTrayIcon,
    QShortcut,
    QGraphicsOpacityEffect,
    QSizePolicy,
    QToolTip,
    QStyledItemDelegate,
    QStyleOptionViewItem,
)
from PyQt5.QtGui import (
    QFont,
    QIcon,
    QKeySequence,
    QTextCursor,
    QHelpEvent,
    QWheelEvent,
    QKeyEvent,
)
from PyQt5.QtCore import (
    Qt,
    QTimer,
    QEvent,
)

from schemas import settings, Settings, recents, history
from gui_tools import (
    render_df_info,
    render_column_value_counts,
    render_row_values,
    markdown_to_html_with_table_styles,
)
from core import Data
from components import (
    get_resource_path,
    QueryThread,
    DataLoaderThread,
    AnimationWidget,
    SQLHighlighter,
    Popup,
    SearchableTextBrowser,
)
from utils import is_valid_font

if TYPE_CHECKING:
    from PyQt5.QtCore import QModelIndex, QPoint
    from PyQt5.QtGui import QCloseEvent

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


def normalize_instance_mode_value(value: str | None) -> str:
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


class ParquetSQLApp(QMainWindow):
    open_windows: ClassVar[list["ParquetSQLApp"]] = []
    RESULT_TABLE_ROW_HEIGHT: ClassVar[int] = 25
    MAX_COLUMN_WIDTH: ClassVar[int] = 600
    SQL_EDIT_CLEAN_BORDER: ClassVar[str] = "1px solid black"
    SQL_EDIT_DIRTY_BORDER: ClassVar[str] = "3px dotted #c1121f"

    @classmethod
    def _open_window_count(cls) -> int:
        return len(cls.open_windows)

    def _is_last_open_window(self) -> bool:
        return len(ParquetSQLApp.open_windows) <= 1

    @classmethod
    def find_window_by_file(cls, file_path: str) -> ParquetSQLApp | None:
        """Find an open window that has the specified file loaded."""
        target_path = Path(file_path).resolve()
        for window in cls.open_windows:
            if window.file_path and window.file_path.resolve() == target_path:
                return window
        return None

    @classmethod
    def focus_window(cls, window: "ParquetSQLApp", ask_reload: bool = False):
        """Bring the specified window to the front and activate it."""
        window.setWindowState(Qt.WindowNoState)
        window.show()
        window.showNormal()
        window.raise_()
        window.activateWindow()
        cls._force_foreground_window(window)
        if ask_reload:
            confirm = QMessageBox.question(
                window,
                "Reload",
                "Do you want to reload the file?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if confirm != QMessageBox.Yes:
                return
            window.reload_action.triggered.emit()
            window.execute_query(add_to_history=False)

    @classmethod
    def _force_foreground_window(cls, window: ParquetSQLApp):
        """Force window to foreground using Windows API (no flashing)."""
        if sys.platform != "win32":
            return
        hwnd = int(window.winId())
        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32
        foreground_hwnd = user32.GetForegroundWindow()
        foreground_thread = user32.GetWindowThreadProcessId(foreground_hwnd, None)
        current_thread = kernel32.GetCurrentThreadId()
        if foreground_thread != current_thread:
            user32.AttachThreadInput(current_thread, foreground_thread, True)
        user32.SetForegroundWindow(hwnd)
        user32.BringWindowToTop(hwnd)
        if foreground_thread != current_thread:
            user32.AttachThreadInput(current_thread, foreground_thread, False)

    def __init__(
        self,
        file_path: str | None = None,
        enable_tray: bool = True,
        launch_minimized: bool = True,
        is_secondary: bool = False,
    ):
        super().__init__()
        self.base_title = "ParVuEx v1.2.0"
        self.setWindowTitle(self.base_title)
        logo_path = get_resource_path("static/logo.jpg")
        if not logo_path.exists():
            raise FileNotFoundError(f"Logo file not found: {logo_path}")
        self.setWindowIcon(QIcon(str(logo_path)))

        self.page = 1
        self.total_pages = None
        self.total_row_count: int | None = None
        self.rows_per_page = int(
            settings.render_vars(settings.result_pagination_rows_per_page)
        )
        self.df = pd.DataFrame()
        self.data: Data | None = None
        self.query_thread: QueryThread | None = None
        self.data_loader: DataLoaderThread | None = None
        self.loading: AnimationWidget | None = None
        self.pending_query: str | None = None
        self._column_names: list[str] = []
        self._last_query: str | None = None
        self._last_query_file: Path | None = None
        self._queried: str | None = None
        self._history_index: int | None = None
        self._history_snapshot: str | None = None
        self._sql_edit_dirty: bool = False
        self.tray_icon: QSystemTrayIcon | None = None
        self._hinted_tray_icon: bool = False
        self.new_window_action: QAction | None = None
        self._new_window_separator: QAction | None = None
        self._recents_separator: QAction | None = None
        self._recent_actions: list[QAction] = []
        self._table_effect: QGraphicsOpacityEffect | None = None
        self._force_close = False
        self._dialog: QDialog | None = None
        self._app_event_filter_installed = False
        self.single_instance_server: QLocalServer | None = None
        self.instance_lock: QLockFile | None = None
        self.launch_minimized = launch_minimized
        self.enable_tray = enable_tray
        self._deleyed_column_saving = QTimer()
        self._deleyed_column_saving.setSingleShot(True)
        self._deleyed_column_saving.timeout.connect(self._save_column_widths)
        self._is_applying_column_widths = False
        self.is_secondary = is_secondary
        # use this variable to store opened files path
        self.file_path: Path | None = Path(file_path) if file_path else None
        self._last_column_widths: list[tuple[str, int]] | None = None

        self.init_UI()
        self.apply_settings_to_UI()
        if self.enable_tray:
            self.init_tray_icon()

        self.update_window_title()

        if self.launch_minimized and self.tray_icon:
            screen = QApplication.desktop().screenGeometry()
            window_width = int(screen.width() * 0.8)
            window_height = int(screen.height() * 0.8)
            x = (screen.width() - window_width) // 2
            y = (screen.height() - window_height) // 2
            self.setGeometry(x, y, window_width, window_height)
            QTimer.singleShot(0, self.minimize_on_launch)
        else:
            # Set window size to 80% of screen dimensions and center it
            screen = QApplication.desktop().screenGeometry()
            window_width = int(screen.width() * 0.8)
            window_height = int(screen.height() * 0.8)
            x = (screen.width() - window_width) // 2
            y = (screen.height() - window_height) // 2
            self.setGeometry(x, y, window_width, window_height)
            self.show()

        ParquetSQLApp.open_windows.append(self)
        if self.file_path:
            self.open_file_path(self.file_path, add_to_recents=True)
            self.execute_query(add_to_history=False)

    def init_UI(self):
        layout = QVBoxLayout()
        self.sql_edit = QTextEdit()
        self.sql_edit.setAcceptRichText(False)
        self.sql_edit.setPlainText(settings.render_vars(settings.default_sql_query))
        self.sql_edit.setMaximumHeight(90)
        self._apply_sql_edit_styles()
        self.sql_edit.installEventFilter(self)
        editor_layout = QHBoxLayout()
        editor_layout.addWidget(self.sql_edit)

        control_layout = QVBoxLayout()
        control_layout.setSpacing(8)

        self.execute_button = QPushButton("Execute")
        self.execute_button.setFixedSize(120, 25)
        self.execute_button.setStyleSheet(
            f"background-color: {settings.colour_executeButton}"
        )
        self.execute_button.clicked.connect(self.execute_query)
        control_layout.addWidget(self.execute_button)

        self.clear_button = QPushButton("Default SQL")
        self.clear_button.setFixedSize(120, 25)
        self.clear_button.setStyleSheet(f"background-color: #bc4749; color: white;")
        self.clear_button.clicked.connect(self.clear_query)
        control_layout.addWidget(self.clear_button)

        # meta info
        self.table_info_button = QPushButton("Table Info")
        self.table_info_button.setFixedSize(120, 25)
        self.table_info_button.setStyleSheet(
            f"background-color: {settings.colour_tableInfoButton}"
        )
        self.table_info_button.clicked.connect(self.toggle_table_info)
        control_layout.addWidget(self.table_info_button)
        control_layout.addStretch()

        editor_layout.addLayout(control_layout)
        layout.addLayout(editor_layout)

        self.result_label = QLabel()
        self.update_result_label()
        layout.addWidget(self.result_label)

        self.result_table = QTableWidget()
        self.result_table.setStyleSheet(
            f"background-color: {settings.colour_resultTable}"
        )
        self.result_table.setWordWrap(True)
        self._wrap_delegate = AutoWrapDelegate(self.result_table, min_wrapped_lines=2)
        self.result_table.setItemDelegate(self._wrap_delegate)
        self.result_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.result_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.result_table.installEventFilter(self)
        self.result_table.viewport().installEventFilter(self)
        self.result_table.customContextMenuRequested.connect(self.show_context_menu)
        self.result_table.currentCellChanged.connect(self.on_current_row_changed)
        self._column_resize_connected = False
        self.configure_result_table_font()
        layout.addWidget(self.result_table, stretch=1)

        # pagination
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

        layout.addLayout(self.pagination_layout)

        self.next_page_shortcut = QShortcut(QKeySequence("Ctrl+Right"), self)
        self.next_page_shortcut.activated.connect(self.next_page)
        self.prev_page_shortcut = QShortcut(QKeySequence("Ctrl+Left"), self)
        self.prev_page_shortcut.activated.connect(self.prev_page)

        layout.setStretch(0, 0)  # editor + controls
        layout.setStretch(1, 0)  # result label
        layout.setStretch(2, 1)  # result table grows with window
        layout.setStretch(3, 0)  # pagination
        layout.setStretch(4, 0)  # loading label

        central_widget = QWidget()
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

        # Create menu bar
        self.create_menu_bar()
        self.update_page_text()

        app = QApplication.instance()
        if app is not None and not self._app_event_filter_installed:
            app.installEventFilter(self)
            self._app_event_filter_installed = True

    def setup_sql_edit(self):
        # Set monospace font for consistency
        font = self.sql_edit.font()
        font.setFamily(settings.default_sql_font)
        font.setPointSize(int(settings.default_sql_font_size))
        self.sql_edit.setFont(font)

        # Apply syntax highlighting
        self.highlighter = SQLHighlighter(self.sql_edit.document(), settings)

    def _apply_sql_edit_styles(self):
        """Configure SQL editor colours and border state."""
        if not hasattr(self, "sql_edit"):
            return
        background_colour = settings.colour_sqlEdit
        border_style = (
            self.SQL_EDIT_DIRTY_BORDER
            if self._sql_edit_dirty
            else self.SQL_EDIT_CLEAN_BORDER
        )
        self.sql_edit.setStyleSheet(
            f"background-color: {background_colour}; border: {border_style};"
        )

    def _mark_sql_edit_dirty(self, dirty: bool):
        if self._sql_edit_dirty == dirty:
            return
        self._sql_edit_dirty = dirty
        self._apply_sql_edit_styles()

    def configure_result_table_font(self):
        table_font = self.result_table.font()
        table_font.setFamily(settings.default_result_font)
        table_font.setPointSize(int(settings.default_result_font_size))
        self.result_table.setFont(table_font)

        header = self.result_table.horizontalHeader()
        header_font = header.font()
        header_font.setFamily(settings.default_result_font)
        header_font.setPointSize(max(1, int(settings.default_result_font_size) - 1))
        header.setFont(header_font)

        vertical_header = self.result_table.verticalHeader()
        if vertical_header:
            vertical_font = vertical_header.font()
            vertical_font.setFamily(settings.default_result_font)
            vertical_font.setPointSize(
                max(1, int(settings.default_result_font_size) - 1)
            )
            vertical_header.setFont(vertical_font)
            self._apply_row_height()

    def _apply_row_height(self):
        """Ensure row height stays compact even after data refreshes."""
        vertical_header = self.result_table.verticalHeader()
        if vertical_header:
            vertical_header.setDefaultSectionSize(self.RESULT_TABLE_ROW_HEIGHT)
            vertical_header.setMinimumSectionSize(self.RESULT_TABLE_ROW_HEIGHT)
            vertical_font = vertical_header.font()
            vertical_font.setPointSize(
                max(1, int(settings.default_result_font_size) - 2)
            )

    def _limit_max_column_widths(self):
        for idx in range(self.result_table.columnCount()):
            width = self.result_table.columnWidth(idx)
            if width > self.MAX_COLUMN_WIDTH:
                self.result_table.setColumnWidth(idx, self.MAX_COLUMN_WIDTH)

    def _restore_column_widths(self) -> bool:
        """Apply persisted column widths for the current file, if any."""
        if not self.file_path or not self._column_names:
            return False
        saved_widths = history.get_col_widths(str(self.file_path))
        if not saved_widths:
            return False
        column_count = min(len(self._column_names), self.result_table.columnCount())
        for idx in range(column_count):
            column_name = self._column_names[idx]
            width = saved_widths.get(column_name)
            if isinstance(width, int) and width > 0:
                width = min(width, self.MAX_COLUMN_WIDTH)
                self.result_table.setColumnWidth(idx, width)

        return True

    def _collect_current_column_widths(self) -> dict[str, int]:
        """Capture the current visible widths for all known columns."""
        widths: dict[str, int] = {}
        if not self._column_names or not hasattr(self, "result_table"):
            return widths
        column_count = min(len(self._column_names), self.result_table.columnCount())
        if self._last_column_widths is None:
            self._last_column_widths = []
            for idx in range(column_count):
                width = self.result_table.columnWidth(idx)
                self._last_column_widths.append((self._column_names[idx], width))

        for idx in range(column_count):
            last_name, last_width = self._last_column_widths[idx]
            width = self.result_table.columnWidth(idx)
            if (
                width > 0
                and last_name == self._column_names[idx]
                and last_width != width
            ):
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

    def apply_settings_to_UI(self):
        """Re-apply dynamic settings such as fonts, colours, and pagination."""
        self.rows_per_page = int(
            settings.render_vars(settings.result_pagination_rows_per_page)
        )
        self._apply_sql_edit_styles()
        self.execute_button.setStyleSheet(
            f"background-color: {settings.colour_executeButton}"
        )
        self.table_info_button.setStyleSheet(
            f"background-color: {settings.colour_tableInfoButton}"
        )
        self.result_table.setStyleSheet(
            f"background-color: {settings.colour_resultTable}"
        )
        header = self.result_table.horizontalHeader()
        if header:
            header.setStyleSheet("QHeaderView::section { padding: 6px 4px; }")

        self._update_UI_font()

        self.setup_sql_edit()
        self.configure_result_table_font()
        self.update_result_label()
        self.update_page_text()

    def _update_UI_font(self):

        def _change_size(component: QWidget):
            font = component.font()
            font.setPointSize(int(settings.default_ui_font_size))
            component.setFont(font)

        _change_size(self.execute_button)
        _change_size(self.clear_button)
        _change_size(self.table_info_button)
        _change_size(self.result_label)
        _change_size(self.first_button)
        _change_size(self.prev_button)
        _change_size(self.page_label)
        _change_size(self.next_button)
        _change_size(self.last_button)

    def _current_page_row_offset(self) -> int:
        page_index = max(self.page - 1, 0)
        return page_index * self.rows_per_page

    def update_result_label(self, row: int | None = None, column: int | None = None):
        page_rows = len(self.df.index)
        total_cols = len(self.df.columns)
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
            total_rows_text = f"{self.total_row_count:,}"
            select_text = (
                f"Select: {row_text}×{col_text}"
                if (row_text + col_text).strip()
                else ""
            )
            self.result_label.setText(
                f"Rows: {total_rows_text}   Page Rows: {str(page_rows)}    {visible_range}   Cols: {total_cols}   {select_text}"
            )
            if hasattr(self, "last_button"):
                self.last_button.setText(str(self.total_pages))
        else:
            self.result_label.setText("No data loaded")
            if hasattr(self, "last_button"):
                self.last_button.setText("")

    def on_current_row_changed(
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

    def update_window_title(self):
        if self.file_path:
            self.setWindowTitle(
                f"{self.base_title} ({self.file_path.name} @ {self.file_path.parent})"
            )
        else:
            self.setWindowTitle(self.base_title)

    def update_action_states(self):
        has_file = self.file_path is not None
        if hasattr(self, "viewAction"):
            self.view_action.setEnabled(has_file)
        if hasattr(self, "closeFileAction"):
            self.close_file_action.setEnabled(has_file)
        if hasattr(self, "exportAction"):
            self.export_action.setEnabled(has_file)
        if hasattr(self, "resetTableSizeAction"):
            self.reset_table_size_action.setEnabled(has_file)
        if hasattr(self, "reloadAction"):
            self.reload_action.setEnabled(has_file)
        self.update_instance_actions()
        if not has_file:
            self._last_column_widths = None

    def update_instance_actions(self):
        multi_mode = is_multi_window_mode()
        if self.new_window_action:
            self.new_window_action.setVisible(multi_mode)
            self.new_window_action.setEnabled(multi_mode)
        if self._new_window_separator:
            self._new_window_separator.setVisible(multi_mode)

    @classmethod
    def refresh_all_instance_actions(cls):
        for window in list(cls.open_windows):
            window.update_instance_actions()

    @classmethod
    def refresh_all_recents_menus(cls):
        for window in list(cls.open_windows):
            if hasattr(window, "file_menu"):
                window.update_recents_menu()

    def attach_instance_server(self, server: QLocalServer | None):
        """Register the local server used to communicate with secondary launches."""
        self.single_instance_server = server
        if server:
            server.setParent(self)
            server.newConnection.connect(self._handle_incoming_instance_request)

    def attach_instance_lock(self, lock: QLockFile | None):
        """Store the lock so it remains held until the primary instance quits."""
        self.instance_lock = lock

    def _handle_incoming_instance_request(self):
        if not self.single_instance_server:
            return
        socket = self.single_instance_server.nextPendingConnection()
        if not socket:
            return
        socket.readyRead.connect(self._handle_instance_socket_data)

    def _handle_instance_socket_data(self):
        socket = self.sender()
        if not isinstance(socket, QLocalSocket):
            return
        data = bytes(socket.readAll()).decode("utf-8").strip()
        socket.disconnectFromServer()
        socket.deleteLater()
        self._handle_instance_message(data)

    def _handle_instance_message(self, payload: str):
        multi_mode = is_multi_window_mode()
        file_to_open: str | None = None
        message = None
        if payload:
            try:
                message = json.loads(payload)
            except json.JSONDecodeError:
                message = None
        if isinstance(message, dict):
            file_candidate = cast(str | None, message.get(INSTANCE_MESSAGE_KEY))
            if isinstance(file_candidate, str):
                file_candidate = file_candidate.strip()
            file_to_open = file_candidate or None

        if multi_mode:
            self._open_additional_window(file_to_open)
            return

        self.restore_from_tray()
        if file_to_open:
            self.open_file_path(file_to_open, add_to_recents=True)

    def _open_additional_window(self, file_to_open: str | None):
        if file_to_open:
            existing_window = ParquetSQLApp.find_window_by_file(file_to_open)
            if existing_window:
                ParquetSQLApp.focus_window(existing_window, ask_reload=True)
                return
            first_window = (
                ParquetSQLApp.open_windows[0] if ParquetSQLApp.open_windows else None
            )
            if first_window and not first_window.file_path:
                first_window.open_file_path(file_to_open, add_to_recents=True)
                ParquetSQLApp.focus_window(first_window)
                return
        new_window = ParquetSQLApp.spawn_additional_window(file_to_open)
        ParquetSQLApp.focus_window(new_window)

    @classmethod
    def spawn_additional_window(cls, file_to_open: str | None):
        window = cls(
            file_path=None, enable_tray=False, launch_minimized=False, is_secondary=True
        )
        if file_to_open:
            window.open_file_path(file_to_open, add_to_recents=True)
        return window

    def _close_instance_server(self):
        if self.single_instance_server:
            self.single_instance_server.close()
            self.single_instance_server.deleteLater()
            self.single_instance_server = None

    def _release_instance_lock(self):
        if self.instance_lock:
            self.instance_lock.unlock()
            self.instance_lock = None

    def close_file(self):
        if not self.file_path:
            return
        self.release_resources()
        self.file_path = None
        self._last_query = None
        self._last_query_file = None
        self.update_window_title()
        self.update_action_states()
        self._reset_history_navigation()

    def reset_table_size(self):
        if not self.file_path:
            return
        if not hasattr(self, "resultTable") or self.result_table.columnCount() == 0:
            return

        self.result_table.resizeColumnsToContents()
        self._limit_max_column_widths()
        self._apply_row_height()
        history.add_col_width(str(self.file_path), None)

    def exit_application(self):
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
        self.release_resources()
        self._close_instance_server()
        self._release_instance_lock()
        if app := QApplication.instance():
            app.quit()

    def create_menu_bar(self):
        menubar = self.menuBar()
        # app menu
        sys_menu = menubar.addMenu("App")
        self.new_window_action = QAction("New Window", self)
        self.new_window_action.triggered.connect(self.open_new_window_instance)
        sys_menu.addAction(self.new_window_action)
        self._new_window_separator = sys_menu.addSeparator()
        settings_action = QAction("Settings", self)
        settings_action.triggered.connect(self.edit_settings)
        sys_menu.addAction(settings_action)
        # quit app
        sys_menu.addSeparator()
        exit_menu_action = QAction("Quit App", self)
        exit_menu_action.triggered.connect(self.exit_application)
        sys_menu.addAction(exit_menu_action)

        # file menu
        self.file_menu = menubar.addMenu("File")
        browse_action = QAction("Open...", self)
        browse_action.triggered.connect(self.browse_file)
        self.file_menu.addAction(browse_action)
        self.reload_action = QAction("Reload", self)
        self.reload_action.triggered.connect(self.reload_file)
        self.file_menu.addAction(self.reload_action)
        self.close_file_action = QAction("Close", self)
        self.close_file_action.triggered.connect(self.close_file)
        self.file_menu.addAction(self.close_file_action)
        self.file_menu.addSeparator()
        self.export_action = QAction("Export...", self)
        self.export_action.triggered.connect(self.export_results)
        self.file_menu.addAction(self.export_action)
        self.update_recents_menu()

        # view file
        action_menu = menubar.addMenu("Actions")
        self.view_action = QAction("Show all data", self)
        self.view_action.triggered.connect(self.view_file)
        action_menu.addAction(self.view_action)
        action_menu.addSeparator()
        self.reset_table_size_action = QAction("Reset row/col height/width", self)
        self.reset_table_size_action.triggered.connect(self.reset_table_size)
        action_menu.addAction(self.reset_table_size_action)

        # help
        help_menu = menubar.addMenu("Help")
        help_action = QAction("Help/Info", self)
        help_action.triggered.connect(self.show_help_dialog)
        help_menu.addAction(help_action)
        self.update_action_states()

    def init_tray_icon(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return

        logo_path = get_resource_path("static/logo.jpg")
        if not logo_path.exists():
            raise FileNotFoundError(f"Logo file not found: {logo_path}")
        tray_icon = QSystemTrayIcon(QIcon(str(logo_path)), self)
        tray_menu = QMenu(self)

        restore_action = QAction("Restore", self)
        restore_action.triggered.connect(self.restore_from_tray)
        new_window_action = QAction("New Window", self)
        new_window_action.triggered.connect(self.open_new_window_instance)
        exit_action = QAction("Exit App", self)
        exit_action.triggered.connect(self.exit_from_tray)

        tray_menu.addAction(restore_action)
        tray_menu.addAction(new_window_action)
        tray_menu.addSeparator()
        tray_menu.addAction(exit_action)

        tray_icon.setContextMenu(tray_menu)
        tray_icon.activated.connect(self.handle_tray_activation)
        tray_icon.show()

        self.tray_icon = tray_icon
        self.tray_menu = tray_menu
        self.restore_action = restore_action
        self.exit_action = exit_action

    def ensure_tray_icon(self) -> bool:
        if self.tray_icon:
            return True
        self.init_tray_icon()
        return self.tray_icon is not None

    def handle_tray_activation(self, reason: int):
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
            self.restore_from_tray()

    def restore_from_tray(self):
        # Ensure proper window state
        self.setWindowState(Qt.WindowNoState)

        # Show and raise the window
        self.show()
        self.showNormal()
        self.raise_()  # Bring to top of window stack
        self.activateWindow()  # Request focus

        if self.data is None and self.file_path:
            self.execute()

    def _remove_stay_on_top(self, original_flags: Qt.WindowFlags):
        """Helper to remove the WindowStaysOnTopHint flag"""
        self.setWindowFlags(original_flags)
        self.show()
        self.raise_()
        self.activateWindow()

    def hint_tray_icon(self):
        if self._hinted_tray_icon or not self.tray_icon:
            return
        self.tray_icon.showMessage(
            "ParVuEx", "ParVuEx is running in the tray.", msecs=3000
        )
        self._hinted_tray_icon = True

    def minimize_on_launch(self):
        if self.tray_icon:
            self.hide()
            self.hint_tray_icon()
        else:
            self.showMinimized()

    def minimize_to_tray(self):
        if self.tray_icon:
            self.hide()
            self.hint_tray_icon()
        else:
            self.showMinimized()

    def exit_from_tray(self):
        self._force_close = True
        self.release_resources()
        self._close_instance_server()
        self._release_instance_lock()
        if app := QApplication.instance():
            app.quit()

    def release_resources(self):
        if self.query_thread and self.query_thread.isRunning():
            self.query_thread.quit()
            self.query_thread.wait()
        self.query_thread = None

        if self.data_loader and self.data_loader.isRunning():
            self.data_loader.quit()
            self.data_loader.wait()
        self.data_loader = None

        self.stop_loading_animation()
        data = self.data
        self.data = None
        if data is not None:
            del data
        self.df = pd.DataFrame()
        self._column_names = []
        self.total_pages = None
        self.total_row_count = None
        self.result_table.clear()
        self.result_table.setRowCount(0)
        self.result_table.setColumnCount(0)
        self.update_result_label()
        self.update_page_text()

    def close_event(self, event: QCloseEvent):
        if not self._force_close and not self.tray_icon and self._is_last_open_window():
            self.ensure_tray_icon()

        if not self._force_close and self.tray_icon:
            event.ignore()
            self.release_resources()
            self.close_file_action.triggered.emit()
            self.minimize_to_tray()
            return

        self.release_resources()
        self._close_instance_server()
        self._release_instance_lock()
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

    def show_help_dialog(self):
        with open(settings.static_dir / "help.md", "r", encoding="utf-8") as f:
            help_text = f.read()

        if self._dialog is not None:
            self._dialog.close()

        dialog = Popup(self, "Help/Info")

        text_browser = SearchableTextBrowser(dialog)
        text_browser.setMarkdown(help_text)
        text_browser.setReadOnly(True)

        layout = QVBoxLayout()
        layout.addWidget(text_browser)
        dialog.setLayout(layout)

        self._center_dialog_relative_to_window(dialog)

        def _clear_dialog_reference(*_):
            if self._dialog is dialog:
                self._dialog = None

        dialog.destroyed.connect(_clear_dialog_reference)
        dialog.finished.connect(_clear_dialog_reference)
        self._dialog = dialog

        dialog.show()

    def open_file_path(
        self,
        file_path: str | Path,
        add_to_recents: bool = False,
        auto_execute: bool = True,
        load_prev_history: bool = True,
    ) -> bool:
        path = Path(file_path)
        if not path.exists():
            self.result_label.setText(f"File not found: {path}")
            QMessageBox.warning(self, "File Not Found", f"File not found: {path}")
            return False

        self.file_path = path
        self.update_window_title()
        self.update_action_states()
        self.release_resources()
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

        history_loaded = False
        if load_prev_history and self.execute_button.text() == "Execute":
            history_loaded = self.show_previous_history_entry()

        if auto_execute:
            if not history_loaded:
                self.execute()
            else:
                self.execute_query(add_to_history=False)

        return True

    def reload_file(self):
        if not self.file_path:
            return
        self.open_file_path(
            self.file_path, add_to_recents=False, load_prev_history=False
        )

    def browse_file(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getOpenFileName(
            self,
            "Open File",
            "",
            "Data Files (*.parquet *.csv);;All Files (*)",
            options=options,
        )
        if fileName:
            existing_window = ParquetSQLApp.find_window_by_file(fileName)
            if existing_window:
                ParquetSQLApp.focus_window(existing_window, ask_reload=True)
                return False
            self.close_file()
            self.open_file_path(fileName, add_to_recents=True, auto_execute=False)
            self.execute_query(add_to_history=False)

    def open_new_window_instance(self):
        if not is_multi_window_mode():
            QMessageBox.information(
                self,
                "Multi-Window Disabled",
                "Enable multi-window mode in Settings to open additional windows.",
            )
            return
        self._open_additional_window(None)

    def view_file(self):
        if not self.file_path:
            self.result_label.setText("Browse file first...")
            return

        if not self.file_path.exists():
            self.result_label.setText(f"File not found: {self.file_path}")
            return

        self.release_resources()
        self.execute()

    def execute(self):
        self._last_query = None
        self._last_query_file = self.file_path
        self.page = 1
        self.load_page()
        self.update_page_text()
        self._last_column_widths = None

    def clear_query(self):
        self.sql_edit.clear()
        self.sql_edit.setPlainText(settings.render_vars(settings.default_sql_query))
        self._reset_history_navigation()
        self.execute_query(add_to_history=False)
        self._last_column_widths = None

    def execute_query(self, add_to_history: bool = True):
        query_text = self.sql_edit.toPlainText()
        self._mark_sql_edit_dirty(False)
        if add_to_history:
            self._add_query_to_history(query_text)
        self.page = 1
        self.load_page(query=query_text)
        self.update_page_text()
        self._queried = query_text
        self._last_column_widths = None

    def start_loading_animation(self):
        if self.loading:
            self.loading.stop()
        if self._table_effect is None:
            self._table_effect = QGraphicsOpacityEffect(self.result_table)
        self._table_effect.setOpacity(0.35)
        self.result_table.setGraphicsEffect(self._table_effect)
        self.result_table.setDisabled(True)
        if not self.isHidden():
            self.loading = AnimationWidget(self)
            self.loading.show()

    def stop_loading_animation(self):
        self.result_table.setEnabled(True)
        self.result_table.setGraphicsEffect(None)
        self._table_effect = None
        if self.loading:
            self.loading.stop()
            self.loading = None

    def start_data_loader(self, file_path: str):
        if self.data_loader and self.data_loader.isRunning():
            return

        self.data_loader = DataLoaderThread(
            file_path=file_path,
            virtual_table_name=settings.render_vars(settings.default_data_var_name),
            batchsize=self.rows_per_page,
        )
        self.data_loader.data_ready.connect(self.on_data_ready)
        self.data_loader.error_occurred.connect(self.handle_error)
        self.data_loader.start()

    def on_data_ready(self, data: Data):
        self.data = data
        loader = self.data_loader
        if loader is not None:
            loader.wait()
            loader.deleteLater()
        self.data_loader = None
        self.calc_total_pages(force=True)
        self._start_query_thread(self.pending_query)

    def _start_query_thread(self, query: str | None):
        if not self.data:
            return

        if self.query_thread and self.query_thread.isRunning():
            self.query_thread.quit()
            self.query_thread.wait()

        self.query_thread = QueryThread(
            data=self.data, query=query, nth_batch=self.page, app=self
        )
        self.query_thread.result_ready.connect(self.handle_results)
        self.query_thread.error_occurred.connect(self.handle_error)
        self.query_thread.finished.connect(self.update_page_text)
        self.query_thread.start()

    def load_page(self, query: str | None = None):
        if not self.file_path:
            self.result_label.setText("Browse file first...")
            return
        if not self.file_path.exists():
            self.result_label.setText(f"File not found: {self.file_path}")
            return
        if isinstance(query, str) and query.strip():
            self._last_query = query
            self._last_query_file = self.file_path
        self.pending_query = query
        self.start_loading_animation()

        if self.data is None:
            self.start_data_loader(str(self.file_path))
        else:
            self._start_query_thread(query)

    def handle_results(self, df: pd.DataFrame):
        if self.query_thread:
            self.query_thread.quit()
            self.query_thread.wait()
            self.query_thread = None
        self.df = df
        self.calc_total_pages(force=True)
        self.display_results(df)
        self.stop_loading_animation()

    def handle_error(self, error: str):
        if self.query_thread and self.query_thread.isRunning():
            self.query_thread.quit()
            self.query_thread.wait()
            self.query_thread = None

        if self.data_loader and self.data_loader.isRunning():
            self.data_loader.quit()
            self.data_loader.wait()
        self.data_loader = None

        self.result_label.setText(f"Error: {error}")
        self.stop_loading_animation()

    def display_results(self, df: pd.DataFrame):
        self.update_result_label()
        # Set the table dimensions
        self.result_table.setColumnCount(len(df.columns))
        self.result_table.setRowCount(len(df.index))

        # Set the column headers
        self._column_names = [str(col) for col in df.columns]  # type: ignore
        header_labels = [
            f"{idx + 1}\n{name}" for idx, name in enumerate(self._column_names)
        ]
        if header_labels:
            self.result_table.setHorizontalHeaderLabels(header_labels)
        else:
            self.result_table.setHorizontalHeaderLabels([])
        header = self.result_table.horizontalHeader()
        if header:
            header.setDefaultAlignment(Qt.AlignCenter)
            # Connect column resize signal once the table has columns
            if not self._column_resize_connected:
                header.sectionResized.connect(self._on_column_section_resized)
                self._column_resize_connected = True

        # Fill the table with the DataFrame data
        for i in range(len(df.index)):
            for j in range(len(df.columns)):
                item = QTableWidgetItem(str(df.iat[i, j]))
                self.result_table.setItem(i, j, item)

        page_rows = len(df.index)
        start_index = self._current_page_row_offset()
        if page_rows:
            row_labels = [str(start_index + idx + 1) for idx in range(page_rows)]
            self.result_table.setVerticalHeaderLabels(row_labels)
        else:
            self.result_table.setVerticalHeaderLabels([])

        self._apply_row_height()

        if len(df.index) and len(df.columns):
            self.result_table.setCurrentCell(0, 0)

        self._is_applying_column_widths = True
        self.result_table.resizeColumnsToContents()
        self._limit_max_column_widths()
        self._restore_column_widths()
        self._collect_current_column_widths()
        self._is_applying_column_widths = False
        self.highlighter.update_columns(self._column_names)

    def update_page_text(self):
        """set next / prev button text"""
        total_pages_value = (
            self.total_pages if isinstance(self.total_pages, int) else None
        )
        has_data = self.data is not None
        if hasattr(self, "page_label"):
            self.page_label.setText(f"Page {self.page}")
        can_go_prev = has_data and self.page > 1
        at_last_page = (
            isinstance(total_pages_value, int) and self.page >= total_pages_value
        )
        can_go_next = has_data and (
            not isinstance(total_pages_value, int) or not at_last_page
        )

        if hasattr(self, "prev_button"):
            self.prev_button.setEnabled(can_go_prev)
        if hasattr(self, "first_button"):
            self.first_button.setEnabled(can_go_prev)
        if hasattr(self, "next_button"):
            self.next_button.setEnabled(can_go_next)
        if hasattr(self, "last_button"):
            self.last_button.setEnabled(
                has_data and isinstance(total_pages_value, int) and not at_last_page
            )

    def first_page(self):
        if not self.data or self.page == 1:
            return
        self.page = 1
        self.load_page()
        self.update_page_text()

    def prev_page(self):
        if not self.data:
            return
        if self.page > 1:
            self.page -= 1
            self.load_page()
            self.update_page_text()

    def next_page(self):
        # if is the last page, do nothing
        if not self.data:
            return
        total_batches = (
            self.data.total_batches
            if isinstance(self.data.total_batches, int)
            else self.total_pages
        )
        if isinstance(total_batches, int) and self.page >= total_batches:
            return
        self.page += 1
        self.load_page()
        self.update_page_text()

    def last_page(self):
        if not self.data:
            return
        self.calc_total_pages(force=True)
        if not isinstance(self.total_pages, int) or self.page >= self.total_pages:
            return
        self.page = self.total_pages
        self.load_page()
        self.update_page_text()

    def calc_total_pages(self, force: bool = False):
        """calculate how many pages data will have, if `force` is False then won't recalculate it"""
        if self.data:
            if force or self.total_pages is None:
                self.total_pages = self.data.calc_n_batches()
                self.total_row_count = self.data.calc_total_rows()
                self.update_page_text()
                self.update_result_label()

    def show_context_menu(self, pos: QPoint):
        contextMenu = QMenu(self)

        header = self.result_table.horizontalHeader()
        column = header.logicalIndexAt(pos.x())
        row = self.result_table.indexAt(pos).row()

        if column >= 0:
            column_name = self._get_column_name(column)
            value_counts = QAction("Show Value Counts", self)
            value_counts.triggered.connect(
                lambda: self.show_column_value_counts(column_name)
            )
            contextMenu.addAction(value_counts)
            row_values = QAction("Show This Row", self)
            row_values.triggered.connect(lambda: self.show_row_values(row))
            contextMenu.addAction(row_values)
            contextMenu.addSeparator()

            # Create Copy Submenu
            copy_column_action = QAction("Copy Column Name", self)
            copy_column_action.triggered.connect(lambda: self.copy_column_name(column))
            contextMenu.addAction(copy_column_action)

            copy_column_values_action = QAction("Copy Whole Column", self)
            copy_column_values_action.triggered.connect(
                lambda: self.copy_column_values(column)
            )
            contextMenu.addAction(copy_column_values_action)

            if row >= 0:
                copy_row_values_action = QAction("Copy Whole Row", self)
                copy_row_values_action.triggered.connect(
                    lambda: self.copy_row_values(row)
                )
                contextMenu.addAction(copy_row_values_action)
                copy_row_values_action = QAction("Copy Whole Row as Dict", self)
                copy_row_values_action.triggered.connect(
                    lambda: self.copy_row_values(row, as_dict=True)
                )
                contextMenu.addAction(copy_row_values_action)

        contextMenu.exec_(self.result_table.mapToGlobal(pos))

    def _close_active_dialog(self):
        if self._dialog is not None:
            self._dialog.close()

    def show_column_value_counts(self, column: str):
        if not self.data:
            return
        table_info = render_column_value_counts(
            self.data.reader.duckdf_query,
            column,
            self.data.reader.duckdf,
            max_rows=50,
        )

        if self._dialog is not None:
            self._dialog.close()

        dialog = Popup(self, f"Value Counts for {column}")
        text_browser = SearchableTextBrowser(dialog)
        table_font = QFont(
            settings.default_result_font, int(settings.default_result_font_size) + 1
        )
        styled_html = markdown_to_html_with_table_styles(table_info, table_font)
        text_browser.setHtml(styled_html)
        text_browser.setFont(table_font)
        text_browser.setReadOnly(True)
        text_browser.setOpenExternalLinks(True)

        layout = QVBoxLayout()
        layout.addWidget(text_browser)
        dialog.setLayout(layout)
        self._center_dialog_relative_to_window(dialog)

        def _clear_dialog_reference(*_):
            if self._dialog is dialog:
                self._dialog = None

        dialog.destroyed.connect(_clear_dialog_reference)
        dialog.finished.connect(_clear_dialog_reference)

        self._dialog = dialog
        dialog.show()

    def show_row_values(self, row: int):
        dict_values = cast(dict[str, Any], self.df.iloc[row, :].to_dict())
        table_info = render_row_values(dict_values)

        if self._dialog is not None:
            self._dialog.close()

        dialog = Popup(self, f"Values for Row {row}")
        text_browser = SearchableTextBrowser(dialog)
        table_font = QFont(
            settings.default_result_font, int(settings.default_result_font_size)
        )
        text_browser.setFont(table_font)
        styled_html = markdown_to_html_with_table_styles(table_info, table_font)
        text_browser.setHtml(styled_html)
        text_browser.setReadOnly(True)
        text_browser.setOpenExternalLinks(True)

        layout = QVBoxLayout()
        layout.addWidget(text_browser)
        dialog.setLayout(layout)
        self._center_dialog_relative_to_window(dialog)

        def _clear_dialog_reference(*_):
            if self._dialog is dialog:
                self._dialog = None

        dialog.destroyed.connect(_clear_dialog_reference)
        dialog.finished.connect(_clear_dialog_reference)

        self._dialog = dialog
        dialog.show()

    def copy_column_name(self, column: int):
        column_name = self._get_column_name(column)
        if not column_name:
            return
        clipboard = QApplication.clipboard()
        clipboard.setText(column_name)

    def copy_column_values(self, column: int):
        values = cast(list[Any], self.df.iloc[:, column].tolist())
        clipboard = QApplication.clipboard()
        clipboard.setText(str(values))

    def copy_row_values(self, row: int, as_dict: bool = False):
        if not as_dict:
            values = cast(list[Any], self.df.iloc[row, :].tolist())
        else:
            txt = cast(dict[str, Any], self.df.iloc[row, :].to_dict())
            for key in list(txt.keys()):
                if txt[key] is None or isinstance(txt[key], (int, float, bool, str)):
                    continue
                txt[key] = str(txt[key])  # try to convert to string

            values = json.dumps(txt, indent=4, ensure_ascii=False)
        clipboard = QApplication.clipboard()
        clipboard.setText(str(values))

    def _get_column_name(self, column: int) -> str:
        if 0 <= column < len(self._column_names):
            return self._column_names[column]
        if 0 <= column < len(self.df.columns):
            return str(self.df.columns[column])
        header_item = self.result_table.horizontalHeaderItem(column)
        if header_item:
            parts = header_item.text().splitlines()
            return parts[-1].strip()
        return ""

    def eventFilter(self, obj: QWidget, event: QEvent):
        dialog = self._dialog
        if (
            dialog is not None
            and dialog.isVisible()
            and event.type() == QEvent.MouseButtonPress
            # and isinstance(obj, QWidget)
        ):
            if obj is not dialog and not dialog.isAncestorOf(obj):
                if obj is self or self.isAncestorOf(obj):
                    self._close_active_dialog()

        table_viewport = (
            self.result_table.viewport() if hasattr(self, "result_table") else None
        )
        if obj in (self.result_table, table_viewport):

            if isinstance(event, QWheelEvent) and event.modifiers() & Qt.ShiftModifier:  # type: ignore
                scroll_bar = self.result_table.horizontalScrollBar()
                delta_point = event.pixelDelta()
                if not delta_point.isNull():
                    scroll_delta = delta_point.x() or delta_point.y()
                    if scroll_delta:
                        scroll_bar.setValue(scroll_bar.value() - scroll_delta)
                        return True

                angle_delta = event.angleDelta()
                scroll_delta = angle_delta.x() or angle_delta.y()
                if scroll_delta:
                    single_step = max(1, scroll_bar.singleStep())
                    steps = scroll_delta / 120
                    scroll_bar.setValue(int(scroll_bar.value() - steps * single_step))
                    return True

            if event.type() == QEvent.ToolTip:
                help_event = cast(QHelpEvent, event)
                viewport = self.result_table.viewport()
                viewport_pos = viewport.mapFromGlobal(help_event.globalPos())
                index = self.result_table.indexAt(viewport_pos)
                if index.isValid():
                    item = self.result_table.item(index.row(), index.column())
                    if item is not None:
                        QToolTip.showText(
                            help_event.globalPos(),
                            item.text(),
                            self.result_table,
                        )
                        return True
                QToolTip.hideText()
                event.ignore()
                return True

        if obj is self.sql_edit and isinstance(event, QKeyEvent):
            key = event.key()
            modifiers = event.modifiers()
            ctrl_only = bool(modifiers & Qt.ControlModifier) and not (
                modifiers & (Qt.ShiftModifier | Qt.AltModifier)
            )  # type: ignore
            if ctrl_only:
                if key == Qt.Key_Up:
                    if self.show_previous_history_entry():
                        return True
                    return super().eventFilter(obj, event)
                if key == Qt.Key_Down:
                    if self.show_next_history_entry():
                        return True
                    return super().eventFilter(obj, event)
                if key == Qt.Key_Left:
                    self.prev_page()
                    return True
                if key == Qt.Key_Right:
                    self.next_page()
                    return True

            if key in (Qt.Key_Return, Qt.Key_Enter):
                if modifiers & Qt.ShiftModifier:  # type: ignore
                    if self._history_index is not None:
                        self._reset_history_navigation()
                    return False
                self.execute_query()
                return True

            self._handle_edit_check()

        return super().eventFilter(obj, event)

    def _handle_edit_check(self, handle_history: bool = True):
        def delayed_handle_edit_check():
            if self._queried is None:
                return
            text_changed = self._queried.strip() != self.sql_edit.toPlainText().strip()
            if handle_history and self._history_index is not None and text_changed:
                self._reset_history_navigation()

            if text_changed:
                self._mark_sql_edit_dirty(True)
            else:
                self._mark_sql_edit_dirty(False)

        QTimer.singleShot(50, delayed_handle_edit_check)

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
        self.execute_button.setText("Execute")

    def _begin_history_navigation(self) -> bool:
        if (
            str(self.file_path) not in history.queries
            or not history.queries[str(self.file_path)]
        ):
            return False
        return True

    def _apply_history_entry(self, text: str):
        previous_state = self.sql_edit.blockSignals(True)
        try:
            self.sql_edit.setPlainText(text)
            cursor = self.sql_edit.textCursor()
            cursor.movePosition(QTextCursor.End)
            self.sql_edit.setTextCursor(cursor)
        finally:
            self.sql_edit.blockSignals(previous_state)

    def show_previous_history_entry(self) -> bool:
        if not self._begin_history_navigation():
            return False
        if self._history_index is None:
            self._history_snapshot = self.sql_edit.toPlainText()
            self._history_index = 0
        elif self._history_index + 1 < len(history.queries[str(self.file_path)]):
            self._history_index += 1
        entries = history.queries[str(self.file_path)]
        entry = entries[self._history_index]
        self._apply_history_entry(entry)
        self.execute_button.setText(
            f"Execute (-{self._history_index + 1}/{len(entries)})"
        )
        self._handle_edit_check(handle_history=False)
        return True

    def show_next_history_entry(self) -> bool:
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
            entries = history.queries[str(self.file_path)]
            entry = entries[self._history_index]
            self._apply_history_entry(entry)
            self.execute_button.setText(
                f"Execute (-{self._history_index + 1}/{len(entries)})"
            )
            self._handle_edit_check(handle_history=False)
            return True

        if self._history_snapshot is not None:
            snapshot = self._history_snapshot
            self._reset_history_navigation()
            self._apply_history_entry(snapshot)
            self._handle_edit_check()
            return True

        return False

    def export_results(self):
        if not self.data:
            self.result_label.setText("No data to export")
            return
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Results",
            "",
            "CSV Files (*.csv);;Parquet Files (*.parquet);;All Files (*)",
            options=options,
        )
        if file_path:
            if file_path.endswith(".csv"):
                self.data.reader.duckdf_query.to_csv(file_path)
            # elif filePath.endswith('.xlsx'):
            # # todo: add support for xlsx(https://duckdb.org/docs/guides/file_formats/excel_export.html)
            #     self.DATA.reader.duckdf_query.to(filePath, index=False)

            elif file_path.endswith(".parquet"):
                self.data.reader.duckdf_query.to_parquet(file_path)
            else:
                QMessageBox.warning(
                    self,
                    "Invalid File Type",
                    "Please select a valid file type (CSV or XLSX).",
                )

    def toggle_table_info(self):
        if self.file_path and self.file_path.exists():
            if not self.data:
                return

            table_info = render_df_info(self.data.reader.duckdf_query)

            if self._dialog is not None:
                self._dialog.close()

            dialog = Popup(self, "Table Info")

            text_browser = SearchableTextBrowser(dialog)
            table_font = QFont(
                settings.default_result_font, int(settings.default_result_font_size)
            )
            text_browser.setFont(table_font)
            styled_html = markdown_to_html_with_table_styles(table_info, table_font)
            text_browser.setHtml(styled_html)
            text_browser.setReadOnly(True)
            text_browser.setOpenExternalLinks(True)

            layout = QVBoxLayout()
            layout.addWidget(text_browser)
            dialog.setLayout(layout)

            self._center_dialog_relative_to_window(dialog)

            def _clear_dialog_reference(*_):
                if self._dialog is dialog:
                    self._dialog = None

            dialog.destroyed.connect(_clear_dialog_reference)
            dialog.finished.connect(_clear_dialog_reference)
            self._dialog = dialog
            dialog.show()

    def edit_settings(self):
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
                self.init_UI()

            def validate_settings(self):
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

            def init_UI(self):
                layout = QFormLayout()

                self.fields: dict[str, QLineEdit] = {}
                for field, value in self.settings.model_dump().items():
                    if field in self.read_only_fields:
                        continue

                    line_edit = QLineEdit()
                    # line_edit.setPlaceholderText(str(value))
                    line_edit.setText(str(value).replace("\n", "\\n"))
                    self.fields[field] = line_edit
                    layout.addRow(QLabel(field), line_edit)

                help_text = QLabel(self.help_text)
                help_text.setFont(QFont("Courier", 9, weight=QFont.Bold))
                layout.addRow(help_text)

                button_layout = QHBoxLayout()

                save_button = QPushButton("Save")
                save_button.clicked.connect(self.save_settings)
                button_layout.addWidget(save_button)

                reset_button = QPushButton("Reset to Default")
                reset_button.clicked.connect(self.reset_settings)
                button_layout.addWidget(reset_button)

                layout.addRow(button_layout)

                self.setLayout(layout)
                self.setWindowTitle("Edit Settings")
                self.resize(400, 300)

            def save_settings(self):
                if not self.validate_settings():
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
                            if field.endswith("_font"):
                                if is_valid_font(line_edit.text()) == False:
                                    QMessageBox.critical(
                                        self,
                                        "Error",
                                        f"Can't find font family: {line_edit.text()}",
                                    )
                            setattr(
                                self.settings,
                                field,
                                line_edit.text().replace("\\n", "\n"),
                            )

                self.settings.save_settings()
                self.accept()

            def reset_settings(self):
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
            self.handle_settings_changed()

    def handle_settings_changed(self):
        self._reload_settings_model()
        self.apply_settings_to_UI()
        ParquetSQLApp.refresh_all_instance_actions()
        self._refresh_data_after_settings_change()

    def _reload_settings_model(self):
        refreshed_settings = Settings.load_settings()
        for field_name in Settings.model_fields:
            setattr(settings, field_name, getattr(refreshed_settings, field_name))

    def _refresh_data_after_settings_change(self):
        if not self.file_path:
            return
        if self.data is None and self.data_loader is None:
            return
        has_query = isinstance(self._last_query, str) and self._last_query.strip()
        query_to_run = (
            self._last_query
            if has_query and self._last_query_file == self.file_path
            else None
        )
        self.release_resources()
        self.page = 1
        if query_to_run:
            self.load_page(query=query_to_run)
        else:
            self._last_query = None
            self._last_query_file = self.file_path
            self.load_page()

    def update_recents_menu(self):
        """Refresh the File menu to show the latest recents list."""
        if not hasattr(self, "file_menu"):
            return

        for action in self._recent_actions:
            self.file_menu.removeAction(action)
            action.deleteLater()
        self._recent_actions = []
        self._recents_separator = None

        if not recents.recents:
            return

        separator = self.file_menu.addSeparator()
        self._recents_separator = separator
        self._recent_actions.append(separator)

        for recent in recents.recents:
            filename = Path(recent).name
            name = f"{filename} @ {Path(recent).parent}"
            recent_action = QAction(name, self)

            def make_handler(path: str):
                def handler(checked: bool) -> None:
                    self.open_recent_file(checked, path)

                return handler

            recent_action.triggered.connect(make_handler(recent))
            self.file_menu.addAction(recent_action)
            self._recent_actions.append(recent_action)

        clear_action = QAction("Clear List", self)
        clear_action.setFont(QFont("Courier", 9, weight=QFont.Bold))
        clear_action.triggered.connect(self.clearRecents)
        self.file_menu.addAction(clear_action)
        self._recent_actions.append(clear_action)

    def clearRecents(self):
        recents.recents = []
        recents.save_recents()
        ParquetSQLApp.refresh_all_recents_menus()

    def open_recent_file(self, checked: bool, file_path: str):
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
            ParquetSQLApp.focus_window(existing_window, ask_reload=True)
            return False
        self.open_file_path(file_path)
