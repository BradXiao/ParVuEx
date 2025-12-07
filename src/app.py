from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, cast
from pathlib import Path
import json
import sys
from PyQt5.QtNetwork import QLocalServer, QLocalSocket

from PyQt5.QtWidgets import (
    QVBoxLayout,
    QHBoxLayout,
    QMenu,
    QAction,
    QMessageBox,
    QSystemTrayIcon,
    QShortcut,
    QGraphicsOpacityEffect,
    QToolTip,
    QWidget,
    QMainWindow,
    QApplication,
)
from PyQt5.QtGui import (
    QIcon,
    QKeySequence,
    QHelpEvent,
    QWheelEvent,
    QKeyEvent,
    QWindow,
)
from PyQt5.QtCore import (
    Qt,
    QTimer,
    QEvent,
    QLockFile,
)

from schemas import settings, recents, history
from gui_tools import is_multi_window_mode
from components import DataContainer, get_resource_path, AnimationWidget
from utils import force_foreground_window

if TYPE_CHECKING:
    from PyQt5.QtGui import QCloseEvent

INSTANCE_MESSAGE_KEY = "file"


class ParquetSQLApp(QMainWindow):
    open_windows: ClassVar[list[ParquetSQLApp]] = []

    @classmethod
    def find_window_by_file(cls, file_path: str) -> ParquetSQLApp | None:
        """Find an open window that has the specified file loaded."""
        target_path = Path(file_path).resolve()
        for window in cls.open_windows:
            opened_file = window.data_container.get_file_path()
            if opened_file and opened_file.resolve() == target_path:
                return window
        return None

    @classmethod
    def focus_window(cls, window: ParquetSQLApp, ask_reload: bool = False):
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
            window.menu_controller.reload_action.triggered.emit()
            window.sql_edit_controller.execute_query(add_to_history=False)

    @classmethod
    def refresh_all_instance_actions(cls):
        for window in list(cls.open_windows):
            window.menu_controller.update_instance_actions()

    @classmethod
    def refresh_all_recents_menus(cls):
        for window in list(cls.open_windows):
            window.menu_controller.update_recents_menu()

    @classmethod
    def spawn_additional_window(cls, file_to_open: str | None):
        window = cls(
            file_path=None, enable_tray=False, launch_minimized=False, is_secondary=True
        )
        if file_to_open:
            window.data_container.open_file_path(file_to_open, add_to_recents=True)
        return window

    @classmethod
    def _open_window_count(cls) -> int:
        return len(cls.open_windows)

    @classmethod
    def _force_foreground_window(cls, window: ParquetSQLApp):
        if sys.platform != "win32":
            return
        force_foreground_window(int(window.winId()))

    def __init__(
        self,
        file_path: str | None = None,
        enable_tray: bool = True,
        launch_minimized: bool = True,
        is_secondary: bool = False,
    ):
        super().__init__()

        self.setWindowTitle(settings.BASE_TITLE)
        logo_path = get_resource_path("static/logo.jpg")
        if not logo_path.exists():
            raise FileNotFoundError(f"Logo file not found: {logo_path}")
        self.setWindowIcon(QIcon(str(logo_path)))

        self._loading: AnimationWidget | None = None
        self._tray_icon: QSystemTrayIcon | None = None
        self._hinted_tray_icon: bool = False
        self._app_event_filter_installed: bool = False

        self._table_effect: QGraphicsOpacityEffect | None = None
        self._force_close = False
        self._single_instance_server: QLocalServer | None = None
        self._instance_lock: QLockFile | None = None
        self._launch_minimized = launch_minimized
        self._enable_tray = enable_tray
        self._is_secondary = is_secondary

        self._init_ui_components()
        self.menu_controller.update_settings()

        if self._enable_tray:
            self._init_tray_icon()

        self.update_window_title()

        self._init_window_geometry()

        if self._launch_minimized and self._tray_icon:
            QTimer.singleShot(0, self.minimize_on_launch)
        else:
            self.show()
            if sys.platform == "win32":
                force_foreground_window(int(self.winId()))

        ParquetSQLApp.open_windows.append(self)

        if file_path:
            self.data_container.open_file_path(file_path, add_to_recents=True)
            self.sql_edit_controller.execute_query(add_to_history=False)

    def update_window_title(self):
        path = self.data_container.get_file_path()
        base_title = settings.BASE_TITLE
        if path:
            self.setWindowTitle(f"{base_title} ({path.name} @ {path.parent})")
        else:
            self.setWindowTitle(base_title)

    def attach_instance_server(self, server: QLocalServer | None):
        """Register the local server used to communicate with secondary launches."""
        self._single_instance_server = server
        if server:
            server.setParent(self)
            server.newConnection.connect(self._handle_incoming_instance_request)

    def attach_instance_lock(self, lock: QLockFile | None):
        """Store the lock so it remains held until the primary instance quits."""
        self._instance_lock = lock

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

    def ensure_tray_icon(self) -> bool:
        if self._tray_icon:
            return True
        self._init_tray_icon()
        return self._tray_icon is not None

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
            self._restore_from_tray()

    def hint_tray_icon(self):
        if self._hinted_tray_icon or not self._tray_icon:
            return
        self._tray_icon.showMessage(
            "ParVuEx", "ParVuEx is running in the tray.", msecs=3000
        )
        self._hinted_tray_icon = True

    def minimize_on_launch(self):
        if self._tray_icon:
            self.hide()
            self.hint_tray_icon()
        else:
            self.showMinimized()

    def minimize_to_tray(self):
        if self._tray_icon:
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
        self.stop_loading_animation()
        self.data_container.release_resources()
        self.result_controller.release_resources()

    def closeEvent(self, event: QCloseEvent):
        if (
            not self._force_close
            and not self._tray_icon
            and self._is_last_open_window()
        ):
            self.ensure_tray_icon()

        if not self._force_close and self._tray_icon:
            event.ignore()
            self.release_resources()
            self.menu_controller.close_file_action.triggered.emit()
            self.minimize_to_tray()
            return

        self.release_resources()
        self._close_instance_server()
        self._release_instance_lock()
        super().closeEvent(event)
        if event.isAccepted() and self in ParquetSQLApp.open_windows:
            ParquetSQLApp.open_windows.remove(self)

    def open_new_window_instance(self):
        if not is_multi_window_mode(settings):
            QMessageBox.information(
                self,
                "Multi-Window Disabled",
                "Enable multi-window mode in Settings to open additional windows.",
            )
            return
        self._open_additional_window(None)

    def start_loading_animation(self):
        if self._loading:
            self._loading.stop()
        if self._table_effect is None:
            self._table_effect = QGraphicsOpacityEffect(
                self.result_controller.result_table
            )
        self._table_effect.setOpacity(0.35)
        self.result_controller.result_table.setGraphicsEffect(self._table_effect)
        self.result_controller.result_table.setDisabled(True)
        if not self.isHidden():
            self._loading = AnimationWidget(self)
            self._loading.show()

    def stop_loading_animation(self):
        self.result_controller.result_table.setEnabled(True)
        self.result_controller.result_table.setGraphicsEffect(None)
        self._table_effect = None
        if self._loading:
            self._loading.stop()
            self._loading = None

    def eventFilter(self, obj: QWidget | QWindow, event: QEvent):

        if event.type() == QEvent.MouseButtonPress and isinstance(obj, QWidget):
            self.dialog_controller.auto_close_dialog(obj)

        if obj in (
            self.result_controller.result_table,
            self.result_controller.result_table.viewport(),
        ):
            if isinstance(event, QWheelEvent) and event.modifiers() & Qt.ShiftModifier:  # type: ignore
                scroll_bar = self.result_controller.result_table.horizontalScrollBar()
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
                viewport = self.result_controller.result_table.viewport()
                viewport_pos = viewport.mapFromGlobal(help_event.globalPos())
                index = self.result_controller.result_table.indexAt(viewport_pos)
                if index.isValid():
                    item = self.result_controller.result_table.item(
                        index.row(), index.column()
                    )
                    if item is not None:
                        QToolTip.showText(
                            help_event.globalPos(),
                            item.text(),
                            self.result_controller.result_table,
                        )
                        return True
                QToolTip.hideText()
                event.ignore()
                return True
        elif obj is self.sql_edit_controller.sql_edit and isinstance(event, QKeyEvent):
            if event.type() != QEvent.KeyPress:
                return False
            key = event.key()
            modifiers = event.modifiers()
            ctrl_only = bool(modifiers & Qt.ControlModifier) and not (
                modifiers & (Qt.ShiftModifier | Qt.AltModifier)
            )  # type: ignore
            print(f"fire {key} {modifiers}")
            if ctrl_only and key in (Qt.Key_Up, Qt.Key_Down):
                if self.sql_edit_controller.handle_history_hotkeys(key):
                    return True
                return super().eventFilter(obj, event)
            elif ctrl_only and key in (Qt.Key_Left, Qt.Key_Right):
                if self.result_controller.handle_page_hotkeys(key):
                    return True
                return super().eventFilter(obj, event)

            if key in (Qt.Key_Return, Qt.Key_Enter) and not (modifiers & Qt.ShiftModifier):  # type: ignore
                self.sql_edit_controller.execute_query()
                return True

            self.sql_edit_controller.handle_edit_check()

        return super().eventFilter(obj, event)

    def _init_window_geometry(self):
        screen = QApplication.desktop().screenGeometry()
        window_width = int(screen.width() * 0.8)
        window_height = int(screen.height() * 0.8)
        x = (screen.width() - window_width) // 2
        y = (screen.height() - window_height) // 2
        self.setGeometry(x, y, window_width, window_height)

    def _init_ui_components(self):
        from com_results import ResultsController
        from com_sql_edit import SqlEditController
        from com_dialog import DialogController

        self.data_container = DataContainer(self, settings)
        layout = QVBoxLayout()

        self.dialog_controller = DialogController(self, settings)

        self.sql_edit_controller = SqlEditController(
            settings, history, self.data_container, self.dialog_controller
        )

        editor_layout = QHBoxLayout()
        editor_layout.addWidget(self.sql_edit_controller.sql_edit)
        control_layout = QVBoxLayout()
        control_layout.setSpacing(8)
        control_layout.addWidget(self.sql_edit_controller.execute_button)
        control_layout.addWidget(self.sql_edit_controller.default_button)
        control_layout.addWidget(self.sql_edit_controller.table_info_button)
        control_layout.addStretch()
        editor_layout.addLayout(control_layout)
        layout.addLayout(editor_layout)

        self.result_controller = ResultsController(
            self, settings, history, self.dialog_controller
        )
        layout.addWidget(self.result_controller.result_label)
        layout.addWidget(self.result_controller.result_table, stretch=1)

        self.pagination_layout = self.result_controller.pagination_layout
        layout.addLayout(self.pagination_layout)

        self.next_page_shortcut = QShortcut(QKeySequence("Ctrl+Right"), self)
        self.next_page_shortcut.activated.connect(self.result_controller.next_page)
        self.prev_page_shortcut = QShortcut(QKeySequence("Ctrl+Left"), self)
        self.prev_page_shortcut.activated.connect(self.result_controller.prev_page)

        layout.setStretch(0, 0)  # editor + controls
        layout.setStretch(1, 0)  # result label
        layout.setStretch(2, 1)  # result table grows with window
        layout.setStretch(3, 0)  # pagination
        layout.setStretch(4, 0)  # loading label

        central_widget = QWidget()
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

        self._create_menu_bar()
        self.result_controller.update_page_text()

        app = QApplication.instance()
        if app is not None and not self._app_event_filter_installed:
            app.installEventFilter(self)
            self._app_event_filter_installed = True

    def _handle_incoming_instance_request(self):
        if not self._single_instance_server:
            return
        socket = self._single_instance_server.nextPendingConnection()
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
        multi_mode = is_multi_window_mode(settings)
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

        if not file_to_open:
            return

        if multi_mode:
            self._open_additional_window(file_to_open)
        else:
            self._restore_from_tray(auto_execute=False)
            first_window = ParquetSQLApp.open_windows[0]
            opened_file = first_window.data_container.get_file_path()
            ask_reload = True
            if opened_file and opened_file.resolve() != Path(file_to_open).resolve():
                ask_reload = False
                self.data_container.open_file_path(file_to_open, add_to_recents=True)
            ParquetSQLApp.focus_window(first_window, ask_reload=ask_reload)

    def _open_additional_window(self, file_to_open: str | None):
        if file_to_open:
            file_opened_window = ParquetSQLApp.find_window_by_file(file_to_open)
            if file_opened_window:
                ParquetSQLApp.focus_window(file_opened_window, ask_reload=True)
                return

            if len(ParquetSQLApp.open_windows) > 0:
                first_window = ParquetSQLApp.open_windows[0]
                if not first_window.data_container.is_file_open():
                    first_window.data_container.open_file_path(
                        file_to_open, add_to_recents=True
                    )
                    ParquetSQLApp.focus_window(first_window)
                    return

        new_window = ParquetSQLApp.spawn_additional_window(file_to_open)
        ParquetSQLApp.focus_window(new_window)

    def _close_instance_server(self):
        if self._single_instance_server:
            self._single_instance_server.close()
            self._single_instance_server.deleteLater()
            self._single_instance_server = None

    def _release_instance_lock(self):
        if self._instance_lock:
            self._instance_lock.unlock()
            self._instance_lock = None

    def _create_menu_bar(self):
        from com_menu import MenuController

        self.menu_controller = MenuController(self, settings, recents)
        self.menu_controller.new_window_action.triggered.connect(
            self.open_new_window_instance
        )
        self.menu_controller.exit_action.triggered.connect(self.exit_application)

    def _init_tray_icon(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return

        logo_path = get_resource_path("static/logo.jpg")
        if not logo_path.exists():
            raise FileNotFoundError(f"Logo file not found: {logo_path}")
        tray_icon = QSystemTrayIcon(QIcon(str(logo_path)), self)
        tray_menu = QMenu(self)

        restore_action = QAction("Restore", self)
        restore_action.triggered.connect(self._restore_from_tray)
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

        self._tray_icon = tray_icon

    def _restore_from_tray(self, auto_execute: bool = True):
        # Ensure proper window state
        self.setWindowState(Qt.WindowNoState)

        # Show and raise the window
        self.show()
        self.showNormal()
        self.raise_()  # Bring to top of window stack
        self.activateWindow()  # Request focus

        if (
            auto_execute
            and self.data_container.is_file_open()
            and self.data_container.data is None
        ):
            self.result_controller.execute()

    def _is_last_open_window(self) -> bool:
        return len(ParquetSQLApp.open_windows) <= 1
