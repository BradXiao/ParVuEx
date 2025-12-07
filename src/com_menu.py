from typing import TYPE_CHECKING
from pathlib import Path
from PyQt5.QtWidgets import QAction, QFileDialog, QMessageBox
from PyQt5.QtGui import QFont
from gui_tools import is_multi_window_mode
from com_settings import SettingsController
from main import ParquetSQLApp

if TYPE_CHECKING:
    from schemas import Recents, Settings


class MenuController:
    def __init__(
        self,
        parent: ParquetSQLApp,
        settings: Settings,
        recents: Recents,
    ):
        self._parent = parent
        self._recents = recents
        self._settings = settings
        self._recent_actions: list[QAction] = []
        self._new_window_action: QAction | None = None
        self._new_window_separator: QAction | None = None
        # app menu
        self._settings_controller = SettingsController(
            parent, settings, self.update_settings
        )
        menubar = parent.menuBar()
        sys_menu = menubar.addMenu("App")
        self.new_window_action = QAction("New Window", parent)
        sys_menu.addAction(self.new_window_action)
        self.new_window_separator = sys_menu.addSeparator()
        self.settings_action = QAction("Settings", parent)
        sys_menu.addAction(self.settings_action)
        self.settings_action.triggered.connect(self._settings_controller.edit_settings)
        # quit app
        sys_menu.addSeparator()
        self.exit_action = QAction("Quit App", parent)
        sys_menu.addAction(self.exit_action)

        # file menu
        self.file_menu = menubar.addMenu("File")
        self.browse_action = QAction("Open...", parent)
        self.file_menu.addAction(self.browse_action)
        self.browse_action.triggered.connect(self._browse_file)
        self.reload_action = QAction("Reload", parent)
        self.file_menu.addAction(self.reload_action)
        self.reload_action.triggered.connect(self._parent.data_container.reload_file)
        self.close_file_action = QAction("Close", parent)
        self.file_menu.addAction(self.close_file_action)
        self.close_file_action.triggered.connect(self._close_file)
        self.file_menu.addSeparator()
        self.export_action = QAction("Export...", parent)
        self.file_menu.addAction(self.export_action)
        self.export_action.triggered.connect(self._export_results)

        # view file
        action_menu = menubar.addMenu("Actions")
        self.view_action = QAction("Show all data", parent)
        action_menu.addAction(self.view_action)
        self.view_action.triggered.connect(self._view_file)
        action_menu.addSeparator()
        self.reset_table_size_action = QAction("Reset row/col height/width", parent)
        action_menu.addAction(self.reset_table_size_action)
        self.reset_table_size_action.triggered.connect(
            self._parent.result_controller.reset_table_size
        )

        self.update_recents_menu()
        # help
        help_menu = menubar.addMenu("Help")
        self.help_action = QAction("Help/Info", parent)
        help_menu.addAction(self.help_action)
        self.help_action.triggered.connect(self._show_help_dialog)

        self.update_action_states()

    def update_recents_menu(self):
        """Refresh the File menu to show the latest recents list."""
        if not hasattr(self, "file_menu"):
            return

        for action in self._recent_actions:
            self.file_menu.removeAction(action)
            action.deleteLater()
        self._recent_actions = []
        if not self._recents.recents:
            return

        separator = self.file_menu.addSeparator()
        self._recent_actions.append(separator)

        for recent in self._recents.recents:
            filename = Path(recent).name
            name = f"{filename} @ {Path(recent).parent}"
            recent_action = QAction(name, self._parent)

            def make_handler(path: str):
                def handler(checked: bool) -> None:
                    self._open_recent_file(checked, path)

                return handler

            recent_action.triggered.connect(make_handler(recent))
            self.file_menu.addAction(recent_action)
            self._recent_actions.append(recent_action)

        clear_action = QAction("Clear List", self._parent)
        clear_action.setFont(QFont("Courier", 9, weight=QFont.Bold))
        clear_action.triggered.connect(self._clear_recents)
        self.file_menu.addAction(clear_action)
        self._recent_actions.append(clear_action)

    def update_action_states(self):
        has_file = self._parent.data_container.is_file_open()
        self.view_action.setEnabled(has_file)
        self.close_file_action.setEnabled(has_file)
        self.export_action.setEnabled(has_file)
        self.reset_table_size_action.setEnabled(has_file)
        self.reload_action.setEnabled(has_file)
        self.update_instance_actions()
        if not has_file:
            self._last_column_widths = None

    def update_instance_actions(self):
        multi_mode = is_multi_window_mode(self._settings)
        if self._new_window_action:
            self._new_window_action.setVisible(multi_mode)
            self._new_window_action.setEnabled(multi_mode)
        if self._new_window_separator:
            self._new_window_separator.setVisible(multi_mode)

    def add_recent(self, path: Path):
        if self._settings.save_file_history not in (
            "True",
            "true",
            "1",
            True,
            1,
        ):
            return
        self._recents.add_recent(str(path))
        ParquetSQLApp.refresh_all_recents_menus()

    def update_settings(self):
        self._parent.menu_controller.update_action_states()
        self._parent.sql_edit_controller.apply_styles()
        self._parent.result_controller.apply_styles()

        if not self._parent.data_container.data:
            return

        self._parent.data_container.load_page(page=1)

    def _view_file(self):
        path = self._parent.data_container.get_file_path()
        if path is None:
            self._parent.result_controller.result_label.setText("Browse file first...")
            return

        if not path.exists():
            self._parent.result_controller.result_label.setText(
                f"File not found: {path}"
            )
            return

        self._parent.result_controller.execute()

    def _browse_file(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getOpenFileName(
            self._parent,
            "Open File",
            "",
            "Data Files (*.parquet *.csv);;All Files (*)",
            options=options,
        )
        if not fileName:
            return

        existing_window = ParquetSQLApp.find_window_by_file(fileName)
        if existing_window:
            ParquetSQLApp.focus_window(existing_window, ask_reload=True)
            return

        self._close_file()
        self._parent.data_container.open_file_path(fileName, add_to_recents=True)

    def _close_file(self):
        if not self._parent.data_container.is_file_open():
            return

        self._parent.data_container.close_file()
        self._parent.update_window_title()
        self._parent.menu_controller.update_action_states()
        self._parent.sql_edit_controller.reset_history_navigation()
        self._parent.result_controller.release_resources()

    def _show_help_dialog(self):
        with open(self._settings.static_dir / "help.md", "r", encoding="utf-8") as f:
            help_text = f.read()

        return self._parent.dialog_controller.show_dialog("Help/Info", help_text)

    def _export_results(self):
        if not self._parent.data_container.data:
            self._parent.result_controller.result_label.setText("No data to export")
            return
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getSaveFileName(
            self._parent,
            "Export Results",
            "",
            "CSV Files (*.csv);;Parquet Files (*.parquet);;All Files (*)",
            options=options,
        )
        if file_path:
            if file_path.endswith(".csv"):
                self._parent.data_container.data.reader.duckdf_query.to_csv(file_path)
            # elif filePath.endswith('.xlsx'):
            # # todo: add support for xlsx(https://duckdb.org/docs/guides/file_formats/excel_export.html)
            #     self.DATA.reader.duckdf_query.to(filePath, index=False)

            elif file_path.endswith(".parquet"):
                self._parent.data_container.data.reader.duckdf_query.to_parquet(
                    file_path
                )
            else:
                QMessageBox.warning(
                    self._parent,
                    "Invalid File Type",
                    "Please select a valid file type (CSV or XLSX).",
                )

    def _clear_recents(self):
        self._recents.recents = []
        self._recents.save_recents()
        ParquetSQLApp.refresh_all_recents_menus()

    def _open_recent_file(self, checked: bool, file_path: str):
        if not Path(file_path).exists():
            reply = QMessageBox.question(
                self._parent,
                "File Not Found",
                f"The file {file_path} does not exist. Do you want to remove it from recents?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self._recents.recents.remove(file_path)
                self._recents.save_recents()
                ParquetSQLApp.refresh_all_recents_menus()
            return
        print(file_path)
        existing_window = ParquetSQLApp.find_window_by_file(file_path)
        if existing_window:
            ParquetSQLApp.focus_window(existing_window, ask_reload=True)
            return False
        self._parent.data_container.open_file_path(file_path)
