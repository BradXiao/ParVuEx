from pathlib import Path
from typing import Callable
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
)

from gui_tools import normalize_instance_mode_value
from schemas import Settings
from utils import is_valid_font
from main import ParquetSQLApp


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
        self._settings = settings
        self.default_settings_file = default_settings_file
        self._init_ui()

    def _validate_settings(self):
        for field, line_edit in self.fields.items():
            if field in self.read_only_fields:
                continue

            if field == "default_data_var_name":
                if line_edit.text().upper() in self._settings.sql_keywords:
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

    def _init_ui(self):
        layout = QFormLayout()

        self.fields: dict[str, QLineEdit] = {}
        for field, value in self._settings.model_dump().items():
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
        save_button.clicked.connect(self._save_settings)
        button_layout.addWidget(save_button)

        reset_button = QPushButton("Reset to Default")
        reset_button.clicked.connect(self._reset_settings)
        button_layout.addWidget(reset_button)

        layout.addRow(button_layout)

        self.setLayout(layout)
        self.setWindowTitle("Edit Settings")
        self.resize(400, 300)

    def _save_settings(self):
        if not self._validate_settings():
            QMessageBox.critical(self, "Error", "Please fix the errors before saving.")
            return
        for field, line_edit in self.fields.items():
            if line_edit.text():
                if field == "sql_keywords":
                    # replace stringed list into list[str]
                    kws = line_edit.text()
                    kws = [i.strip().replace("'", "") for i in kws[1:-1].split(",")]
                    setattr(self._settings, field, kws)
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
                    setattr(self._settings, field, normalized_mode)
                else:
                    if field.endswith("_font"):
                        if is_valid_font(line_edit.text()) == False:
                            QMessageBox.critical(
                                self,
                                "Error",
                                f"Can't find font family: {line_edit.text()}",
                            )
                    setattr(
                        self._settings,
                        field,
                        line_edit.text().replace("\\n", "\n"),
                    )

        self._settings.save_settings()
        self.accept()

    def _reset_settings(self):
        default_settings_file = (
            Path(__file__).parent / "settings" / "default_settings.json"
        )
        with open(default_settings_file.as_posix(), "r") as f:
            default_settings_data = f.read()
        with self._settings.usr_settings_file.open("w") as f:
            f.write(default_settings_data)
        self._settings = Settings.load_settings()
        QMessageBox.information(
            self,
            "Settings Reset",
            "Settings have been reset to default values. Please restart the application for changes to take effect.",
        )
        self.accept()


class SettingsController:
    def __init__(
        self, parent: ParquetSQLApp, settings: Settings, update_fn: Callable[[], None]
    ):
        self._settings = settings
        self._parent = parent
        self._update_fn = update_fn

    def edit_settings(self):
        settings_file = self._settings.usr_settings_file
        default_settings_file = self._settings.default_settings_file
        if not Path(settings_file).exists():
            QMessageBox.critical(
                self._parent,
                "Error",
                f"Settings file '{settings_file}' does not exist.",
            )
            return

        dialog = SettingsDialog(self._settings, default_settings_file)
        if dialog.exec_() == QDialog.Accepted:
            self._handle_settings_changed()

    def _handle_settings_changed(self):
        refreshed_settings = Settings.load_settings()
        for field_name in Settings.model_fields:
            setattr(self._settings, field_name, getattr(refreshed_settings, field_name))
        ParquetSQLApp.refresh_all_instance_actions()
        self._update_fn()
