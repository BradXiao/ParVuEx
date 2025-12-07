from typing import TYPE_CHECKING
from PyQt5.QtWidgets import QVBoxLayout, QWidget
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QDialog
from gui_tools import markdown_to_html_with_table_styles
from components import Popup, SearchableTextBrowser

if TYPE_CHECKING:
    from main import ParquetSQLApp
    from schemas import Settings


class DialogController:
    def __init__(self, parent: ParquetSQLApp, settings: Settings):
        self.settings = settings
        self._parent = parent
        self._dialog: QDialog | None = None

    def close_dialog(self):
        if self._dialog is not None:
            self._dialog.close()

    def auto_close_dialog(self, obj: QWidget):
        if self._dialog is not None and self._dialog.isVisible():
            if obj is not self._dialog and not self._dialog.isAncestorOf(obj):
                if obj is self._parent or self._parent.isAncestorOf(obj):
                    self.close_dialog()

    def show_table_dialog(self, title: str, table_info: str, font_offset: int = 0):
        if self._dialog is not None:
            self._dialog.close()

        dialog = Popup(self._parent, title)
        text_browser = SearchableTextBrowser(dialog)
        table_font = QFont(
            self.settings.default_result_font,
            int(self.settings.default_result_font_size) + font_offset,
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

    def show_dialog(self, title: str, text: str):
        if self._dialog is not None:
            self._dialog.close()

        dialog = Popup(self._parent, title)

        text_browser = SearchableTextBrowser(dialog)
        table_font = QFont(
            self.settings.default_result_font,
            int(self.settings.default_result_font_size),
        )
        text_browser.setFont(table_font)
        text_browser.setMarkdown(text)
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

    def _center_dialog_relative_to_window(
        self, dialog: QDialog, width_ratio: float = 0.8, height_ratio: float = 0.8
    ):
        """Resize dialog relative to the main window and center it."""
        parent_geom = self._parent.geometry()
        if not parent_geom.isValid():
            parent_geom = self._parent.frameGeometry()

        parent_width = max(1, parent_geom.width())
        parent_height = max(1, parent_geom.height())

        dialog_width = max(1, int(parent_width * width_ratio))
        dialog_height = max(1, int(parent_height * height_ratio))
        dialog.resize(dialog_width, dialog_height)

        target_x = parent_geom.x() + (parent_width - dialog_width) // 2
        target_y = parent_geom.y() + (parent_height - dialog_height) // 2
        dialog.move(target_x, target_y)
