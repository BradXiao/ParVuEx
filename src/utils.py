"""module contains general purpose tools"""

from PyQt5.QtGui import QFontDatabase


def is_valid_font(family: str) -> bool:
    """check if font is valid"""
    db = QFontDatabase()
    if family and family in db.families():
        return True
    return False
