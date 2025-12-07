"""module contains general purpose tools"""

import ctypes
import os
from pathlib import Path
import sys
from PyQt5.QtGui import QFontDatabase
import chardet
from loguru import logger


def is_valid_font(family: str) -> bool:
    """check if font is valid"""
    db = QFontDatabase()
    if family and family in db.families():
        return True
    return False


def detect_encoding(file_path: Path) -> str:

    try:
        with open(file_path, "rb") as buffer:
            sample = buffer.read(8192)
        result = chardet.detect(sample) or {}
        return result.get("encoding") or "cp1252"
    except Exception as exc:
        logger.warning(f"Encoding detection failed, defaulting to cp1252: {exc}")
        return "cp1252"


def convert_to_utf8(file_path: Path) -> tuple[Path, str]:
    encoding = detect_encoding(file_path)
    with open(file_path, "r", encoding=encoding, errors="replace") as src:
        tmp_file = file_path.with_suffix(".tmp.csv")
        if tmp_file.exists():
            os.remove(tmp_file)
        with open(tmp_file, "w", encoding="UTF8") as tmp:
            for chunk in iter(lambda: src.read(8192), ""):
                tmp.write(chunk)
            return tmp_file, encoding


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


def force_foreground_window(hwnd: int):
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
