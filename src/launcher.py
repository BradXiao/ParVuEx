from pathlib import Path
import tempfile
import json
import time
import subprocess
import sys

SINGLE_INSTANCE_SERVER_NAME = "ParVuExSingleInstance"
INSTANCE_LOCK_PATH = Path(tempfile.gettempdir()) / "parvuex-single-instance.lock"
INSTANCE_MESSAGE_KEY = "file"

_parvuex_exe_cache = None


def _notify_running_instance(file_path: str | None) -> bool:
    from PyQt5.QtNetwork import QLocalSocket

    message = json.dumps({INSTANCE_MESSAGE_KEY: file_path})
    attempts = 3
    for attempt in range(attempts):
        socket = QLocalSocket()
        socket.connectToServer(SINGLE_INSTANCE_SERVER_NAME)
        if socket.waitForConnected(300):
            payload = message.encode("utf-8")
            socket.write(payload)
            socket.flush()
            socket.waitForBytesWritten(300)
            socket.disconnectFromServer()
            return True
        socket.abort()
        if attempt < attempts - 1:
            time.sleep(0.1)
    return False


def _acquire_instance_lock():
    from PyQt5.QtCore import QLockFile

    lock = QLockFile(str(INSTANCE_LOCK_PATH))
    if lock.tryLock(0):
        return lock
    if lock.removeStaleLockFile() and lock.tryLock(0):
        return lock
    return None


def _parvuex_executable() -> Path:
    """
    ParVuEx.exe lives next to Launcher.exe once PyInstaller has unpacked.
    When running from sources we fall back to the script directory.
    """
    global _parvuex_exe_cache
    if _parvuex_exe_cache is not None:
        return _parvuex_exe_cache

    if getattr(sys, "frozen", False):
        # Avoid resolve() if not needed - just use parent directly
        base_path = Path(sys.executable).parent
    else:
        base_path = Path(__file__).parent
    candidate = base_path / "ParVuEx.exe"
    if not candidate.exists():
        raise FileNotFoundError(f"ParVuEx.exe was not found at {candidate}")
    _parvuex_exe_cache = candidate
    return candidate


if __name__ == "__main__":

    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    instance_lock = _acquire_instance_lock()
    if instance_lock is None:
        if _notify_running_instance(file_path):
            sys.exit(0)
        print(
            "Another ParVuEx instance appears to be running but could not be contacted."
        )
        sys.exit(0)

    instance_lock.unlock()
    parvuex_exe = _parvuex_executable()
    result = subprocess.call([str(parvuex_exe), *sys.argv[1:]])
    sys.exit(result)
