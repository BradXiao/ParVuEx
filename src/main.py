from pathlib import Path
import tempfile
import sys
from PyQt5.QtWidgets import QApplication
from PyQt5.QtNetwork import QLocalServer
from app import ParquetSQLApp

SINGLE_INSTANCE_SERVER_NAME = "ParVuExSingleInstance"
INSTANCE_LOCK_PATH = Path(tempfile.gettempdir()) / "parvuex-single-instance.lock"
INSTANCE_MESSAGE_KEY = "file"


def _acquire_instance_lock():
    from PyQt5.QtCore import QLockFile

    lock = QLockFile(str(INSTANCE_LOCK_PATH))
    if lock.tryLock(0):
        return lock
    if lock.removeStaleLockFile() and lock.tryLock(0):
        return lock
    return None


if __name__ == "__main__":
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    instance_lock = _acquire_instance_lock()
    if instance_lock is None:
        print(
            "Another ParVuEx instance appears to be running but could not be contacted."
        )
        sys.exit(0)

    app = QApplication(sys.argv)
    instance_server: QLocalServer | None = None
    QLocalServer.removeServer(SINGLE_INSTANCE_SERVER_NAME)
    server = QLocalServer()
    if server.listen(SINGLE_INSTANCE_SERVER_NAME):
        instance_server = server
    else:
        print(f"Warning: Unable to enforce single instance ({server.errorString()}).")
        server.close()

    ex = ParquetSQLApp(file_path, enable_tray=True if not file_path else False)
    ex.attach_instance_lock(instance_lock)
    if instance_server and instance_server.isListening():
        ex.attach_instance_server(instance_server)
    ex.show()
    sys.exit(app.exec_())
