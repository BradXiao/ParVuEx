from pathlib import Path
from dataclasses import dataclass
from typing import Union
import shutil

from pydantic import BaseModel
from loguru import logger


@dataclass
class BadQueryException:
    """if result is query means that query is fixed and just give warning"""

    name: str
    message: str
    result: str | None = None


class Settings(BaseModel):
    """
    Params:
        default_data_var_name: str - name for read dataframe in query
        default_limit: Union[int, str] - default limit for sql query
        default_sql_font_size: Union[int, str]
        default_result_font_size: Union[int, str]
        default_sql_query: str
        default_sql_font: str
        default_result_font: str
        sql_keywords: list[str]
        result_pagination_rows_per_page: str
        save_file_history: str
        max_rows: str - max rows for limit in sql query
        instance_mode: str - "single" (default) or "multi_window"
    """

    # data
    default_data_var_name: str
    default_limit: Union[int, str]
    default_sql_font_size: Union[int, str]
    default_result_font_size: Union[int, str]
    default_result_font: str
    default_sql_query: str
    default_sql_font: str
    default_ui_font_size: Union[int, str]
    sql_keywords: list[str]
    result_pagination_rows_per_page: str
    save_file_history: str
    max_rows: str
    instance_mode: str = "single"
    # colors
    colour_browseButton: str
    colour_sqlEdit: str
    colour_executeButton: str
    colour_resultTable: str
    colour_tableInfoButton: str
    # dirs
    user_app_settings_dir: Path = Path.home() / ".ParVuEx"
    recents_file: Path = Path(__file__).parent / "history" / "recents.json"
    settings_file: Path = Path(__file__).parent / "settings" / "settings.json"
    usr_recents_file: Path = user_app_settings_dir / "history" / "recents.json"
    usr_history_file: Path = user_app_settings_dir / "history" / "history.json"
    usr_settings_file: Path = user_app_settings_dir / "settings" / "settings.json"
    default_settings_file: Path = (
        Path(__file__).parent / "settings" / "default_settings.json"
    )
    static_dir: Path = Path(__file__).parent / "static"
    user_logs_dir: Path = user_app_settings_dir / "logs"

    def process(self):
        self.sql_keywords = list(set([i.upper().strip() for i in self.sql_keywords]))
        self.recents_file = Path(self.recents_file).resolve()
        self.settings_file = Path(self.settings_file).resolve()
        self.default_settings_file = Path(self.default_settings_file).resolve()
        self.static_dir = Path(self.static_dir).resolve()

    def render_vars(self, query: str) -> str:
        """render inside the query the vars of the settings"""

        query = query.replace(
            "$(default_data_var_name)", str(self.default_data_var_name)
        )
        query = query.replace("$(default_limit)", str(self.default_limit))
        query = query.replace(
            "$(default_sql_font_size)", str(self.default_sql_font_size)
        )
        query = query.replace(
            "$(default_result_font_size)", str(self.default_result_font_size)
        )
        query = query.replace("$(default_ui_font_size)", str(self.default_ui_font_size))
        query = query.replace("$(default_sql_query)", str(self.default_sql_query))
        query = query.replace("$(default_sql_font)", str(self.default_sql_font))
        query = query.replace("$(default_result_font)", str(self.default_result_font))
        return query

    @classmethod
    def reset_user_settings(cls):
        """ """
        user_app_settings_dir: Path = Path.home() / ".ParVuEx"
        shutil.copytree(
            Path(__file__).parent / "settings",
            user_app_settings_dir / "settings",
            dirs_exist_ok=True,
        )

        shutil.copytree(
            Path(__file__).parent / "history",
            user_app_settings_dir / "history",
            dirs_exist_ok=True,
        )

        # fill with default settings
        with (Path(__file__).parent / "settings" / "default_settings.json").open(
            "r", encoding="utf-8"
        ) as f:
            with open(
                Path(__file__).parent / "history" / "recents.json", encoding="utf-8"
            ) as r:
                (user_app_settings_dir / "settings" / "settings.json").write_text(
                    f.read()
                )
                (user_app_settings_dir / "history" / "recents.json").write_text(
                    r.read()
                )

    @classmethod
    def get_user_settings(cls):
        user_app_settings_dir: Path = Path.home() / ".ParVuEx"
        settings = user_app_settings_dir / "settings" / "settings.json"
        with settings.open("r", encoding="utf-8") as f:
            settings_data = f.read()

            return cls.model_validate_json(settings_data)

    @classmethod
    def load_settings(cls):
        user_app_settings_dir: Path = Path.home() / ".ParVuEx"
        # app settings dir doesn't exist - maybe first start
        if not user_app_settings_dir.exists():
            cls.reset_user_settings()
        try:
            # read from user dir
            model = cls.get_user_settings()
            model.process()

        except Exception as e:
            # reset and load
            logger.error(e)
            logger.critical(f"Resetting user settings")
            cls.reset_user_settings()
            model = cls.get_user_settings()
            model.process()

        return model

    def save_settings(self):
        # Save current settings to JSON file
        settings_json = self.model_dump_json()
        # settings_file = self.usr_settings_file.as_posix()
        with open(self.usr_settings_file, "w", encoding="utf-8") as f:
            f.writelines(settings_json.splitlines())


settings = Settings.load_settings()


class Recents(BaseModel):
    """Recent opened files history"""

    recents: list[str]

    @classmethod
    def load_recents(cls):
        if not settings.usr_recents_file.exists():
            return cls(recents=[])
        # Load recents from JSON file
        with open(settings.usr_recents_file, "r", encoding="utf-8") as f:
            recents_data = f.read()

        model = cls.model_validate_json(recents_data)
        return model

    def add_recent(self, path: str):
        # add browsed file to recents
        try:
            idx = self.recents.index(path)
            self.recents.insert(0, self.recents.pop(idx))
        except:
            self.recents.insert(0, path)
            self.recents = list(set(self.recents))
        self.save_recents()

    def save_recents(self):
        # Save current recents to JSON file
        recents_json = self.model_dump_json()
        with open(settings.usr_recents_file, "w", encoding="utf-8") as f:
            f.writelines(recents_json.splitlines())


recents = Recents.load_recents()


class History(BaseModel):
    queries: dict[str, list[str]]
    col_width: dict[str, dict[str, int]]

    @classmethod
    def load_history(cls):
        # Load recents from JSON file
        if not settings.usr_history_file.exists():
            return cls(queries={}, col_width={})
        with open(settings.usr_history_file, "r", encoding="utf-8") as f:
            history_data = f.read()

        model = cls.model_validate_json(history_data)
        return model

    def add_col_width(self, file_path: str, column_widths: dict[str, int] | None):
        """Persist the latest column widths for a file."""
        changed = False
        if column_widths:
            if file_path not in self.col_width:
                self.col_width[file_path] = column_widths
                changed = True
            else:
                if self.col_width.get(file_path) != column_widths:
                    self.col_width[file_path] = column_widths
                    changed = True
        else:
            if file_path in self.col_width:
                del self.col_width[file_path]
                changed = True

        if changed:
            self.save_history()

    def get_col_widths(self, file_path: str) -> dict[str, int]:
        """Return a shallow copy of stored column widths for a file."""
        stored = self.col_width.get(file_path, {})
        return dict(stored)

    def add_query(self, file_path: str, query: str):
        # add query to history
        try:
            idx = self.queries[file_path].index(query)
            self.queries[file_path].insert(0, self.queries[file_path].pop(idx))
        except:
            if file_path not in self.queries:
                self.queries[file_path] = []
            self.queries[file_path].insert(0, query)
        if len(self.queries[file_path]) > 50:
            self.queries[file_path] = self.queries[file_path][:50]
        elif len(self.queries[file_path]) == 0:
            del self.queries[file_path]
        self.save_history()

    def save_history(self):
        # Save current recents to JSON file
        history_json = self.model_dump_json()
        with open(settings.usr_history_file, "w", encoding="utf-8") as f:
            f.writelines(history_json.splitlines())


history = History.load_history()
