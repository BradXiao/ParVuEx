from pathlib import Path
from typing import Optional, Union, List, Callable
import chardet
import os

import pandas as pd
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.types as patypes
from loguru import logger

from utils import read_table, nth_from_generator, copy_count_gen_items
from schemas import settings


logger.add(settings.user_logs_dir / "file_{time}.log")


def _pyarrow_types_mapper(pa_type: pa.DataType):
    """
    Ensure nullable-compatible pandas dtypes (Int64, Float64, boolean, string[pyarrow]).
    This preserves integer columns with nulls so they don't get coerced to floats.
    """

    if patypes.is_integer(pa_type):
        return pd.Int64Dtype()
    if patypes.is_boolean(pa_type):
        return pd.BooleanDtype()
    if patypes.is_floating(pa_type):
        return pd.Float64Dtype()
    if patypes.is_string(pa_type) or patypes.is_large_string(pa_type):
        string_dtype = getattr(pd, "StringDtype", None)
        if callable(string_dtype):
            try:
                return string_dtype(storage="pyarrow")
            except TypeError:
                return string_dtype()
    if hasattr(pd, "ArrowDtype") and any(
        checker(pa_type)
        for checker in (
            patypes.is_timestamp,
            patypes.is_date32,
            patypes.is_date64,
            patypes.is_time32,
            patypes.is_time64,
        )
    ):
        # Keeps timestamp/date/time precision using pyarrow-backed dtype.
        return pd.ArrowDtype(pa_type)
    return None


class Reader:
    """
    Read data from a file.
    """

    def __init__(self, path: Path, virtual_table_name: str, batchsize: int):
        """
        Parameters:
            - path: Path to read data from.
            - virtual_table_name: Name of virtual table, will be used in sql queries
            - batchsize: rows per page
        """
        self.path = path
        self.virtual_table_name = virtual_table_name
        self.batchsize = batchsize
        self._tmp_csv_path: Optional[Path] = None

        logger.info(
            f"Initializing Reader with path: {path} and virtual_table_name: {virtual_table_name}"
        )

        self.validate()
        # origin file
        self.duckdf = self.__read_into_duckdf()  # .sort("__index_level_0__")
        # for querying
        self.duckdf_query = self.duckdf
        self.batches: List[pa.RecordBatch] = self._relation_to_batches()
        self.total_rows: int = 0
        self.columns_query = list(self.duckdf_query.columns)
        self.columns = list(self.duckdf.columns)
        self._update_row_metadata()

        logger.debug(f"Reader initialized with columns: {self.columns}")

    def _relation_to_batches(self) -> List[pa.RecordBatch]:
        """Convert current DuckDB relation into uniform-sized Arrow batches."""
        table = self.duckdf_query.to_arrow_table()
        if hasattr(table, "combine_chunks"):
            table = table.combine_chunks()
        return table.to_batches(self.batchsize)

    def update_batches(self):
        """updates self.batches using self.duckdf_query"""
        logger.debug("Updating batches")
        self.batches = self._relation_to_batches()
        self._update_column_metadata()
        self._update_row_metadata()

    def _update_row_metadata(self):
        self.total_rows = sum(batch.num_rows for batch in self.batches)

    def _update_column_metadata(self):
        try:
            self.columns_query = list(self.duckdf_query.columns)
        except Exception:
            self.columns_query = list(self.columns)

    def __read_into_duckdf(self) -> duckdb.DuckDBPyRelation:
        path_str = str(self.path)
        logger.debug(f"Reading data from {path_str}")
        if self.path.suffix.lower() == ".parquet":
            return duckdb.read_parquet(path_str)
        elif self.path.suffix.lower() == ".csv":
            # clear any previous temp file from earlier reads
            self._cleanup_tmp_csv()
            try:
                return duckdb.read_csv(path_str, encoding="UTF8")
            except Exception as exc:
                logger.warning(f"duckdb.read_csv UTF-8 failed: {exc}")

            # guess encoding type
            encoding = self._detect_encoding()
            try:
                # convert text from that encoding to utf-8 to a tmp file
                with open(path_str, "r", encoding=encoding, errors="replace") as src:
                    tmp_file = self.path.with_suffix(".tmp.csv")
                    if tmp_file.exists():
                        os.remove(tmp_file)
                    with open(tmp_file, "w", encoding="UTF8") as tmp:
                        for chunk in iter(lambda: src.read(8192), ""):
                            tmp.write(chunk)
                        self._tmp_csv_path = tmp_file
                logger.info(
                    f"Retrying CSV read after converting from {encoding} to UTF-8"
                )
                # read tmp file with duckdb
                return duckdb.read_csv(str(self._tmp_csv_path), encoding="UTF8")
            except Exception as inner_exc:
                logger.error(
                    f"Failed to read CSV after encoding conversion ({encoding} -> UTF-8): {inner_exc}"
                )

            raise ValueError("Failed to read CSV file with any supported encoding")
        elif self.path.suffix.lower() == ".json":
            return duckdb.read_json(path_str)
        else:
            raise ValueError(f"File extension {self.path.suffix} is not supported")

    def __del__(self):
        self._cleanup_tmp_csv()

    def _detect_encoding(self) -> str:
        """
        Best-effort CSV encoding detection.
        Uses chardet when available, otherwise falls back to cp1252 (common on Windows).
        """

        try:
            with open(self.path, "rb") as buffer:
                sample = buffer.read(8192)
            result = chardet.detect(sample) or {}
            return result.get("encoding") or "cp1252"
        except Exception as exc:
            logger.warning(f"Encoding detection failed, defaulting to cp1252: {exc}")
            return "cp1252"

    def _cleanup_tmp_csv(self):
        """Remove any temporary CSV created during encoding conversion."""
        if self._tmp_csv_path and self._tmp_csv_path.exists():
            try:
                self._tmp_csv_path.unlink()
            except Exception as exc:
                logger.warning(
                    f"Failed to remove temp CSV '{self._tmp_csv_path}': {exc}"
                )
        self._tmp_csv_path = None

    def validate(self):
        assert (
            self.path.exists()
            and self.path.is_file()
            and self.path.suffix.lower() in [".parquet", ".csv", ".json"]
        ), "Path must be a valid Parquet, CSV or JSON file"
        logger.info(f"Validated path: {self.path}")

    def get_generator(self, chunksize: int) -> List[pa.RecordBatch]:
        """returns a list of pyarrow batches"""
        logger.debug(f"Getting generator with chunksize: {chunksize}")
        return self.duckdf_query.to_arrow_table().to_batches(chunksize)

    def get_total_rows(self) -> int:
        return self.total_rows

    def get_nth_batch(self, n: int, as_df: bool = True):
        logger.debug(
            f"Getting {n}th batch with chunksize: {self.batchsize} as_df: {as_df}"
        )
        pa_batch = nth_from_generator(self.batches, n - 1)
        if pa_batch is None:
            return pd.DataFrame(columns=self.columns_query) if as_df else None
        if not as_df:
            return pa_batch
        return pa_batch.to_pandas(types_mapper=_pyarrow_types_mapper)

    def search(
        self, search_query: str, column: str, as_df: bool = False, case: bool = False
    ) -> Union[duckdb.DuckDBPyRelation, pd.DataFrame]:
        """
        search query string inside column
            Parameters:
                - query: query string
                - column: column name
                - as_df: return as pandas dataframe
                - case: case sensitive
        """
        logger.info(
            f"Searching for '{search_query}' in column '{column}' with case sensitivity: {case}"
        )
        like = "LIKE" if case else "ILIKE"
        sql_query = f"""
                    SELECT * 
                    FROM {self.virtual_table_name}
                    WHERE CAST({column} AS VARCHAR) {like} '%{search_query}%'
                    """
        duck_res = self.duckdf.query(
            virtual_table_name=self.virtual_table_name, sql_query=sql_query
        )

        return duck_res.to_df() if as_df else duck_res

    def query(
        self, query: str, as_df: bool = False
    ) -> Union[duckdb.DuckDBPyRelation, pd.DataFrame]:
        """run provided sql query with class lvl setted virtual_table_name name"""
        logger.info(
            f"Executing query: '{query}' on virtual_table_name: {self.virtual_table_name}"
        )
        duck_res = self.duckdf.query(
            virtual_table_name=self.virtual_table_name, sql_query=query
        )
        # update duckdf_query and batches
        logger.debug(f"Updating duckdf_query with query result")
        self.duckdf_query = duck_res
        self._update_column_metadata()
        self.update_batches()
        return duck_res.to_pandas() if as_df else duck_res

    def agg_get_uniques(self, column_name: str) -> List[str]:
        """get unique values for given column"""
        logger.debug(f"Getting unique values for column: {column_name}")
        return self.duckdf_query.unique(column_name).to_df()[column_name].to_list()

    def __str__(self):
        return f"<ParVuDataReader:{self.path.as_posix()}[{self.columns}]>"

    def __repr__(self):
        return self.__str__()


class Data:
    def __init__(self, path: Path, virtual_table_name: str, batchsize: int):
        """
        Parameters:
            - `path`: Path to read data from.
            - `virtual_table_name`: Name of virtual table, will be used in sql queries
            - `query`: query string
            - `batchsize`: rows per page
        """
        self.path = Path(path)
        self.virtual_table_name = virtual_table_name
        self.reader = Reader(
            path=self.path,
            virtual_table_name=self.virtual_table_name,
            batchsize=batchsize,
        )

        self.ftype = "pq" if self.path.suffix == ".parquet" else "txt"
        # for big data bunch counting can be slow, so do if manually called by `calc_n_batches`
        self.total_batches = "???"
        self.columns = self.reader.columns.copy()

        logger.info(
            f"""Data initialized with path: {path}, 
                    virtual_table_name: {virtual_table_name}, 
                    file type: {self.ftype} and
                    batchsize: {batchsize}"""
        )

    def get_nth_batch(
        self, n: int, as_df: bool = True
    ) -> Union[pd.DataFrame, pa.RecordBatch]:
        logger.debug(
            f"Getting {n}th batch with chunksize: {self.reader.batchsize} as_df: {as_df}"
        )
        batch = self.reader.get_nth_batch(n, as_df)
        logger.debug(f"Items in batch: {len(batch)}")
        return batch

    def get_generator(self, chunksize: int) -> List[pa.RecordBatch]:
        logger.debug(f"Getting generator with chunksize: {chunksize}")
        return self.reader.get_generator(chunksize)

    def get_uniques(self, column_name: str) -> List[str]:
        """get unique values for given column"""
        logger.debug(f"Getting unique values for column: {column_name}")
        return self.reader.agg_get_uniques(column_name)

    def execute_query(
        self, query: str, as_df: bool = False
    ) -> Union[List[pa.RecordBatch], pd.DataFrame]:
        """executes provided query and update duckdf_query"""
        max_chunksize = self.reader.batchsize
        logger.info(f"Executing query: '{query}' with max_chunksize: {max_chunksize}")
        if not as_df:
            relation = self.reader.query(query, as_df)
            table = relation.to_arrow_table()
            if hasattr(table, "combine_chunks"):
                table = table.combine_chunks()
            batches = table.to_batches(max_chunksize=max_chunksize)
            # update total_pages:
            self.total_batches = len(batches)
            return batches
        else:
            res = self.reader.query(query, as_df)
            self.total_batches = self.calc_n_batches()
            return res

    def search(
        self, query: str, column: str, as_df: bool = True, case: bool = False
    ) -> Union[duckdb.DuckDBPyRelation, pd.DataFrame]:
        logger.info(
            f"Searching for '{query}' in column '{column}' with case sensitivity: {case}"
        )
        return self.reader.search(query, column, as_df, case)

    def calc_n_batches(self) -> int:
        """calculates the number of batches in data. Use carefully, because it can take time for big data"""
        chunksize = self.reader.batchsize
        logger.debug(f"Calculating number of batches with chunksize: {chunksize}")
        # gen, n = copy_count_gen_items(self.reader.batches)
        logger.debug(f"Number of batches: {len(self.reader.batches)}")
        return len(self.reader.batches)

    def calc_total_rows(self) -> int:
        return self.reader.get_total_rows()

    def reset_duckdb(self):
        """reset query result table to file table"""
        logger.debug("Resetting duckdf_query to original duckdf")
        self.reader.duckdf_query = self.reader.duckdf
        self.reader.update_batches()

    def __str__(self):
        return f"<ParVuDataInstance:{self.path.as_posix()}[{self.reader.columns}]>"

    def __repr__(self):
        return self.__str__()
