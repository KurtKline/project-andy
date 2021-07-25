import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd
import pytz
from loguru import logger

from config import db_config
from db import PostgresDB


class FileProcessor:
    def __init__(self):
        self.raw_file_path_template = Path(__file__).parent / 'test_files'

    @staticmethod
    def add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, "timestamp"] = datetime.now(tz=pytz.timezone('America/Los_Angeles'))
        return df

    @staticmethod
    def _has_header(file_path: Path) -> bool:
        with open(file_path, 'r') as csv_file:
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(csv_file.read(2048))
            csv_file.seek(0)

        return has_header

    def _get_num_rows_in_file(self, file_path: Path) -> int:
        """

        Args:
            file_path:

        Returns:

        """
        num_rows = sum(1 for _ in open(file_path))
        adj_num_rows = num_rows - 1 if self._has_header(file_path) else num_rows
        return adj_num_rows

    @staticmethod
    def import_file(file_path: Path, date_columns: List[str], dtype_dict: Dict) -> pd.DataFrame:
        """
        Imports a file and loads it into a dataframe

        Args:
            dtype_dict
            date_columns
            file_path

        Returns:
            A pd.DataFrame
        """
        try:
            df = pd.read_csv(
                file_path,
                parse_dates=date_columns,
                dtype=dtype_dict
            )

        except FileNotFoundError as e:
            logger.info(f"Error: {e}")
            raise FileNotFoundError(e)

        return df

    @staticmethod
    def insert_into_db(file_path: Path, table_name: str) -> None:
        """
        Inserts a csv file into a PostgreSQL table

        Args:
            file_path
            table_name
        """

        with PostgresDB(
                database=db_config.database,
                user=db_config.user,
                password=db_config.password,
                host=db_config.host,
                port=db_config.port
        ) as db_conn:
            db_conn.insert_csv(
                file_path=file_path,
                table_name=table_name
            )

    @staticmethod
    def remove_file(file_path: Path) -> None:
        """
        Deletes a file

        Args:
            file_path:
        """
        logger.info(f"Removing file: {file_path}")
        file_path.unlink(missing_ok=True)
