import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from loguru import logger

from config import db_config
from db import PostgresDB
from file_processor import FileProcessor


class ProjectFileProcessor(FileProcessor):
    def __init__(self):
        super().__init__()
        self.db_table = 'projects'
        self.date_cols = ["start_date", "end_date"]
        self.dtypes = {
            "project_id": int,
            "project_name": str,
            "site_id": int,
            "client_id": int
        }

    def process_file(self, data_date: str) -> None:
        raw_file_path = self.raw_file_path_template / f"test_projects_{data_date}.csv"
        logger.info(f"Attempting to process file: {raw_file_path}")
        num_rows = self._get_num_rows_in_file(raw_file_path)
        logger.info(f"Rows in raw file: {num_rows}")
        df = self.import_file(
            raw_file_path,
            date_columns=self.date_cols,
            dtype_dict=self.dtypes
        )
        filtered_df = self.filter_out_old_projects(df, '2020-01-01').copy()
        filtered_df = self.add_timestamp_column(filtered_df)
        logger.info(f"Rows in filtered DataFrame: {len(filtered_df)}")
        csv_file_path = raw_file_path.parent / f"{raw_file_path.stem}_clean{raw_file_path.suffix}"
        filtered_df.to_csv(csv_file_path, index=False)
        self.insert_into_db(csv_file_path, self.db_table)
        self.remove_file(csv_file_path)

    # @staticmethod
    # def add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    #     df.loc[:, "timestamp"] = datetime.now(tz=pytz.timezone('America/Los_Angeles'))
    #     return df
    #
    # @staticmethod
    # def _has_header(file_path: Path) -> bool:
    #     with open(file_path, 'r') as csv_file:
    #         sniffer = csv.Sniffer()
    #         has_header = sniffer.has_header(csv_file.read(2048))
    #         csv_file.seek(0)
    #
    #     return has_header
    #
    # def _get_num_rows_in_file(self, file_path: Path) -> int:
    #     """
    #
    #     Args:
    #         file_path:
    #
    #     Returns:
    #
    #     """
    #     num_rows = sum(1 for _ in open(file_path))
    #     adj_num_rows = num_rows - 1 if self._has_header(file_path) else num_rows
    #     return adj_num_rows

    # @staticmethod
    # def import_file(file_path: Path) -> pd.DataFrame:
    #     """
    #     Imports a file and loads it into a dataframe
    #
    #     Args:
    #         file_path
    #
    #     Returns:
    #         A pd.DataFrame
    #     """
    #     try:
    #         df = pd.read_csv(
    #             file_path,
    #             parse_dates=["start_date", "end_date"],
    #             dtype={
    #                 "project_id": int,
    #                 "project_name": str,
    #                 "site_id": int,
    #                 "client_id": int
    #             }
    #         )
    #
    #     except FileNotFoundError as e:
    #         logger.info(f"Error: {e}")
    #         raise FileNotFoundError(e)
    #
    #     return df

    @staticmethod
    def filter_out_old_projects(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
        """
        Removes projects which came before 2020.


        Args:
            date_str:
            df

        Returns:
            A filtered pd.DataFrame

        """

        filtered_df = df.loc[df["end_date"] >= date_str]
        return filtered_df

    # @staticmethod
    # def insert_into_db(file_path: Path, table_name: str) -> None:
    #     """
    #     Inserts a csv file into a PostgreSQL table
    #
    #     Args:
    #         file_path
    #         table_name
    #     """
    #
    #     with PostgresDB(
    #             database=db_config.database,
    #             user=db_config.user,
    #             password=db_config.password,
    #             host=db_config.host,
    #             port=db_config.port
    #     ) as db_conn:
    #         db_conn.insert_csv(
    #             file_path=file_path,
    #             table_name=table_name
    #         )
    #
    # @staticmethod
    # def remove_file(file_path: Path) -> None:
    #     """
    #     Deletes a file
    #
    #     Args:
    #         file_path:
    #     """
    #     logger.info(f"Removing file: {file_path}")
    #     file_path.unlink(missing_ok=True)
