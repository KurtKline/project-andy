import csv
from datetime import datetime
from pathlib import Path

import pytz
import pandas as pd
import pendulum as pendulum

from config import db_config
from db import PostgresDB
from loguru_setup import get_logger

logger = get_logger(Path(__file__).parent / 'logs/run.log')


def import_file(path: Path) -> pd.DataFrame:
    """
    Imports a file and loads it into a dataframe

    Args:
        path:

    Returns:
        A pd.DataFrame
    """
    try:
        df = pd.read_csv(
            path,
            parse_dates=["start_date", "end_date"],
            dtype={
                "project_id": int,
                "project_name": str,
                "site_id": int,
                "client_id": int
            }
        )

    except FileNotFoundError as e:
        logger.info(f"Error: {e}")
        raise FileNotFoundError(e)

    return df


def filter_out_old_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes projects which came before 2020.


    Args:
        df

    Returns:
        A filtered pd.DataFrame

    """

    filtered_df = df.loc[df["end_date"] >= '2020-01-01']
    return filtered_df


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


def remove_file(file_path: Path) -> None:
    """
    Deletes a file

    Args:
        file_path:
    """
    logger.info(f"Removing file: {file_path}")
    file_path.unlink(missing_ok=True)


# def parse_dates(text: str) -> datetime.date:
#     """
#     Converts date column text values into
#     datetime.date objects.
#
#     Args:
#         text:
#
#     Returns:
#         datetime.date objects
#     """
#     parts = text.split('-')
#     return datetime.date(
#         int(parts[0]),
#         int(parts[1]),
#         int(parts[2])
#     )


def _has_header(file_path: Path) -> bool:
    with open(file_path, 'r') as csvfile:
        sniffer = csv.Sniffer()
        has_header = sniffer.has_header(csvfile.read(2048))
        csvfile.seek(0)

    return has_header


def get_num_rows_in_file(file_path: Path) -> int:
    """

    Args:
        file_path:

    Returns:

    """
    num_rows = sum(1 for _ in open(file_path))
    return num_rows - 1 if _has_header(file_path) else num_rows


def add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "timestamp"] = datetime.now(tz=pytz.timezone('America/Los_Angeles'))
    return df


def main():
    table_name = 'projects'
    raw_file_path = Path(__file__).parent / 'test_files/test_projects.csv'
    num_rows = get_num_rows_in_file(raw_file_path)
    logger.info(f"Rows in raw file: {num_rows}")
    df = import_file(raw_file_path)

    filtered_df = filter_out_old_projects(df).copy()
    filtered_df = add_timestamp_column(filtered_df)

    logger.info(f"Rows in filtered DataFrame: {len(filtered_df)}")

    csv_file_path = raw_file_path.parent / f"{raw_file_path.stem}_clean{raw_file_path.suffix}"
    filtered_df.to_csv(csv_file_path, index=False)
    insert_into_db(csv_file_path, table_name)
    remove_file(csv_file_path)


if __name__ == '__main__':
    main()
