import pandas as pd
from loguru import logger

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
