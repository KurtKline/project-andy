from pathlib import Path

import psycopg2

from loguru_setup import get_logger

logger = get_logger(Path(__file__).parent / 'logs/db.log')


class PostgresDB:
    def __init__(
            self,
            database: str,
            user: str,
            password: str,
            host: str,
            port: int
    ):
        self.conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def query(self, query: str):

        with self.conn.cursor as cur:
            cur.execute(query)
            records = cur.fetchall()

        return records

    def insert_csv(self, file_path: Path, table_name: str) -> None:
        """
        Inserts a csv file's data into a specified table.

        Args:
            file_path
            table_name
        """
        tmp_table_name = 'tmp_table'

        tmp_table_query = f"""
        CREATE TEMP TABLE {tmp_table_name}
        AS
        SELECT * 
        FROM {table_name}
        WITH NO DATA;
        """

        insert_query = f"""
        INSERT INTO {table_name}
        SELECT *
        FROM {tmp_table_name}
        ON CONFLICT DO NOTHING;
        """

        with self.conn.cursor() as cur:
            try:
                cur.execute(tmp_table_query)

                with open(file_path, 'r') as file:
                    next(file)  # Skips header row
                    cur.copy_from(file, tmp_table_name, sep=',')
                    logger.info(f"Rows inserted into {tmp_table_name}: {cur.rowcount}")

                cur.execute(insert_query)
                self.conn.commit()
                logger.info(f"Rows inserted into {table_name}: {cur.rowcount}")

            except (Exception, psycopg2.DatabaseError) as error:
                logger.info(f"Error: {error}")
                self.conn.rollback()
                raise Exception
