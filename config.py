from dataclasses import dataclass


@dataclass
class Config:
    database: str
    user: str
    password: str
    host: str
    port: int


db_config = Config(
    database="postgres",
    user="postgres",
    password="",
    host="localhost",
    port=5432
)