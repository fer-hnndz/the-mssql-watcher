from dataclasses import dataclass


@dataclass
class ColumnSchema:
    COLUMN_NAME: str
    DATA_TYPE: str
    CHARACTER_MAXIMUM_LENGTH: int
    NUMERIC_PRECISION: int
    DATETIME_PRECISION: int
    NUMERIC_SCALE: int
    CHARACTER_OCTET_LENGTH: int
