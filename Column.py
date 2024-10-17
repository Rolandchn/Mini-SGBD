from dataclasses import dataclass

import enum

from PageId import PageId


class ColumnType(enum.Enum):
    INT = enum.auto()
    REAL = enum.auto()

@dataclass
class Char:
    size: int
    

@dataclass
class VarChar:
    max_size: int



@dataclass
class ColumnInfo:
    name: str
    type: ColumnType






if __name__ == "__main__":
    c = ColumnInfo("test", PageId)
    print(c)