from dataclasses import dataclass

import enum

from PageId import PageId


class ColumnType(enum.Enum):
    INT = enum.auto()
    REAL = enum.auto()
    CHAR = enum.auto()
    VALCHAR = enum.auto()    


@dataclass
class Column:
    name: str
    type: type






if __name__ == "__main__":
    c = Column("test", PageId)
    print(c)