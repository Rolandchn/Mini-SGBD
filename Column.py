from dataclasses import dataclass


@dataclass
class Base:
    def __post_init__(self):
        for value, value_type in self.__annotations__.items():
            assert isinstance(getattr(self, value), value_type)


@dataclass
class Number:
    type: int | float
    size: int = 4


@dataclass
class Char:
    size: int
    var: bool = False


@dataclass
class ColumnInfo(Base):
    name: str
    type: Number | Char


if __name__ == "__main__":
    c = Char(20)
    cv = Char(20, var=True)
    n = Number(int)
    f = Number(float)

    column_info = ColumnInfo("test", char)

    print(isinstance(c, Char))
