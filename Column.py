from dataclasses import dataclass


@dataclass
class Base:
    def __post_init__(self):
        for value, value_type in self.__annotations__.items():
            assert isinstance(getattr(self, value), value_type)


@dataclass
class Number:
    size: int = 4


@dataclass
class Char:
    size: int


@dataclass
class CharVar:
    size_var: int


@dataclass
class ColumnInfo(Base):
    name: str
    type: Number | Char | CharVar



if __name__ == "__main__":
    char = Char(20)
    number = Number()

    column_info = ColumnInfo("test", char)

    print(column_info)