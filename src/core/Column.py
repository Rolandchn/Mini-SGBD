from dataclasses import dataclass


@dataclass
class Base:
    def __post_init__(self):
        for value, value_type in self.__annotations__.items():
            assert isinstance(getattr(self, value), value_type)



@dataclass
class Int:
    size: int = 4

@dataclass
class Float:
    size: int = 4

@dataclass
class Char:
    size: int


@dataclass
class VarChar:
    size: int

@dataclass
class ColumnInfo(Base):
    name: str
    type: Int | Float | Char | VarChar


if __name__ == "__main__":
    c = Char(40)
    vc = VarChar(40)
    i = Int()
    f = Float()

    print(i)
