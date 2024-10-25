from dataclasses import dataclass




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
class ColumnInfo:
    name: str
    type: Number | Char | CharVar



if __name__ == "__main__":
    c = Char(20)
    cv = CharVar(20)
    n = Number()
    f = Number()

    print(c)
    print(cv)
    print(n)
    print(f)

    print(isinstance(c, CharVar))