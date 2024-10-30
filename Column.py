from dataclasses import dataclass




@dataclass
class Number:
    type: int | float
    size: int = 4


@dataclass
class Char:
    size: int
    var: bool = False


@dataclass
class ColumnInfo:
    name: str
    type: Number | Char


if __name__ == "__main__":
    c = Char(20)
    cv = Char(20, var=True)
    n = Number(int)
    f = Number(float)

    print(c)
    print(cv)
    print(n)
    print(f)

    print(isinstance(c, Char))