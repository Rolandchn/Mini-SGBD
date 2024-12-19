from dataclasses import dataclass, field
from typing import List


@dataclass(order=True)
class A():
    value: int = 0

@dataclass
class B():
    liste: list = field(default_factory=list) 



if __name__ == "__main__":
    a = A()
    b = B()

    print(a)

    b.liste.append(a)

    print(b)

    a.value = 5

    print(b)

