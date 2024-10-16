from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class PageId:
    fileIdx: int
    pageIdx: int



if __name__ == "__main__":
    p1 = PageId(1, 2)
    p2 = PageId(1, 3)

    data_dict = {"fileIdx": 5, "pageIdx": 5}
    p3 = PageId(**data_dict)

    print(p1)
    print(p1 == p2)
    print(p3)
    print(p3.__dict__)
    print(p2 <= p1)