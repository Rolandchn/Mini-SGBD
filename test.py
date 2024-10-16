from dataclasses import dataclass

@dataclass
class One:
    a: int = 1 
    b: int



if __name__ == "__main__":
    print(One(1, 2))
