from abc import ABC, abstractmethod
from typing import Optional
from Record import Record

class IRecordIterator(ABC):
    @abstractmethod
    def GetNextRecord(self) -> Optional[Record]:
        """
        Retourne le record courant et avance d'un cran le curseur de l'itérateur.
        Retourne None lorsqu'il ne reste plus de record dans l'ensemble de tuples.
        """
        pass

    @abstractmethod
    def Close(self):
        """
        Signale qu'on n'utilise plus cet itérateur.
        """
        pass

    @abstractmethod
    def Reset(self):
        """
        Met ou remet le curseur au début de l'ensemble des records à parcourir.
        """
        pass
