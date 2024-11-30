
from DBconfig import DBconfig
from BufferManager import BufferManager
from DiskManager import DiskManager

from Buffer import Buffer
from PageId import PageId

from Relation import Relation
from Record import Record


if __name__ == "__main__":
    buffManager = BufferManager("DBconfig.json")