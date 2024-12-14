
from DBconfig import DBconfig
from BufferManager import BufferManager
from DiskManager import DiskManager

from Buffer import Buffer
from PageId import PageId

from Relation import Relation
from Record import Record

import Column

import os
from pathlib import Path


if __name__ == "__main__":
    ## File path
    DB_path = os.path.join(os.path.dirname(__file__), "..", "config", "DBconfig.json")
    db_file_path = Path(__file__).parent / "../../storage/database/test1.json"

    ## Init
    buffManager = BufferManager.setup(DB_path)

    ## Relation
    # Relation 1: nom | age | id
    relation1 = Relation("camarade", 
                        3, 
                        [Column.ColumnInfo("nom", Column.VarChar(10)), Column.ColumnInfo("age", Column.Int()), Column.ColumnInfo("id", Column.Int())],
                        buffManager.disk,
                        buffManager)
    
    r1_1 = Record(["Leo", 21, 123456])
    r1_2 = Record(["Hector", 22, 121212])
    r1_3 = Record(["Marc", 24, 654321])

    # Relation 2: nom | prix
    relation2 = Relation("fruit", 
                        2, 
                        [Column.ColumnInfo("nom", Column.VarChar(10)), Column.ColumnInfo("prix", Column.Float())],
                        buffManager.disk,
                        buffManager)
    
    r2_1 = Record(["Pomme", 6.5])
    r2_2 = Record(["Orange", 6])
    r2_3 = Record(["Banane", 5.32])

    ## Buffer
    #PageId(0, 0) contient le headerPage relation 1 
    #PageId(0, 1) contient le headerPage relation 2 

    ## Record & DataPage
    dataPageId1 = relation1.addDataPage()

    # Ecriture
    relation1.writeRecordToDataPage(r1_1, dataPageId1)
    relation1.writeRecordToDataPage(r1_2, dataPageId1)
    relation1.writeRecordToDataPage(r1_3, dataPageId1)
    
    for x in relation1.getRecordsInDataPage(dataPageId1):
        print(x.values)

    