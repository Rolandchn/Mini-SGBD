from typing import Optional, List
from Record import Record
from BufferManager import BufferManager, Buffer
from IRecordIterator import IRecordIterator

class PageOrientedJoinOperator(IRecordIterator):
    def __init__(self, r_iterator: IRecordIterator, s_iterator: IRecordIterator, join_condition):
        self.r_iterator = r_iterator
        self.s_iterator = s_iterator
        self.join_condition = join_condition
        self.current_r_record = None
        self.current_s_record = None
        self.s_iterator_reset = True
        self.joined_records = []

    def GetNextRecord(self) -> Optional[List[Record]]:
        while True:
            if self.current_r_record is None:
                self.current_r_record = self.r_iterator.GetNextRecord()
                if self.current_r_record is None:
                    return None
                self.s_iterator.Reset()
                self.s_iterator_reset = True

            if self.s_iterator_reset:
                self.current_s_record = self.s_iterator.GetNextRecord()
                self.s_iterator_reset = False

            while self.current_s_record is not None:
                if self.join_condition(self.current_r_record, self.current_s_record):
                    joined_record = self.join_records(self.current_r_record, self.current_s_record)
                    self.joined_records.append(joined_record)
                self.current_s_record = self.s_iterator.GetNextRecord()

            if self.joined_records:
                result = self.joined_records
                self.joined_records = []
                return result

            self.current_r_record = None

    def join_records(self, r_record: Record, s_record: Record) -> Record:
        return r_record + s_record

    def Close(self):
        self.r_iterator.Close()
        self.s_iterator.Close()

    def Reset(self):
        self.r_iterator.Reset()
        self.s_iterator.Reset()
        self.current_r_record = None
        self.current_s_record = None
        self.s_iterator_reset = True
        self.joined_records = []
