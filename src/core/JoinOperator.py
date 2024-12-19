from PageDirectoryIterator import PageDirectoryIterator
from DataPageHoldRecordIterator import DataPageHoldRecordIterator
from BufferManager import BufferManager
from Relation import Relation
from Condition import Condition
from Column import ColumnInfo
from Record import Record
from IRecordIterator import IRecordIterator

class PageOrientedJoinOperator(IRecordIterator):
    def __init__(self, relation1: Relation, relation2: Relation, conditions: list[Condition], buffer_manager: BufferManager):
        self.relation1 = relation1
        self.relation2 = relation2
        self.conditions = conditions
        self.buffer_manager = buffer_manager
        self.result = []
        self.current_index = 0
        self.has_performed_join = False

    def GetNextRecord(self) -> Record:
        if not self.has_performed_join:
            self.perform_join()
            self.has_performed_join = True

        if self.current_index < len(self.result):
            record = Record(self.result[self.current_index])
            self.current_index += 1
            return record
        return None

    def Reset(self):
        self.current_index = 0
        self.has_performed_join = False
        self.result = []

    def Close(self):
        self.Reset()

    def perform_join(self):
        # Create PageDirectoryIterator for both relations
        r_page_iterator = PageDirectoryIterator(self.relation1)

        while True:
            r_page_id = r_page_iterator.GetNextDataPageId()
            if r_page_id is None:
                break

            # Keep R page loaded throughout S iterations
            r_buffer = self.buffer_manager.getPage(r_page_id)

            r_record_iterator = DataPageHoldRecordIterator(
                r_page_id,
                self.relation1
            )

            # Iterate through all records in current R page
            while True:
                r_record = r_record_iterator.GetNextRecord()
                if r_record is None:
                    break

                # Reset S page iterator to start from the first page
                s_page_iterator = PageDirectoryIterator(self.relation2)

                # Iterate through all pages of relation S
                while True:
                    s_page_id = s_page_iterator.GetNextDataPageId()
                    if s_page_id is None:
                        break

                    # Keep S page loaded for all record comparisons
                    s_buffer = self.buffer_manager.getPage(s_page_id)

                    s_record_iterator = DataPageHoldRecordIterator(
                        s_page_id,
                        self.relation2
                    )

                    # Iterate through all records in current S page
                    while True:
                        s_record = s_record_iterator.GetNextRecord()
                        if s_record is None:
                            break

                        # Combine records for condition evaluation
                        combined_record = r_record.values + s_record.values

                        # Prepare column list for condition evaluation
                        combined_columns = (
                            [ColumnInfo(f"T1.{col.name}", col.type) for col in self.relation1.columns] +
                            [ColumnInfo(f"T2.{col.name}", col.type) for col in self.relation2.columns]
                        )

                        # If no conditions, do a Cartesian product
                        if not self.conditions:
                            self.result.append(combined_record)
                        else:
                            combined_record_inst = Record(combined_record)
                            # Check if ALL conditions are met
                            if all(
                                condition.evaluate(
                                    combined_record_inst,
                                    combined_columns
                                )
                                for condition in self.conditions
                            ):
                                self.result.append(combined_record)

                    # Free S page buffer after processing all its records
                    self.buffer_manager.FreePage(s_page_id)

            # Free R page buffer after processing all S pages
            self.buffer_manager.FreePage(r_page_id)

        return self.result  # Return the result for backward compatibility

    def get_all_results(self):
        """Method to get all results at once, for backward compatibility"""
        if not self.has_performed_join:
            self.perform_join()
            self.has_performed_join = True
        return self.result