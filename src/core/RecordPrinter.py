from IRecordIterator import IRecordIterator

class RecordPrinter:
    def __init__(self, iterator: IRecordIterator):
        self.iterator = iterator

    def print_records(self):
        filtered_records = []

        while True:
            record = self.iterator.GetNextRecord()
            if record is None:
                break
            filtered_records.append(record)

        for record in filtered_records:
            print(" ; ".join(str(value) for value in record.values))
        print(f"Total records={len(filtered_records)}")

        self.iterator.Close()
