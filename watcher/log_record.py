from dataclasses import dataclass


@dataclass
class LogRecord:
    operation: str
    context: str
    transaction_id: int
    alloc_unit: str
    raw_data: bytes
    raw_data2: bytes
    begin_operation: str
    end_operation: str
    username: str
    current_lsn: str

    def __str__(self) -> str:
        return f"LogRecord(operation={self.operation}, context={self.context}, transaction_id={self.transaction_id}, alloc_unit={self.alloc_unit}, raw_data={self.raw_data}, raw_data2={self.raw_data2})"

    def __repr__(self) -> str:
        return self.__str__()
