from dbt.adapters.base.column import Column


# @dataclass
class FireboltColumn(Column):
    # column_index: int = column_index
    # Building a catalog requires that all columns are indexed.

    #     def __init__(self, column_name: str, dtype: str, column_index: int = 0):
    #         super.__init__(dtype=dtype, column=column_name)
    #         self.column_index = column_index

    @classmethod
    def string_type(cls, size: int) -> str:
        return 'text'

    @classmethod
    def is_date(self) -> bool:
        return self.dtype.lower() == 'date'
