from dbt.adapters.base.column import Column


class FireboltColumn(Column):
    # Building a catalog requires that all columns are indexed.
    column_index: int = 0

    @classmethod
    def string_type(cls, size: int) -> str:
        return 'text'

    @classmethod
    def is_date(self) -> bool:
        return self.dtype.lower() == 'date'
