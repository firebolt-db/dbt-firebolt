from dataclasses import dataclass

from dbt.adapters.base.column import Column


@dataclass
class FireboltColumn(Column):
    @classmethod
    def string_type(cls, size: int) -> str:
        return 'text'

    @classmethod
    def is_date(self) -> bool:
        return self.dtype.lower() == 'date'
