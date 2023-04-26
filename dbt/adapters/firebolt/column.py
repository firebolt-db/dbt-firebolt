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

    @classmethod
    def from_description(cls, name: str, raw_data_type: str) -> Column:
        if raw_data_type.startswith('ARRAY'):
            return cls(name, raw_data_type)
        return super().from_description(name, raw_data_type)
