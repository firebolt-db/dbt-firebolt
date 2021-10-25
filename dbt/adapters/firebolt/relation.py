from typing import Optional

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException


@dataclass
class FireboltQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class FireboltIncludePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class FireboltRelation(BaseRelation):
    quote_policy: FireboltQuotePolicy = FireboltQuotePolicy()
    include_policy: FireboltIncludePolicy = FireboltIncludePolicy()
    quote_character: str = '"'
    is_delta: Optional[bool] = None
    information: str = None

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                'Got a Firebolt relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()
