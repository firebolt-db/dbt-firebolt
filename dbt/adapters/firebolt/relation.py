from typing import Optional

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.contracts.relation import ComponentName
from dbt.exceptions import RuntimeException
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import filter_null_values


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

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        search = filter_null_values({
            ComponentName.Database: database,
            ComponentName.Schema: schema,
            ComponentName.Identifier: identifier
        })
        logger.info('relation.match: BEGIN')

        if not search:
            # nothing was passed in
            raise dbt.exceptions.RuntimeException(
                "Tried to match relation, but no search path was passed!")

        exact_match = True
        approximate_match = True

        for k, v in search.items():
            if not self._is_exactish_match(k, v):
                exact_match = False

            if (
                self.path.get_lowered_part(k).strip(self.quote_character) != 
                v.lower().strip(self.quote_character)
            ):
                approximate_match = False

        if approximate_match and not exact_match:
            target = self.create(
                database=database, schema=schema, identifier=identifier
            )
            logger.info('relation.match: END')
            dbt.exceptions.approximate_relation_match(target, self)
        
        logger.info('relation.match: END')
        return exact_match