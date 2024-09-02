from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import Policy, RelationType
from dbt.adapters.relation_configs.config_base import RelationConfigBase
from dbt_common.exceptions import DbtRuntimeError


@dataclass
class FireboltQuotePolicy(Policy):
    database: bool = True
    schema: bool = False
    identifier: bool = False


@dataclass
class FireboltIncludePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class FireboltRelation(BaseRelation):
    quote_policy: FireboltQuotePolicy = field(
        default_factory=lambda: FireboltQuotePolicy()
    )
    include_policy: FireboltIncludePolicy = field(
        default_factory=lambda: FireboltIncludePolicy()
    )
    quote_character: str = '"'
    is_delta: Optional[bool] = None
    information: Optional[str] = None
    relation_configs: Dict[RelationType, RelationConfigBase] = field(
        default_factory=lambda: {}
    )
    # list relations that can be renamed (e.g. `RENAME my_relation TO my_new_name;`)
    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset({})
    )
    # list relations that can be atomically replaced
    # (e.g. `CREATE OR REPLACE my_relation..` versus `DROP` and `CREATE`)
    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
            }
        )
    )

    def render(self) -> str:
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                'Got a Firebolt relation with schema and database set to '
                'include, but only one can be set.'
            )
        return super().render()
