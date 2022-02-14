#!/usr/bin/bash
dbt debug
dbt clean
dbt deps
dbt compile
dbt run-operation stage_external_sources
dbt seed
dbt run
dbt source freshness
dbt test
dbt docs generate
