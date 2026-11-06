# Changelog

## 0.3.0

The plugin no longer depends on any PostgreSQL extension. Partition management moved from
pg_timeseries, pg_partman and pg_cron into the plugin itself.

### Added
* A partition maintenance thread that creates partitions through `partitionLeadTime`, drops
  partitions past `retentionPolicy`, moves stray rows out of the default partition into proper
  partitions, and (if `citus_columnar` happens to be installed) compresses partitions past
  `compressionPolicy`. Runs every `maintenanceIntervalMinutes` (default 60) and once at start-up.
* A `pgtimeseries_config` table holding the partition policy; it replaces the extension's
  `ts_config` and is imported from it on upgrade.
* Configuration properties `partitionLeadTime`, `maintenanceIntervalMinutes` and `managePartitions`.
* Shell command `opennms-pgtimeseries:maintenance [status|run|dry-run]`.
* Shell command `opennms-pgtimeseries:migrate-from-extension [--dry-run] [--drop-orphans]` to hand a
  0.2.x database over to the plugin and remove the extension.
* `ts-config` can now change the partition width (`-p`, future partitions only) and lead time (`-l`),
  and clear retention or compression with `none`.
* Maintenance metrics (`maintenance.*`) in `opennms-pgtimeseries:stats` and over JMX.
* If a flush fails because no partition accepts a row, the partitions for the lead window are created
  (bounded wait) and the batch retried once.
* `ts-config` validates intervals: bare numbers (which PostgreSQL reads as seconds), non-positive
  intervals, widths under a minute, and retention or compression shorter than the partition width are
  rejected with an explanation instead of being applied.

### Changed
* Installation needs no superuser, no `CREATE EXTENSION`, no `shared_preload_libraries` and no
  `cron.database_name`. `adminDatasourceURL` is only used by `migrate-from-extension`.
* Expired partitions are dropped and their disk reclaimed. The extension-based versions only
  detached them (pg_partman's `retention_keep_table` default), leaving the tables behind.
* `show-partition-info`, `show-table-info` and `ts-config` read the catalog and the plugin's own
  config table instead of the extension's views. `show-table-info` also reports the metric and tag
  tables.
* `install` creates the schema and initial partitions only; it no longer installs an extension.
* Start-up never issues DDL for objects that already exist, so a role without CREATE on the schema
  can run the plugin against a schema a DBA created, and a failed partition bootstrap is logged
  rather than preventing the plugin from starting.
* A problem with one partition (an orphan table in the way, a drop that fails) is reported and the
  remaining partitions of that run are still handled.
* `ts-config` and `maintenance` remain available in the shell when the storage service failed to
  start.
* The blueprint now calls `destroy()` on shutdown, so buffered samples are flushed and the
  maintenance thread stopped when the feature is uninstalled.
* Integration tests run against a stock `postgres:16-alpine` image; one upgrade test still uses the
  pg_timeseries image.

### Fixed
* Metric ids cached during a failed flush are evicted again, so a retry cannot write samples that
  reference a rolled-back metric row.

### Removed
* The `timeseries` extension install path (`PGTimeseriesDatabaseInitializer`) and the hard-coded
  `opennms` owner assignment of its tables.

## 0.2.3
* Handle counter wraps more gracefully.
* Add explicit options for rrd / jrb handling.
