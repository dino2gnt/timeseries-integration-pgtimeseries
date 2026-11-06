# Timeseries Integration pgtimeseries Plugin

This plugin exposes an implementation of the OpenNMS TimeSeriesStorage interface that stores
time series data in a range-partitioned PostgreSQL table. It can be used in OpenNMS to store and
retrieve collected metrics in the OpenNMS database itself or in a separate PostgreSQL database.

The plugin manages its partitions itself: it creates partitions ahead of time, drops them when
they age past the retention policy, and moves stray rows into the right partition, all from a
maintenance thread inside OpenNMS. No PostgreSQL extension, no superuser, and no change to
`postgresql.conf` is needed.

## Prerequisites
* PostgreSQL 16 or greater.
* A database role that owns the target database (the normal `opennms` role is fine). A more restricted
  role works too, provided it owns the plugin's tables (partition maintenance attaches and drops
  child tables) or a DBA created them and manages partitions (``managePartitions = false``).

Versions 0.2.x and earlier relied on the `pg_timeseries`, `pg_partman` and `pg_cron` extensions.
See [Upgrading from 0.2.x](#upgrading-from-02x) if you have such a database.

## Usage
### Compile from source
* compile: ``mvn clean install`` (needs JDK 17).
* copy the `.kar` from the `./assembly/kar/target` folder to `$OPENNMS_HOME/deploy`

### Or grab a release kar
* See "Releases". We compile for OpenNMS Plugin API versions ``1.6.1`` (Meridian 2024, 2025, Horizon 35 and older) and ``2.0.0`` (Horizon 36 and newer, Meridian 2026 and newer)

### Enable the Time Series Storage layer
* In the `${OPENMS_HOME}` directory: ``echo "org.opennms.timeseries.strategy=integration" >> etc/opennms.properties.d/timeseries.properties``

### Enable storeByForeignSource
* In the `${OPENMS_HOME}` directory: ``echo "org.opennms.rrd.storeByForeignSource=true" >> etc/opennms.properties.d/timeseries.properties``
* *Updating system properties requires restarting OpenNMS

### Activate in the Karaf shell:
  * ``ssh -p 8101 admin@localhost``
  * ``feature:install opennms-pgtimeseries-tss-plugin``
  * The plugin creates its tables, policy row and initial partitions on start-up if ``createTablesOnInstall = true`` (the default).
  * if ``createTablesOnInstall = false`` you can use the ``opennms-pgtimeseries:install`` command to create them.

### Configuration properties and default values
 * Config file can be created as ``$OPENNMS_HOME/etc/org.opennms.plugins.timeseries.pgtimeseries.cfg``
 * Honored properties are:
   *  **``externalDatasourceURL``**: a PostgreSQL JDBC URL, including username and password, that describes an external database that will be used to store timeseries metrics. When empty the OpenNMS database is used. Default: ``""``
   *  **``adminDatasourceURL``**: a PostgreSQL JDBC URL, including username and password, for a superuser connection. Only used by ``opennms-pgtimeseries:migrate-from-extension`` to drop the old extension; not needed for a fresh install. Default: ``""``
   *  **``partitionDuration``**: An SQL _Interval_ as described in the [PostgreSQL Docs](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT) that sets the time width of a single partition of the timeseries metric table. Default: ``"1 week"``
   *  **``partitionLeadTime``**: An SQL _Interval_; partitions are kept pre-created this far ahead of the current time. Default: ``"1 month"``
   *  **``retentionPolicy``**: An SQL _Interval_ that sets the total duration of metric retention. Partitions whose whole time range is older than this are dropped. Default: ``"1 year"``
   *  **``compressionPolicy``**: An SQL _Interval_ after which partitions are converted to columnar storage. Only has an effect when the optional ``citus_columnar`` extension is installed; otherwise it is ignored with a log message. Default: ``"3 months"``
   *  **``backfillStart``**: A ``timestamptz`` as described in the [PostgreSQL Docs](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME) used to determine the start of the table's first partition. When backfilling historical data, this is when the data starts. Default: ``null``.
   *  **``createTablesOnInstall``**: The plugin creates its tables and partitions on start-up if this is true. If this is false it skips that step and installation can be performed with the ``opennms-pgtimeseries:install`` Karaf shell command. Default: ``true``
   *  **``managePartitions``**: When ``false`` the plugin never creates, drops or compresses partitions (something else is expected to). Default: ``true``
   *  **``maintenanceIntervalMinutes``**: How often, in minutes, the plugin's maintenance thread creates future partitions and applies the retention and compression policies. Default: ``60``
   *  **``maxBatchSize``**: Metrics are batched out to PostgreSQL; this is the largest number of metrics that will be written in as a single batch. Default: `100`.
   *  **``connectionPoolSize``**: If ``externalDatasourceURL`` is defined, a connection pool is created for this data source. This limits the total number of pooled connections to the target PostgreSQL database. Default: ``10``
   *  **``flushMaxSamples``**: Samples are buffered across collection sets and flushed once this many are pending. Default: ``1000``
   *  **``flushMaxIntervalMs``**: Upper bound, in milliseconds, on how long a buffered sample waits before being flushed, regardless of ``flushMaxSamples``. Default: ``5000``
   *  **``maxBufferedSamples``**: Most samples held in the write-behind buffer. When the database is down or cannot keep up, a collection set that does not fit is refused (OpenNMS counts it as dropped) instead of growing the heap without bound. ``0`` disables the limit. Default: ``100000``

The partition policy (``partitionDuration``, ``partitionLeadTime``, ``retentionPolicy``,
``compressionPolicy``) is copied from the configuration file into the ``pgtimeseries_config``
table when the tables are first created. After that the table is authoritative, so later changes
go through ``opennms-pgtimeseries:ts-config`` rather than the file.

### Karaf shell commands
 * ``opennms-pgtimeseries:stats``: Shows sample read, write, lost, and partition maintenance metrics for the plugin.
 * ``opennms-pgtimeseries:show-table-info``: Displays table and index sizes (the time series table summed over its partitions).
 * ``opennms-pgtimeseries:show-partition-info``: Displays every partition of the time series table with its time range and sizes.
 * ``opennms-pgtimeseries:ts-config``: Displays the partition policy (partition duration, lead time, retention, compression). ``-r``/``-c`` change retention and compression (``none`` clears them), ``-p`` changes the width of future partitions, ``-l`` changes the lead time. Changes apply on the next maintenance run.
 * ``opennms-pgtimeseries:maintenance [status|run|dry-run]``: Shows when partition maintenance last ran and what it did, runs it now, or previews what a run would create, drop, or move.
 * ``opennms-pgtimeseries:install``: Creates the plugin's tables, policy row, and initial partitions if they do not exist. No extension is required.
 * ``opennms-pgtimeseries:migrate-from-extension [--dry-run] [--drop-orphans]``: For databases set up by plugin 0.2.x: imports the policy from the pg_timeseries extension, unschedules its pg_cron jobs, unregisters the table from pg_partman, drops the extension, and lists (or with ``--drop-orphans`` drops) partitions that pg_partman detached but never dropped. Uses ``adminDatasourceURL`` when set.
 * ``opennms-pgtimeseries:backfill-from-rrd``: Backfill time series metrics from local RRD (or jrb) files. Use with ``backfillStart`` to create partitions to hold backfilled data.

## How partitions are managed
The time series table ``pgtimeseries_time_series`` is range-partitioned on ``time``. Child
partitions are named ``pgtimeseries_time_series_pYYYYMMDD`` after their lower bound (with an
``_HH24MISS`` suffix for widths under one day), plus a default partition
``pgtimeseries_time_series_default``. This is the same layout the pg_partman-based versions used,
so existing databases carry over unchanged.

Once an hour (``maintenanceIntervalMinutes``) the maintenance thread:

1. moves any rows that landed in the default partition (timestamps outside every existing
   partition, for example a backfill older than the first partition or a collector with a wrong
   clock) into properly bounded partitions, creating them as needed;
2. creates partitions so that everything up to ``now + partitionLeadTime`` has one, filling any
   gaps and aligning new partitions to the existing ones;
3. drops partitions whose upper bound is older than ``now - retentionPolicy``, never the last one;
4. if ``citus_columnar`` is installed, converts partitions older than ``now - compressionPolicy``
   to columnar storage.

Each step runs in its own transaction, a problem with one partition is reported and the rest are
still handled, and nothing is ever surfaced to the write path. A session-level advisory lock makes it safe for several OpenNMS instances to share a
database. Partition bounds and names are computed by PostgreSQL in the JDBC session's timezone
(the OpenNMS JVM's timezone), which matches the old pg_cron based naming whenever OpenNMS and
PostgreSQL share a timezone.

Note that steps 1 and 3 run in the same pass: backfilled rows older than the retention policy are
moved into a proper partition and that partition is dropped within the hour. Raise the retention
(``ts-config -r``) before importing old data you want to keep.

Notes on compression: columnar partitions do not support ``DELETE``, so deleting a metric
(for example when a node is removed) fails once any of its samples live in a compressed partition.
Leave ``compressionPolicy`` empty (or set it to ``none`` with ``ts-config``) if that matters to you.

## Upgrading from 0.2.x
Databases set up by 0.2.x used the ``pg_timeseries`` extension, which in turn had ``pg_partman``
create the partitions and ``pg_cron`` run hourly maintenance. The new plugin adopts such a
database as is:

* On start-up it creates the ``pgtimeseries_config`` table and imports the policy from the
  extension's ``ts_config`` table, so a retention or compression value you set with ``ts-config``
  survives.
* Partitions are discovered by their bounds, so the ones pg_partman created are used unchanged and
  the plugin's own maintenance simply continues the series.
* While the extension is still installed the plugin logs a warning on every maintenance run. Both
  managers can run side by side safely, but you should hand over explicitly:

```
opennms-pgtimeseries:migrate-from-extension --dry-run
opennms-pgtimeseries:migrate-from-extension --drop-orphans
```

On its first start against a 0.2.x database the plugin also adds an index on ``pgtimeseries_tag (key, value)``,
which the old schema lacked, so looking metrics up by tag (resource graphs, the resource browser) no longer
scans the whole tag table. A role that may not create it gets a warning; run
``CREATE INDEX pgtimeseries_tag_key_value_idx ON pgtimeseries_tag (key, value)`` as the table's owner.

The command unschedules the extension's two pg_cron jobs, removes the table's pg_partman
configuration and template table, and drops the ``timeseries`` extension. Dropping the extension
needs its owner (normally a superuser); set ``adminDatasourceURL`` or run the SQL the command
prints as a superuser. ``pg_cron`` and ``pg_partman`` are left installed; drop them yourself if
nothing else uses them, and the ``cron.database_name`` and ``shared_preload_libraries`` settings
can be reverted.

One behaviour change to be aware of: with the extension, "retention" only detached expired
partitions (pg_partman's ``retention_keep_table`` default) and never dropped them, so every
deployment past its retention age has old weekly tables lying around. The plugin really drops
expired partitions. ``--drop-orphans`` removes the tables the extension left behind; without it
they are only listed, and ``opennms-pgtimeseries:maintenance status`` keeps reporting them.

## Links:
* Introduction to the Time Series Storage Layer: https://docs.opennms.com/horizon/latest/operation/operation/timeseries/introduction.html
