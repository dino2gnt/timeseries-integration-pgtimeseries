# Timeseries Integration pg_timeseries Plugin

This plugin exposes an implementation of the TimeSeriesStorage interface.
It stores the data in a pg_timeseries table.
It can be used in OpenNMS to store and retrieve timeseries data.

## Prerequisite
* PostgreSQL 15 or greater
* The pg_timeseries plugin must be installed in the target postgres database.
* For testing purposes you can run: ``docker run -d --name pg-timeseries -p 5432:5432 -e POSTGRES_PASSWORD=postgres quay.io/tembo/timeseries-pg:latest``
* The pg_timeseries extension depends on:
  * The [pg_ivm](https://github.com/tembo-io/pg_ivm) extension (as of timeseries 0.1.6)
  * The [pg_cron](https://github.com/citusdata/pg_cron) extension
  * The [pg_partman](https://github.com/pgpartman/pg_partman) extension
  * The [Hydra Columnar](https://github.com/hydradatabase/hydra) extension


## Usage
### Compile from source
* compile: ``mvn clean install``.  You will probably have to add `-Dskiptests=True`; actual tests are on the roadmap.
* copy the `.kar` from the `./assembly/kar/target` folder to `$OPENNMS_HOME/deploy`
### superuser database operations
Superuser access to the database is required for the plugin to install extensions and create the required tables.
#### Define a JDBC url for a superuser connection:
* `echo 'adminDatasourceURL="jdbc:postgresql://localhost:5432/opennms?user=postgres&password=opennms"' >> $OPENNMS_HOME/etc/org.opennms.plugins.pgtimeseries.config.cfg`

### Add config for `pg_cron`
* `echo "cron.database_name = 'opennms' >> /var/lib/pgsql/data/postgresql.conf`
* `echo "shared_preload_libraries = 'pg_cron'" >> /var/lib/pgsql/data/postgresql.conf`
* Restart postgresql
### enable the Time Series Storage layer
* In the `${OPENMS_HOME}` directory: ``echo "org.opennms.timeseries.strategy=integration" >> etc/opennms.properties.d/timeseries.properties``

### Activate in the Karaf shell:
  * ``ssh -p 8101 admin@localhost``
  * ``feature:install opennms-pgtimeseries-tss-plugin``
  * The plugin will automatically create the necessary tables and attempt to install the necessary extensions if they don't already exist, if ``createTablesOnInstall = true``
  * if ``createTablesOnInstall = false`` you can use the ``opennms-pgtimeseries:install`` command to load the extensions and create tables.
 
### Configuration properties and default values
 * Config file can be created as ``$OPENNMS_HOME/etc/org.opennms.plugins.timeseries.pgtimeseries.cfg``
 * Honored properties are:
   *  **``externalDatasourceURL``**: a PostgreSQL JDBC URL, including username and password, that describes an external database that will be used to store timeseries metrics. Default: ``""``
   *  **``adminDatasourceURL``**: a PostgreSQL JDBC URL, including username and password, that describes a connection which has superuser access to the database.  This is used to install the required extensions and create the required tables.  This is only used for initial install and can be removed once the plugin has been configured. Default: ``""``
   *  **``partitionDuration``**: An SQL _Interval_ as described in the [PostgreSQL Docs](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT) to set the time width of a single partition of the timeseries metric table. Default: ``"1 week"``
   *  **``retentionPolicy``**: An SQL _Interval_ as described in the [PostgreSQL Docs](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT) to set the total duration of metric retention.  Metric partitions older than this are deleted. Default: ``"1 year"``
   *  **``compressionPolicy``**: An SQL _Interval_ as described in the [PostgreSQL Docs](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT) tehe defines the point after which partitions are automatically compressed to reduce storage consumption. Default: ``"3 months"``
   *  **``backfillStart``**: A ``timestamptz`` as described in the [PostgreSQL Docs](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME) used to determine the start of the table's first partition. When backfilling historical data, this is when the data starts.  Default: ``null``.
   *  **``createTablesOnInstall``**: The timeseries plugin will attempt to install the extension and create its tables on install if this is true.  If this is false, it will skip these steps and installation can be performed with the ``opennms-pgtimeseries:install`` Karaf shell command.
   *  **``maxBatchSize``**: Metrics are batched out to PostgreSQL; this is the largest number of metrics that will be written in as a single batch.  Default: `100`.
   *  **``connectionPoolSize``**: If ``externalDatasourceURL`` is defined, a connection pool is created for this data source. This limits the total number of pooled connections to the target PostgreSQL database.  Default: ``10``
 
### Karaf shell commands
 * ``opennms-pgtimeseries:stats``: Shows sample read, write, and lost metrics for the plugin.
 * ``opennms-pgtimeseries:show-table-info``: Displays timeseries table and index size information.
 * ``opennms-pgtimeseries:show-partition-info``: Displays partition information for the timeseries table.
 * ``opennms-pgtimeseries:ts-config``: Allows changing retention and compression interval. With no arguments, it displays the Partition Duration, Partition Lead Time, Retention, and Compression settings for the timeseries table.
 * ``opennms-pgtimeseries:install``: Checks for the existence of the required timeseries extensions and tables and creates them if they do not exist.
 * ``opennms-pgtimeseries:backfill-from-rrd``: Backfill time series metrics from local RRD (or jrb) files. Use with ``backfillStart`` to create partitions to hold backfilled data.

## Links:
* Introduction to the Time Series Storage Layer: https://docs.opennms.com/horizon/latest/operation/operation/timeseries/introduction.html
* pg_timeseries: https://github.com/tembo-io/pg_timeseries

## Roadmap / To do:
* (Done) Make the datasources configurable to allow timeseries to be persisted to an external database
* (Done) Use the `opennms-admin` datasource to install the extensions and create the tables if possible
* (Done) Make all other options configurable at install (retention, compression, partition interval, etc) and at runtime via Karaf shell commands where possible
* (Done) Add Karaf shell commands to expose [ts_table_info](https://github.com/tembo-io/pg_timeseries/blob/main/doc/reference.md#ts_table_info) and [ts_part_info](https://github.com/tembo-io/pg_timeseries/blob/main/doc/reference.md#ts_part_info)
* (Done) Rename `show-ts-config` to `ts-config` and add options to allow the retention and compression intervals to be set on the fly. (pgtimeseries doesn't support changing the partition interval yet)
* (Done) Figure out how to use the external data source everywhere the regular datasource is used.
* (Done but rough) Make something that can backfill the database from existing rrd or jrb
* Move connection bits from the initializer to a separate helper class
* It probably needs more tests.  I don't know how to write tests. PRs welcome.
