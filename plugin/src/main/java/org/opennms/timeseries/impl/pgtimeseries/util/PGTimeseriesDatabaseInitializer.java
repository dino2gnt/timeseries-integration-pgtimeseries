package org.opennms.timeseries.impl.pgtimeseries.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;

/**
 * The pgtimeseries plugin uses the opennms database. But it needs extra tables.
 * This class offers helper methods to check for and create the tables.
 */
@AllArgsConstructor
@Slf4j
public class PGTimeseriesDatabaseInitializer {

    private static DataSource dataSource;

    private static PGTimeseriesConfig config;

    private static HikariDataSource hikariExtDs = new HikariDataSource();
    private static HikariDataSource hikariAdmDs = new HikariDataSource();
    private static final MetricRegistry extMetrics = new MetricRegistry();
    public static final JmxReporter ExtReporter = JmxReporter.forRegistry(extMetrics).inDomain("org.opennms.timeseries.impl.pgtimeseries").build();


    public PGTimeseriesDatabaseInitializer(final DataSource dataSource, final PGTimeseriesConfig config) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.config =  Objects.requireNonNull(config);
    }

    public static boolean isAdminDatasourceURLAvailable() throws SQLException {
        try {
            if (config.getAdminDatasourceURL() != null && !config.getAdminDatasourceURL().isEmpty()) {
                if (hikariAdmDs.getJdbcUrl() == null || !hikariAdmDs.getJdbcUrl().equals(config.getAdminDatasourceURL())) {
                    hikariAdmDs.setJdbcUrl(config.getAdminDatasourceURL());
                    hikariAdmDs.setPoolName("pgtimeseries-admin");
                    return true;
                }
                else {
                    return true;
                }
            }
        } catch ( Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public static boolean isExternalDatasourceURLAvailable() throws SQLException {
        try {
            if (config.getExternalDatasourceURL() != null && !config.getExternalDatasourceURL().isEmpty()) {
                if (hikariExtDs.getJdbcUrl() == null || !hikariExtDs.getJdbcUrl().equals(config.getExternalDatasourceURL())) {
                    hikariExtDs.setJdbcUrl(config.getExternalDatasourceURL());
                    hikariExtDs.setPoolName("pgtimeseries-external");
                    hikariExtDs.setMaximumPoolSize(config.getConnectionPoolSize());
                    hikariExtDs.setMetricRegistry(extMetrics);
                    ExtReporter.start();
                    return true;
                }
                else {
                    return true;
                }
            }
        } catch ( Exception e) {
            log.debug("Something threw an exception? : " + e);
            return false;
        }
        return false;
    }

    public static Connection getWhichDataSourceConnection() throws SQLException {
        if (isExternalDatasourceURLAvailable()) {
            return hikariExtDs.getConnection();
        }
        else {
            return dataSource.getConnection();
        }
    }

    public static boolean isPGTimeseriesExtensionInstalled() throws SQLException {
        DBUtils db = new DBUtils();
        try {
            Connection conn = getWhichDataSourceConnection();
            if (conn == null) {
                log.error("Hrmm. The connection is null and it shouldn't be. Did the connection fail?");
            }
            db.watch(conn);
            Statement stmt = conn.createStatement();
            db.watch(stmt);

            ResultSet result = stmt.executeQuery("select count(*) from pg_extension where extname = 'timeseries'");
            db.watch(result);
            result.next();
            return result.getInt(1) > 0;
        } finally {
            db.cleanUp();
        }
    }

    static boolean doesPGTimeseriesTableExist(String tableName) throws SQLException {
        DBUtils db = new DBUtils();
        try {
            Connection conn = getWhichDataSourceConnection();
            db.watch(conn);
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, null, tableName, null);
            db.watch(tables);
            return tables.next();
        } finally {
            db.cleanUp();
        }
    }

    public static boolean isPGTimeseriesTablesExisting() throws SQLException {
        return doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TIME_SERIES)
                && doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_METRIC)
                && doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TAG);
    }

    public static void createTables() throws SQLException {
        DBUtils db = new DBUtils();
        String sql;
        PreparedStatement statement;
        try {
            Connection conn = getWhichDataSourceConnection();
            /*
            Need to create the tables as the regular user so the ownership is correct...
            if (isAdminDatasourceURLAvailable()) {
                log.info("Using admin datasource to create tables: " + hikariAdmDs.toString());
                conn = hikariAdmDs.getConnection();
            }
            else {
                conn = getWhichDataSourceConnection();
            }
            */
            db.watch(conn);
            Statement stmt = conn.createStatement();
            db.watch(stmt);
            // Create the table to hold the series
            executeQuery(stmt, "CREATE TABLE IF NOT EXISTS pgtimeseries_time_series(keyid bigint, time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NULL) PARTITION BY RANGE (time)");
            // create the metric sequence
            executeQuery(stmt, "CREATE SEQUENCE IF NOT EXISTS pgtimeseries_metric_seq no cycle");
            // Metrics table
            executeQuery(stmt, "CREATE TABLE IF NOT EXISTS pgtimeseries_metric(keyid bigint, key TEXT NOT NULL PRIMARY KEY, UNIQUE (key))");
            // tag table
            executeQuery(stmt, "CREATE TABLE IF NOT EXISTS pgtimeseries_tag(keyid bigint, key TEXT, value TEXT NOT NULL, type TEXT NOT NULL, UNIQUE (keyid, key, value, type))");
            // let pg_timseries take over the table; default partition for 1 week duration
            if (config.getbackfillStart() != null && !config.getbackfillStart().isEmpty()) {
                log.info("Using backfillStart timestamp {}", config.getbackfillStart());
                sql = "SELECT enable_ts_table('pgtimeseries_time_series', partition_duration := cast(? as interval), initial_table_start := cast(? as timestamptz))";
                statement = conn.prepareStatement(sql);
                db.watch(statement);
                statement.setString(1, config.getpartitionDuration());
                statement.setString(2, config.getbackfillStart());
                statement.executeQuery();
            }
            else {
                sql = "SELECT enable_ts_table('pgtimeseries_time_series', partition_duration := cast(? as interval))";
                statement = conn.prepareStatement(sql);
                db.watch(statement);
                statement.setString(1, config.getpartitionDuration());
                statement.executeQuery();
            }

            // Default retention for 1 year
            sql = "SELECT set_ts_retention_policy('pgtimeseries_time_series', cast(? as interval))";
            statement = conn.prepareStatement(sql);
            db.watch(statement);
            statement.setString(1, config.getretentionPolicy());
            statement.executeQuery();

            // enable compression after 3 months
            sql =  "SELECT set_ts_compression_policy('pgtimeseries_time_series', cast(? as interval))";
            statement = conn.prepareStatement(sql);
            db.watch(statement);
            statement.setString(1, config.getcompressionPolicy());
            statement.executeQuery();

            //Indexes.  This is a WAG.
            executeQuery(stmt, "CREATE INDEX ON pgtimeseries_time_series(time DESC)");
            executeQuery(stmt, "CREATE INDEX ON pgtimeseries_time_series(keyid)");
            executeQuery(stmt, "CREATE INDEX ON pgtimeseries_metric(key)");
        } finally {
            db.cleanUp();
        }
    }

    public static void installExtension() throws SQLException {
        DBUtils db = new DBUtils();
        try {
            Connection conn = getWhichDataSourceConnection();
            if (isAdminDatasourceURLAvailable()) {
                log.info("Using admin datasource to install extension: " + hikariAdmDs.toString());
                conn = hikariAdmDs.getConnection();
            }
            else {
                log.info("Using configured datasource to install extension: " + conn.toString());
            }
            if (conn == null) {
                log.error("Connection is null and it shouldn't be. Did the connection fail?");
            }
            db.watch(conn);
            Statement stmt = conn.createStatement();
            db.watch(stmt);
            executeQuery(stmt, "CREATE EXTENSION timeseries CASCADE");
            executeQuery(stmt, "ALTER TABLE ts_config OWNER TO opennms");
            executeQuery(stmt, "ALTER TABLE part_config OWNER TO opennms");
            executeQuery(stmt, "ALTER TABLE part_config_sub OWNER TO opennms");
            executeQuery(stmt, "ALTER TABLE ts_table_info OWNER TO opennms");
            executeQuery(stmt, "ALTER TABLE ts_part_info OWNER TO opennms");
        } finally {
            db.cleanUp();
        }
    }

    private static void executeQuery(Statement stmt, final String sql) throws SQLException {
        log.debug(sql);
        stmt.execute(sql);
    }

    public void initializeIfNeeded() throws SQLException {
        log.debug("Starting up pgtimeseries plugin with config: " + config.toString());
        if (config.getCreateTablesOnInstall()) {
            // Check Plugin
            if (!isPGTimeseriesExtensionInstalled()) {
                log.info("It looks like pg_timeseries extension is not installed. Attempting to install the extension....");
                installExtension();
            }

            // Check and create tables
            if (isPGTimeseriesTablesExisting()) {
                log.info("pg_timeseries tables exist. We are good to go.");
            } else {
                log.info("pg_timeseries tables are missing. Will create them now.");
                createTables();
                log.info("pg_timeseries tables created.");
            }
        }
        else {
            log.info("Skipping table creation as createTablesOnInstall is false");
        }
    }
}
