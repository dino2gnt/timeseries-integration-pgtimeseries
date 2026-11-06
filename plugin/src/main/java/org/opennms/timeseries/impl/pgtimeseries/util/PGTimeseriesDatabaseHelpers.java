package org.opennms.timeseries.impl.pgtimeseries.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;

/**
 * Datasource selection for the pgtimeseries plugin.
 *
 * <p>The plugin reads and writes either the local OpenNMS database (the {@link DataSource} OpenNMS provides) or,
 * when {@code externalDatasourceURL} is set, a connection pool of its own. {@code adminDatasourceURL} names a
 * third, privileged connection that only {@link LegacyMigration} uses. This class owns that shared state because
 * the Karaf shell commands must make the same choice the storage service made. Schema creation lives in
 * {@link SchemaManager}.</p>
 *
 * <p>The plugin's pools are built lazily from the current configuration and rebuilt when the URL (or pool size)
 * changes, so an edited {@code .cfg} takes effect on the next connection instead of being ignored. A pool that
 * cannot be set up or connected is reported as an error; the plugin never silently falls back to the local
 * database when an external one was configured.</p>
 */
@Slf4j
public final class PGTimeseriesDatabaseHelpers {

    private static final String JMX_DOMAIN = "org.opennms.timeseries.impl.pgtimeseries";

    private static DataSource dataSource;
    private static PGTimeseriesConfig config;

    private static HikariDataSource external;
    private static HikariDataSource admin;
    private static final MetricRegistry extMetrics = new MetricRegistry();
    private static final JmxReporter extReporter = JmxReporter.forRegistry(extMetrics).inDomain(JMX_DOMAIN).build();
    private static boolean reporterStarted;

    private PGTimeseriesDatabaseHelpers() {
        // static utility class
    }

    /** Sets the shared local datasource and configuration. Must be called before any other method. */
    public static synchronized void configure(final DataSource dataSource, final PGTimeseriesConfig config) {
        PGTimeseriesDatabaseHelpers.dataSource = Objects.requireNonNull(dataSource);
        PGTimeseriesDatabaseHelpers.config = Objects.requireNonNull(config);
    }

    public static synchronized PGTimeseriesConfig getConfig() {
        return config;
    }

    public static synchronized DataSource getDataSource() {
        return dataSource;
    }

    /** True when an {@code adminDatasourceURL} is configured; its pool is (re)built as needed. */
    public static synchronized boolean isAdminDatasourceURLAvailable() throws SQLException {
        final String url = config == null ? null : config.getAdminDatasourceURL();
        if (isBlank(url)) {
            admin = close(admin);
            return false;
        }
        admin = pool(admin, url, "pgtimeseries-admin", 2, null);
        return true;
    }

    /** True when an {@code externalDatasourceURL} is configured; its pool is (re)built as needed. */
    public static synchronized boolean isExternalDatasourceURLAvailable() throws SQLException {
        final String url = config == null ? null : config.getExternalDatasourceURL();
        if (isBlank(url)) {
            external = close(external);
            return false;
        }
        external = pool(external, url, "pgtimeseries-external", config.getConnectionPoolSize(), extMetrics);
        return true;
    }

    /** A connection to the time series database: the external pool when configured, else the local datasource. */
    public static Connection getWhichDataSourceConnection() throws SQLException {
        final DataSource ds;
        synchronized (PGTimeseriesDatabaseHelpers.class) {
            // choose under the lock, connect outside it: a pool may block up to its connection timeout
            ds = isExternalDatasourceURLAvailable() ? external : dataSource;
        }
        if (ds == null) {
            throw new SQLException("The pgtimeseries datasource has not been configured yet");
        }
        return ds.getConnection();
    }

    /** A connection from the admin pool. Fails when no {@code adminDatasourceURL} is configured. */
    public static Connection getAdminConnection() throws SQLException {
        final DataSource ds;
        synchronized (PGTimeseriesDatabaseHelpers.class) {
            ds = isAdminDatasourceURLAvailable() ? admin : null;
        }
        if (ds == null) {
            throw new SQLException("No adminDatasourceURL is configured");
        }
        return ds.getConnection();
    }

    /** Closes the plugin's own pools and their JMX reporter. The local OpenNMS datasource is not ours to close. */
    public static synchronized void shutdown() {
        external = close(external);
        admin = close(admin);
        if (reporterStarted) {
            extReporter.stop();
            reporterStarted = false;
        }
    }

    /**
     * Returns {@code current} if it already serves {@code url} with {@code size} connections, otherwise closes it
     * and builds a replacement. The pool connects lazily, so a database that is down surfaces as an
     * {@link SQLException} from {@code getConnection()}, exactly like the local datasource would.
     */
    private static HikariDataSource pool(final HikariDataSource current, final String url, final String name,
                                         final int size, final MetricRegistry metrics) throws SQLException {
        final int poolSize = Math.max(1, size);
        if (current != null && !current.isClosed() && url.equals(current.getJdbcUrl())
                && current.getMaximumPoolSize() == poolSize) {
            return current;
        }
        close(current); // unregisters its metrics, so the replacement can register under the same pool name
        try {
            final HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl(url);
            ds.setPoolName(name);
            ds.setMaximumPoolSize(poolSize);
            // pgjdbc turns a batch of single-row INSERTs into multi-row statements: far fewer round trips per flush.
            ds.addDataSourceProperty("reWriteBatchedInserts", "true");
            if (metrics != null) {
                ds.setMetricRegistry(metrics);
                if (!reporterStarted) {
                    extReporter.start();
                    reporterStarted = true;
                }
            }
            log.info("Using {} pool for {} (max {} connections)", name, redact(url), poolSize);
            return ds;
        } catch (RuntimeException e) {
            throw new SQLException("Could not set up the " + name + " connection pool for " + redact(url), e);
        }
    }

    private static HikariDataSource close(final HikariDataSource ds) {
        if (ds != null) {
            try {
                ds.close();
            } catch (RuntimeException e) {
                log.warn("Could not close the {} connection pool", ds.getPoolName(), e);
            }
        }
        return null;
    }

    private static boolean isBlank(final String s) {
        return s == null || s.isBlank();
    }

    /** The JDBC URL without its query string, which is where the password usually lives. */
    static String redact(final String url) {
        final int q = url.indexOf('?');
        return q < 0 ? url : url.substring(0, q) + "?...";
    }
}
