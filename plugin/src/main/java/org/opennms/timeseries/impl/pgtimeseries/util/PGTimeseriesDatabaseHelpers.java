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
 * Datasource selection helpers for the pgtimeseries plugin.
 *
 * <p>The plugin can run against the local OpenNMS database, an external database, and/or an admin
 * connection used to install the extension. This class owns the shared datasource/config state and
 * decides which connection to hand out. It is intentionally separate from
 * {@link PGTimeseriesDatabaseInitializer}, which is concerned only with installing the extension and
 * creating tables.</p>
 */
@Slf4j
public final class PGTimeseriesDatabaseHelpers {

    private static DataSource dataSource;
    private static PGTimeseriesConfig config;

    private static final HikariDataSource hikariExtDs = new HikariDataSource();
    private static final HikariDataSource hikariAdmDs = new HikariDataSource();
    private static final MetricRegistry extMetrics = new MetricRegistry();
    public static final JmxReporter ExtReporter = JmxReporter.forRegistry(extMetrics).inDomain("org.opennms.timeseries.impl.pgtimeseries").build();

    private PGTimeseriesDatabaseHelpers() {
        // static utility class
    }

    /** Sets the shared local datasource and configuration. Must be called before any other method. */
    public static void configure(final DataSource dataSource, final PGTimeseriesConfig config) {
        PGTimeseriesDatabaseHelpers.dataSource = Objects.requireNonNull(dataSource);
        PGTimeseriesDatabaseHelpers.config = Objects.requireNonNull(config);
    }

    public static PGTimeseriesConfig getConfig() {
        return config;
    }

    public static DataSource getDataSource() {
        return dataSource;
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

    /** Returns a connection from the admin pool. Only valid when {@link #isAdminDatasourceURLAvailable()} is true. */
    public static Connection getAdminConnection() throws SQLException {
        return hikariAdmDs.getConnection();
    }
}
