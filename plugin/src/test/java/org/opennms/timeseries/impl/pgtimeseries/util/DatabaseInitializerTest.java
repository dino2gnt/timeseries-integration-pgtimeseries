package org.opennms.timeseries.impl.pgtimeseries.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * Integration test for {@link PGTimeseriesDatabaseInitializer} against a real pg_timeseries database.
 *
 * <p>The plugin connects to the database as the OpenNMS application role (here {@code opennms}), so this test
 * does the same: it creates that role and runs the initializer through it. That mirrors production and lets us
 * assert that the plugin's tables end up owned by the application role rather than a superuser.</p>
 *
 * <p>Requires a container runtime (docker/nerdctl/podman/ctr); the class is skipped (not failed) when none is
 * available. The image is configurable via {@code -Dpgtimeseries.test.image=...}.</p>
 */
@Slf4j
public class DatabaseInitializerTest {

    /** The application role the plugin connects as; the OpenNMS database user is conventionally "opennms". */
    private static final String APP_ROLE = "opennms";
    private static final String APP_PASSWORD = "opennms";

    private static ContainerizedDatabase database;
    private static DataSource dataSource;

    @BeforeClass
    public static void setUpContainer() throws Exception {
        Assume.assumeTrue("No container runtime is available; skipping pg_timeseries integration test.",
                ContainerizedDatabase.isRuntimeAvailable());

        try {
            database = ContainerizedDatabase.start();
        } catch (RuntimeException e) {
            // A runtime is present but cannot actually launch a container here (e.g. rootless containerd not
            // configured, image pull blocked). That is environmental, so skip rather than fail the build.
            Assume.assumeNoException("Container runtime present but could not start a database; skipping.", e);
        }

        // Create the application role as a superuser login (it must be able to install the extension), using the
        // bootstrap superuser the image ships with.
        try (Connection c = DriverManager.getConnection(database.jdbcUrl(), database.username(), database.password());
             Statement st = c.createStatement()) {
            st.execute("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '" + APP_ROLE + "') "
                    + "THEN CREATE ROLE " + APP_ROLE + " WITH LOGIN SUPERUSER PASSWORD '" + APP_PASSWORD + "'; END IF; END $$;");
        }

        // The plugin connects as the application role, so the schema it creates is owned by that role.
        dataSource = applicationDatasource();
        new PGTimeseriesDatabaseInitializer(dataSource, PGTimeseriesConfig.builder().build()).initializeIfNeeded();
    }

    @AfterClass
    public static void tearDown() {
        if (database != null) {
            database.close();
        }
    }

    private static DataSource applicationDatasource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(APP_ROLE);
        config.setPassword(APP_PASSWORD);
        return new HikariDataSource(config);
    }

    @Test
    public void installsExtensionAndCreatesTables() throws SQLException {
        assertTrue("pg_timeseries extension should be installed",
                PGTimeseriesDatabaseInitializer.isPGTimeseriesExtensionInstalled());
        assertTrue(PGTimeseriesDatabaseInitializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TIME_SERIES));
        assertTrue(PGTimeseriesDatabaseInitializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_METRIC));
        assertTrue(PGTimeseriesDatabaseInitializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TAG));
    }

    @Test
    public void pluginTablesAreOwnedByTheApplicationRole() throws SQLException {
        // The plugin deliberately creates its tables as the connecting (application) role so that the running
        // OpenNMS instance owns them; verify that ownership for each table, including the partitioned one.
        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_TIME_SERIES));
        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_METRIC));
        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_TAG));
    }

    @Test
    public void initializeIsIdempotent() throws SQLException {
        // A second run (e.g. plugin restart) must not fail and must leave the schema intact.
        new PGTimeseriesDatabaseInitializer(dataSource, PGTimeseriesConfig.builder().build()).initializeIfNeeded();
        assertTrue(PGTimeseriesDatabaseInitializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TIME_SERIES));
        assertTrue(PGTimeseriesDatabaseInitializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_METRIC));
        assertTrue(PGTimeseriesDatabaseInitializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TAG));
    }

    /**
     * Returns the owning role of a table. Uses pg_class/pg_get_userbyid so it works for both ordinary tables and
     * the partitioned {@code pgtimeseries_time_series} parent (relkind 'p').
     */
    private static String tableOwner(String table) throws SQLException {
        final String sql = "SELECT pg_catalog.pg_get_userbyid(c.relowner) "
                + "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "WHERE n.nspname = 'public' AND c.relname = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
