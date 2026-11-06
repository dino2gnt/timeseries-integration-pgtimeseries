package org.opennms.timeseries.impl.pgtimeseries.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.partition.Partition;
import org.opennms.timeseries.impl.pgtimeseries.partition.PartitionManager;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Integration test for {@link SchemaManager} against a stock PostgreSQL database with no extensions.
 *
 * <p>The plugin connects as the OpenNMS application role (here {@code opennms}, owner of the database as the
 * OpenNMS installer sets it up), so this test does the same and asserts that everything the plugin creates is
 * owned by that role and that no extension gets installed.</p>
 *
 * <p>Requires a container runtime (docker/nerdctl/podman/ctr); the class is skipped (not failed) when none is
 * available.</p>
 */
public class SchemaManagerTest {

    private static final String APP_ROLE = "opennms";
    private static final String APP_PASSWORD = "opennms";

    /** A fixed "now" so the bootstrap partition layout is deterministic. */
    private static final Instant NOW = Instant.parse("2025-01-15T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private static ContainerizedDatabase database;
    private static DataSource dataSource;

    @BeforeClass
    public static void setUpContainer() throws Exception {
        Assume.assumeTrue("No container runtime is available; skipping pgtimeseries integration test.",
                ContainerizedDatabase.isRuntimeAvailable());
        // Partition bounds and names are computed in the JDBC session's timezone, which pgjdbc takes from the JVM.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            database = ContainerizedDatabase.start();
        } catch (RuntimeException e) {
            Assume.assumeNoException("Container runtime present but could not start a database; skipping.", e);
        }
        try (Connection c = DriverManager.getConnection(database.jdbcUrl(), database.username(), database.password());
             Statement st = c.createStatement()) {
            st.execute("CREATE ROLE " + APP_ROLE + " WITH LOGIN PASSWORD '" + APP_PASSWORD + "'");
            st.execute("ALTER DATABASE postgres OWNER TO " + APP_ROLE);
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(APP_ROLE);
        config.setPassword(APP_PASSWORD);
        dataSource = new HikariDataSource(config);
    }

    @AfterClass
    public static void tearDown() {
        if (database != null) {
            database.close();
        }
    }

    @Before
    public void dropEverything() throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            // Drop every plugin relation, including standalone tables with partition-style names left by tests.
            st.execute("DO $$ DECLARE r record; BEGIN "
                    + "FOR r IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND (c.relname LIKE 'pgtimeseries_%' OR c.relname = 'ts_config') "
                    + "LOOP EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', r.relname); END LOOP; END $$");
            st.execute("DROP SEQUENCE IF EXISTS pgtimeseries_metric_seq");
        }
    }

    private static SchemaManager manager(PGTimeseriesConfig config) {
        return new SchemaManager(dataSource, config);
    }

    private static PartitionManager partitionManager() {
        return new PartitionManager(dataSource::getConnection, TableNames.PGTIMESERIES_TIME_SERIES,
                TableNames.PGTIMESERIES_CONFIG, null, CLOCK);
    }

    private static List<Partition> rangePartitions() throws SQLException {
        return partitionManager().listPartitions().stream().filter(Partition::isRange).collect(Collectors.toList());
    }

    @Test
    public void createsTheSchemaWithoutAnyExtension() throws SQLException {
        manager(PGTimeseriesConfig.builder().build());
        SchemaManager.createSchema(CLOCK);

        assertTrue(SchemaManager.tablesExist());
        assertTrue(SchemaManager.doesTableExist(TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        assertFalse(SchemaManager.isLegacyExtensionPresent());
        assertFalse(SchemaManager.isLegacyPartmanManaged());
        assertEquals("no extension may be installed", 0, scalarInt(
                "SELECT count(*) FROM pg_extension WHERE extname IN ('timeseries', 'pg_partman', 'pg_cron', 'citus_columnar')"));
    }

    @Test
    public void addsTheTagLookupIndexToASchemaThatPredatesIt() throws SQLException {
        final String count = "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = '"
                + SchemaManager.TAG_LOOKUP_INDEX + "'";
        manager(PGTimeseriesConfig.builder().build());
        SchemaManager.createSchema(CLOCK);
        assertEquals(1, scalarInt(count));

        // a database set up by 0.2.x has the tables but not this index
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP INDEX " + SchemaManager.TAG_LOOKUP_INDEX);
        }
        assertEquals(0, scalarInt(count));
        SchemaManager.createSchema(CLOCK);
        assertEquals(1, scalarInt(count));
    }

    @Test
    public void everythingIsOwnedByTheApplicationRole() throws SQLException {
        manager(PGTimeseriesConfig.builder().build());
        SchemaManager.createSchema(CLOCK);

        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_TIME_SERIES));
        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_METRIC));
        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_TAG));
        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_CONFIG));
        assertEquals(APP_ROLE, tableOwner(TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        for (Partition p : rangePartitions()) {
            assertEquals(APP_ROLE, tableOwner(p.name()));
        }
    }

    @Test
    public void seedsThePolicyFromTheConfiguration() throws SQLException {
        manager(PGTimeseriesConfig.builder()
                .partitionDuration("1 day")
                .partitionLeadTime("2 weeks")
                .retentionPolicy("6 months")
                .compressionPolicy("")
                .build());
        SchemaManager.createSchema(CLOCK);

        assertEquals("1 day", policyColumn("partition_duration"));
        assertEquals("14 days", policyColumn("partition_lead_time"));
        assertEquals("6 mons", policyColumn("retention_duration"));
        assertNull("blank compression policy means none", policyColumn("compression_duration"));
    }

    @Test
    public void bootstrapsPartitionsAroundNowLikePartmanDid() throws SQLException {
        // 1 week partitions, 1 month lead: from date_trunc('day', now - 1 month) through now + 1 month.
        manager(PGTimeseriesConfig.builder().build());
        SchemaManager.createSchema(CLOCK);

        List<Partition> ranges = rangePartitions();
        assertEquals(9, ranges.size());
        assertEquals(Instant.parse("2024-12-15T00:00:00Z"), ranges.get(0).lowerBound());
        assertEquals(Instant.parse("2025-02-16T00:00:00Z"), ranges.get(ranges.size() - 1).upperBound());
        assertEquals("pgtimeseries_time_series_p20241215", ranges.get(0).name());
        assertEquals("pgtimeseries_time_series_p20250209", ranges.get(ranges.size() - 1).name());
        assertContiguous(ranges);
        assertTrue("the partition containing now must exist",
                ranges.stream().anyMatch(p -> p.contains(NOW)));
    }

    @Test
    public void honoursBackfillStartForTheFirstPartition() throws SQLException {
        manager(PGTimeseriesConfig.builder().backfillStart("2024-11-01T00:00:00Z").build());
        SchemaManager.createSchema(CLOCK);

        List<Partition> ranges = rangePartitions();
        assertEquals(Instant.parse("2024-11-01T00:00:00Z"), ranges.get(0).lowerBound());
        // weekly from 2024-11-01: the last partition is the one whose lower bound is still within now + lead
        assertEquals(Instant.parse("2025-02-14T00:00:00Z"), ranges.get(ranges.size() - 1).lowerBound());
        assertEquals(16, ranges.size());
        assertContiguous(ranges);
    }

    @Test
    public void isIdempotent() throws SQLException {
        manager(PGTimeseriesConfig.builder().build());
        SchemaManager.createSchema(CLOCK);
        int before = rangePartitions().size();

        SchemaManager.createSchema(CLOCK);
        SchemaManager.createSchema(CLOCK);

        assertEquals(before, rangePartitions().size());
        assertEquals(1, scalarInt("SELECT count(*) FROM pgtimeseries_config"));
    }

    @Test
    public void importsThePolicyFromALegacyTsConfigTable() throws SQLException {
        // A database set up by the 0.2.x plugin has the extension's ts_config table with the policy an operator
        // may have tuned with ts-config. That must win over the configuration file on upgrade.
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pgtimeseries_time_series(keyid bigint, time TIMESTAMPTZ NOT NULL, "
                    + "value DOUBLE PRECISION NULL) PARTITION BY RANGE (time)");
            st.execute("CREATE TABLE ts_config(table_id regclass PRIMARY KEY, partition_duration interval NOT NULL, "
                    + "partition_lead_time interval NOT NULL, retention_duration interval, compression_duration interval)");
            st.execute("INSERT INTO ts_config VALUES ('pgtimeseries_time_series'::regclass, '1 day', '2 weeks', '9 months', NULL)");
        }
        manager(PGTimeseriesConfig.builder().build()); // file says 1 week / 1 year / 3 months
        SchemaManager.createSchema(CLOCK);

        assertEquals("1 day", policyColumn("partition_duration"));
        assertEquals("14 days", policyColumn("partition_lead_time"));
        assertEquals("9 mons", policyColumn("retention_duration"));
        assertNull(policyColumn("compression_duration"));
        // and the bootstrap used the imported width
        List<Partition> ranges = rangePartitions();
        assertEquals(Instant.parse("2025-01-01T00:00:00Z"), ranges.get(0).lowerBound());
        assertEquals(java.time.Duration.ofDays(1),
                java.time.Duration.between(ranges.get(0).lowerBound(), ranges.get(0).upperBound()));
    }

    @Test
    public void initializeIfNeededRespectsCreateTablesOnInstall() throws SQLException {
        manager(PGTimeseriesConfig.builder().createTablesOnInstall(false).build()).initializeIfNeeded();
        assertFalse(SchemaManager.tablesExist());

        manager(PGTimeseriesConfig.builder().createTablesOnInstall(true).build()).initializeIfNeeded();
        assertTrue(SchemaManager.tablesExist());
    }

    @Test
    public void aRoleWithoutDdlPrivilegesCanStartOnACompleteSchema() throws Exception {
        // A locked-down role (no CREATE on the schema, does not own the tables) must still be able to start the
        // plugin when the schema is already complete: no DDL may be attempted for objects that exist, and a
        // failing partition bootstrap must not abort initialisation.
        manager(PGTimeseriesConfig.builder().build());
        SchemaManager.createSchema(CLOCK);
        // the role is created by the bootstrap superuser; the grants by the application role that owns the objects
        try (Connection su = DriverManager.getConnection(database.jdbcUrl(), database.username(), database.password());
             Statement st = su.createStatement()) {
            st.execute("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'restricted') "
                    + "THEN CREATE ROLE restricted LOGIN PASSWORD 'restricted'; END IF; END $$");
        }
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO restricted");
            st.execute("GRANT USAGE, SELECT ON SEQUENCE pgtimeseries_metric_seq TO restricted");
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername("restricted");
        config.setPassword("restricted");
        try (HikariDataSource restricted = new HikariDataSource(config)) {
            new SchemaManager(restricted, PGTimeseriesConfig.builder().build()).initializeIfNeeded();
            assertTrue(SchemaManager.tablesExist());

            // far in the future the bootstrap has to create partitions, which this role cannot do: reported, not thrown
            org.opennms.timeseries.impl.pgtimeseries.partition.MaintenanceReport report =
                    SchemaManager.createSchema(Clock.fixed(Instant.parse("2031-01-01T00:00:00Z"), ZoneOffset.UTC));
            assertTrue(report.hasErrors());
            assertTrue(report.getErrors().get(0), report.getErrors().get(0).contains("permission denied"));
            new SchemaManager(restricted, PGTimeseriesConfig.builder().build()).initializeIfNeeded();
        } finally {
            manager(PGTimeseriesConfig.builder().build());
        }
    }

    @Test
    public void managePartitionsFalseCreatesNoRangePartitions() throws SQLException {
        manager(PGTimeseriesConfig.builder().managePartitions(false).build());
        SchemaManager.createSchema(CLOCK);

        assertTrue(SchemaManager.tablesExist());
        assertTrue(SchemaManager.doesTableExist(TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        assertEquals(0, rangePartitions().size());
    }

    // ---- helpers ----------------------------------------------------------

    private static void assertContiguous(List<Partition> ranges) {
        for (int i = 1; i < ranges.size(); i++) {
            assertEquals("partition " + ranges.get(i).name() + " must start where the previous one ends",
                    ranges.get(i - 1).upperBound(), ranges.get(i).lowerBound());
        }
    }

    private static String policyColumn(String column) throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT " + column + "::text FROM pgtimeseries_config WHERE table_name = 'pgtimeseries_time_series'")) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    /** Owning role of a table; works for ordinary tables and the partitioned parent (relkind 'p'). */
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
