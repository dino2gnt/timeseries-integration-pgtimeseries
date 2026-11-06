package org.opennms.timeseries.impl.pgtimeseries.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.util.ContainerizedDatabase;
import org.opennms.timeseries.impl.pgtimeseries.util.LegacyMigration;
import org.opennms.timeseries.impl.pgtimeseries.util.SchemaManager;
import org.opennms.timeseries.impl.pgtimeseries.util.TableNames;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Upgrade test: a database that the 0.2.x plugin set up through the pg_timeseries extension (pg_partman
 * partitions, ts_config policy, pg_cron jobs) must be picked up by the new extension-free code without any
 * change to the partition layout.
 *
 * <p>Uses the pg_timeseries image ({@code -Dpgtimeseries.test.legacyImage=...} to override) and is skipped when
 * no container runtime is available or the image cannot be started.</p>
 */
public class LegacyExtensionCompatibilityTest {

    private static final String LEGACY_IMAGE =
            System.getProperty("pgtimeseries.test.legacyImage", "ghcr.io/chuckhend/pg18-timeseries:latest");

    private static ContainerizedDatabase database;
    private static DataSource dataSource;

    @BeforeClass
    public static void setUpLegacyDatabase() throws Exception {
        Assume.assumeTrue("No container runtime is available; skipping legacy compatibility test.",
                ContainerizedDatabase.isRuntimeAvailable());
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            database = ContainerizedDatabase.start(LEGACY_IMAGE);
        } catch (RuntimeException e) {
            Assume.assumeNoException("Could not start the pg_timeseries image; skipping.", e);
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.username());
        config.setPassword(database.password());
        dataSource = new HikariDataSource(config);

        // Exactly what PGTimeseriesDatabaseInitializer did in 0.2.x (the extension owner tweaks aside).
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE EXTENSION timeseries CASCADE");
            st.execute("CREATE TABLE pgtimeseries_time_series(keyid bigint, time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NULL) PARTITION BY RANGE (time)");
            st.execute("CREATE SEQUENCE pgtimeseries_metric_seq no cycle");
            st.execute("CREATE TABLE pgtimeseries_metric(keyid bigint DEFAULT nextval('pgtimeseries_metric_seq'), key TEXT NOT NULL PRIMARY KEY)");
            st.execute("CREATE TABLE pgtimeseries_tag(keyid bigint, key TEXT, value TEXT NOT NULL, type TEXT NOT NULL, UNIQUE (keyid, key, value, type))");
            st.execute("SELECT enable_ts_table('pgtimeseries_time_series', partition_duration := '1 week'::interval)");
            st.execute("SELECT set_ts_retention_policy('pgtimeseries_time_series', '9 months'::interval)");
            st.execute("SELECT set_ts_compression_policy('pgtimeseries_time_series', '3 months'::interval)");
            st.execute("CREATE INDEX pgtimeseries_time_series_keyid_time_idx ON pgtimeseries_time_series(keyid, time DESC)");
            st.execute("CREATE INDEX pgtimeseries_time_series_time_idx ON pgtimeseries_time_series(time DESC)");
        }
    }

    @AfterClass
    public static void tearDown() {
        if (database != null) {
            database.close();
        }
    }

    @Test
    public void adoptsAPartmanManagedTableWithoutChangingItsLayout() throws SQLException {
        // The retention an operator set through ts-config lives in ts_config, not in the config file.
        new SchemaManager(dataSource, PGTimeseriesConfig.builder().retentionPolicy("1 year").build());
        assertTrue(SchemaManager.isLegacyExtensionPresent());
        assertTrue(SchemaManager.isLegacyPartmanManaged());

        PartitionManager pm = new PartitionManager(dataSource::getConnection, TableNames.PGTIMESERIES_TIME_SERIES,
                TableNames.PGTIMESERIES_CONFIG, null, Clock.systemUTC());
        // pg_partman's layout before we touch anything (it premakes 5 weeks either side of now)
        List<Partition> before = pm.listPartitions();
        List<Partition> rangesBefore = before.stream().filter(Partition::isRange).collect(Collectors.toList());
        assertTrue("partman should have created partitions", rangesBefore.size() >= 5);
        assertTrue(before.get(0).isDefault());
        assertEquals(TableNames.PGTIMESERIES_DEFAULT_PARTITION, before.get(0).name());
        for (Partition p : rangesBefore) {
            assertTrue("partman naming expected, got " + p.name(), p.name().matches("pgtimeseries_time_series_p\\d{8}"));
            assertTrue(p.lowerBound().isBefore(p.upperBound()));
        }
        assertTrue(rangesBefore.stream().anyMatch(p -> p.contains(Instant.now())));

        SchemaManager.createSchema();

        // Policy was imported from ts_config, not seeded from the configuration file.
        try (Connection c = dataSource.getConnection()) {
            PartitionPolicy policy = pm.loadPolicy(c);
            assertEquals("7 days", policy.partitionDuration());
            assertEquals("1 mon", policy.partitionLeadTime());
            assertEquals("9 mons", policy.retention());
            assertEquals("3 mons", policy.compression());
        }

        // partman already covers now + 1 month, so nothing was created and the layout is byte-for-byte the same.
        List<Partition> after = pm.listPartitions();
        assertEquals(names(before), names(after));
        for (int i = 0; i < before.size(); i++) {
            assertEquals(before.get(i).lowerBound(), after.get(i).lowerBound());
            assertEquals(before.get(i).upperBound(), after.get(i).upperBound());
        }

        // A full maintenance pass against the partman layout is clean: nothing to rescue, create or drop.
        MaintenanceReport report = pm.runMaintenance();
        assertFalse(report.wasSkipped());
        assertTrue(report.getErrors().toString(), report.getErrors().isEmpty());
        assertFalse(report.didWork());

        // The extension's own objects are untouched so far; migration is a separate, explicit step.
        assertEquals(2, cronJobs());

        // ---- migration -------------------------------------------------------

        // Simulate what pg_partman's keep-table retention leaves behind: a detached, never dropped partition.
        String orphan = rangesBefore.get(0).name();
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("ALTER TABLE pgtimeseries_time_series DETACH PARTITION " + orphan);
        }

        LegacyMigration.Result dry = LegacyMigration.run(new LegacyMigration.Options(true, true));
        assertTrue(dry.getLines().toString(), dry.getLines().stream().anyMatch(l -> l.startsWith("Would drop the pg_timeseries extension")));
        assertEquals("dry run must not change anything", 2, cronJobs());
        assertTrue(SchemaManager.isLegacyExtensionPresent());
        assertEquals(1, dry.getOrphansRemaining().size());
        assertEquals(orphan, dry.getOrphansRemaining().get(0).name());

        LegacyMigration.Result result = LegacyMigration.run(new LegacyMigration.Options(false, true));
        assertTrue(result.getLines().toString(), result.isComplete());
        assertEquals(0, cronJobs());
        assertFalse(SchemaManager.isLegacyExtensionPresent());
        assertFalse(SchemaManager.isLegacyPartmanManaged());
        assertTrue(SchemaManager.tablesExist());
        assertEquals("orphan must be dropped", 0, scalarInt("SELECT count(*) FROM pg_class WHERE relname = '" + orphan + "'"));
        assertEquals("pg_partman's template table must be gone", 0,
                scalarInt("SELECT count(*) FROM pg_class WHERE relname LIKE 'template%pgtimeseries_time_series'"));
        assertEquals("the extension's config table must be gone", 0, scalarInt("SELECT count(*) FROM pg_class WHERE relname = 'ts_config'"));

        // The remaining layout is exactly partman's minus the orphan, the imported policy survived, and a
        // maintenance pass is still clean.
        List<String> expected = names(after);
        expected.remove(orphan);
        assertEquals(expected, names(pm.listPartitions()));
        try (Connection c = dataSource.getConnection()) {
            assertEquals("9 mons", pm.loadPolicy(c).retention());
            assertTrue(pm.listOrphans(c).isEmpty());
        }
        MaintenanceReport afterMigration = pm.runMaintenance();
        assertTrue(afterMigration.getErrors().toString(), afterMigration.getErrors().isEmpty());
        assertFalse(afterMigration.didWork());

        // Running it again is a harmless no-op.
        LegacyMigration.Result again = LegacyMigration.run(new LegacyMigration.Options(false, false));
        assertTrue(again.getLines().get(0).startsWith("Nothing to migrate"));
    }

    private static int cronJobs() throws SQLException {
        return scalarInt("SELECT count(*) FROM cron.job WHERE jobname IN ('partman-maintenance', 'timeseries-compression')");
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static List<String> names(List<Partition> partitions) {
        return partitions.stream().map(Partition::name).collect(Collectors.toList());
    }
}
