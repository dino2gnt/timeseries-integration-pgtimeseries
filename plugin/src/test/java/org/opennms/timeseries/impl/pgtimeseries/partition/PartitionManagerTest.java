package org.opennms.timeseries.impl.pgtimeseries.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
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
import org.opennms.timeseries.impl.pgtimeseries.util.ContainerizedDatabase;
import org.opennms.timeseries.impl.pgtimeseries.util.MutableClock;
import org.opennms.timeseries.impl.pgtimeseries.util.SchemaManager;
import org.opennms.timeseries.impl.pgtimeseries.util.TableNames;

import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Integration test for {@link PartitionManager} and {@link MaintenanceService} against a stock PostgreSQL
 * database. Time is driven by a {@link MutableClock}, so "a month passes" is a method call.
 *
 * <p>Every test starts from a freshly bootstrapped schema laid out around {@link #T0} with the default policy
 * (1 week partitions, 1 month lead, 1 year retention, 3 months compression), which is nine partitions from
 * 2024-12-15 through 2025-02-16 plus the default partition.</p>
 */
public class PartitionManagerTest {

    private static final Instant T0 = Instant.parse("2025-01-15T12:00:00Z");
    private static final String PARENT = TableNames.PGTIMESERIES_TIME_SERIES;

    private static ContainerizedDatabase database;
    private static DataSource dataSource;

    private MutableClock clock;
    private PartitionManager pm;

    @BeforeClass
    public static void setUpContainer() {
        Assume.assumeTrue("No container runtime is available; skipping pgtimeseries integration test.",
                ContainerizedDatabase.isRuntimeAvailable());
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            database = ContainerizedDatabase.start();
        } catch (RuntimeException e) {
            Assume.assumeNoException("Container runtime present but could not start a database; skipping.", e);
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.username());
        config.setPassword(database.password());
        dataSource = new HikariDataSource(config);
    }

    @AfterClass
    public static void tearDown() {
        if (database != null) {
            database.close();
        }
    }

    @Before
    public void freshSchema() throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            // Drop every plugin relation, including standalone tables with partition-style names left by tests.
            st.execute("DO $$ DECLARE r record; BEGIN "
                    + "FOR r IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND (c.relname LIKE 'pgtimeseries_%' OR c.relname = 'ts_config') "
                    + "LOOP EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', r.relname); END LOOP; END $$");
            st.execute("DROP SEQUENCE IF EXISTS pgtimeseries_metric_seq");
        }
        clock = new MutableClock(T0);
        new SchemaManager(dataSource, PGTimeseriesConfig.builder().build());
        SchemaManager.createSchema(clock);
        pm = new PartitionManager(dataSource::getConnection, PARENT, TableNames.PGTIMESERIES_CONFIG, null, clock);
    }

    // ---- listing ----------------------------------------------------------

    @Test
    public void listsTheDefaultPartitionFirstAndParsesRangeBounds() throws SQLException {
        List<Partition> all = pm.listPartitions();

        assertEquals(10, all.size());
        Partition def = all.get(0);
        assertTrue(def.isDefault());
        assertEquals(TableNames.PGTIMESERIES_DEFAULT_PARTITION, def.name());
        assertNull(def.lowerBound());
        assertEquals("public", def.schema());
        assertEquals("heap", def.accessMethod());

        Partition first = all.get(1);
        assertTrue(first.isRange());
        assertEquals("pgtimeseries_time_series_p20241215", first.name());
        assertEquals(Instant.parse("2024-12-15T00:00:00Z"), first.lowerBound());
        assertEquals(Instant.parse("2024-12-22T00:00:00Z"), first.upperBound());
        assertTrue(first.totalBytes() >= 0);
    }

    @Test
    public void newPartitionsCarryTheParentsIndexes() throws SQLException {
        // Two indexes are defined on the parent; each partition must have both attached.
        for (Partition p : ranges()) {
            assertEquals("indexes on " + p.name(), 2, scalarInt(
                    "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND tablename = '" + p.name() + "'"));
        }
    }

    // ---- create ahead -----------------------------------------------------

    @Test
    public void createsExactlyTheMissingPartitionsAsTimeAdvances() throws SQLException {
        clock.plus(Duration.ofDays(21)); // 2025-02-05T12:00Z, so lead reaches 2025-03-05T12:00Z

        List<String> created = names(ensureFuture());

        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250216", "pgtimeseries_time_series_p20250223",
                "pgtimeseries_time_series_p20250302"), created);
        assertEquals(12, ranges().size());
        assertContiguous(ranges());

        assertTrue("a second run must be a no-op", ensureFuture().isEmpty());
        assertEquals(12, ranges().size());
    }

    @Test
    public void doesNotCreatePartitionsWhileLeadTimeIsStillCovered() throws SQLException {
        // now + lead is 2025-02-15T13:00Z, still inside the last bootstrap partition (which ends 2025-02-16)
        clock.plus(Duration.ofHours(1));
        assertTrue(ensureFuture().isEmpty());

        // a day later now + lead has crossed into the next week, so exactly one partition is needed
        clock.plus(Duration.ofDays(1));
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250216"), names(ensureFuture()));
    }

    @Test
    public void fillsGapsAndIgnoresUnrelatedFarFuturePartitions() throws SQLException {
        // A DBA dropped a middle partition and someone created an oddly aligned one far in the future.
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE pgtimeseries_time_series_p20250119");
            st.execute("CREATE TABLE pgtimeseries_time_series_manual PARTITION OF " + PARENT
                    + " FOR VALUES FROM ('2030-01-03 00:00:00+00') TO ('2030-01-10 00:00:00+00')");
        }

        List<String> created = names(ensureFuture());

        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250119"), created);
        Partition gap = ranges().stream().filter(p -> p.name().endsWith("p20250119")).findFirst().orElseThrow();
        assertEquals(Instant.parse("2025-01-19T00:00:00Z"), gap.lowerBound());
        assertEquals(Instant.parse("2025-01-26T00:00:00Z"), gap.upperBound());
        assertTrue(ranges().stream().anyMatch(p -> p.name().equals("pgtimeseries_time_series_manual")));
    }

    @Test
    public void wideningThePartitionWidthLeavesNoGap() throws SQLException {
        // ts-config -p '1 month' on a weekly series: the monthly series must start exactly where the last weekly
        // partition ends, not at a lattice point derived from an older partition's lower bound.
        setPolicy("partition_duration", "1 month");
        clock.plus(Duration.ofDays(40)); // 2025-02-24T12:00Z; lead reaches 2025-03-24T12:00Z

        List<Partition> created = ensureFuture();

        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250216", "pgtimeseries_time_series_p20250316"), names(created));
        assertEquals(Instant.parse("2025-02-16T00:00:00Z"), created.get(0).lowerBound());
        assertEquals(Instant.parse("2025-03-16T00:00:00Z"), created.get(0).upperBound());
        assertEquals(Instant.parse("2025-04-16T00:00:00Z"), created.get(1).upperBound());
        assertContiguous(ranges());
        assertTrue(ensureFuture().isEmpty());
        clock.plus(Duration.ofDays(30));
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250416"), names(ensureFuture()));
        assertContiguous(ranges());

        // a row in the formerly uncovered piece is routed straight to its partition, not to the default one
        insertSamples(Instant.parse("2025-02-20T00:00:00Z"), 1);
        assertEquals(1, scalarInt("SELECT count(*) FROM ONLY pgtimeseries_time_series_p20250216"));
    }

    @Test
    public void rescueFillsAGapLeftByAnEarlierWidthChange() throws SQLException {
        // A series that already has a gap (created by the pre-fix code, or by a DBA): weekly partitions ending
        // 2025-02-16 and a monthly one starting 2025-03-09, with rows stranded in between.
        setPolicy("partition_duration", "1 month");
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pgtimeseries_time_series_p20250309 PARTITION OF " + PARENT
                    + " FOR VALUES FROM ('2025-03-09 00:00:00+00') TO ('2025-04-09 00:00:00+00')");
        }
        insertSamples(Instant.parse("2025-02-20T00:00:00Z"), 2);
        assertEquals(2, scalarInt("SELECT count(*) FROM ONLY " + TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        clock.plus(Duration.ofDays(40));

        MaintenanceReport report = pm.runMaintenance();

        assertTrue(report.getErrors().toString(), report.getErrors().isEmpty());
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250216"), report.getRescued());
        assertEquals(0, scalarInt("SELECT count(*) FROM ONLY " + TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        assertEquals(2, scalarInt("SELECT count(*) FROM ONLY pgtimeseries_time_series_p20250216"));
        Partition gap = ranges().stream().filter(p -> p.name().endsWith("p20250216")).findFirst().orElseThrow();
        assertEquals(Instant.parse("2025-03-09T00:00:00Z"), gap.upperBound());
        assertContiguous(ranges());
    }

    @Test
    public void uncoveredCarvesACandidateAroundExistingPartitions() {
        Partition a = new Partition("public", "a", false, Instant.parse("2025-01-08T00:00:00Z"), Instant.parse("2025-01-15T00:00:00Z"), 0, 0, 0, null);
        Partition b = new Partition("public", "b", false, Instant.parse("2025-01-20T00:00:00Z"), Instant.parse("2025-01-22T00:00:00Z"), 0, 0, 0, null);
        PartitionManager.Candidate cand = new PartitionManager.Candidate(Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2025-02-01T00:00:00Z"), false);

        List<PartitionManager.Candidate> parts = PartitionManager.uncovered(Arrays.asList(a, b), cand);

        assertEquals(3, parts.size());
        assertEquals(Instant.parse("2025-01-01T00:00:00Z"), parts.get(0).lo());
        assertEquals(Instant.parse("2025-01-08T00:00:00Z"), parts.get(0).hi());
        assertEquals(Instant.parse("2025-01-15T00:00:00Z"), parts.get(1).lo());
        assertEquals(Instant.parse("2025-01-20T00:00:00Z"), parts.get(1).hi());
        assertEquals(Instant.parse("2025-01-22T00:00:00Z"), parts.get(2).lo());
        assertEquals(Instant.parse("2025-02-01T00:00:00Z"), parts.get(2).hi());
        assertTrue(PartitionManager.uncovered(Arrays.asList(a), new PartitionManager.Candidate(a.lowerBound(), a.upperBound(), false)).isEmpty());
    }

    @Test
    public void refusesToReuseAnOrphanTableWithThePartitionsName() throws SQLException {
        // What pg_partman's keep-table retention leaves behind: a detached table with a partition-style name.
        clock.plus(Duration.ofDays(21));
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pgtimeseries_time_series_p20250223 (LIKE " + PARENT + ")");
        }

        MaintenanceReport report = pm.runMaintenance();

        assertEquals(1, report.getErrors().size());
        assertTrue(report.getErrors().get(0).contains("pgtimeseries_time_series_p20250223"));
        // the partitions on either side of the orphan were still created, the orphan was not touched
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250216", "pgtimeseries_time_series_p20250302"), report.getCreated());
        assertEquals(1, scalarInt("SELECT count(*) FROM pg_class WHERE relname = 'pgtimeseries_time_series_p20250223' AND relispartition = false"));
        // and the next run reports the same single problem again rather than giving up
        assertEquals(1, pm.runMaintenance().getErrors().size());
    }

    // ---- retention --------------------------------------------------------

    @Test
    public void retentionDropsExpiredPartitionsAndReclaimsTheTables() throws SQLException {
        setPolicy("retention_duration", "2 weeks"); // cutoff 2025-01-01T12:00Z

        List<String> dropped;
        try (Connection c = dataSource.getConnection()) {
            dropped = names(pm.applyRetention(c, pm.loadPolicy(c), clock.instant()));
        }

        assertEquals(Arrays.asList("pgtimeseries_time_series_p20241215", "pgtimeseries_time_series_p20241222"), dropped);
        assertEquals(7, ranges().size());
        assertEquals("tables must be dropped, not merely detached", 0,
                scalarInt("SELECT count(*) FROM pg_class WHERE relname IN ('pgtimeseries_time_series_p20241215', 'pgtimeseries_time_series_p20241222')"));
        assertEquals("pgtimeseries_time_series_p20241229", ranges().get(0).name());
    }

    @Test
    public void retentionNeverDropsTheLastPartition() throws SQLException {
        setPolicy("retention_duration", "1 hour");
        clock.set(Instant.parse("2031-01-01T00:00:00Z"));

        try (Connection c = dataSource.getConnection()) {
            pm.applyRetention(c, pm.loadPolicy(c), clock.instant());
        }

        assertEquals(1, ranges().size());
        assertEquals("pgtimeseries_time_series_p20250209", ranges().get(0).name());
    }

    @Test
    public void retentionIsDisabledWhenThePolicyIsNull() throws SQLException {
        setPolicy("retention_duration", null);
        clock.set(Instant.parse("2031-01-01T00:00:00Z"));

        try (Connection c = dataSource.getConnection()) {
            assertTrue(pm.applyRetention(c, pm.loadPolicy(c), clock.instant()).isEmpty());
        }
        assertEquals(9, ranges().size());
    }

    // ---- default partition rescue -----------------------------------------

    @Test
    public void rescueMovesStrayRowsIntoAProperlyAlignedPartition() throws SQLException {
        // Older than the first partition (e.g. an RRD backfill): rows land in the default partition.
        Instant stray = Instant.parse("2024-06-01T10:00:00Z");
        insertSamples(stray, 3);
        assertEquals(3, scalarInt("SELECT count(*) FROM ONLY " + TableNames.PGTIMESERIES_DEFAULT_PARTITION));

        List<Partition> rescued;
        try (Connection c = dataSource.getConnection()) {
            rescued = pm.rescueDefaultPartition(c, pm.loadPolicy(c), clock.instant());
        }

        assertEquals(1, rescued.size());
        // 2024-12-15 minus 29 weeks; aligned to the existing lattice, not to the row's own timestamp
        assertEquals("pgtimeseries_time_series_p20240526", rescued.get(0).name());
        assertEquals(Instant.parse("2024-05-26T00:00:00Z"), rescued.get(0).lowerBound());
        assertEquals(Instant.parse("2024-06-02T00:00:00Z"), rescued.get(0).upperBound());
        assertEquals(0, scalarInt("SELECT count(*) FROM ONLY " + TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        assertEquals(3, scalarInt("SELECT count(*) FROM ONLY pgtimeseries_time_series_p20240526"));
        assertEquals("still visible through the parent", 3, scalarInt("SELECT count(*) FROM " + PARENT
                + " WHERE time = '" + stray + "'"));
        assertEquals("no empty intermediate partitions", 10, ranges().size());
    }

    @Test
    public void rescueCreatesOnePartitionPerOccupiedRangeOnly() throws SQLException {
        insertSamples(Instant.parse("2024-06-01T10:00:00Z"), 2);
        insertSamples(Instant.parse("2024-08-15T10:00:00Z"), 2);
        insertSamples(Instant.parse("2027-03-03T10:00:00Z"), 1); // far future, also stray

        List<String> rescued;
        try (Connection c = dataSource.getConnection()) {
            rescued = names(pm.rescueDefaultPartition(c, pm.loadPolicy(c), clock.instant()));
        }

        assertEquals(Arrays.asList("pgtimeseries_time_series_p20240526", "pgtimeseries_time_series_p20240811",
                "pgtimeseries_time_series_p20270228"), rescued);
        assertEquals(0, scalarInt("SELECT count(*) FROM ONLY " + TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        assertEquals(12, ranges().size());
        // and the far-future rescue does not derail the create-ahead step
        assertTrue(ensureFuture().isEmpty());
        clock.plus(Duration.ofDays(21));
        assertEquals(3, ensureFuture().size());
    }

    // ---- compression ------------------------------------------------------

    @Test
    public void compressionIsSkippedWithoutCitusColumnar() throws SQLException {
        setPolicy("compression_duration", "1 day");
        clock.set(Instant.parse("2031-01-01T00:00:00Z"));
        setPolicy("retention_duration", null);

        MaintenanceReport report = pm.runMaintenance();

        assertFalse(report.wasSkipped());
        assertTrue(report.getErrors().toString(), report.getErrors().isEmpty());
        assertTrue(report.getCompressed().isEmpty());
        assertTrue(ranges().stream().allMatch(p -> "heap".equals(p.accessMethod())));
    }

    // ---- full run ---------------------------------------------------------

    @Test
    public void runMaintenanceReportsEveryStep() throws SQLException {
        setPolicy("retention_duration", "2 weeks");
        insertSamples(Instant.parse("2024-06-01T10:00:00Z"), 1);
        clock.plus(Duration.ofDays(21)); // 2025-02-05; retention cutoff 2025-01-22

        MaintenanceReport report = pm.runMaintenance();

        assertFalse(report.wasSkipped());
        assertTrue(report.getErrors().toString(), report.getErrors().isEmpty());
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20240526"), report.getRescued());
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20250216", "pgtimeseries_time_series_p20250223",
                "pgtimeseries_time_series_p20250302"), report.getCreated());
        // everything with upper bound before 2025-01-22, including the rescued partition
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20240526", "pgtimeseries_time_series_p20241215",
                "pgtimeseries_time_series_p20241222", "pgtimeseries_time_series_p20241229",
                "pgtimeseries_time_series_p20250105", "pgtimeseries_time_series_p20250112"), report.getDropped());
        assertTrue(report.didWork());
        assertNotNull(report.getFinishedAt());
    }

    @Test
    public void dryRunReportsEverythingButChangesNothing() throws SQLException {
        setPolicy("retention_duration", "2 weeks");
        insertSamples(Instant.parse("2024-06-01T10:00:00Z"), 1);
        clock.plus(Duration.ofDays(21));

        MaintenanceReport dry = pm.runMaintenance(true);

        assertTrue(dry.isDryRun());
        assertTrue(dry.getErrors().toString(), dry.getErrors().isEmpty());
        assertEquals(Arrays.asList("pgtimeseries_time_series_p20240526"), dry.getRescued());
        assertEquals(3, dry.getCreated().size());
        assertEquals(5, dry.getDropped().size()); // the rescued partition does not exist, so it is not in the drop list
        assertTrue(dry.summary().startsWith("dry run"));
        // nothing happened
        assertEquals(9, ranges().size());
        assertEquals(1, scalarInt("SELECT count(*) FROM ONLY " + TableNames.PGTIMESERIES_DEFAULT_PARTITION));

        // and the real run does what the dry run announced (plus dropping the freshly rescued partition)
        MaintenanceReport real = pm.runMaintenance();
        assertEquals(dry.getCreated(), real.getCreated());
        assertEquals(dry.getRescued(), real.getRescued());
        assertEquals(6, real.getDropped().size());
    }

    @Test
    public void listsOrphanTablesAndStrayRowCount() throws SQLException {
        insertSamples(Instant.parse("2024-06-01T10:00:00Z"), 4);
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("ALTER TABLE " + PARENT + " DETACH PARTITION pgtimeseries_time_series_p20241215");
            st.execute("CREATE TABLE pgtimeseries_time_series_p20200101_000000 (LIKE " + PARENT + ")"); // sub-day style, never attached
            st.execute("CREATE TABLE pgtimeseries_time_series_p20250101_backup (LIKE " + PARENT + ")"); // someone's backup: not an orphan
            st.execute("CREATE TABLE pgtimeseries_time_series_backup (LIKE " + PARENT + ")"); // not partition-style
        }

        try (Connection c = dataSource.getConnection()) {
            assertEquals(4, pm.countDefaultPartitionRows(c));
            List<PartitionManager.OrphanTable> orphans = pm.listOrphans(c);
            assertEquals(Arrays.asList("pgtimeseries_time_series_p20200101_000000", "pgtimeseries_time_series_p20241215"),
                    orphans.stream().map(PartitionManager.OrphanTable::name).collect(Collectors.toList()));
            pm.dropOrphan(c, orphans.get(0));
            assertEquals(1, pm.listOrphans(c).size());
        }
        assertEquals(8, ranges().size());
    }

    @Test
    public void rescueOfAFarFutureRowIsCheapAndExact() throws SQLException {
        // A seconds-versus-milliseconds mix-up puts a row thousands of years out; the rescue must not enumerate
        // every cell in between (the old lattice did) and must still land it in a lattice-aligned cell.
        Instant far = Instant.parse("9000-06-15T00:00:00Z");
        insertSamples(far, 1);
        insertSamples(Instant.parse("2024-06-01T10:00:00Z"), 1);

        long started = System.nanoTime();
        MaintenanceReport report = pm.runMaintenance();
        long millis = (System.nanoTime() - started) / 1_000_000;

        assertTrue(report.getErrors().toString(), report.getErrors().isEmpty());
        assertEquals(2, report.getRescued().size());
        assertEquals("pgtimeseries_time_series_p20240526", report.getRescued().get(0));
        Partition farCell = ranges().stream().filter(p -> p.contains(far)).findFirst().orElseThrow();
        assertEquals(java.time.Duration.ofDays(7), java.time.Duration.between(farCell.lowerBound(), farCell.upperBound()));
        assertEquals(0, scalarInt("SELECT count(*) FROM ONLY " + TableNames.PGTIMESERIES_DEFAULT_PARTITION));
        assertTrue("took " + millis + "ms", millis < 5_000);
        // and create-ahead is unaffected by the far-future partition
        clock.plus(Duration.ofDays(21));
        assertEquals(3, ensureFuture().size());
    }

    @Test
    public void unboundedPartitionsAreUnderstood() throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pgtimeseries_time_series_pmin PARTITION OF " + PARENT
                    + " FOR VALUES FROM (MINVALUE) TO ('2024-12-15 00:00:00+00')");
            st.execute("CREATE TABLE pgtimeseries_time_series_pmax PARTITION OF " + PARENT
                    + " FOR VALUES FROM ('2025-02-16 00:00:00+00') TO (MAXVALUE)");
        }
        List<Partition> all = pm.listPartitions();
        Partition min = all.stream().filter(p -> p.name().endsWith("pmin")).findFirst().orElseThrow();
        Partition max = all.stream().filter(p -> p.name().endsWith("pmax")).findFirst().orElseThrow();
        assertTrue(min.isRange());
        assertFalse(PartitionManager.isFinite(min.lowerBound()));
        assertEquals(Instant.parse("2024-12-15T00:00:00Z"), min.upperBound());
        assertFalse(PartitionManager.isFinite(max.upperBound()));
        assertTrue(max.contains(Instant.parse("2999-01-01T00:00:00Z")));

        // everything is covered: nothing to create, nothing to rescue, no errors
        clock.plus(Duration.ofDays(400));
        insertSamples(Instant.parse("2020-01-01T00:00:00Z"), 1); // lands in pmin, not in the default partition
        MaintenanceReport report = pm.runMaintenance();
        assertTrue(report.getErrors().toString(), report.getErrors().isEmpty());
        assertTrue(report.getCreated().isEmpty());
        assertTrue(report.getRescued().isEmpty());
    }

    @Test
    public void runMaintenanceIsSkippedWhileAnotherSessionHoldsTheLock() throws SQLException {
        try (Connection other = dataSource.getConnection(); Statement st = other.createStatement()) {
            st.execute("SELECT pg_advisory_lock(" + PartitionManager.ADVISORY_LOCK_KEY + ")");
            try {
                MaintenanceReport report = pm.runMaintenance();
                assertTrue(report.wasSkipped());
                assertTrue(report.getSkippedReason().contains("advisory lock"));
            } finally {
                st.execute("SELECT pg_advisory_unlock(" + PartitionManager.ADVISORY_LOCK_KEY + ")");
            }
        }
        assertFalse(pm.runMaintenance().wasSkipped());
    }

    @Test
    public void runMaintenanceIsSkippedWhenTheSchemaIsNotInstalled() throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE pgtimeseries_config");
        }
        MaintenanceReport report = pm.runMaintenance();
        assertTrue(report.wasSkipped());
        assertFalse(report.hasErrors());
    }

    // ---- service ----------------------------------------------------------

    @Test
    public void maintenanceServiceRunsOnStartAndOnDemand() throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        MaintenanceService service = new MaintenanceService(pm, 60, metrics, () -> false);
        try {
            service.start();
            assertTrue(service.isRunning());
            long deadline = System.currentTimeMillis() + 10_000;
            while (service.getLastReport() == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertNotNull("the service must run once immediately after start", service.getLastReport());

            clock.plus(Duration.ofDays(21));
            MaintenanceReport report = service.runNow();
            assertEquals(3, report.getCreated().size());
            assertEquals(3, metrics.meter("maintenance.partitionsCreated").getCount());
            assertEquals(2, metrics.timer("maintenance.duration").getCount());

            // the write path's cheap variant: only the create step, with a bounded wait
            clock.plus(Duration.ofDays(7));
            assertEquals(1, service.ensurePartitionsNow(30).size());
            assertTrue(service.ensurePartitionsNow(30).isEmpty());
            assertEquals(4, metrics.meter("maintenance.partitionsCreated").getCount());
        } finally {
            service.stop();
        }
        assertFalse(service.isRunning());
    }

    // ---- helpers ----------------------------------------------------------

    private List<Partition> ensureFuture() throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            return pm.ensureFuturePartitions(c);
        }
    }

    private List<Partition> ranges() throws SQLException {
        return pm.listPartitions().stream().filter(Partition::isRange).collect(Collectors.toList());
    }

    private static List<String> names(List<Partition> partitions) {
        return partitions.stream().map(Partition::name).collect(Collectors.toList());
    }

    private static void assertContiguous(List<Partition> ranges) {
        for (int i = 1; i < ranges.size(); i++) {
            assertEquals(ranges.get(i - 1).upperBound(), ranges.get(i).lowerBound());
        }
    }

    private static void setPolicy(String column, String interval) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("UPDATE pgtimeseries_config SET " + column + " = ?::interval")) {
            ps.setString(1, interval);
            ps.executeUpdate();
        }
    }

    private static void insertSamples(Instant at, int count) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("INSERT INTO " + PARENT + "(keyid, time, value) VALUES (?, ?, ?)")) {
            for (int i = 0; i < count; i++) {
                ps.setLong(1, i + 1);
                ps.setObject(2, at.atOffset(java.time.ZoneOffset.UTC));
                ps.setDouble(3, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
