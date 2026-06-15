package org.opennms.timeseries.impl.pgtimeseries;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.util.ContainerizedDatabase;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * Integration test for {@link PGTimeseriesStorage} against a real PostgreSQL + pg_timeseries database.
 *
 * <p>Unlike the generic {@code AbstractStorageIntegrationTest}, this test is written against this plugin's
 * actual read semantics: counters are returned as a rate, gauges as raw/aggregated values, and every bucket
 * in the requested window is emitted (gaps filled with NaN). It therefore filters out NaN gap-fill points and
 * asserts on the values that were actually stored.</p>
 *
 * <p>Requires a container runtime (docker/nerdctl/podman/ctr). When none is available the whole class is
 * skipped (not failed). The database image is configurable via {@code -Dpgtimeseries.test.image=...} so CI can
 * point at a known-good build.</p>
 */
@Slf4j
public class PGTimeseriesStorageTest {

    private static ContainerizedDatabase database;
    private static DataSource dataSource;
    private static PGTimeseriesStorage storage;

    private Instant t0;

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
        dataSource = createDatasource();

        // The extension installer reassigns ownership of the pg_timeseries config tables to the "opennms" role,
        // which exists in a real OpenNMS database but not in a bare container; create it so install succeeds.
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'opennms') "
                    + "THEN CREATE ROLE opennms; END IF; END $$;");
        }

        // High flush thresholds so buffering is deterministic: nothing auto-flushes during the test, leaving
        // flushes under the control of flush()/read-triggered flushPending().
        PGTimeseriesConfig config = PGTimeseriesConfig.builder()
                .flushMaxSamples(1_000_000)
                .flushMaxIntervalMs(3_600_000L)
                .build();
        storage = new PGTimeseriesStorage(config, dataSource);
        storage.init();
    }

    @AfterClass
    public static void tearDownContainer() {
        if (storage != null) {
            storage.destroy();
        }
        if (database != null) {
            database.close();
        }
    }

    @Before
    public void setUp() {
        // Use a per-test reference time; combined with per-test unique metric keys this isolates tests without
        // dropping the pg_timeseries-managed tables between runs.
        this.t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    }

    @After
    public void drainBuffer() throws Exception {
        // Leave the buffer empty for the next test regardless of what a test did.
        storage.flush();
    }

    private static DataSource createDatasource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.username());
        config.setPassword(database.password());
        return new HikariDataSource(config);
    }

    // ---- helpers ----------------------------------------------------------

    private static Metric metric(String mtype) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        return ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, "name_" + uuid)
                .intrinsicTag(IntrinsicTagNames.resourceId, "snmp:1:res_" + uuid)
                .metaTag(MetaTagNames.mtype, mtype)
                .build();
    }

    private static Metric metric(String mtype, String name, String resourceId) {
        return ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.name, name)
                .intrinsicTag(IntrinsicTagNames.resourceId, resourceId)
                .metaTag(MetaTagNames.mtype, mtype)
                .build();
    }

    private Sample sampleAt(Metric m, int plusSeconds, double value) {
        return ImmutableSample.builder()
                .metric(m)
                .time(t0.plusSeconds(plusSeconds))
                .value(value)
                .build();
    }

    private void storeAndFlush(List<Sample> samples) throws Exception {
        storage.store(samples);
        storage.flush();
    }

    private List<Sample> fetch(Metric m, Aggregation aggregation, int windowSeconds, int stepSeconds) throws Exception {
        TimeSeriesFetchRequest request = ImmutableTimeSeriesFetchRequest.builder()
                .metric(m)
                .start(t0)
                .end(t0.plusSeconds(windowSeconds))
                .aggregation(aggregation)
                .step(Duration.ofSeconds(stepSeconds))
                .build();
        return storage.getTimeseries(request);
    }

    /** Returns only the non-NaN points (i.e. the buckets that actually had data), keyed by timestamp. */
    private static java.util.Map<Instant, Double> realValues(List<Sample> samples) {
        return samples.stream()
                .filter(s -> !Double.isNaN(s.getValue()))
                .collect(Collectors.toMap(Sample::getTime, Sample::getValue));
    }

    private int countPersistedSamples(String metricKey) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM pgtimeseries_time_series ts "
                             + "JOIN pgtimeseries_metric m ON ts.keyid = m.keyid WHERE m.key = ?")) {
            ps.setString(1, metricKey);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private int countMetricRows(String metricKey) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM pgtimeseries_metric WHERE key = ?")) {
            ps.setString(1, metricKey);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private static Set<String> keys(List<Metric> metrics) {
        return metrics.stream().map(Metric::getKey).collect(Collectors.toSet());
    }

    // ---- core: store / fetch ---------------------------------------------

    @Test
    public void storesAndReadsBackRawGaugeValues() throws Exception {
        Metric m = metric("gauge");
        storeAndFlush(asList(sampleAt(m, 1, 10.0), sampleAt(m, 2, 20.0), sampleAt(m, 3, 30.0)));

        java.util.Map<Instant, Double> values = realValues(fetch(m, Aggregation.NONE, 5, 1));

        assertEquals(3, values.size());
        assertEquals(10.0, values.get(t0.plusSeconds(1)), 0.0001);
        assertEquals(20.0, values.get(t0.plusSeconds(2)), 0.0001);
        assertEquals(30.0, values.get(t0.plusSeconds(3)), 0.0001);
    }

    @Test
    public void fetchingUnknownMetricReturnsEmpty() throws Exception {
        List<Sample> result = fetch(metric("gauge"), Aggregation.NONE, 5, 1);
        assertTrue(result.stream().allMatch(s -> Double.isNaN(s.getValue())) || result.isEmpty());
    }

    // ---- counter rate + aggregation --------------------------------------

    @Test
    public void counterIsReturnedAsARate() throws Exception {
        Metric m = metric("counter");
        // raw counter values; per-second delta is a constant 60
        storeAndFlush(asList(sampleAt(m, 1, 100.0), sampleAt(m, 2, 160.0), sampleAt(m, 3, 220.0)));

        java.util.Map<Instant, Double> rates = realValues(fetch(m, Aggregation.NONE, 5, 1));

        // first bucket has no predecessor for lag() so it is NaN; the next two are the rate (delta / step)
        assertEquals(2, rates.size());
        assertEquals(60.0, rates.get(t0.plusSeconds(2)), 0.0001);
        assertEquals(60.0, rates.get(t0.plusSeconds(3)), 0.0001);
    }

    @Test
    public void gaugeAggregatesWithinABucket() throws Exception {
        Metric m = metric("gauge");
        storeAndFlush(asList(sampleAt(m, 1, 10.0), sampleAt(m, 2, 20.0), sampleAt(m, 3, 30.0)));

        // a single 60s bucket starting at t0 captures all three samples
        assertEquals(20.0, onlyValue(fetch(m, Aggregation.AVERAGE, 60, 60)), 0.0001);
        assertEquals(30.0, onlyValue(fetch(m, Aggregation.MAX, 60, 60)), 0.0001);
        assertEquals(10.0, onlyValue(fetch(m, Aggregation.MIN, 60, 60)), 0.0001);
    }

    private double onlyValue(List<Sample> samples) {
        java.util.Map<Instant, Double> values = realValues(samples);
        assertEquals("expected exactly one non-NaN bucket", 1, values.size());
        return values.values().iterator().next();
    }

    // ---- findMetrics ------------------------------------------------------

    @Test
    public void findMetricsMatchesByTags() throws Exception {
        String name = "name_" + UUID.randomUUID().toString().replace("-", "");
        String resA = "snmp:1:resA_" + UUID.randomUUID();
        String resB = "snmp:1:resB_" + UUID.randomUUID();
        Metric a = metric("gauge", name, resA);
        Metric b = metric("gauge", name, resB);
        storeAndFlush(asList(sampleAt(a, 1, 1.0)));
        storeAndFlush(asList(sampleAt(b, 1, 1.0)));

        // single matcher on the shared name -> both
        List<Metric> bySharedName = storage.findMetrics(singletonList(
                new ImmutableTagMatcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name, name)));
        assertEquals(keys(asList(a, b)), keys(bySharedName));

        // INTERSECT of name + resourceId -> exactly one
        List<Metric> intersect = storage.findMetrics(asList(
                new ImmutableTagMatcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name, name),
                new ImmutableTagMatcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.resourceId, resA)));
        assertEquals(singletonList(a.getKey()).stream().collect(Collectors.toSet()), keys(intersect));

        // NOT_EQUALS excludes a -> only b
        List<Metric> notA = storage.findMetrics(asList(
                new ImmutableTagMatcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name, name),
                new ImmutableTagMatcher(TagMatcher.Type.NOT_EQUALS, IntrinsicTagNames.resourceId, resA)));
        assertEquals(singletonList(b.getKey()).stream().collect(Collectors.toSet()), keys(notA));

        // regex on the shared name -> both
        List<Metric> byRegex = storage.findMetrics(singletonList(
                new ImmutableTagMatcher(TagMatcher.Type.EQUALS_REGEX, IntrinsicTagNames.name, name.substring(0, 8) + ".*")));
        assertTrue(keys(byRegex).containsAll(keys(asList(a, b))));
    }

    // ---- delete -----------------------------------------------------------

    @Test
    public void deleteRemovesSamplesTagsAndMetric() throws Exception {
        Metric m = metric("gauge");
        storeAndFlush(asList(sampleAt(m, 1, 1.0), sampleAt(m, 2, 2.0)));

        assertEquals(2, countPersistedSamples(m.getKey()));
        assertEquals(1, countMetricRows(m.getKey()));
        assertEquals(1, storage.findMetrics(singletonList(
                new ImmutableTagMatcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name,
                        m.getFirstTagByKey(IntrinsicTagNames.name).getValue()))).size());

        storage.delete(m);

        assertEquals(0, countPersistedSamples(m.getKey()));
        assertEquals(0, countMetricRows(m.getKey()));
        assertTrue(storage.findMetrics(singletonList(
                new ImmutableTagMatcher(TagMatcher.Type.EQUALS, IntrinsicTagNames.name,
                        m.getFirstTagByKey(IntrinsicTagNames.name).getValue()))).isEmpty());
    }

    @Test
    public void deletingUnknownMetricDoesNotThrow() throws Exception {
        storage.delete(metric("gauge")); // never stored
    }

    // ---- write-behind buffer ---------------------------------------------

    @Test
    public void samplesAreBufferedUntilFlush() throws Exception {
        Metric m = metric("gauge");
        storage.store(asList(sampleAt(m, 1, 1.0), sampleAt(m, 2, 2.0), sampleAt(m, 3, 3.0)));

        // nothing persisted yet: thresholds are high and we have not read or flushed
        assertEquals(0, countPersistedSamples(m.getKey()));

        storage.flush();

        assertEquals(3, countPersistedSamples(m.getKey()));
    }

    @Test
    public void readsFlushPendingWritesForReadAfterWriteConsistency() throws Exception {
        Metric m = metric("gauge");
        // store but deliberately do NOT call flush(); the read must flush pending writes itself
        storage.store(asList(sampleAt(m, 1, 7.0), sampleAt(m, 2, 8.0)));

        java.util.Map<Instant, Double> values = realValues(fetch(m, Aggregation.NONE, 5, 1));

        assertEquals(2, values.size());
        assertEquals(7.0, values.get(t0.plusSeconds(1)), 0.0001);
        assertEquals(8.0, values.get(t0.plusSeconds(2)), 0.0001);
    }
}
